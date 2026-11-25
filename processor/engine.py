from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import json
from typing import Any


REQUIRED_FIELDS = (
    "event_id",
    "card_token",
    "merchant_id",
    "merchant_category",
    "amount",
    "currency",
    "country",
    "channel",
    "event_time",
    "status",
)


@dataclass
class ProcessResult:
    is_valid: bool
    dlq_reason: str | None = None
    is_duplicate: bool = False
    is_late_dropped: bool = False
    event: dict[str, Any] | None = None
    card_30d_avg: float = 0.0
    amount_to_avg_ratio: float = 0.0
    alerts: list[dict[str, Any]] = field(default_factory=list)


class StreamGuardEngine:
    def __init__(
        self,
        config: dict[str, Any],
        card_baselines: dict[str, float] | None = None,
    ) -> None:
        self.config = config
        self.dq_cfg = config.get("data_quality", {})
        self.stream_cfg = config.get("stream", {})
        self.rules_cfg = config.get("rules", {})

        self.watermark_seconds = int(self.stream_cfg.get("watermark_seconds", 120))
        self.card_baselines: dict[str, float] = dict(card_baselines or {})

        self.seen_event_ids: set[str] = set()
        self.max_event_time: datetime | None = None

        # Per-card history of (event_time, country, amount)
        self.card_history: dict[str, deque[tuple[datetime, str, float]]] = defaultdict(deque)

    @staticmethod
    def _parse_iso_utc(raw_ts: Any) -> datetime | None:
        if not isinstance(raw_ts, str) or not raw_ts.strip():
            return None
        try:
            normalized = raw_ts.strip().replace("Z", "+00:00")
            dt = datetime.fromisoformat(normalized)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            return None

    @staticmethod
    def _make_alert_id(rule_name: str, card_token: str, anchor_id: str) -> str:
        raw = f"{rule_name}:{card_token}:{anchor_id}".encode("utf-8")
        return hashlib.sha256(raw).hexdigest()

    def _validate(
        self, raw_event: dict[str, Any] | str, now: datetime
    ) -> tuple[dict[str, Any] | None, datetime | None, str | None]:
        if isinstance(raw_event, str):
            try:
                event = json.loads(raw_event)
            except json.JSONDecodeError:
                return None, None, "Malformed JSON payload"
        elif isinstance(raw_event, dict):
            event = dict(raw_event)
        else:
            return None, None, "Event must be a JSON object"

        for req_field in REQUIRED_FIELDS:
            if req_field not in event or event[req_field] is None or event[req_field] == "":
                return None, None, f"Missing required field: {req_field}"

        card_token = str(event["card_token"])
        if not card_token.startswith("tok_"):
            return None, None, "Invalid card_token format; must use tokenized identifier"

        try:
            amount = float(event["amount"])
        except (TypeError, ValueError):
            return None, None, "Amount must be numeric"

        min_amount = float(self.dq_cfg.get("min_amount", 0.01))
        max_amount = float(self.dq_cfg.get("max_amount", 50000.0))
        if amount < min_amount or amount > max_amount:
            return None, None, f"Amount {amount} out of allowed range [{min_amount}, {max_amount}]"

        allowed_currencies = set(self.dq_cfg.get("allowed_currencies", ["USD", "EUR", "GBP", "INR"]))
        currency = str(event["currency"]).upper()
        if currency not in allowed_currencies:
            return None, None, f"Disallowed currency: {currency}"

        event_dt = self._parse_iso_utc(event["event_time"])
        if event_dt is None:
            return None, None, "Malformed event_time timestamp"

        max_future_hours = int(self.dq_cfg.get("max_future_hours", 24))
        if event_dt > now + timedelta(hours=max_future_hours):
            return None, None, f"Event time exceeds {max_future_hours}h future limit"

        event["amount"] = amount
        event["currency"] = currency
        return event, event_dt, None

    def process_event(
        self,
        raw_event: dict[str, Any] | str,
        now: datetime | None = None,
    ) -> ProcessResult:
        current_time = now.astimezone(timezone.utc) if now else datetime.now(timezone.utc)

        event, event_dt, dlq_reason = self._validate(raw_event, current_time)
        if dlq_reason is not None or event is None or event_dt is None:
            return ProcessResult(is_valid=False, dlq_reason=dlq_reason)

        event_id = str(event["event_id"])
        if event_id in self.seen_event_ids:
            return ProcessResult(is_valid=True, is_duplicate=True, event=event)

        if self.max_event_time is not None:
            watermark_cutoff = self.max_event_time - timedelta(seconds=self.watermark_seconds)
            if event_dt < watermark_cutoff:
                self.seen_event_ids.add(event_id)
                return ProcessResult(is_valid=True, is_late_dropped=True, event=event)

        self.seen_event_ids.add(event_id)
        if self.max_event_time is None or event_dt > self.max_event_time:
            self.max_event_time = event_dt

        card_token = str(event["card_token"])
        merchant_id = str(event["merchant_id"])
        country = str(event["country"]).upper()
        amount = float(event["amount"])

        # Maintain up to 35 minutes of per-card history for windowed rules
        history = self.card_history[card_token]
        history.append((event_dt, country, amount))
        retention_cutoff = self.max_event_time - timedelta(minutes=35)
        while history and history[0][0] < retention_cutoff:
            history.popleft()

        default_avg = float(self.rules_cfg.get("amount_spike", {}).get("default_30d_avg", 100.0))
        avg_30d = float(self.card_baselines.get(card_token, default_avg))
        ratio = round(amount / avg_30d, 2) if avg_30d > 0 else 0.0

        alerts: list[dict[str, Any]] = []

        # Rule 1: Velocity (> max_tx_per_minute within window_seconds)
        vel_cfg = self.rules_cfg.get("velocity", {})
        if vel_cfg.get("enabled", True):
            vel_window = int(vel_cfg.get("window_seconds", 60))
            max_tx = int(vel_cfg.get("max_tx_per_minute", 5))
            vel_start = event_dt - timedelta(seconds=vel_window)
            recent_tx_count = sum(1 for ts, _, _ in history if vel_start <= ts <= event_dt)
            if recent_tx_count > max_tx:
                bucket = event_dt.replace(second=0, microsecond=0).isoformat()
                alerts.append(
                    {
                        "alert_id": self._make_alert_id("velocity", card_token, bucket),
                        "event_id": event_id,
                        "card_token": card_token,
                        "rule_name": "velocity",
                        "severity": vel_cfg.get("severity", "HIGH"),
                        "reason": f"{recent_tx_count} transactions in {vel_window}s (limit {max_tx})",
                        "amount": amount,
                        "country": country,
                        "merchant_id": merchant_id,
                        "event_time": event_dt.isoformat(),
                    }
                )

        # Rule 2: Impossible Travel (>= 2 countries within window_minutes)
        travel_cfg = self.rules_cfg.get("impossible_travel", {})
        if travel_cfg.get("enabled", True):
            travel_mins = int(travel_cfg.get("window_minutes", 30))
            min_countries = int(travel_cfg.get("min_distinct_countries", 2))
            travel_start = event_dt - timedelta(minutes=travel_mins)
            countries_in_window = sorted(
                {ctry for ts, ctry, _ in history if travel_start <= ts <= event_dt}
            )
            if len(countries_in_window) >= min_countries:
                bucket = f"{event_dt.strftime('%Y-%m-%dT%H')}:{'-'.join(countries_in_window)}"
                alerts.append(
                    {
                        "alert_id": self._make_alert_id("impossible_travel", card_token, bucket),
                        "event_id": event_id,
                        "card_token": card_token,
                        "rule_name": "impossible_travel",
                        "severity": travel_cfg.get("severity", "CRITICAL"),
                        "reason": f"Card used across countries {countries_in_window} within {travel_mins}m",
                        "amount": amount,
                        "country": country,
                        "merchant_id": merchant_id,
                        "event_time": event_dt.isoformat(),
                    }
                )

        # Rule 3: Amount Spike (> multiplier * 30d average)
        spike_cfg = self.rules_cfg.get("amount_spike", {})
        if spike_cfg.get("enabled", True):
            multiplier = float(spike_cfg.get("multiplier", 5.0))
            if amount > multiplier * avg_30d:
                alerts.append(
                    {
                        "alert_id": self._make_alert_id("amount_spike", card_token, event_id),
                        "event_id": event_id,
                        "card_token": card_token,
                        "rule_name": "amount_spike",
                        "severity": spike_cfg.get("severity", "HIGH"),
                        "reason": f"Amount {amount} exceeds {multiplier}x 30d avg ({avg_30d})",
                        "amount": amount,
                        "country": country,
                        "merchant_id": merchant_id,
                        "event_time": event_dt.isoformat(),
                    }
                )

        # Rule 4: Merchant Blacklist
        bl_cfg = self.rules_cfg.get("merchant_blacklist", {})
        if bl_cfg.get("enabled", True):
            blacklisted = set(bl_cfg.get("blacklisted_merchants", []))
            if merchant_id in blacklisted:
                alerts.append(
                    {
                        "alert_id": self._make_alert_id("merchant_blacklist", card_token, event_id),
                        "event_id": event_id,
                        "card_token": card_token,
                        "rule_name": "merchant_blacklist",
                        "severity": bl_cfg.get("severity", "CRITICAL"),
                        "reason": f"Merchant {merchant_id} is on blacklist",
                        "amount": amount,
                        "country": country,
                        "merchant_id": merchant_id,
                        "event_time": event_dt.isoformat(),
                    }
                )

        return ProcessResult(
            is_valid=True,
            event=event,
            card_30d_avg=avg_30d,
            amount_to_avg_ratio=ratio,
            alerts=alerts,
        )