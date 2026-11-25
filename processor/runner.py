from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from processor.engine import StreamGuardEngine
from tests.test_streamguard import load_simple_rules


def run_batch(
    events: list[dict[str, Any]],
    config: dict[str, Any],
    baselines: dict[str, float],
    store: dict[str, Any],
) -> dict[str, int]:
    engine = StreamGuardEngine(config=config, card_baselines=baselines)

    valid_tx_count = 0
    dup_count = 0
    late_count = 0
    dlq_count = 0
    new_alerts_count = 0

    for ev in events:
        res = engine.process_event(ev)

        if not res.is_valid:
            dlq_count += 1
            store["dlq"].append(
                {
                    "dlq_reason": res.dlq_reason,
                    "raw_event": ev,
                }
            )
            continue

        if res.is_duplicate:
            dup_count += 1
            continue

        if res.is_late_dropped:
            late_count += 1
            continue

        assert res.event is not None
        event_id = str(res.event["event_id"])
        card_token = str(res.event["card_token"])
        amount = float(res.event["amount"])
        country = str(res.event["country"])
        event_dt = datetime.fromisoformat(
            str(res.event["event_time"]).replace("Z", "+00:00")
        ).astimezone(timezone.utc)
        minute_bucket = event_dt.replace(second=0, microsecond=0).isoformat()

        # Idempotent upsert into enriched_transactions by event_id
        store["enriched_transactions"][event_id] = {
            **res.event,
            "card_30d_avg": res.card_30d_avg,
            "amount_to_avg_ratio": res.amount_to_avg_ratio,
            "is_flagged": len(res.alerts) > 0,
        }
        valid_tx_count += 1

        # Idempotent 1-minute aggregate by (card_token, minute_bucket)
        agg_key = f"{card_token}|{minute_bucket}"
        agg = store["tx_1min_aggregates"].setdefault(
            agg_key,
            {
                "card_token": card_token,
                "window_start": minute_bucket,
                "tx_count": 0,
                "total_amount": 0.0,
                "countries": [],
            },
        )
        agg["tx_count"] += 1
        agg["total_amount"] = round(float(agg["total_amount"]) + amount, 2)
        if country not in agg["countries"]:
            agg["countries"].append(country)

        # Idempotent upsert into fraud_alerts by alert_id
        for alert in res.alerts:
            aid = alert["alert_id"]
            if aid not in store["fraud_alerts"]:
                new_alerts_count += 1
            store["fraud_alerts"][aid] = {
                **alert,
                "alert_status": store["fraud_alerts"].get(aid, {}).get("alert_status", "OPEN"),
            }

    return {
        "valid_tx": valid_tx_count,
        "duplicates_skipped": dup_count,
        "late_dropped": late_count,
        "dlq_routed": dlq_count,
        "new_alerts_inserted": new_alerts_count,
        "total_unique_alerts_in_store": len(store["fraud_alerts"]),
    }


def compute_daily_precision(
    alerts: dict[str, dict[str, Any]],
    feedback: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, int]] = defaultdict(
        lambda: {"total_alerts": 0, "reviewed": 0, "tp": 0, "fp": 0}
    )

    for alert_id, alert in alerts.items():
        day = str(alert.get("event_time", ""))[:10] or "unknown"
        rule = str(alert.get("rule_name", "unknown"))
        bucket = grouped[(day, rule)]
        bucket["total_alerts"] += 1

        fb = feedback.get(alert_id)
        if fb:
            bucket["reviewed"] += 1
            if fb.get("verdict") == "TRUE_POSITIVE":
                bucket["tp"] += 1
            elif fb.get("verdict") == "FALSE_POSITIVE":
                bucket["fp"] += 1

    rows: list[dict[str, Any]] = []
    for (day, rule), stats in sorted(grouped.items()):
        reviewed = stats["reviewed"]
        tp = stats["tp"]
        precision = round(tp / reviewed, 4) if reviewed > 0 else 0.0
        rows.append(
            {
                "alert_date": day,
                "rule_name": rule,
                "total_alerts": stats["total_alerts"],
                "reviewed_alerts": reviewed,
                "true_positives": tp,
                "false_positives": stats["fp"],
                "precision_score": precision,
            }
        )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="StreamGuard batch & replay runner")
    parser.add_argument("--input", type=str, default="producer/sample_events.jsonl")
    parser.add_argument("--out-dir", type=str, default="processor/out")
    args = parser.parse_args()

    input_path = Path(args.input)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    events: list[dict[str, Any]] = []
    with input_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(json.loads(line))

    config = load_simple_rules("config/rules.yaml")
    baselines = {
        "tok_demo_normal_01": 120.0,
        "tok_demo_spike_01": 100.0,
        "tok_demo_velocity_01": 50.0,
        "tok_demo_travel_01": 200.0,
        "tok_demo_blacklist_01": 150.0,
        "tok_user_001": 100.0,
        "tok_user_002": 100.0,
    }

    store: dict[str, Any] = {
        "enriched_transactions": {},
        "tx_1min_aggregates": {},
        "fraud_alerts": {},
        "dlq": [],
    }

    run1_stats = run_batch(events, config, baselines, store)
    # Replay the exact same input batch to verify idempotency
    run2_stats = run_batch(events, config, baselines, store)

    # Sample analyst feedback on up to first 2 alerts for daily precision report
    feedback: dict[str, dict[str, Any]] = {}
    for idx, alert_id in enumerate(list(store["fraud_alerts"].keys())[:2]):
        feedback[alert_id] = {
            "alert_id": alert_id,
            "analyst_id": "analyst_01",
            "verdict": "TRUE_POSITIVE" if idx == 0 else "FALSE_POSITIVE",
        }

    precision_report = compute_daily_precision(store["fraud_alerts"], feedback)

    (out_dir / "summary.json").write_text(
        json.dumps(
            {
                "run_1": run1_stats,
                "run_2_replay": run2_stats,
                "daily_rule_precision": precision_report,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    print("Run 1:", json.dumps(run1_stats))
    print("Run 2 (Replay):", json.dumps(run2_stats))
    print("Daily Precision Report:", json.dumps(precision_report))


if __name__ == "__main__":
    main()