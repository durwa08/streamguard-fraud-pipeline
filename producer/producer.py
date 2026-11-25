from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import random
import time
from typing import Any
import uuid


CURRENCIES = ["USD", "EUR", "GBP", "INR"]
COUNTRIES = ["IN", "US", "GB", "DE", "SG"]
CATEGORIES = ["electronics", "grocery", "travel", "dining", "retail"]
CHANNELS = ["online", "pos", "mobile"]
MERCHANTS = [
    "merch_101",
    "merch_102",
    "merch_103",
    "merch_104",
    "merch_blacklisted_01",
    "merch_blacklisted_02",
]
CARD_TOKENS = [
    "tok_demo_normal_01",
    "tok_demo_spike_01",
    "tok_demo_velocity_01",
    "tok_demo_travel_01",
    "tok_demo_blacklist_01",
    "tok_user_001",
    "tok_user_002",
]


def build_valid_event(
    event_id: str | None = None,
    card_token: str | None = None,
    merchant_id: str | None = None,
    amount: float | None = None,
    currency: str | None = None,
    country: str | None = None,
    event_time: datetime | None = None,
    status: str = "approved",
) -> dict[str, Any]:
    now_dt = event_time or datetime.now(timezone.utc)
    return {
        "event_id": event_id or f"evt_{uuid.uuid4().hex[:12]}",
        "card_token": card_token or random.choice(CARD_TOKENS),
        "merchant_id": merchant_id or random.choice(MERCHANTS),
        "merchant_category": random.choice(CATEGORIES),
        "amount": amount if amount is not None else round(random.uniform(5.0, 450.0), 2),
        "currency": currency or random.choice(CURRENCIES),
        "country": country or random.choice(COUNTRIES),
        "channel": random.choice(CHANNELS),
        "event_time": now_dt.astimezone(timezone.utc).isoformat(),
        "status": status,
    }


def generate_batch(
    count: int,
    inject_bad: bool = False,
    inject_duplicates: bool = False,
    base_time: datetime | None = None,
) -> list[dict[str, Any]]:
    t0 = base_time or datetime.now(timezone.utc)
    events: list[dict[str, Any]] = []

    for idx in range(count):
        ev = build_valid_event(event_time=t0 + timedelta(milliseconds=idx * 5))

        if inject_bad and idx % 8 == 0 and idx > 0:
            bad_mode = idx % 4
            if bad_mode == 1:
                ev["amount"] = -50.0
            elif bad_mode == 2:
                ev["currency"] = "JPY"
            elif bad_mode == 3:
                ev["event_time"] = (t0 + timedelta(hours=30)).isoformat()
            else:
                ev["amount"] = 999999.0

        events.append(ev)

        if inject_duplicates and idx % 10 == 0 and idx > 0:
            events.append(dict(ev))

    return events


def stream_events(
    eps: int = 200,
    burst_eps: int = 1000,
    duration_sec: int = 5,
    burst_every_sec: int = 3,
    inject_bad: bool = True,
    inject_duplicates: bool = True,
    sink_file: str | None = None,
) -> list[dict[str, Any]]:
    all_generated: list[dict[str, Any]] = []
    start = time.monotonic()
    last_emit = start

    while (time.monotonic() - start) < duration_sec:
        elapsed = time.monotonic() - start
        is_burst = burst_every_sec > 0 and int(elapsed) % burst_every_sec == 1
        target_rate = burst_eps if is_burst else eps

        now_mono = time.monotonic()
        dt_elapsed = max(now_mono - last_emit, 0.01)
        last_emit = now_mono

        batch_size = max(1, int(target_rate * dt_elapsed))
        batch = generate_batch(
            count=batch_size,
            inject_bad=inject_bad,
            inject_duplicates=inject_duplicates,
        )
        all_generated.extend(batch)
        time.sleep(0.05)

    if sink_file:
        with open(sink_file, "w", encoding="utf-8") as f:
            for item in all_generated:
                f.write(json.dumps(item) + "\n")

    return all_generated


def main() -> None:
    parser = argparse.ArgumentParser(description="StreamGuard synthetic payment generator")
    parser.add_argument("--eps", type=int, default=200)
    parser.add_argument("--burst-eps", type=int, default=1000)
    parser.add_argument("--duration", type=int, default=2)
    parser.add_argument("--no-bad", action="store_true")
    parser.add_argument("--no-dupes", action="store_true")
    parser.add_argument("--out-jsonl", type=str, default="producer/sample_events.jsonl")
    args = parser.parse_args()

    events = stream_events(
        eps=args.eps,
        burst_eps=args.burst_eps,
        duration_sec=args.duration,
        inject_bad=not args.no_bad,
        inject_duplicates=not args.no_dupes,
        sink_file=args.out_jsonl,
    )
    print(f"Generated {len(events)} events -> {args.out_jsonl}")


if __name__ == "__main__":
    main()