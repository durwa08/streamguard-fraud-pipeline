import copy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import unittest

from processor.engine import StreamGuardEngine


def load_simple_rules(path: str = "config/rules.yaml") -> dict:
    try:
        import yaml  # type: ignore

        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except ModuleNotFoundError:
        config = {
            "data_quality": {
                "min_amount": 0.01,
                "max_amount": 50000.0,
                "allowed_currencies": ["USD", "EUR", "GBP", "INR"],
                "max_future_hours": 24,
            },
            "stream": {
                "watermark_seconds": 120,
                "dedup_ttl_seconds": 3600,
            },
            "rules": {
                "velocity": {
                    "enabled": True,
                    "max_tx_per_minute": 5,
                    "window_seconds": 60,
                    "severity": "HIGH",
                },
                "impossible_travel": {
                    "enabled": True,
                    "window_minutes": 30,
                    "min_distinct_countries": 2,
                    "severity": "CRITICAL",
                },
                "amount_spike": {
                    "enabled": True,
                    "multiplier": 5.0,
                    "default_30d_avg": 100.0,
                    "severity": "HIGH",
                },
                "merchant_blacklist": {
                    "enabled": True,
                    "severity": "CRITICAL",
                    "blacklisted_merchants": [
                        "merch_blacklisted_01",
                        "merch_blacklisted_02",
                        "merch_fraud_ring_99",
                    ],
                },
            },
        }
        yaml_path = Path(path)
        if yaml_path.exists():
            current_section = None
            current_rule = None
            in_blacklist = False
            for raw_line in yaml_path.read_text(encoding="utf-8").splitlines():
                stripped = raw_line.strip()
                if not stripped or stripped.startswith("#"):
                    continue
                indent = len(raw_line) - len(raw_line.lstrip(" "))
                if indent == 0 and stripped.endswith(":"):
                    current_section = stripped[:-1]
                    current_rule = None
                    in_blacklist = False
                elif current_section == "rules" and indent == 2 and stripped.endswith(":"):
                    current_rule = stripped[:-1]
                    config["rules"].setdefault(current_rule, {})
                    in_blacklist = False
                elif stripped.startswith("- "):
                    val = stripped[2:].strip().strip("'\"")
                    if in_blacklist and current_rule == "merchant_blacklist":
                        config["rules"]["merchant_blacklist"].setdefault(
                            "blacklisted_merchants", []
                        ).append(val)
                elif ":" in stripped:
                    key, val = [part.strip() for part in stripped.split(":", 1)]
                    val_clean = val.strip("'\"")
                    if key == "blacklisted_merchants":
                        in_blacklist = True
                        config["rules"]["merchant_blacklist"]["blacklisted_merchants"] = []
                        continue
                    in_blacklist = False
                    parsed_val: object = val_clean
                    if val_clean.lower() == "true":
                        parsed_val = True
                    elif val_clean.lower() == "false":
                        parsed_val = False
                    else:
                        try:
                            parsed_val = int(val_clean)
                        except ValueError:
                            try:
                                parsed_val = float(val_clean)
                            except ValueError:
                                parsed_val = val_clean

                    if current_section and current_rule:
                        config["rules"][current_rule][key] = parsed_val
                    elif current_section in config and isinstance(config[current_section], dict):
                        config[current_section][key] = parsed_val
        return config


def make_event(
    event_id="evt_1",
    card_token="tok_clean",
    merchant_id="merch_good_01",
    amount=100.0,
    currency="USD",
    country="IN",
    event_time=None,
):
    if event_time is None:
        event_time = datetime(2026, 11, 25, 12, 0, 0, tzinfo=timezone.utc)
    if isinstance(event_time, datetime):
        event_time = event_time.isoformat()
    return {
        "event_id": event_id,
        "card_token": card_token,
        "merchant_id": merchant_id,
        "merchant_category": "retail",
        "amount": amount,
        "currency": currency,
        "country": country,
        "channel": "online",
        "event_time": event_time,
        "status": "approved",
    }


class StreamGuardTests(unittest.TestCase):
    def setUp(self):
        self.config = load_simple_rules("config/rules.yaml")
        self.baselines = {
            "tok_spike": 100.0,
            "tok_velocity": 50.0,
            "tok_travel": 200.0,
            "tok_blacklist": 150.0,
            "tok_clean": 100.0,
        }
        self.engine = StreamGuardEngine(config=self.config, card_baselines=self.baselines)

    def test_velocity_rule_boundary(self):
        base = datetime(2026, 11, 25, 12, 0, 0, tzinfo=timezone.utc)
        for i in range(1, 6):
            res = self.engine.process_event(
                make_event(
                    event_id=f"evt_vel_{i}",
                    card_token="tok_velocity",
                    amount=20.0,
                    event_time=base + timedelta(seconds=i * 5),
                )
            )
            self.assertTrue(res.is_valid)
            self.assertFalse(any(a["rule_name"] == "velocity" for a in res.alerts))

        res6 = self.engine.process_event(
            make_event(
                event_id="evt_vel_6",
                card_token="tok_velocity",
                amount=20.0,
                event_time=base + timedelta(seconds=35),
            )
        )
        self.assertTrue(any(a["rule_name"] == "velocity" for a in res6.alerts))

    def test_impossible_travel_triggers_and_clears_after_window(self):
        t0 = datetime(2026, 11, 25, 12, 0, 0, tzinfo=timezone.utc)
        r1 = self.engine.process_event(
            make_event("evt_tr_1", "tok_travel", country="IN", event_time=t0)
        )
        self.assertEqual(r1.alerts, [])

        r2 = self.engine.process_event(
            make_event(
                "evt_tr_2",
                "tok_travel",
                country="GB",
                event_time=t0 + timedelta(minutes=20),
            )
        )
        self.assertTrue(any(a["rule_name"] == "impossible_travel" for a in r2.alerts))

        r3 = self.engine.process_event(
            make_event(
                "evt_tr_3",
                "tok_travel",
                country="US",
                event_time=t0 + timedelta(minutes=56),
            )
        )
        self.assertFalse(any(a["rule_name"] == "impossible_travel" for a in r3.alerts))

    def test_amount_spike_boundary(self):
        r_exact = self.engine.process_event(
            make_event("evt_sp_1", "tok_spike", amount=500.0)
        )
        self.assertFalse(any(a["rule_name"] == "amount_spike" for a in r_exact.alerts))

        r_over = self.engine.process_event(
            make_event("evt_sp_2", "tok_spike", amount=500.01)
        )
        self.assertTrue(any(a["rule_name"] == "amount_spike" for a in r_over.alerts))

    def test_merchant_blacklist(self):
        r_bad = self.engine.process_event(
            make_event(
                "evt_bl_1",
                "tok_blacklist",
                merchant_id="merch_blacklisted_01",
                amount=25.0,
            )
        )
        self.assertTrue(any(a["rule_name"] == "merchant_blacklist" for a in r_bad.alerts))

    def test_deduplication_and_replay_idempotency(self):
        batch = [
            make_event("evt_rep_1", "tok_spike", amount=50.0),
            make_event("evt_rep_2", "tok_spike", amount=900.0),
        ]

        eng1 = StreamGuardEngine(config=self.config, card_baselines={"tok_spike": 100.0})
        alerts_run_1 = []
        for ev in batch + [copy.deepcopy(batch[1])]:
            res = eng1.process_event(ev)
            alerts_run_1.extend(res.alerts)

        eng2 = StreamGuardEngine(config=self.config, card_baselines={"tok_spike": 100.0})
        alerts_run_2 = []
        for ev in batch:
            res = eng2.process_event(ev)
            alerts_run_2.extend(res.alerts)

        self.assertEqual(len(alerts_run_1), 1)
        self.assertEqual(len(alerts_run_2), 1)
        self.assertEqual(alerts_run_1[0]["alert_id"], alerts_run_2[0]["alert_id"])

    def test_malformed_events_routed_to_dlq(self):
        now = datetime(2026, 11, 25, 12, 0, 0, tzinfo=timezone.utc)
        bad_cases = [
            {"amount": -10.0},
            {"amount": 0.0},
            {"amount": 999999.0},
            {"currency": "BTC"},
            {"event_time": "not-a-ts"},
        ]
        for idx, overrides in enumerate(bad_cases):
            with self.subTest(case=idx, overrides=overrides):
                ev = make_event(f"evt_bad_{idx}", event_time=now)
                ev.update(overrides)
                res = self.engine.process_event(ev, now=now)
                self.assertFalse(res.is_valid)
                self.assertIsNotNone(res.dlq_reason)
                self.assertEqual(res.alerts, [])

    def test_future_event_beyond_24h_goes_to_dlq(self):
        now = datetime(2026, 11, 25, 12, 0, 0, tzinfo=timezone.utc)
        too_far = now + timedelta(hours=24, minutes=1)
        res = self.engine.process_event(make_event("evt_fut", event_time=too_far), now=now)
        self.assertFalse(res.is_valid)
        self.assertIn("future", (res.dlq_reason or "").lower())

    def test_watermark_accepts_within_2m_and_drops_beyond_2m(self):
        t_latest = datetime(2026, 11, 25, 12, 5, 0, tzinfo=timezone.utc)
        self.engine.process_event(make_event("evt_wm_1", "tok_clean", event_time=t_latest))

        r_within = self.engine.process_event(
            make_event(
                "evt_wm_2",
                "tok_clean",
                event_time=t_latest - timedelta(seconds=90),
            )
        )
        self.assertTrue(r_within.is_valid)
        self.assertFalse(r_within.is_late_dropped)

        r_late = self.engine.process_event(
            make_event(
                "evt_wm_3",
                "tok_clean",
                event_time=t_latest - timedelta(seconds=180),
            )
        )
        self.assertTrue(r_late.is_late_dropped)


if __name__ == "__main__":
    unittest.main(verbosity=2)