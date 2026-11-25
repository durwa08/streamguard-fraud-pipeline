# StreamGuard Operations & Replay Runbook

## 1. Core Observability Metrics (Pipeline Health)
- **Consumer Lag:** Offset difference between latest topic offset and committed consumer group offset.
- **Processing Latency:** Time from `event_time` / ingest to alert/transaction commit.
- **DLQ Rate:** Count/percentage of incoming events failing validation and routed to `dlq-payments`.
- **Alerts Per Minute:** Rate of fraud alerts emitted by rule (`velocity`, `impossible_travel`, `amount_spike`, `merchant_blacklist`).

## 2. Lag Alert Threshold
- Warning if consumer lag exceeds `5,000` messages for `> 1` minute or DLQ rate exceeds `5%` of total events.
- Check processor logs and PostgreSQL connectivity first.

## 3. Offset Reset & Replay Procedure
1. Pause or stop the stream processor consumer cleanly.
2. Confirm the target topic (`raw-payments`) and partition offsets to replay.
3. Reset consumer group offset to earliest or target timestamp/offset:
   - Kafka CLI (inside container):
     `docker exec -it streamguard-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group streamguard-consumer --reset-offsets --to-earliest --topic raw-payments --execute`
   - Local runner replay:
     `python -m processor.runner --input producer/sample_events.jsonl`
4. Start the processor again and verify:
   - `fraud_alerts` row count does NOT increase on replayed alerts (deterministic `alert_id` + idempotent upsert).
   - `enriched_transactions` and `tx_1min_aggregates` match expected totals without duplicates.
```[cite: 1, 2]

---

### 3. Final Hackathon Demo & Pitch Checklist (5 Slides)

* **Slide 1 — Problem & Use Case:** Next-day batch fraud reviews catch fraud too late; StreamGuard flags risky card events in near real time using tokenized card IDs (`tok_*`) for fraud analysts, risk managers, and platform on-call engineers[cite: 1].
* **Slide 2 — End-to-End Architecture:** Synthetic producer (200 EPS steady, 1,000 EPS bursts) $\rightarrow$ Kafka (`raw-payments`, `fraud-alerts`, `dlq-payments`) $\rightarrow$ stream processor (dedup, 2-min watermark, 4 configurable rules) $\rightarrow$ PostgreSQL + Superset[cite: 1, 2].
* **Slide 3 — Key Design Choices & Rules:** `config/rules.yaml` thresholds without code edits, DLQ routing for bad/future (`>24h`) events, and deterministic `alert_id` upserts for safe replays[cite: 1, 2].
* **Slide 4 — Testing, Replay & Precision Loop:** 8 unit tests passing, replay test proving zero duplicate alerts, and `alert_feedback` (`TRUE_POSITIVE` / `FALSE_POSITIVE`) feeding daily rule precision[cite: 1, 2].
* **Slide 5 — Live Demo & Observability:** Show `python -m unittest discover -s tests -v`, `python -m processor.runner --input producer/sample_events.jsonl` (including `Run 2 (Replay)` idempotency + `Daily Precision Report`), and Superset dashboard panels (alerts/min, top rules, backlog, pipeline health)[cite: 1, 2].

