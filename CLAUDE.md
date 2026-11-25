# StreamGuard — Real-Time Payment Fraud Signals

## Project Goal
Near-real-time payment fraud detection pipeline ingesting synthetic card events from Kafka, validating & deduplicating them, evaluating configurable fraud rules with a 2-minute watermark, writing idempotent results to PostgreSQL, and surfacing metrics in Apache Superset.

## Strict Guardrails
- Card Tokens Only: NEVER generate, log, or store real or Luhn-valid PANs. Use tok_<alphanumeric> only.
- Idempotency: Replaying the same Kafka offset range MUST NOT create duplicate rows or duplicate alerts in PostgreSQL.
- No Hardcoded Thresholds: All fraud thresholds and blacklists MUST be loaded from config/rules.yaml (or DB config), never hardcoded in Python.

## Kafka Topics & Keys
- raw-payments (partition key: card_token)
- fraud-alerts (partition key: card_token)
- dlq-payments (partition key: event_id or unknown)

## Canonical Event Schema (JSON)
{
  "event_id": "evt_9f8e7d6c5b4a",
  "card_token": "tok_user_001",
  "merchant_id": "merch_104",
  "merchant_category": "electronics",
  "amount": 249.50,
  "currency": "USD",
  "country": "IN",
  "channel": "online",
  "event_time": "2026-11-25T12:00:00Z",
  "status": "approved"
}

## Data Quality Rules (Fail -> dlq-payments with failure_reason)
1. Valid JSON with all required fields present and non-null.
2. amount > 0 and amount <= 50000.00.
3. currency in ["USD", "EUR", "GBP", "INR"].
4. event_time parseable ISO-8601 UTC and event_time <= processing_time + 24 hours.

## Stream Processing & Fraud Rules
- Deduplication: Ignore duplicate event_id values seen within state window / DB primary key.
- Watermark: 2 minutes (120 seconds) on event_time. Events arriving beyond watermark are dropped/counted as late without crashing.
- Deterministic Alert ID: sha256(f"{rule_name}:{card_token}:{anchor_id}") where anchor_id is event_id (for point rules: amount_spike, merchant_blacklist) or minute/window bucket ISO timestamp (for windowed rules: velocity, impossible_travel).
- Four Fraud Rules:
  1. velocity: > 5 valid transactions per card_token within a 1-minute window.
  2. impossible_travel: >= 2 distinct country values for the same card_token within 30 minutes.
  3. amount_spike: amount > 5.0 * card_30d_avg_amount.
  4. merchant_blacklist: merchant_id in configured blacklisted_merchants.

## Code & Testing Conventions
- Python 3.11+, type hints, structured JSON logging.
- Keep business logic pure in processor/engine.py so pytest can test rules without spinning up Kafka.
```[cite: 1, 2]

#### 2. `config/rules.yaml` banao
VS Code ke left sidebar mein **`config`** folder par right-click karo -> **New File** -> naam likho `rules.yaml` -> `Enter` dabao.

Iske andar ye paste karke **Save (`Ctrl + S`)** kar do:

```yaml
data_quality:
  min_amount: 0.01
  max_amount: 50000.00
  allowed_currencies:
    - USD
    - EUR
    - GBP
    - INR
  max_future_hours: 24

stream:
  watermark_seconds: 120
  dedup_ttl_seconds: 3600

rules:
  velocity:
    enabled: true
    max_tx_per_minute: 5
    window_seconds: 60
    severity: HIGH

  impossible_travel:
    enabled: true
    window_minutes: 30
    min_distinct_countries: 2
    severity: CRITICAL

  amount_spike:
    enabled: true
    multiplier: 5.0
    default_30d_avg: 100.0
    severity: HIGH

  merchant_blacklist:
    enabled: true
    severity: CRITICAL
    blacklisted_merchants:
      - merch_blacklisted_01
      - merch_blacklisted_02
      - merch_fraud_ring_99
```[cite: 1, 2]

---

### Check Karo
Dono files save ho jaane par VS Code ke sidebar mein `CLAUDE.md` aur `config/rules.yaml` dikhne chahiye.

Jab ye ho jaye, toh batao **"Step 1 done"** (aur ye bhi bata dena ki kya Docker Desktop chal raha hai ya nahi) — phir hum **Step 2** par chalenge!