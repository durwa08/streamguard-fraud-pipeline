CREATE TABLE IF NOT EXISTS card_baselines (
    card_token VARCHAR(64) PRIMARY KEY,
    avg_amount_30d NUMERIC(12, 2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS enriched_transactions (
    event_id VARCHAR(64) PRIMARY KEY,
    card_token VARCHAR(64) NOT NULL,
    merchant_id VARCHAR(64) NOT NULL,
    merchant_category VARCHAR(64) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    currency VARCHAR(8) NOT NULL,
    country VARCHAR(8) NOT NULL,
    channel VARCHAR(32) NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status VARCHAR(32) NOT NULL,
    card_30d_avg NUMERIC(12, 2) NOT NULL,
    amount_to_avg_ratio NUMERIC(10, 2) NOT NULL,
    is_flagged BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_enriched_card_time ON enriched_transactions (card_token, event_time);

CREATE TABLE IF NOT EXISTS tx_1min_aggregates (
    card_token VARCHAR(64) NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    tx_count INT NOT NULL,
    total_amount NUMERIC(14, 2) NOT NULL,
    distinct_countries INT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (card_token, window_start)
);

CREATE TABLE IF NOT EXISTS fraud_rules (
    rule_name VARCHAR(64) PRIMARY KEY,
    description TEXT NOT NULL,
    threshold_json JSONB NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id VARCHAR(64) PRIMARY KEY,
    event_id VARCHAR(64) NOT NULL,
    card_token VARCHAR(64) NOT NULL,
    rule_name VARCHAR(64) NOT NULL,
    severity VARCHAR(16) NOT NULL,
    reason TEXT NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    country VARCHAR(8) NOT NULL,
    merchant_id VARCHAR(64) NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    alert_status VARCHAR(16) NOT NULL DEFAULT 'OPEN'
);

CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON fraud_alerts (created_at);
CREATE INDEX IF NOT EXISTS idx_alerts_rule_name ON fraud_alerts (rule_name);

CREATE TABLE IF NOT EXISTS alert_feedback (
    alert_id VARCHAR(64) PRIMARY KEY REFERENCES fraud_alerts(alert_id) ON DELETE CASCADE,
    analyst_id VARCHAR(64) NOT NULL,
    verdict VARCHAR(16) NOT NULL CHECK (verdict IN ('TRUE_POSITIVE', 'FALSE_POSITIVE')),
    notes TEXT,
    reviewed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id BIGSERIAL PRIMARY KEY,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    consumer_lag BIGINT NOT NULL,
    avg_processing_latency_ms NUMERIC(10, 2) NOT NULL,
    dlq_events_per_min INT NOT NULL,
    alerts_per_min INT NOT NULL
);

CREATE OR REPLACE VIEW daily_rule_precision AS
SELECT
    DATE_TRUNC('day', a.created_at)::DATE AS alert_date,
    a.rule_name,
    COUNT(a.alert_id) AS total_alerts,
    COUNT(f.alert_id) AS reviewed_alerts,
    COUNT(*) FILTER (WHERE f.verdict = 'TRUE_POSITIVE') AS true_positives,
    COUNT(*) FILTER (WHERE f.verdict = 'FALSE_POSITIVE') AS false_positives,
    ROUND(
        COALESCE(
            COUNT(*) FILTER (WHERE f.verdict = 'TRUE_POSITIVE')::NUMERIC /
            NULLIF(COUNT(f.alert_id), 0),
            0
        ),
        4
    ) AS precision_score
FROM fraud_alerts a
LEFT JOIN alert_feedback f ON a.alert_id = f.alert_id
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

INSERT INTO card_baselines (card_token, avg_amount_30d) VALUES
    ('tok_demo_normal_01', 120.00),
    ('tok_demo_spike_01', 100.00),
    ('tok_demo_velocity_01', 50.00),
    ('tok_demo_travel_01', 200.00),
    ('tok_demo_blacklist_01', 150.00)
ON CONFLICT (card_token) DO NOTHING;