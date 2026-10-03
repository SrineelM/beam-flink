-- PostgreSQL initial DDL schema for Enterprise Beam + Flink platform

CREATE TABLE IF NOT EXISTS aggregated_metrics (
    id BIGSERIAL PRIMARY KEY,
    window_start TIMESTAMP WITH TIME ZONE NOT NULL,
    window_end TIMESTAMP WITH TIME ZONE NOT NULL,
    window_type VARCHAR(64) NOT NULL,
    region VARCHAR(64) NOT NULL DEFAULT 'UNKNOWN',
    event_type VARCHAR(64),
    gross_revenue NUMERIC(15, 2) DEFAULT 0.00,
    cancelled_revenue NUMERIC(15, 2) DEFAULT 0.00,
    total_revenue NUMERIC(15, 2) DEFAULT 0.00,
    order_count BIGINT DEFAULT 0,
    avg_order_value NUMERIC(15, 2) DEFAULT 0.00,
    failed_payment_count BIGINT DEFAULT 0,
    failed_payment_rate DOUBLE PRECISION DEFAULT 0.0,
    cancellation_count BIGINT DEFAULT 0,
    unique_customers BIGINT DEFAULT 0,
    pane_index INT DEFAULT 0,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_agg_metrics_window ON aggregated_metrics(window_start, window_end);
CREATE UNIQUE INDEX IF NOT EXISTS idx_agg_metrics_unique ON aggregated_metrics(window_start, window_type, region);

CREATE TABLE IF NOT EXISTS risk_signals (
    id BIGSERIAL PRIMARY KEY,
    customer_id VARCHAR(128) NOT NULL,
    risk_score DOUBLE PRECISION NOT NULL,
    payment_failure_count INT DEFAULT 0,
    order_attempt_count INT DEFAULT 0,
    total_amount_last_1hr NUMERIC(15, 2) DEFAULT 0.00,
    signal_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    escalated BOOLEAN DEFAULT FALSE,
    signal_type VARCHAR(32) NOT NULL DEFAULT 'FRAUD_RISK',
    signal_key VARCHAR(256),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_risk_signals_customer ON risk_signals(customer_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_risk_signals_signal_key ON risk_signals(signal_key);

CREATE TABLE IF NOT EXISTS dlq_events (
    id BIGSERIAL PRIMARY KEY,
    correlation_id VARCHAR(64),
    original_payload TEXT,
    error_category VARCHAR(64),
    error_code VARCHAR(64),
    error_message TEXT,
    stack_trace TEXT,
    pipeline_stage VARCHAR(128),
    processing_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    kafka_topic VARCHAR(128),
    kafka_partition INT,
    kafka_offset BIGINT,
    dlq_key VARCHAR(128),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dlq_category ON dlq_events(error_category);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dlq_events_dlq_key ON dlq_events(dlq_key);

-- Additive migrations for existing databases (idempotent)
ALTER TABLE aggregated_metrics ADD COLUMN IF NOT EXISTS region VARCHAR(64) NOT NULL DEFAULT 'UNKNOWN';
ALTER TABLE aggregated_metrics ADD COLUMN IF NOT EXISTS gross_revenue NUMERIC(15, 2) DEFAULT 0.00;
ALTER TABLE aggregated_metrics ADD COLUMN IF NOT EXISTS cancelled_revenue NUMERIC(15, 2) DEFAULT 0.00;
ALTER TABLE aggregated_metrics ADD COLUMN IF NOT EXISTS pane_index INT DEFAULT 0;
ALTER TABLE aggregated_metrics ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP;
CREATE UNIQUE INDEX IF NOT EXISTS idx_agg_metrics_unique ON aggregated_metrics(window_start, window_type, region);

ALTER TABLE risk_signals ADD COLUMN IF NOT EXISTS signal_type VARCHAR(32) NOT NULL DEFAULT 'FRAUD_RISK';
ALTER TABLE risk_signals ADD COLUMN IF NOT EXISTS signal_key VARCHAR(256);
CREATE UNIQUE INDEX IF NOT EXISTS idx_risk_signals_signal_key ON risk_signals(signal_key);

ALTER TABLE dlq_events ADD COLUMN IF NOT EXISTS dlq_key VARCHAR(128);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dlq_events_dlq_key ON dlq_events(dlq_key);
