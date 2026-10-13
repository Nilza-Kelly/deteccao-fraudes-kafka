
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id UUID PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    city VARCHAR(100) NOT NULL,
    transaction_timestamp TIMESTAMPTZ NOT NULL,
    is_suspicious BOOLEAN NOT NULL DEFAULT FALSE,
    fraud_types JSONB NOT NULL DEFAULT '[]'::jsonb,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_transactions_customer
    ON transactions (customer_id);

CREATE INDEX IF NOT EXISTS idx_transactions_timestamp
    ON transactions (transaction_timestamp);

CREATE INDEX IF NOT EXISTS idx_transactions_suspicious
    ON transactions (is_suspicious);
