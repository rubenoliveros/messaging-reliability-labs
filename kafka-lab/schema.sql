-- Append-only ledger. UNIQUE(external_id) makes the consumer idempotent:
-- replays, redeliveries and producer resends can't double-count money.
CREATE TABLE IF NOT EXISTS ledger_entries (
    id              BIGSERIAL PRIMARY KEY,
    external_id     TEXT        NOT NULL UNIQUE,
    account_id      TEXT        NOT NULL,
    kind            TEXT        NOT NULL CHECK (kind IN ('charge', 'payment')),
    amount_cents    BIGINT      NOT NULL CHECK (amount_cents > 0),
    correlation_id  TEXT        NOT NULL,
    source_partition INT        NOT NULL,
    source_offset   BIGINT      NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Latest-state table (not append-only): an account's status, versioned by the partner.
-- The upsert only applies a newer version, so out-of-order events can't roll state back.
CREATE TABLE IF NOT EXISTS account_status (
    account_id  TEXT        PRIMARY KEY,
    status      TEXT        NOT NULL,
    version     BIGINT      NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- balance = charges - payments
CREATE OR REPLACE VIEW account_balances AS
SELECT account_id,
       SUM(CASE WHEN kind = 'charge'  THEN amount_cents ELSE 0 END)             AS charges_cents,
       SUM(CASE WHEN kind = 'payment' THEN amount_cents ELSE 0 END)             AS payments_cents,
       SUM(CASE WHEN kind = 'charge'  THEN amount_cents ELSE -amount_cents END) AS balance_cents
FROM ledger_entries
GROUP BY account_id;
