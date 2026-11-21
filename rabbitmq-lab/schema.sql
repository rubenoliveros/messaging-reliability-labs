-- Append-only ledger. A balance is never stored or edited: it is computed from entries.
-- The UNIQUE constraint on external_id is what makes the consumer idempotent:
-- a duplicate delivery hits the constraint and is ignored instead of double-counting money.
CREATE TABLE IF NOT EXISTS ledger_entries (
    id              BIGSERIAL PRIMARY KEY,
    external_id     TEXT        NOT NULL UNIQUE,
    account_id      TEXT        NOT NULL,
    kind            TEXT        NOT NULL CHECK (kind IN ('charge', 'payment')),
    amount_cents    BIGINT      NOT NULL CHECK (amount_cents > 0),
    correlation_id  TEXT        NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- balance = charges - payments
CREATE OR REPLACE VIEW account_balances AS
SELECT account_id,
       SUM(CASE WHEN kind = 'charge'  THEN amount_cents ELSE 0 END)             AS charges_cents,
       SUM(CASE WHEN kind = 'payment' THEN amount_cents ELSE 0 END)             AS payments_cents,
       SUM(CASE WHEN kind = 'charge'  THEN amount_cents ELSE -amount_cents END) AS balance_cents
FROM ledger_entries
GROUP BY account_id;
