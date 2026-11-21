# RabbitMQ Lab: idempotent ledger consumer

A small, realistic integration: a "partner" publishes charges and payments, our consumer writes them to an append-only ledger in Postgres, and a reconciliation job proves both sides match.

You will break it on purpose (duplicates, poison messages, a flaky downstream, a crash between commit and ack) and watch the design absorb each failure.

## What's inside

| File | What it does |
| :--- | :--- |
| `docker-compose.yml` | RabbitMQ with the management UI, and Postgres |
| `schema.sql` | Append-only `ledger_entries` with `UNIQUE(external_id)` and a balances view (charges minus payments) |
| `common.py` | Topology (exchange, main queue, 3 retry tiers, DLQ), validation, structured logs |
| `producer.py` | The partner: publishes events with publisher confirms and persistent messages; can send duplicates and poison messages |
| `consumer.py` | Manual acks after DB commit, `prefetch`, idempotent insert, tiered retries, DLQ |
| `redrive.py` | Inspect the DLQ and replay messages gradually |
| `reconcile.py` | Source vs destination: missing, unexpected, field mismatches, balance differences |
| `queue_stats.py` | Queue depth and consumers (what you'd alert on) |

## Setup (Windows, PowerShell)

Requires Docker Desktop and Python 3.11+.

```powershell
cd C:\Projects\rabbitmq-lab
docker compose up -d
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Management UI: http://localhost:15672 (user `guest`, password `guest`).

## Exercises

Run the consumer in one terminal and everything else in a second terminal.

### 1. Happy path
```powershell
python producer.py --count 100 --duplicate-rate 0 --reset-source
python consumer.py          # terminal 1, leave it running
python reconcile.py         # expect exit code 0
```

### 2. Duplicates (at-least-once delivery)
```powershell
python producer.py --count 100 --duplicate-rate 0.3
python reconcile.py
```
Look for `"duplicate ignored"` in the consumer logs. Balances still match, because `UNIQUE(external_id)` + `ON CONFLICT DO NOTHING` makes the insert idempotent.

### 3. Poison messages
```powershell
python producer.py --count 20 --poison 3
python redrive.py --list
```
Poison goes straight to the DLQ with the reason in `x-last-error`. Retrying would never fix bad data.

### 4. Flaky downstream: retries with backoff, then DLQ
Stop the consumer and restart it as:
```powershell
python consumer.py --fail-rate 0.6
```
Then publish 50 events. Watch the retry queues fill in the UI (5s, 20s, 60s), then drain. Messages that fail 4 times land in the DLQ. Run `reconcile.py`: the missing events are exactly the ones in the DLQ.

Fix the "downstream" (restart the consumer without `--fail-rate`), then:
```powershell
python redrive.py --skip-poison --rate 5
python reconcile.py         # back to exit code 0
```

### 5. Crash between commit and ack
```powershell
python consumer.py --crash-after-commit 0.1
```
It will die. Restart it. The unacked message is redelivered (`redelivered: true` in the log), the insert hits the unique constraint, and nothing is double-counted.

### 6. Prefetch and competing consumers
Start two consumers with `--prefetch 1`, publish 200 events, watch both share the work. Ask yourself: is order preserved across the two consumers? (No. Per-queue order only holds with a single consumer. That's why our ledger doesn't depend on order: balances are sums.)

### 7. Broker restart
Publish 500 events with the consumer stopped, run `docker compose restart rabbitmq`, then start the consumer. Nothing is lost: durable queues + persistent messages.

## How to talk about it (interview)

- **Delivery guarantee:** "RabbitMQ gives at-least-once when you ack manually, so I make consumers idempotent: the partner's external ID is a unique key, and a redelivery becomes a no-op."
- **Ack timing:** "I ack after the DB commit, never before. Ack-first can lose data; commit-first can only duplicate, and duplicates I can absorb."
- **Retries:** "Only transient errors are retried, with increasing delays. I use one delay queue per tier with a TTL that dead-letters back to the main exchange, so I don't get head-of-line blocking from per-message TTLs."
- **Poison vs transient:** "Bad data goes straight to the DLQ with the reason. Retrying it only burns capacity."
- **DLQ and redrive:** "The DLQ is an inbox for humans. After the fix I replay gradually, skipping poison, and then reconcile."
- **Publisher side:** "Publisher confirms, persistent messages, durable queues, and `mandatory` so an unroutable message fails loudly."
- **Backpressure:** "`prefetch` caps how many unacked messages a consumer holds, so a slow consumer doesn't hoard work."
- **Ordering:** "Order holds per queue with one consumer. With competing consumers I design so order doesn't matter, or I partition by key."
- **Proof:** "A reconciliation job compares source and destination by key and by balance, and classifies every difference."
- **Observability:** "A correlation ID in every log line, plus alerts on DLQ depth and on main queue depth growing."

## Ideas to extend (good practice with Claude Code)

- Add pytest tests for `validate()` and for the idempotent insert.
- Add a `quorum` queue type and compare it with classic queues.
- Replace the tiered retry queues with the delayed-message plugin and compare.
- Make the consumer call a fake HTTP "partner API" and handle the dual-write case (partner says 200, our DB write fails).
