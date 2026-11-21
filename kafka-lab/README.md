# Kafka Lab: idempotent ledger consumer with ordering, replay and DLQ

A "partner" publishes charges, payments and versioned account status changes to a Kafka topic keyed by `account_id`. Our consumer group writes them to Postgres. A reconciliation job proves both sides match.

You will cause duplicates, poison records, a flaky downstream, a crash between DB commit and offset commit, out-of-order versions, a rebalance and a full replay, and watch the design hold.

## What's inside

| File | What it does |
| :--- | :--- |
| `docker-compose.yml` | Kafka 3.8 in KRaft mode (no ZooKeeper), Kafka UI, Postgres |
| `schema.sql` | Append-only `ledger_entries` (`UNIQUE(external_id)`), versioned `account_status`, balances view |
| `create_topics.py` | `ledger.events` (3 partitions, 7-day retention) and `ledger.events.dlq` |
| `producer.py` | Idempotent producer (`enable.idempotence`, `acks=all`), key = `account_id`; can send duplicates, poison and shuffled versions |
| `consumer.py` | Consumer group, auto-commit off, commit after DB commit, blocking retries with backoff and jitter, DLQ topic, rebalance logs |
| `replay.py` | Show lag, or move the group's offsets back (to the beginning or N minutes ago) for a backfill |
| `redrive_dlq.py` | Inspect the DLQ and replay records gradually |
| `reconcile.py` | Source vs destination: entries, balances, latest status version |

## Setup (Windows, PowerShell)

Requires Docker Desktop and Python 3.11+.

```powershell
cd C:\Projects\kafka-lab
docker compose up -d
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python create_topics.py
```

Kafka UI: http://localhost:8080 (topics, partitions, messages, consumer groups and lag).

## Exercises

Run the consumer in one terminal and everything else in another.

### 1. Happy path and partitioning
```powershell
python producer.py --count 100 --duplicate-rate 0 --reset-source
python consumer.py          # terminal 1
python reconcile.py         # expect exit code 0
```
In the UI, open `ledger.events`: every record for the same `account_id` sits in the same partition. That's what keeps per-account order.

### 2. Duplicates
```powershell
python producer.py --count 100 --duplicate-rate 0.3
python reconcile.py
```
The consumer logs `"duplicate ignored"`; balances still match. Note the two kinds of dedup: `enable.idempotence` stops duplicates from the producer's own network retries; `UNIQUE(external_id)` stops duplicates the partner sends on purpose.

### 3. Out-of-order versions
```powershell
python producer.py --count 0 --status-updates 6 --shuffle-status
python reconcile.py
```
Logs show `"stale version ignored"`. The upsert only applies a version newer than what's stored, so the final status is right even though versions arrived shuffled.

### 4. Poison records
```powershell
python producer.py --count 10 --poison 3
python redrive_dlq.py --list
```

### 5. Flaky downstream: retries, then DLQ
Restart the consumer as `python consumer.py --fail-rate 0.7`, publish 30 events, and watch the backoff (0.5s, 1s, 2s plus jitter). Records that fail 4 times go to the DLQ and the consumer moves on. `reconcile.py` shows them as missing.

Restart the consumer normally, then:
```powershell
python redrive_dlq.py --skip-poison --rate 5
python reconcile.py         # back to 0
```

### 6. Crash between DB commit and offset commit
```powershell
python consumer.py --crash-after-commit 0.1
```
Restart it after it dies. The uncommitted record is read again; the constraint turns it into a no-op.

### 7. Consumer group rebalance
Open three terminals and run `python consumer.py` in each. Watch `partitions assigned / revoked`: 3 partitions, 3 consumers, one each. Start a fourth: it sits idle (more consumers than partitions adds nothing). Kill one: its partition moves to a survivor.

### 8. Lag, replay and backfill
```powershell
python replay.py --show                 # committed offset, log end, lag per partition
# stop every consumer in the group, then:
python replay.py --to-beginning         # or --since-minutes 30
python consumer.py
python reconcile.py
```
The whole topic is reprocessed and nothing is double-counted. That's why idempotency is the precondition for safe replay.

## How to talk about it (interview)

- **Model:** "Kafka is a partitioned, replicated log. Consumers track their own position with offsets, so replay is a first-class feature, unlike a queue where an acked message is gone."
- **Ordering:** "Order is guaranteed only within a partition. I key by the entity, account ID here, so each account's events stay in order. Across partitions there's no global order, and I don't design for one."
- **Delivery guarantee:** "With auto-commit off and the commit after the DB write, it's at-least-once. A crash before the commit re-reads the record, and idempotent writes absorb it."
- **Exactly once:** "Kafka has idempotent producers and transactions, which give exactly-once inside Kafka (read, process, write to Kafka). The moment you write to an external database, you're back to at-least-once plus idempotency, so that's what I design for."
- **Producer settings:** "`acks=all` plus `enable.idempotence=true`: no loss on leader failover, and no duplicates from the producer's own retries."
- **Retries:** "Blocking retries keep partition order but stall the partition, so they're short and capped. Longer retries go to a retry or DLQ topic, accepting that the retried record leaves its original order. For a ledger I can accept that because balances are sums, not sequences. For state, I use versions."
- **Out-of-order state:** "Versioned upsert: apply only if the incoming version is newer. Stale updates become no-ops."
- **Consumer groups:** "Partitions are the unit of parallelism. One partition goes to one consumer in a group; extra consumers sit idle. A rebalance happens when consumers join or leave, so the commit-after-processing rule matters most then."
- **Lag:** "Consumer lag (log end offset minus committed offset) is the main health metric. I alert on lag growing, and on DLQ volume."
- **Replay and backfill:** "Stop the group, move offsets back to a timestamp, restart. Safe only because the consumer is idempotent; then reconcile."
- **Retention and compaction:** "Retention decides how far back I can replay. Log compaction keeps the latest record per key, which suits state topics, not a ledger."

## RabbitMQ vs Kafka in one breath

"RabbitMQ is a broker that routes messages to queues and deletes them on ack: great for work queues, routing and per-message retries. Kafka is a log you read by offset: great for event streams, several independent consumers, ordering per key and replay. Either way I build consumers the same way: idempotent, commit or ack after the write, capped retries with backoff, a DLQ with a redrive process, correlation IDs, and reconciliation."

## Ideas to extend (good practice with Claude Code)

- Add pytest tests for `validate()` and for the versioned upsert.
- Add a non-blocking retry topic (`ledger.events.retry`) and compare it with the blocking retry.
- Add a schema to the events (JSON Schema or Avro with a schema registry) and try a breaking change.
- Use Kafka transactions for a read-process-write between two topics.
