"""Idempotent Kafka consumer: manual offset commits, blocking retries, dead-letter topic.

Rules it follows:
  1. auto commit is OFF. The offset is committed only AFTER the DB transaction commits.
     A crash in between means the record is read again, and the DB constraints make that harmless.
  2. Retries happen in place (blocking) with exponential backoff + jitter. That keeps
     per-partition order. After MAX_ATTEMPTS the record goes to the DLQ topic and we move on.
  3. Poison records go to the DLQ immediately.
  4. Ledger entries: INSERT ... ON CONFLICT (external_id) DO NOTHING.
     Account status: upsert that only applies a NEWER version (out-of-order safe).

Examples:
  python consumer.py
  python consumer.py --fail-rate 0.5
  python consumer.py --crash-after-commit 0.05
  python consumer.py --group ledger-writer    # run 2-3 copies to watch a rebalance
"""
import argparse
import json
import os
import random
import time

import psycopg
from confluent_kafka import Consumer, KafkaError, Producer

from common import (BACKOFF_BASE_S, BOOTSTRAP, CONSUMER_GROUP, DLQ_TOPIC, MAX_ATTEMPTS, TOPIC,
                    PoisonMessage, TransientError, connect_db, header, log, validate)

INSERT_ENTRY = """
INSERT INTO ledger_entries (external_id, account_id, kind, amount_cents, correlation_id, source_partition, source_offset)
VALUES (%(external_id)s, %(account_id)s, %(kind)s, %(amount_cents)s, %(correlation_id)s, %(p)s, %(o)s)
ON CONFLICT (external_id) DO NOTHING
"""

UPSERT_STATUS = """
INSERT INTO account_status (account_id, status, version)
VALUES (%(account_id)s, %(status)s, %(version)s)
ON CONFLICT (account_id) DO UPDATE
   SET status = EXCLUDED.status, version = EXCLUDED.version, updated_at = now()
 WHERE account_status.version < EXCLUDED.version
"""


def apply(db, event: dict, corr: str, msg) -> str:
    with db.cursor() as cur:
        if event["type"] == "ledger_entry":
            cur.execute(INSERT_ENTRY, {**event, "correlation_id": corr, "p": msg.partition(), "o": msg.offset()})
            result = "stored" if cur.rowcount == 1 else "duplicate ignored"
        else:
            cur.execute(UPSERT_STATUS, event)
            result = "status applied" if cur.rowcount == 1 else "stale version ignored"
    db.commit()
    return result


def to_dlq(dlq: Producer, msg, corr: str, attempts: int, error: str):
    dlq.produce(DLQ_TOPIC, key=msg.key(), value=msg.value(), headers=[
        ("correlation_id", corr),
        ("error", error[:200]),
        ("attempts", str(attempts)),
        ("source_topic", msg.topic()),
        ("source_partition", str(msg.partition())),
        ("source_offset", str(msg.offset())),
    ])
    dlq.flush(10)   # make sure it's in the DLQ before we commit past it


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--group", default=CONSUMER_GROUP)
    ap.add_argument("--fail-rate", type=float, default=0.0)
    ap.add_argument("--crash-after-commit", type=float, default=0.0)
    args = ap.parse_args()

    db = connect_db()
    dlq = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True, "acks": "all"})
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": args.group,
        "enable.auto.commit": False,      # we decide when a record counts as done
        "auto.offset.reset": "earliest",  # a brand-new group starts from the oldest retained record
    })

    def on_assign(consumer, partitions):
        log("INFO", "partitions assigned (rebalance)", partitions=[p.partition for p in partitions])

    def on_revoke(consumer, partitions):
        log("INFO", "partitions revoked (rebalance)", partitions=[p.partition for p in partitions])

    c.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)
    log("INFO", "consuming", topic=TOPIC, group=args.group, fail_rate=args.fail_rate)

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log("ERROR", "consumer error", error=str(msg.error()))
                continue

            corr = header(msg, "correlation_id", "none")
            where = {"partition": msg.partition(), "offset": msg.offset(), "key": msg.key().decode() if msg.key() else None}

            try:
                event = validate(json.loads(msg.value()))
            except (PoisonMessage, json.JSONDecodeError) as e:
                to_dlq(dlq, msg, corr, 0, f"poison: {e}")
                c.commit(message=msg, asynchronous=False)
                log("ERROR", "poison -> DLQ", correlation_id=corr, error=str(e), **where)
                continue

            for attempt in range(1, MAX_ATTEMPTS + 1):
                try:
                    if random.random() < args.fail_rate:
                        raise TransientError("simulated downstream timeout")
                    result = apply(db, event, corr, msg)
                    if random.random() < args.crash_after_commit:
                        log("WARN", "simulated crash after DB commit, before offset commit", correlation_id=corr, **where)
                        os._exit(1)   # offset not committed -> this record is read again after restart
                    c.commit(message=msg, asynchronous=False)
                    log("INFO", result, correlation_id=corr, attempt=attempt, **where)
                    break
                except (TransientError, psycopg.OperationalError) as e:
                    db.rollback()
                    if attempt == MAX_ATTEMPTS:
                        to_dlq(dlq, msg, corr, attempt, f"max attempts: {e}")
                        c.commit(message=msg, asynchronous=False)
                        log("ERROR", "max attempts -> DLQ", correlation_id=corr, attempt=attempt, **where)
                        break
                    sleep_s = BACKOFF_BASE_S * 2 ** (attempt - 1) + random.uniform(0, 0.25)
                    log("WARN", "transient failure, retrying", correlation_id=corr, attempt=attempt,
                        sleep_s=round(sleep_s, 2), error=str(e), **where)
                    time.sleep(sleep_s)
    except KeyboardInterrupt:
        pass
    finally:
        c.close()   # leaves the group cleanly so partitions are reassigned right away
        db.close()


if __name__ == "__main__":
    main()
