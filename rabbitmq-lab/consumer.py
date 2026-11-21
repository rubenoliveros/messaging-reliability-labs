"""Idempotent consumer with manual acks, tiered retries and a dead-letter queue.

Rules it follows:
  1. Ack only AFTER the database commit. If we crash before the ack, RabbitMQ redelivers
     the message, and the UNIQUE(external_id) constraint makes that redelivery harmless.
  2. Poison messages (can never succeed) go straight to the DLQ. No retries.
  3. Transient failures go to a retry tier (5s, 20s, 60s). After MAX_ATTEMPTS, to the DLQ.
  4. Every log line carries the correlation_id.

Examples:
  python consumer.py
  python consumer.py --fail-rate 0.3            # simulate a flaky downstream
  python consumer.py --crash-after-commit 0.05  # crash between DB commit and ack, then restart it
"""
import argparse
import json
import os
import random

import pika
import psycopg

from common import (DLQ, MAIN_Q, MAX_ATTEMPTS, RETRY_TIERS_MS, PoisonMessage, TransientError,
                    connect_amqp, connect_db, declare_topology, log, retry_queue_name, validate)

INSERT_SQL = """
INSERT INTO ledger_entries (external_id, account_id, kind, amount_cents, correlation_id)
VALUES (%(external_id)s, %(account_id)s, %(kind)s, %(amount_cents)s, %(correlation_id)s)
ON CONFLICT (external_id) DO NOTHING
"""


def republish(ch, queue: str, body: bytes, props, attempt: int, error: str):
    headers = dict(props.headers or {})
    headers["x-attempt"] = attempt
    headers["x-last-error"] = error[:200]
    ch.basic_publish(
        exchange="",          # default exchange: routing key = queue name
        routing_key=queue,
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,
            content_type=props.content_type,
            correlation_id=props.correlation_id,
            message_id=props.message_id,
            headers=headers,
        ),
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefetch", type=int, default=10, help="max unacked messages this consumer holds")
    ap.add_argument("--fail-rate", type=float, default=0.0, help="probability of a simulated transient failure")
    ap.add_argument("--crash-after-commit", type=float, default=0.0, help="probability of crashing after commit, before ack")
    args = ap.parse_args()

    db = connect_db()
    conn = connect_amqp()
    ch = conn.channel()
    declare_topology(ch)
    ch.basic_qos(prefetch_count=args.prefetch)

    def on_message(ch, method, props, body):
        attempt = int((props.headers or {}).get("x-attempt", 1))
        corr = props.correlation_id or "none"
        try:
            event = validate(json.loads(body))
        except (PoisonMessage, json.JSONDecodeError) as e:
            republish(ch, DLQ, body, props, attempt, f"poison: {e}")
            ch.basic_ack(method.delivery_tag)
            log("ERROR", "poison -> DLQ", correlation_id=corr, error=str(e))
            return

        try:
            if random.random() < args.fail_rate:
                raise TransientError("simulated downstream timeout")
            with db.cursor() as cur:
                cur.execute(INSERT_SQL, {**event, "correlation_id": corr})
                inserted = cur.rowcount == 1
            db.commit()

            if random.random() < args.crash_after_commit:
                log("WARN", "simulated crash after commit, before ack", correlation_id=corr,
                    external_id=event["external_id"])
                os._exit(1)   # message stays unacked -> redelivered when we restart

            ch.basic_ack(method.delivery_tag)
            log("INFO", "stored" if inserted else "duplicate ignored", correlation_id=corr,
                external_id=event["external_id"], attempt=attempt, redelivered=method.redelivered)

        except (TransientError, psycopg.OperationalError) as e:
            db.rollback()
            if attempt < MAX_ATTEMPTS:
                delay = RETRY_TIERS_MS[attempt - 1]
                republish(ch, retry_queue_name(delay), body, props, attempt + 1, str(e))
                log("WARN", "transient failure -> retry", correlation_id=corr, attempt=attempt,
                    retry_in_s=delay // 1000, error=str(e))
            else:
                republish(ch, DLQ, body, props, attempt, f"max attempts: {e}")
                log("ERROR", "max attempts -> DLQ", correlation_id=corr, attempt=attempt, error=str(e))
            # Publish first, then ack. A crash in between causes a duplicate, which idempotency absorbs.
            ch.basic_ack(method.delivery_tag)

    ch.basic_consume(MAIN_Q, on_message_callback=on_message, auto_ack=False)
    log("INFO", "consuming", queue=MAIN_Q, prefetch=args.prefetch, fail_rate=args.fail_rate)
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        ch.stop_consuming()
    finally:
        conn.close()
        db.close()


if __name__ == "__main__":
    main()
