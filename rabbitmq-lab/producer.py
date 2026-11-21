"""Plays the 'partner system': publishes ledger events (charges and payments).

It also records every valid event it sent in source_events.jsonl. That file is the
partner's version of the truth, which reconcile.py compares against our database.

Examples:
  python producer.py --count 200
  python producer.py --count 200 --duplicate-rate 0.2 --poison 3
"""
import argparse
import json
import random
import uuid

import pika

from common import EXCHANGE, ROUTING_KEY, SOURCE_FILE, connect_amqp, declare_topology, log


def make_event(accounts: int) -> dict:
    return {
        "external_id": f"evt-{uuid.uuid4().hex[:12]}",   # the partner's ID: our idempotency key
        "account_id": f"acct-{random.randint(1, accounts):03d}",
        "kind": random.choice(["charge", "charge", "payment"]),
        "amount_cents": random.randint(500, 50_000),
    }


def make_poison() -> dict:
    return random.choice([
        {"external_id": f"bad-{uuid.uuid4().hex[:8]}", "account_id": "acct-999", "kind": "refund", "amount_cents": 100},
        {"external_id": f"bad-{uuid.uuid4().hex[:8]}", "account_id": "acct-999", "kind": "charge", "amount_cents": -5},
        {"external_id": f"bad-{uuid.uuid4().hex[:8]}", "kind": "charge"},
    ])


def publish(ch, event: dict, correlation_id: str):
    ch.basic_publish(
        exchange=EXCHANGE,
        routing_key=ROUTING_KEY,
        body=json.dumps(event).encode(),
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,   # survives a broker restart (queue is durable too)
            content_type="application/json",
            correlation_id=correlation_id,
            message_id=event.get("external_id"),
            headers={"x-attempt": 1},
        ),
        mandatory=True,   # fail loudly if no queue is bound, instead of silently dropping
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--count", type=int, default=100)
    ap.add_argument("--accounts", type=int, default=10)
    ap.add_argument("--duplicate-rate", type=float, default=0.1, help="share of events sent twice (at-least-once in the wild)")
    ap.add_argument("--poison", type=int, default=0, help="number of malformed events to send")
    ap.add_argument("--reset-source", action="store_true", help="start a fresh source_events.jsonl")
    args = ap.parse_args()

    conn = connect_amqp()
    ch = conn.channel()
    declare_topology(ch)
    ch.confirm_delivery()   # publisher confirms: basic_publish raises if the broker did not accept the message

    mode = "w" if args.reset_source else "a"
    sent = dupes = 0
    with open(SOURCE_FILE, mode, encoding="utf-8") as src:
        for _ in range(args.count):
            event = make_event(args.accounts)
            corr = f"corr-{uuid.uuid4().hex[:8]}"
            publish(ch, event, corr)
            src.write(json.dumps(event) + "\n")
            sent += 1
            if random.random() < args.duplicate_rate:
                publish(ch, event, corr)   # same external_id, same payload: a resend
                dupes += 1
        for _ in range(args.poison):
            publish(ch, make_poison(), f"corr-{uuid.uuid4().hex[:8]}")

    conn.close()
    log("INFO", "published", events=sent, duplicates=dupes, poison=args.poison, source_file=SOURCE_FILE)


if __name__ == "__main__":
    main()
