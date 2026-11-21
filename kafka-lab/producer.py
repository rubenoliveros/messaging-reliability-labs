"""Plays the 'partner system': publishes ledger entries and versioned account status events.

Every record is keyed by account_id, so all events for one account land in the same
partition and keep their order there. Valid events are also written to source_events.jsonl
(the partner's version of the truth) for reconcile.py.

Examples:
  python producer.py --count 200 --reset-source
  python producer.py --count 200 --duplicate-rate 0.2 --poison 3
  python producer.py --status-updates 5 --shuffle-status    # out-of-order versions
"""
import argparse
import json
import random
import uuid

from confluent_kafka import Producer

from common import BOOTSTRAP, SOURCE_FILE, TOPIC, log

STATUSES = ["applicant", "approved", "active", "delinquent", "active", "closed"]


def delivery_report(err, msg):
    if err is not None:
        log("ERROR", "delivery failed", error=str(err), key=msg.key())


def send(p: Producer, event: dict, correlation_id: str):
    p.produce(
        TOPIC,
        key=event.get("account_id", "unknown"),
        value=json.dumps(event),
        headers=[("correlation_id", correlation_id), ("attempt", "1")],
        on_delivery=delivery_report,
    )
    p.poll(0)   # serve delivery callbacks


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--count", type=int, default=100, help="ledger entries to send")
    ap.add_argument("--accounts", type=int, default=10)
    ap.add_argument("--duplicate-rate", type=float, default=0.1, help="share of events the partner resends")
    ap.add_argument("--poison", type=int, default=0)
    ap.add_argument("--status-updates", type=int, default=0, help="status versions per account")
    ap.add_argument("--shuffle-status", action="store_true", help="send status versions out of order")
    ap.add_argument("--reset-source", action="store_true")
    args = ap.parse_args()

    p = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "enable.idempotence": True,   # broker drops duplicates caused by the producer's own retries
        "acks": "all",                # wait for all in-sync replicas
        "linger.ms": 5,               # small batching window
    })

    sent = dupes = 0
    with open(SOURCE_FILE, "w" if args.reset_source else "a", encoding="utf-8") as src:
        for _ in range(args.count):
            ev = {
                "type": "ledger_entry",
                "external_id": f"evt-{uuid.uuid4().hex[:12]}",
                "account_id": f"acct-{random.randint(1, args.accounts):03d}",
                "kind": random.choice(["charge", "charge", "payment"]),
                "amount_cents": random.randint(500, 50_000),
            }
            corr = f"corr-{uuid.uuid4().hex[:8]}"
            send(p, ev, corr)
            src.write(json.dumps(ev) + "\n")
            sent += 1
            if random.random() < args.duplicate_rate:
                send(p, ev, corr)   # application-level resend: same external_id
                dupes += 1

        for a in range(1, args.accounts + 1) if args.status_updates else []:
            acct = f"acct-{a:03d}"
            versions = [{"type": "account_status", "account_id": acct, "version": v,
                         "status": STATUSES[min(v - 1, len(STATUSES) - 1)]}
                        for v in range(1, args.status_updates + 1)]
            final = versions[-1]
            if args.shuffle_status:
                random.shuffle(versions)
            for ev in versions:
                send(p, ev, f"corr-{uuid.uuid4().hex[:8]}")
            src.write(json.dumps(final) + "\n")   # partner's truth: the highest version

        for _ in range(args.poison):
            bad = random.choice([
                {"type": "ledger_entry", "external_id": f"bad-{uuid.uuid4().hex[:8]}", "account_id": "acct-999",
                 "kind": "refund", "amount_cents": 100},
                {"type": "mystery", "account_id": "acct-999"},
            ])
            send(p, bad, f"corr-{uuid.uuid4().hex[:8]}")

    remaining = p.flush(15)
    log("INFO", "published", entries=sent, duplicates=dupes, poison=args.poison,
        status_updates_per_account=args.status_updates, shuffled=args.shuffle_status, undelivered=remaining)


if __name__ == "__main__":
    main()
