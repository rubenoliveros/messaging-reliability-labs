"""Inspect the dead-letter topic and replay records to the main topic, gradually.

The DLQ has its own consumer group, so we only redrive each DLQ record once.

Examples:
  python redrive_dlq.py --list
  python redrive_dlq.py --skip-poison --rate 5
"""
import argparse
import time

from confluent_kafka import Consumer, Producer

from common import BOOTSTRAP, DLQ_TOPIC, TOPIC, header, log


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", action="store_true", help="read without committing (look before you touch)")
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--rate", type=float, default=5.0)
    ap.add_argument("--skip-poison", action="store_true")
    args = ap.parse_args()

    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "ledger-dlq-redrive",
                  "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    c.subscribe([DLQ_TOPIC])
    p = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True, "acks": "all"})

    moved = skipped = idle = 0
    while moved + skipped < args.limit and idle < 5:
        msg = c.poll(1.0)
        if msg is None or msg.error():
            idle += 1
            continue
        idle = 0
        error = header(msg, "error", "")
        corr = header(msg, "correlation_id", "none")
        if args.list:
            log("INFO", "dlq record", correlation_id=corr, error=error,
                source=f"{header(msg, 'source_partition')}:{header(msg, 'source_offset')}", value=msg.value()[:120])
            skipped += 1
            continue
        if args.skip_poison and error.startswith("poison"):
            c.commit(message=msg, asynchronous=False)
            skipped += 1
            continue
        p.produce(TOPIC, key=msg.key(), value=msg.value(),
                  headers=[("correlation_id", corr), ("attempt", "1"), ("redriven", "true")])
        p.flush(10)                               # republished first...
        c.commit(message=msg, asynchronous=False)  # ...then mark the DLQ record as handled
        moved += 1
        time.sleep(1 / args.rate)

    c.close()
    log("INFO", "listed" if args.list else "redrive done", moved=moved, skipped_or_listed=skipped)


if __name__ == "__main__":
    main()
