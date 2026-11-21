"""Inspect the DLQ and replay messages to the main queue, gradually.

Examples:
  python redrive.py --list                 # look before you touch
  python redrive.py --limit 20 --rate 5    # replay 20 messages at 5 per second
  python redrive.py --skip-poison          # replay only the ones that failed for transient reasons
"""
import argparse
import time

import pika

from common import DLQ, EXCHANGE, ROUTING_KEY, connect_amqp, declare_topology, log


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", action="store_true", help="show DLQ contents without consuming them")
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--rate", type=float, default=5.0, help="messages per second")
    ap.add_argument("--skip-poison", action="store_true", help="leave poison messages in the DLQ")
    args = ap.parse_args()

    conn = connect_amqp()
    ch = conn.channel()
    declare_topology(ch)
    ch.confirm_delivery()

    if args.list:
        seen = 0
        while seen < args.limit:
            method, props, body = ch.basic_get(DLQ, auto_ack=False)
            if method is None:
                break
            log("INFO", "dlq message", correlation_id=props.correlation_id,
                reason=(props.headers or {}).get("x-last-error"), body=body.decode()[:120])
            seen += 1
        conn.close()   # closing without ack returns every message to the DLQ
        log("INFO", "listed", count=seen)
        return

    moved = kept = 0
    while moved + kept < args.limit:
        method, props, body = ch.basic_get(DLQ, auto_ack=False)
        if method is None:
            break
        reason = str((props.headers or {}).get("x-last-error", ""))
        if args.skip_poison and reason.startswith("poison"):
            # Leave it unacked: the broker won't hand it to us again on this channel,
            # and closing the connection at the end returns it to the DLQ.
            kept += 1
            continue
        headers = dict(props.headers or {})
        headers.update({"x-attempt": 1, "x-redriven": True})
        ch.basic_publish(EXCHANGE, ROUTING_KEY, body, pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent, content_type=props.content_type,
            correlation_id=props.correlation_id, message_id=props.message_id, headers=headers))
        ch.basic_ack(method.delivery_tag)   # remove from DLQ only after the broker confirmed the republish
        moved += 1
        time.sleep(1 / args.rate)
    conn.close()
    log("INFO", "redrive done", moved=moved, left_in_dlq_as_poison=kept)


if __name__ == "__main__":
    main()
