"""Print depth and consumer count for every lab queue (what you'd put on a dashboard or alert on)."""
from common import DLQ, MAIN_Q, RETRY_TIERS_MS, connect_amqp, declare_topology, log, retry_queue_name


def main():
    conn = connect_amqp()
    ch = conn.channel()
    declare_topology(ch)
    for q in [MAIN_Q, *[retry_queue_name(d) for d in RETRY_TIERS_MS], DLQ]:
        r = ch.queue_declare(q, passive=True)   # passive: read stats, never create or change
        log("INFO", "queue", name=q, ready_messages=r.method.message_count, consumers=r.method.consumer_count)
    conn.close()


if __name__ == "__main__":
    main()
