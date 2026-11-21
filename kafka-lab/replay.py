"""Backfill / replay: move a consumer group's committed offsets back in time.

Stop the consumer group first (offsets can't be changed while the group is active).
Then start the consumer again and it re-reads from the new position. Because the
consumer is idempotent, the replay can't double-count anything.

Examples:
  python replay.py --to-beginning
  python replay.py --since-minutes 30
  python replay.py --show                 # print current committed offsets and lag
"""
import argparse
import time

from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient

from common import BOOTSTRAP, CONSUMER_GROUP, TOPIC, log


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--group", default=CONSUMER_GROUP)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--to-beginning", action="store_true")
    g.add_argument("--since-minutes", type=int)
    g.add_argument("--show", action="store_true")
    args = ap.parse_args()

    # A throwaway consumer (different group) just to look up partitions, watermarks and timestamps.
    lookup = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"{args.group}-offset-lookup",
                       "enable.auto.commit": False})
    partitions = sorted(lookup.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions)

    if args.show:
        reader = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": args.group, "enable.auto.commit": False})
        committed = reader.committed([TopicPartition(TOPIC, p) for p in partitions], timeout=10)
        for tp in committed:
            low, high = lookup.get_watermark_offsets(TopicPartition(TOPIC, tp.partition), timeout=10)
            pos = tp.offset if tp.offset >= 0 else low
            log("INFO", "partition", partition=tp.partition, committed=tp.offset, log_end=high, lag=high - pos)
        reader.close()
        lookup.close()
        return

    if args.to_beginning:
        targets = [TopicPartition(TOPIC, p, lookup.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)[0])
                   for p in partitions]
    else:
        ts_ms = int((time.time() - args.since_minutes * 60) * 1000)
        found = lookup.offsets_for_times([TopicPartition(TOPIC, p, ts_ms) for p in partitions], timeout=10)
        targets = []
        for tp in found:
            if tp.offset < 0:   # nothing newer than ts in this partition: start at the end
                tp = TopicPartition(TOPIC, tp.partition,
                                    lookup.get_watermark_offsets(TopicPartition(TOPIC, tp.partition), timeout=10)[1])
            targets.append(tp)
    lookup.close()

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    futures = admin.alter_consumer_group_offsets([ConsumerGroupTopicPartitions(args.group, targets)])
    for _, fut in futures.items():
        fut.result()
    log("INFO", "offsets moved", group=args.group, offsets={tp.partition: tp.offset for tp in targets})


if __name__ == "__main__":
    main()
