"""Create the lab topics. Safe to run more than once."""
from confluent_kafka.admin import AdminClient, NewTopic

from common import BOOTSTRAP, DLQ_TOPIC, PARTITIONS, TOPIC, log


def main():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    existing = set(admin.list_topics(timeout=10).topics)
    wanted = [
        NewTopic(TOPIC, num_partitions=PARTITIONS, replication_factor=1,
                 config={"retention.ms": str(7 * 24 * 3600 * 1000)}),   # 7 days of replayable history
        NewTopic(DLQ_TOPIC, num_partitions=1, replication_factor=1),
    ]
    todo = [t for t in wanted if t.topic not in existing]
    if not todo:
        log("INFO", "topics already exist", topics=[t.topic for t in wanted])
        return
    for name, fut in admin.create_topics(todo).items():
        fut.result()
        log("INFO", "created topic", topic=name)


if __name__ == "__main__":
    main()
