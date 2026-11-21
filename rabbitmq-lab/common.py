"""Shared settings, topology and helpers for the RabbitMQ lab."""
import json
import os
import sys
import time

import pika
import psycopg

AMQP_URL = os.getenv("LAB_AMQP_URL", "amqp://guest:guest@localhost:5672/%2F")
DB_DSN = os.getenv("LAB_DSN", "postgresql://lab:lab@localhost:5433/ledger")
SOURCE_FILE = os.getenv("LAB_SOURCE_FILE", "source_events.jsonl")

# Topology
EXCHANGE = "ledger"            # direct exchange the producer publishes to
ROUTING_KEY = "entry"
MAIN_Q = "ledger.entries"      # work queue the consumer reads
DLQ = "ledger.entries.dlq"     # dead-letter queue: messages we gave up on, kept for inspection and redrive

# Retry tiers (delay queues). A message waits in a tier until its TTL expires,
# then RabbitMQ dead-letters it back to the main exchange. One queue per delay
# avoids head-of-line blocking (with per-message TTL, only the head message expires).
RETRY_TIERS_MS = [5_000, 20_000, 60_000]
MAX_ATTEMPTS = len(RETRY_TIERS_MS) + 1   # first try + one try per tier


def retry_queue_name(delay_ms: int) -> str:
    return f"ledger.entries.retry.{delay_ms // 1000}s"


def connect_amqp():
    params = pika.URLParameters(AMQP_URL)
    params.heartbeat = 30
    return pika.BlockingConnection(params)


def declare_topology(ch):
    """Idempotent: safe to call from every script on startup."""
    ch.exchange_declare(EXCHANGE, exchange_type="direct", durable=True)
    ch.queue_declare(MAIN_Q, durable=True)
    ch.queue_bind(MAIN_Q, EXCHANGE, routing_key=ROUTING_KEY)
    for delay in RETRY_TIERS_MS:
        ch.queue_declare(
            retry_queue_name(delay),
            durable=True,
            arguments={
                "x-message-ttl": delay,
                "x-dead-letter-exchange": EXCHANGE,
                "x-dead-letter-routing-key": ROUTING_KEY,
            },
        )
    ch.queue_declare(DLQ, durable=True)


def connect_db():
    return psycopg.connect(DB_DSN)


class PoisonMessage(Exception):
    """The message can never succeed (bad shape or bad values). Do not retry."""


class TransientError(Exception):
    """Temporary failure (timeout, DB down, partner 503). Retry with backoff."""


def validate(event: dict) -> dict:
    required = ("external_id", "account_id", "kind", "amount_cents")
    missing = [k for k in required if k not in event]
    if missing:
        raise PoisonMessage(f"missing fields: {missing}")
    if event["kind"] not in ("charge", "payment"):
        raise PoisonMessage(f"invalid kind: {event['kind']!r}")
    if not isinstance(event["amount_cents"], int) or event["amount_cents"] <= 0:
        raise PoisonMessage(f"invalid amount_cents: {event['amount_cents']!r}")
    return event


def log(level: str, msg: str, **fields):
    """Structured log line: one JSON object per line, always with a correlation_id when we have one."""
    line = {"ts": time.strftime("%H:%M:%S"), "level": level, "msg": msg, **fields}
    print(json.dumps(line), file=sys.stdout, flush=True)
