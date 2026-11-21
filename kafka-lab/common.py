"""Shared settings and helpers for the Kafka lab."""
import json
import os
import sys
import time

import psycopg

BOOTSTRAP = os.getenv("LAB_BOOTSTRAP", "localhost:9092")
DB_DSN = os.getenv("LAB_DSN", "postgresql://lab:lab@localhost:5434/ledger")
SOURCE_FILE = os.getenv("LAB_SOURCE_FILE", "source_events.jsonl")

TOPIC = "ledger.events"          # main topic, keyed by account_id
DLQ_TOPIC = "ledger.events.dlq"  # dead-letter topic
PARTITIONS = 3
CONSUMER_GROUP = "ledger-writer"

MAX_ATTEMPTS = 4                 # in-place retries before giving up on a record
BACKOFF_BASE_S = 0.5             # 0.5s, 1s, 2s (+ jitter)


def connect_db():
    return psycopg.connect(DB_DSN)


class PoisonMessage(Exception):
    """The record can never succeed. Send to DLQ, do not retry."""


class TransientError(Exception):
    """Temporary failure. Retry with backoff."""


def validate(event: dict) -> dict:
    etype = event.get("type")
    if etype == "ledger_entry":
        for k in ("external_id", "account_id", "kind", "amount_cents"):
            if k not in event:
                raise PoisonMessage(f"missing field {k}")
        if event["kind"] not in ("charge", "payment"):
            raise PoisonMessage(f"invalid kind {event['kind']!r}")
        if not isinstance(event["amount_cents"], int) or event["amount_cents"] <= 0:
            raise PoisonMessage(f"invalid amount_cents {event['amount_cents']!r}")
    elif etype == "account_status":
        for k in ("account_id", "status", "version"):
            if k not in event:
                raise PoisonMessage(f"missing field {k}")
    else:
        raise PoisonMessage(f"unknown event type {etype!r}")
    return event


def header(msg, name: str, default=None):
    for k, v in (msg.headers() or []):
        if k == name:
            return v.decode() if isinstance(v, bytes) else v
    return default


def log(level: str, msg: str, **fields):
    line = {"ts": time.strftime("%H:%M:%S"), "level": level, "msg": msg, **fields}
    print(json.dumps(line, default=str), file=sys.stdout, flush=True)
