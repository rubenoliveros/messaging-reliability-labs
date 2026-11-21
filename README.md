# Messaging Reliability Labs

Hands-on labs and interactive simulators for moving money-like data between systems without losing it or counting it twice.

**Interactive site:** https://rubenoliveros.github.io/messaging-reliability-labs/

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/rubenoliveros/messaging-reliability-labs)

## What's here

| Folder | What it is |
| :--- | :--- |
| `docs/` | The GitHub Pages site: 7 core ideas, a RabbitMQ simulator and a Kafka simulator, all in the browser |
| `rabbitmq-lab/` | RabbitMQ + Postgres + Python: idempotent consumer, acks after commit, tiered retries, DLQ, redrive, reconciliation |
| `kafka-lab/` | Kafka (KRaft) + Postgres + Python: keyed partitions, offset commits after the write, retries, DLQ, versioned upserts, rebalances, replay, reconciliation |
| `.devcontainer/` | Lets anyone run the real labs in the browser with GitHub Codespaces |

## The scenario

A partner system sends charges and payments for customer accounts. We store them in an append-only ledger (`balance = charges - payments`). Networks fail, consumers crash and partners resend. Each lab shows how to design so that nothing is lost and nothing is double-counted, and how to prove it with reconciliation.

## Run the labs in Codespaces (no install)

1. Click **Open in GitHub Codespaces** above. Dependencies install automatically.
2. Run one lab at a time:

```bash
# RabbitMQ
cd rabbitmq-lab
docker compose up -d
python producer.py --count 100 --reset-source
python consumer.py            # in a second terminal
python reconcile.py

# Kafka
cd kafka-lab
docker compose up -d
sleep 15 && python create_topics.py
python producer.py --count 100 --reset-source
python consumer.py            # in a second terminal
python reconcile.py
```

The management UIs open from the **Ports** tab (RabbitMQ on 15672, Kafka UI on 8080). Stop a lab with `docker compose down` before starting the other.

## Run the labs locally

Docker Desktop and Python 3.11+. Follow the README inside each lab folder (Windows PowerShell commands included).

## Publish the site

In the repo: **Settings > Pages > Build and deployment > Deploy from a branch**, branch `main`, folder `/docs`.

---

Built as a learning lab. The simulators are simplified teaching models, not benchmarks.
