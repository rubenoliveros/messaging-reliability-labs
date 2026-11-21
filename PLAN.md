# Plan: messaging-reliability-labs

## Goal
Hands-on labs and browser simulators that show how to move money-like data through RabbitMQ and Kafka without losing it or counting it twice. Detailed vision per lab: `kafka-lab/PROJECT_VISION.md`, `rabbitmq-lab/PROJECT_VISION.md`.

## Story it proves
Integration reliability: idempotent consumers, ack/commit after the write, retries, DLQ, replay and reconciliation. Learning project, built in public.

## Phase 1: publish what exists (MVP)
- [ ] Rename `RENAME_TO_.devcontainer` to `.devcontainer` (Codespaces badge in README needs it)
- [ ] Push to GitHub; enable Pages from `docs/`
- [ ] Check every link in README and the site
- [ ] Convert each lab to uv (`pyproject.toml`) while keeping `requirements.txt` for the devcontainer
- [ ] CI: lint + import check for both labs

## Phase 2: tests and evidence
- [ ] Integration tests with docker compose in CI (duplicate, poison message, crash between commit and ack, replay)
- [ ] Reconciliation output saved as a CI artifact
- [ ] Short GIF per lab

## Phase 3: chapters from the vision files
- [ ] Guided chapters on the site (analogy, predict, animation, why it matters, interview sentence, code link, official docs)
- [ ] Cloud runs: Kafka lab on Google Cloud, RabbitMQ lab on AWS, with teardown scripts

## Done when
The Pages site is live, both labs run with one command each, CI is green.

## Out of scope
Managed services comparisons, exactly-once claims beyond what the lab demonstrates.
