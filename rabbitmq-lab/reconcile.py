"""Compare the partner's record (source_events.jsonl) with our ledger (Postgres).

Classifies every difference and recomputes balances (charges - payments) on both sides.
Exit code 0 = reconciled, 1 = mismatches found (so it can run in CI or a scheduler).
"""
import json
import sys
from collections import defaultdict

from common import SOURCE_FILE, connect_db, log

FIELDS = ("account_id", "kind", "amount_cents")


def load_source():
    src = {}
    with open(SOURCE_FILE, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                ev = json.loads(line)
                src[ev["external_id"]] = ev
    return src


def load_destination(db):
    with db.cursor() as cur:
        cur.execute("SELECT external_id, account_id, kind, amount_cents FROM ledger_entries")
        return {r[0]: dict(zip(("external_id",) + FIELDS, r)) for r in cur.fetchall()}


def balances(events):
    b = defaultdict(int)
    for ev in events:
        b[ev["account_id"]] += ev["amount_cents"] if ev["kind"] == "charge" else -ev["amount_cents"]
    return b


def main():
    src = load_source()
    with connect_db() as db:
        dst = load_destination(db)

    missing = sorted(set(src) - set(dst))          # partner sent it, we don't have it
    unexpected = sorted(set(dst) - set(src))       # we have it, partner has no record of it
    mismatched = [k for k in set(src) & set(dst)
                  if any(src[k][f] != dst[k][f] for f in FIELDS)]

    src_bal, dst_bal = balances(src.values()), balances(dst.values())
    bal_diff = {a: (src_bal.get(a, 0), dst_bal.get(a, 0))
                for a in set(src_bal) | set(dst_bal) if src_bal.get(a, 0) != dst_bal.get(a, 0)}

    log("INFO", "reconciliation", source_events=len(src), destination_entries=len(dst),
        missing_in_destination=len(missing), unexpected_in_destination=len(unexpected),
        field_mismatches=len(mismatched), accounts_with_balance_diff=len(bal_diff))
    for k in missing[:10]:
        log("WARN", "missing in destination (check DLQ, then redrive)", external_id=k)
    for k in unexpected[:10]:
        log("WARN", "unexpected in destination (who wrote this?)", external_id=k)
    for k in mismatched[:10]:
        log("WARN", "field mismatch", external_id=k, source=src[k], destination=dst[k])
    for a, (s, d) in sorted(bal_diff.items())[:10]:
        log("WARN", "balance differs", account_id=a, source_cents=s, destination_cents=d)

    sys.exit(1 if (missing or unexpected or mismatched) else 0)


if __name__ == "__main__":
    main()
