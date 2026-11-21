"""Compare the partner's record (source_events.jsonl) with our database.

Checks ledger entries (missing, unexpected, field mismatches, balances) and the latest
account status (did out-of-order events leave the right version?).
Exit code 0 = reconciled, 1 = mismatches.
"""
import json
import sys
from collections import defaultdict

from common import SOURCE_FILE, connect_db, log

FIELDS = ("account_id", "kind", "amount_cents")


def main():
    src_entries, src_status = {}, {}
    with open(SOURCE_FILE, encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            ev = json.loads(line)
            if ev["type"] == "ledger_entry":
                src_entries[ev["external_id"]] = ev
            else:
                prev = src_status.get(ev["account_id"])
                if prev is None or ev["version"] > prev["version"]:
                    src_status[ev["account_id"]] = ev

    with connect_db() as db, db.cursor() as cur:
        cur.execute("SELECT external_id, account_id, kind, amount_cents FROM ledger_entries")
        dst_entries = {r[0]: dict(zip(("external_id",) + FIELDS, r)) for r in cur.fetchall()}
        cur.execute("SELECT account_id, status, version FROM account_status")
        dst_status = {r[0]: {"status": r[1], "version": r[2]} for r in cur.fetchall()}

    missing = sorted(set(src_entries) - set(dst_entries))
    unexpected = sorted(set(dst_entries) - set(src_entries))
    mismatched = [k for k in set(src_entries) & set(dst_entries)
                  if any(src_entries[k][f] != dst_entries[k][f] for f in FIELDS)]

    def balances(rows):
        b = defaultdict(int)
        for ev in rows:
            b[ev["account_id"]] += ev["amount_cents"] if ev["kind"] == "charge" else -ev["amount_cents"]
        return b

    sb, db_ = balances(src_entries.values()), balances(dst_entries.values())
    bal_diff = {a: (sb.get(a, 0), db_.get(a, 0)) for a in set(sb) | set(db_) if sb.get(a, 0) != db_.get(a, 0)}
    stale = {a: (s["version"], dst_status.get(a, {}).get("version")) for a, s in src_status.items()
             if dst_status.get(a, {}).get("version") != s["version"]}

    log("INFO", "reconciliation", source_entries=len(src_entries), destination_entries=len(dst_entries),
        missing_in_destination=len(missing), unexpected_in_destination=len(unexpected),
        field_mismatches=len(mismatched), accounts_with_balance_diff=len(bal_diff),
        accounts_with_stale_status=len(stale))
    for k in missing[:10]:
        log("WARN", "missing in destination (check DLQ, then redrive)", external_id=k)
    for k in unexpected[:10]:
        log("WARN", "unexpected in destination", external_id=k)
    for k in mismatched[:10]:
        log("WARN", "field mismatch", external_id=k)
    for a, (s, d) in sorted(bal_diff.items())[:10]:
        log("WARN", "balance differs", account_id=a, source_cents=s, destination_cents=d)
    for a, (s, d) in sorted(stale.items())[:10]:
        log("WARN", "status version differs", account_id=a, source_version=s, destination_version=d)

    sys.exit(1 if (missing or unexpected or mismatched or stale) else 0)


if __name__ == "__main__":
    main()
