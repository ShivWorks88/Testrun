"""T26 — prove that no patient data reaches the AI agents, the approval cards or the incident journal.

    python -m reconcile_ingest.phi_scan            # exit code 0 = clean, 1 = something found

Runs as sa-engine (it already reads recovery and raw). Three checks over every stored row:
  1. record identifiers — patient, encounter, order, panel, result, claim, payment, adjustment and
     diagnosis IDs (batch, incident and approval IDs are allowed: they are not about a person)
  2. sensitive field names carrying a value (given_name, family_name, mrn, birth_date, result_value, …)
  3. real names — every patient's given and family name in raw.patients, matched as whole words (words our own
     code and prompts write, like "master" or "date", are left out: a synthetic surname can be an ordinary word)
The access check (agent_access_check, 9 of 9) separately proves sa-agents cannot read raw data at all.
Prints counts and row references only — never the matched text itself.
"""
from __future__ import annotations

import re
import sys

from reconcile_ingest.engine import ENGINE, INCIDENT_DS, _q
from reconcile_ingest.publish import client_as

# synthetic source IDs: P-000123, P-20261014-0001, E-20261014-00042, R-20261014-000001, C-…, O-…, N-…, G-…, Y-…, A-…
# (a word boundary before the letter keeps INC-2026…, APR-2026… and b-2026… batch IDs out)
RECORD_ID = r"\b(P-[0-9]{6}|[PERONCGYA]-[0-9]{8}-[A-Za-z0-9]+)"
SENSITIVE_KEYS = ("given_name", "family_name", "mrn", "birth_date", "result_value", "patient_id",
                  "encounter_id", "claim_id", "lab_result_id")
# a sensitive key followed by a value (": "x"" or "=x") — a bare column name in a check name is fine
SENSITIVE_KV = r'"?\b(' + "|".join(SENSITIVE_KEYS) + r')\b"?\s*[:=]\s*"?[A-Za-z0-9]'

# (table, id column, text columns) — everything an agent saw or produced, and what people are shown
TARGETS = [
    ("agent_calls", "call_id", ["request_refs", "output"]),
    ("approvals", "approval_id", ["card", "params"]),
    ("incidents", "incident_id", ["evidence", "note"]),
]


# Words the pipeline itself writes (engine and Slack card templates, agent prompts) are not patient data, even
# when a synthetic surname happens to be the same word (e.g. "master" in "master patient index", "date" in
# "business date"). They are left out of the name check only; record-ID and field-value checks are unchanged.
OWN_TEXT = ["engine.py", "slack_notify.py", "agent_gateway.py", "source_checks.py", "breaker.py"]


def own_words() -> list[str]:
    from pathlib import Path
    here = Path(__file__).resolve().parent
    files = [here / f for f in OWN_TEXT] + sorted((here.parents[2] / "agents").rglob("prompts/*.txt"))
    words: set[str] = set()
    for f in files:
        try:
            words |= {w.lower() for w in re.findall(r"[A-Za-z]{4,}", f.read_text(encoding="utf-8"))}
        except OSError:
            continue
    return sorted(words)


def find_record_ids(text: str) -> list[str]:
    return re.findall(RECORD_ID, text or "")


def find_sensitive_values(text: str) -> list[str]:
    return [m.group(1) for m in re.finditer(SENSITIVE_KV, text or "")]


def scan(bq) -> list[dict]:
    p, out, own = bq.project, [], own_words() or ["-"]
    for table, key, cols in TARGETS:
        for col in cols:
            ids = _q(bq, f"""select count(*) n, array_agg({key} ignore nulls limit 5) refs
                             from `{p}.{INCIDENT_DS}.{table}` where regexp_contains(ifnull({col}, ''), @rx)""",
                     rx=RECORD_ID)[0]
            kv = _q(bq, f"""select count(*) n, array_agg({key} ignore nulls limit 5) refs
                            from `{p}.{INCIDENT_DS}.{table}` where regexp_contains(ifnull({col}, ''), @rx)""",
                    rx=SENSITIVE_KV)[0]
            names = _q(bq, f"""
                with n as (select distinct lower(x) w from `{p}.raw.patients`,
                             unnest([given_name, family_name]) x
                           where length(x) >= 4 and lower(x) not in unnest(@own)),
                     t as (select {key} k, lower(ifnull({col}, '')) v from `{p}.{INCIDENT_DS}.{table}`)
                select count(distinct k) n, array_agg(distinct k ignore nulls limit 5) refs
                from t join n on regexp_contains(t.v, concat(r'\\b', n.w, r'\\b'))""", own=own)[0]
            rows = _q(bq, f"select count(*) n from `{p}.{INCIDENT_DS}.{table}`")[0]["n"]
            for kind, r in (("record ids", ids), ("sensitive field values", kv), ("patient names", names)):
                out.append({"table": table, "column": col, "check": kind, "rows_scanned": rows,
                            "found": int(r["n"] or 0), "refs": list(r["refs"] or [])})
    return out


def main() -> int:
    bq = client_as(ENGINE)
    results = scan(bq)
    print("T26 patient-data scan (recovery.agent_calls, approvals, incidents) — counts only")
    for r in results:
        flag = "✅" if r["found"] == 0 else "❌"
        refs = f"  rows: {', '.join(r['refs'])}" if r["found"] else ""
        print(f"  {flag} {r['table']}.{r['column']:13} {r['check']:22} {r['found']} of {r['rows_scanned']}{refs}")
    bad = sum(r["found"] for r in results)
    print(f"T26 {'PASSED' if bad == 0 else 'FAILED'}: {bad} finding(s) in {len(results)} checks")
    return 0 if bad == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
