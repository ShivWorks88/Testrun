"""Stage 3 access check: what may the agents' identity (sa-agents) read?

    python -m reconcile_ingest.agent_access_check

Expected: the two masked views in agent_context are readable; raw, audit, ops, recovery, publication and
evidence_restricted are all DENIED. Model access is shown from IAM by `make tf-plan` output / gcloud (see runbook).
"""
from __future__ import annotations

from google.api_core.exceptions import Forbidden, NotFound

from reconcile_ingest.publish import client_as

CHECKS = [
    ("agent_context.incident_context", True),
    ("agent_context.check_context", True),
    ("raw.claims", False),
    ("raw.encounters", False),
    ("audit.stg_claims", False),
    ("ops.control_totals", False),
    ("recovery.incidents", False),
    ("publication.check_results", False),
    ("evidence_restricted.value_recon_lab_results", False),
]


def main() -> None:
    bq = client_as("agents")
    bad = 0
    for table, should_read in CHECKS:
        try:
            n = list(bq.query(f"select count(*) n from `{bq.project}.{table}`").result())[0]["n"]
            got, note = True, f"{n} rows"
        except (Forbidden, NotFound) as exc:
            got, note = False, type(exc).__name__
        except Exception as exc:  # noqa: BLE001 - report, keep checking
            got, note = False, f"{type(exc).__name__}: {str(exc)[:80]}"
        ok = got == should_read
        bad += not ok
        print(f"  {'✅' if ok else '❌'} sa-agents {'reads ' if got else 'DENIED'} {table:46} "
              f"(expected {'read' if should_read else 'denied'}) {note}")
    print(f"{len(CHECKS) - bad} of {len(CHECKS)} access rules as expected")


if __name__ == "__main__":
    main()
