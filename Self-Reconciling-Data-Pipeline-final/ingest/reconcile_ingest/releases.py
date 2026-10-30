"""Phase 6 pkg 2 — release context: which dbt models did the latest deployment change? (T14, G6)

    python -m reconcile_ingest.releases record --models fct_daily_claims stg_claims --sha 1a2b3c4 --note "PR 42"
    python -m reconcile_ingest.releases list

`scripts/record_release.sh` records a release from git (the dbt models changed since the last recorded one).
State: ops.releases (append-only, written by sa-engine). When a grain or model test fails on a model that a
release within `releases.window_days` changed, the engine's rule `suspected_regression` applies: the report is
held, the incident goes to an engineer WITH the release attached, and no data action runs — re-loading data
cannot fix code. A test release can carry an expiry so it stops counting once the test is over.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import uuid

from google.cloud import bigquery

RELEASES_TABLE = "releases"
SCHEMA = [bigquery.SchemaField(n, t) for n, t in [
    ("release_id", "STRING"), ("released_at", "TIMESTAMP"), ("git_sha", "STRING"), ("changed_models", "STRING"),
    ("note", "STRING"), ("expires_at", "TIMESTAMP"), ("actor", "STRING")]]
REGRESSION_KINDS = {"check_failed", "staging_mismatch", "build_error"}   # code problems a release can cause


def window_days(policy: dict) -> int:
    return int((policy.get("releases") or {}).get("window_days", 7))


def touching(releases: list[dict], models: set[str]) -> dict | None:
    """The newest release whose changed models overlap `models` (pure; unit-tested)."""
    for r in sorted(releases, key=lambda x: str(x["released_at"]), reverse=True):
        changed = set(json.loads(r["changed_models"]) if isinstance(r["changed_models"], str) else r["changed_models"])
        hit = sorted(changed & models)
        if hit:
            return {"release_id": r["release_id"], "git_sha": r.get("git_sha"), "released_at": str(r["released_at"]),
                    "changed_models": hit, "note": r.get("note")}
    return None


def scope_models(finding: dict) -> set[str]:
    """Models a failing check is about: what it is attached to, plus the dataset's staging model."""
    models = {m for m in (finding.get("models") or []) if m}
    if finding.get("dataset"):
        models.add(f"stg_{finding['dataset']}")
    if finding["type"] == "build_error" and (finding.get("evidence") or {}).get("model"):
        models.add(finding["evidence"]["model"])
    return models


def recent(bq, policy: dict, now: dt.datetime | None = None) -> list[dict]:
    from google.api_core.exceptions import NotFound

    from reconcile_ingest.source_state import OPS_DS, _q
    now = now or dt.datetime.now(dt.timezone.utc)
    try:
        return _q(bq, f"""select * from `{bq.project}.{OPS_DS}.{RELEASES_TABLE}`
                          where released_at >= @since and (expires_at is null or expires_at > @now)""",
                  since=now - dt.timedelta(days=window_days(policy)), now=now)
    except NotFound:
        return []


def mark(found: list[dict], releases: list[dict]) -> None:
    """Attach the release to every code-type finding whose scope it changed."""
    for f in found:
        if f["type"] in REGRESSION_KINDS:
            r = touching(releases, scope_models(f))
            if r:
                f["release"] = r
                if isinstance(f.get("evidence"), dict):
                    f["evidence"] = {**f["evidence"], "release": r}


def record(models: list[str], sha: str | None, note: str | None, expires_minutes: int | None = None) -> dict:
    from reconcile_ingest.engine import ENGINE, client_as, service_account
    from reconcile_ingest.source_state import OPS_DS
    bq = client_as(ENGINE)
    table = f"{bq.project}.{OPS_DS}.{RELEASES_TABLE}"
    bq.create_table(bigquery.Table(table, schema=SCHEMA), exists_ok=True)
    now = dt.datetime.now(dt.timezone.utc)
    row = {"release_id": f"REL-{now:%Y%m%d}-{uuid.uuid4().hex[:6]}", "released_at": now.isoformat(),
           "git_sha": sha, "changed_models": json.dumps(sorted(set(models))), "note": note,
           "expires_at": (now + dt.timedelta(minutes=expires_minutes)).isoformat() if expires_minutes else None,
           "actor": service_account(ENGINE)}
    cfg = bigquery.LoadJobConfig(schema=SCHEMA, write_disposition="WRITE_APPEND")
    bq.load_table_from_json([row], table, job_config=cfg).result()
    print(f"recorded {row['release_id']} sha {sha or '-'} models {sorted(set(models))}"
          + (f" (expires in {expires_minutes} min)" if expires_minutes else ""))
    return row


def main() -> None:
    from reconcile_ingest.engine import ENGINE, client_as, load_policy
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("record")
    r.add_argument("--models", nargs="+", required=True)
    r.add_argument("--sha")
    r.add_argument("--note")
    r.add_argument("--expires-minutes", type=int)
    sub.add_parser("list")
    a = ap.parse_args()
    if a.cmd == "record":
        record(a.models, a.sha, a.note, a.expires_minutes)
    else:
        for x in recent(client_as(ENGINE), load_policy()):
            print(f"{x['release_id']}  {str(x['released_at'])[:19]}  sha {x['git_sha'] or '-'}  "
                  f"{x['changed_models']}  {x['note'] or ''}")


if __name__ == "__main__":
    main()
