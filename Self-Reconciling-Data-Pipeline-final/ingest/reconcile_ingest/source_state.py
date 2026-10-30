"""Phase 6 — what we know about each SOURCE, shared by the breaker, the engine and the publication gate.

Everything here reads `ops` (batches, source_health, breakers), which sa-engine, sa-publish and sa-api
can all read already, so no new permissions are needed. The decision functions are pure (no BigQuery)
so they are unit-tested in CI.

    open?   at least `failing_datasets` datasets of one source, or `or_share_of_inventory` of them,
            failed (latest batch FAILED) within `within_minutes`; or the source probe is unreachable
    close?  BOTH the probe passed after the breaker opened AND every dataset of the source has a LOADED
            extract that started after the breaker opened (freshness) — a probe alone is not enough (T21)
    global  `global_open_at_sources` sources open at once → hold all publishing
    stale   the source replica is more than `max_replica_lag_hours` behind (latest probe)
    late    a day's rows are below `min_volume_share` of the median of recent days (late documentation)
"""
from __future__ import annotations

import datetime as dt
import os
import statistics
from pathlib import Path

from reconcile_ingest.datasets import DATASETS, SOURCE_SYSTEMS, datasets_for

OPS_DS = os.environ.get("OPS_DATASET", "ops")
BREAKERS_TABLE = "breakers"
GLOBAL = "GLOBAL"
POLICY_FILE = os.environ.get("POLICY_FILE", "/opt/reconcile/policy/policy.yml")

DEFAULT_OPEN = {"failing_datasets": 3, "or_share_of_inventory": 0.5, "within_minutes": 30}
DEFAULT_FRESHNESS = {"max_replica_lag_hours": 6, "min_volume_share": 0.5, "volume_baseline_days": 14}


# ── policy (read here too, so publish.py does not need the engine) ─────────
def read_policy(path: str = POLICY_FILE) -> dict:
    import yaml
    p = Path(path)
    return yaml.safe_load(p.read_text()) if p.exists() else {}


def open_rule(policy: dict, source: str) -> dict:
    b = policy.get("breakers") or {}
    rule = (b.get(source) or b.get("default") or {}).get("open_when") or {}
    return {**DEFAULT_OPEN, **rule}


def global_threshold(policy: dict) -> int:
    return int((policy.get("breakers") or {}).get("global_open_at_sources", 2))


def freshness(policy: dict) -> dict:
    return {**DEFAULT_FRESHNESS, **(policy.get("freshness") or {})}


# ── pure decisions (unit-tested) ────────────────────────────────────────────
def should_open(failing: list[str], inventory: int, rule: dict, probe_down: bool = False) -> tuple[bool, str]:
    n = len(failing)
    share = n / inventory if inventory else 0.0
    if probe_down:
        return True, "source probe unreachable"
    if n and n >= int(rule["failing_datasets"]):
        return True, f"{n} of {inventory} datasets failing (threshold {rule['failing_datasets']})"
    if n and share >= float(rule["or_share_of_inventory"]):
        return True, f"{n} of {inventory} datasets failing ({share:.0%} ≥ {float(rule['or_share_of_inventory']):.0%})"
    return False, f"{n} of {inventory} datasets failing — below the threshold"


def should_close(probe_ok_after_open: bool, not_reloaded: list[str]) -> tuple[bool, str]:
    if not probe_ok_after_open:
        return False, "stays OPEN: no passing source probe since the breaker opened"
    if not_reloaded:
        return False, f"stays OPEN: probe passes but freshness fails — not reloaded yet: {', '.join(not_reloaded)}"
    return True, "probe passes and every dataset has loaded again"


def is_low_volume(rows: int | None, baseline: list[int], min_share: float) -> tuple[bool, float | None]:
    """Late data: today's rows vs the median of earlier days. Needs at least 2 earlier days."""
    if rows is None or len(baseline) < 2:
        return False, None
    median = statistics.median(baseline)
    if median <= 0:
        return False, None
    share = rows / median
    return share < min_share, round(share, 3)


def blocked_reasons(sources: set[str], datasets: set[str], blocked: dict[str, str],
                    low: dict[str, str]) -> list[str]:
    """Why a report must be held, given its upstream sources/datasets and the current source state."""
    out = []
    if GLOBAL in blocked:
        out.append(blocked[GLOBAL])
    out += [blocked[s] for s in sorted(sources) if s in blocked]
    out += [low[d] for d in sorted(datasets) if d in low]
    return out


# ── BigQuery reads ──────────────────────────────────────────────────────────
def _q(bq, sql: str, **params):
    from google.cloud import bigquery

    def param(k, v):
        if isinstance(v, dt.datetime):
            return bigquery.ScalarQueryParameter(k, "TIMESTAMP", v)
        if isinstance(v, dt.date):
            return bigquery.ScalarQueryParameter(k, "DATE", v)
        if isinstance(v, int):
            return bigquery.ScalarQueryParameter(k, "INT64", v)
        return bigquery.ScalarQueryParameter(k, "STRING", v)
    cfg = bigquery.QueryJobConfig(query_parameters=[param(k, v) for k, v in params.items()])
    return [dict(r) for r in bq.query(sql, job_config=cfg).result()]


def breaker_states(bq) -> dict[str, dict]:
    """Latest row per source from ops.breakers (empty when the table does not exist yet)."""
    from google.api_core.exceptions import NotFound
    try:
        rows = _q(bq, f"""select * from `{bq.project}.{OPS_DS}.{BREAKERS_TABLE}`
                          qualify row_number() over (partition by source_system order by `at` desc) = 1""")
    except NotFound:
        return {}
    return {r["source_system"]: r for r in rows}


def latest_batches(bq, since: dt.datetime) -> dict[str, dict]:
    """Latest batch (any mode) per dataset that started after `since`."""
    rows = _q(bq, f"""select dataset, source_system, business_date, mode, status, started_at, error
                      from `{bq.project}.{OPS_DS}.batches` where started_at >= @since
                      qualify row_number() over (partition by dataset order by started_at desc) = 1""",
              since=since)
    return {r["dataset"]: r for r in rows}


def latest_probes(bq, since: dt.datetime | None = None) -> dict[str, dict]:
    where = "where probed_at >= @since" if since else ""
    rows = _q(bq, f"""select source_system, probed_at, reachable, replica_lag_seconds, error
                      from `{bq.project}.{OPS_DS}.source_health` {where}
                      qualify row_number() over (partition by source_system order by probed_at desc) = 1""",
              **({"since": since} if since else {}))
    return {r["source_system"]: r for r in rows}


def reloaded_since(bq, source: str, since: dt.datetime) -> set[str]:
    rows = _q(bq, f"""select distinct dataset from `{bq.project}.{OPS_DS}.batches`
                      where source_system = @s and status = 'LOADED' and mode = 'extract'
                        and started_at > @since""", s=source, since=since)
    return {r["dataset"] for r in rows}


def stale_sources(bq, policy: dict) -> dict[str, str]:
    """Sources whose latest probe shows the replica too far behind."""
    limit_h = float(freshness(policy)["max_replica_lag_hours"])
    out = {}
    for s, p in latest_probes(bq).items():
        lag = p.get("replica_lag_seconds")
        if p.get("reachable") and lag is not None and lag > limit_h * 3600:
            out[s] = f"stale feed: {s} replica {lag / 3600:.1f} h behind (limit {limit_h:g} h)"
    return out


def volume_report(bq, bd: dt.date, policy: dict, datasets: list[str] | None = None) -> dict[str, dict]:
    """Rows of the latest LOADED extract for `bd` vs the median of earlier days, per dataset."""
    f = freshness(policy)
    start = bd - dt.timedelta(days=int(f["volume_baseline_days"]))
    rows = _q(bq, f"""select dataset, business_date, rows_loaded from `{bq.project}.{OPS_DS}.batches`
                      where status = 'LOADED' and mode = 'extract' and business_date between @s and @d
                      qualify row_number() over (partition by dataset, business_date order by started_at desc) = 1""",
              s=start, d=bd)
    by_ds: dict[str, dict] = {}
    for r in rows:
        e = by_ds.setdefault(r["dataset"], {"today": None, "baseline": []})
        if r["business_date"] == bd:
            e["today"] = r["rows_loaded"]
        elif r["rows_loaded"] is not None:
            e["baseline"].append(int(r["rows_loaded"]))
    out = {}
    for ds, e in by_ds.items():
        if datasets and ds not in datasets:
            continue
        low, share = is_low_volume(e["today"], e["baseline"], float(f["min_volume_share"]))
        out[ds] = {"rows": e["today"], "baseline_days": len(e["baseline"]),
                   "median": statistics.median(e["baseline"]) if e["baseline"] else None,
                   "share": share, "low": low}
    return out


def blocked_sources(bq, policy: dict | None = None) -> dict[str, str]:
    """What the publication gate must respect right now: open breakers (incl. GLOBAL) and stale feeds."""
    policy = policy if policy is not None else read_policy()
    out = {}
    for s, b in breaker_states(bq).items():
        if b["state"] == "OPEN":
            out[s] = (f"outage breaker OPEN for {s} since {str(b['opened_at'])[:16]} "
                      f"(incident {b.get('parent_incident_id') or '-'})")
    for s, why in stale_sources(bq, policy).items():
        out.setdefault(s, why)
    return out


def low_volume_datasets(bq, bd: dt.date, policy: dict | None = None) -> dict[str, str]:
    policy = policy if policy is not None else read_policy()
    out = {}
    for ds, v in volume_report(bq, bd, policy).items():
        if v["low"]:
            out[ds] = (f"late data: {ds} {bd} has {v['rows']} rows, {v['share']:.0%} of the usual "
                       f"{v['median']:.0f} (median of {v['baseline_days']} days)")
    return out


def source_of(dataset: str | None) -> str | None:
    d = DATASETS.get(dataset or "")
    return d.source_system if d else None


def inventory(source: str) -> list[str]:
    return [d.name for d in datasets_for(source)]


SOURCES = list(SOURCE_SYSTEMS)
