"""Ingest DAGs: one per source system (EHR, LIS, Billing) plus a replay DAG.

    probe_source -> ingest_dataset (mapped, one task per dataset) -> emit_run_complete

Each mapped task extracts one dataset in a repeatable-read snapshot, writes its control totals,
archives the Parquet file write-once, and loads raw with delete + insert in one transaction.

Trigger with config to pick a date or inject failures (test project only):
    {"business_date": "2026-10-13", "inject": ["drop_hour"], "datasets": ["encounters"]}
"""
from __future__ import annotations

from datetime import date, datetime, timedelta

import pendulum
from airflow.sdk import Param, dag, get_current_context, task
from reconcile_ingest import gcp, pipeline
from reconcile_ingest.archive import GcsArchive
from reconcile_ingest.datasets import DATASETS, SOURCE_SYSTEMS, datasets_for
from reconcile_ingest.settings import Settings
from reconcile_ingest.warehouse import BigQueryWarehouse

TZ = pendulum.timezone("Asia/Kolkata")
DEFAULT_ARGS = {"owner": "data-platform", "retries": 2, "retry_delay": timedelta(minutes=1)}


def _components():
    s = Settings.from_env()
    s.require_gcp()
    bq = gcp.bigquery_client(s.project, s.impersonate, s.location)
    st = gcp.storage_client(s.project, s.impersonate)
    return s, BigQueryWarehouse(bq, s.project, s.max_bytes_billed), GcsArchive(st, s.archive_bucket)


def _business_date(params: dict) -> date:
    """Explicit param, else the facility-local day before the run (a 01:00 run loads yesterday)."""
    if params.get("business_date"):
        return date.fromisoformat(params["business_date"])
    ctx = get_current_context()
    run_after = ctx["dag_run"].run_after or datetime.now(tz=TZ)
    return (pendulum.instance(run_after).in_timezone(TZ) - timedelta(days=1)).date()


def build_ingest_dag(system: str):
    names = [d.name for d in datasets_for(system)]

    @dag(
        dag_id=f"ingest_{system.lower()}",
        description=f"{SOURCE_SYSTEMS[system]['label']}: {SOURCE_SYSTEMS[system]['description']}",
        schedule="0 1 * * *",
        start_date=pendulum.datetime(2026, 10, 1, tz=TZ),
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        tags=["reconcile", "phase-1", "ingest", system.lower()],
        params={
            "business_date": Param(None, type=["null", "string"], format="date",
                                   description="Business date to load (default: yesterday, facility time)"),
            "datasets": Param(names, type="array", description="Subset of datasets to load"),
            "inject": Param([], type="array",
                            description="Failure injectors (test project only): "
                                        f"{list(pipeline.INJECTIONS) + ['source_timeout']}"),
        },
        doc_md=__doc__,
    )
    def ingest():
        @task
        def resolve() -> dict:
            params = get_current_context()["params"]
            unknown = set(params["datasets"]) - set(names)
            if unknown:
                raise ValueError(f"{sorted(unknown)} are not {system} datasets")
            return {"business_date": _business_date(params).isoformat(), "datasets": params["datasets"],
                    "inject": params["inject"]}

        @task
        def probe_source() -> dict:
            s, wh, _ = _components()
            result = pipeline.probe_source(s, wh, system)
            if not result["reachable"]:
                raise RuntimeError(f"{system} source unreachable: {result['error']}")
            return result

        @task
        def dataset_list(cfg: dict) -> list[str]:
            return cfg["datasets"]

        @task(pool="bq_load", map_index_template="{{ dataset_name }}")
        def ingest_dataset(dataset: str, cfg: dict) -> dict:
            ctx = get_current_context()
            ctx["dataset_name"] = dataset  # labels the mapped task in the UI
            s, wh, ar = _components()
            run_id = ctx["run_id"]
            inject = list(cfg["inject"])
            if "source_timeout" in inject:  # failure suite: the source times out once, Airflow retries
                inject.remove("source_timeout")
                if not s.allow_injections:
                    raise PermissionError("failure injection is disabled (ALLOW_INJECTIONS=true in test only)")
                if ctx["ti"].try_number == 1:
                    raise TimeoutError(f"simulated source timeout for {dataset} (attempt 1; Airflow will retry)")
            res = pipeline.ingest_dataset(s, wh, ar, dataset, date.fromisoformat(cfg["business_date"]),
                                          inject, run_id=run_id)
            return res.as_dict()

        @task
        def emit_run_complete(results: list[dict], cfg: dict) -> str:
            s = Settings.from_env()
            pub = gcp.publisher_client(s.impersonate)
            return pipeline.emit_run_complete(pub, s.project, system, date.fromisoformat(cfg["business_date"]),
                                              list(results), get_current_context()["run_id"])

        cfg = resolve()
        probe = probe_source()
        loaded = ingest_dataset.partial(cfg=cfg).expand(dataset=dataset_list(cfg))
        probe >> loaded
        emit_run_complete(loaded, cfg)

    return ingest()


for _system in SOURCE_SYSTEMS:
    globals()[f"ingest_{_system.lower()}"] = build_ingest_dag(_system)


@dag(
    dag_id="replay_batch",
    description="Reload raw for one dataset and date from the immutable archive (no source access)",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz=TZ),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["reconcile", "phase-1", "replay"],
    params={
        "dataset": Param("encounters", enum=sorted(DATASETS)),
        "business_date": Param(None, type=["null", "string"], format="date"),
        "batch_id": Param(None, type=["null", "string"], description="Default: latest archived batch"),
    },
)
def replay_batch():
    @task(pool="bq_load")
    def replay() -> dict:
        p = get_current_context()["params"]
        s, wh, ar = _components()
        res = pipeline.replay_batch(s, wh, ar, p["dataset"], _business_date(p), p["batch_id"],
                                    run_id=get_current_context()["run_id"])
        return res.as_dict()

    replay()


replay_batch_dag = replay_batch()
