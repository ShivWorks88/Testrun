"""outage_breaker — Phase 6: check every source every 5 minutes; open or close outage breakers.

One parent incident and one alert per source outage instead of one per dataset. The engine also runs
the same check before every decision, so this DAG only makes the breaker react between builds.
State: ops.breakers (newest row per source). See reconcile_ingest/breaker.py and policy.yml `breakers:`.
Turn it on in the Airflow UI (new DAGs start paused).
"""
from __future__ import annotations

import sys

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def _check(**_):
    if "/opt/reconcile/ingest" not in sys.path:
        sys.path.insert(0, "/opt/reconcile/ingest")
    from reconcile_ingest import breaker
    res = breaker.check()
    return {"open": sorted(res["open"]), "events": res["events"]}


with DAG(
    dag_id="outage_breaker",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    tags=["reconcile", "phase6"],
) as dag:
    PythonOperator(task_id="check_sources", python_callable=_check)
