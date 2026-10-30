"""dbt_build — build, test, value-check and publish one business date (Phases 2–4).

Trigger:  airflow dags trigger dbt_build --conf '{"business_date":"2026-10-14"}'
Test only: add "dbt_inject": ["build_error"] (also schema_drift, join_fanout, staging_duplicates); dbt applies
           them only when ALLOW_INJECTIONS=true. Engine-triggered rebuilds never pass switches.
Tasks:    dbt_debug → dbt_build_and_test (sa-dbt) → value_recon (sa-valuerecon) → publish (sa-publish)
          → start_recovery (Phase 4: hands the result to recovery_engine)
`publish` runs even when checks fail (trigger_rule all_done), so a failed day is recorded as HELD.
Phase 6 pkg 2: publish also reads the value_recon results; a failed or missing value-level check holds every
report built from that dataset (fct_lab_results_daily for lab_results). Test switch: dbt_inject value_truncation.
"""
from __future__ import annotations

import sys

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, Param

DBT_DIR = "/opt/reconcile/dbt/reconcile"
DBT = "/home/airflow/dbt-venv/bin/dbt"
AUDIT_TARGET = "/tmp/dbt-target/audit"
VALUERECON_TARGET = "/tmp/dbt-target/valuerecon"
VARS = "--vars '{business_date: {{ params.business_date }}, inject: {{ params.dbt_inject | tojson }}}'"


def dbt(args: str, target_path: str) -> str:
    return f"cd {DBT_DIR} && DBT_TARGET_PATH={target_path} DBT_LOG_PATH=/tmp/dbt-logs {DBT} {args}"


def _publish(params, run_id, dag_run, **_):
    if "/opt/reconcile/ingest" not in sys.path:
        sys.path.insert(0, "/opt/reconcile/ingest")
    from reconcile_ingest.publish import run_gate

    return run_gate(params["business_date"], target_dir=AUDIT_TARGET, gate_run_id=run_id,
                    not_before=dag_run.start_date, value_target_dir=VALUERECON_TARGET)


with DAG(
    dag_id="dbt_build",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    params={"business_date": Param("2026-10-14", type="string"),
            "dbt_inject": Param([], type="array", description="Failure-suite switches (test project only)")},
    tags=["reconcile", "dbt", "publish"],
) as dag:
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"rm -f {AUDIT_TARGET}/run_results.json && "
                     + dbt("debug --profiles-dir . --target audit", AUDIT_TARGET),
    )
    build = BashOperator(
        task_id="dbt_build_and_test",
        bash_command=dbt(f"build --profiles-dir . --target audit {VARS}", AUDIT_TARGET),
    )
    value_recon = BashOperator(
        task_id="value_recon",
        bash_command=dbt(f"build --profiles-dir . --target valuerecon --select tag:valuerecon {VARS}",
                         VALUERECON_TARGET),
    )
    publish = PythonOperator(task_id="publish", python_callable=_publish, trigger_rule="all_done")

    start_recovery = TriggerDagRunOperator(
        task_id="start_recovery",
        trigger_dag_id="recovery_engine",
        conf={"business_date": "{{ params.business_date }}"},
        trigger_rule="all_done",
    )

    dbt_debug >> build >> value_recon >> publish >> start_recovery
