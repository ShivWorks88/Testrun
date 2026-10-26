"""approval_worker — Phase 5 stage 1: acts on the data owner's decisions.

Every 5 minutes (or `airflow dags trigger approval_worker` right after clicking Approve):
  process_decisions (sa-engine) — expire unanswered cards, escalate rejected ones, and for approved
                                  ones re-check fingerprint, incident state, policy version, kill switch
                                  and attempt cap. Only cards that pass become actions.
  → reextract       (sa-ingest)  — same executor as recovery_engine (skipped when nothing was approved)
  → rebuild_and_verify           — triggers dbt_build; its gate decides, and the engine closes the incident

The console never runs anything itself: it only records the decision in recovery.approval_decisions.
"""
from __future__ import annotations

import sys

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG

try:
    from airflow.sdk.exceptions import AirflowSkipException
except ImportError:  # older layout
    from airflow.exceptions import AirflowSkipException


def _engine():
    if "/opt/reconcile/ingest" not in sys.path:
        sys.path.insert(0, "/opt/reconcile/ingest")
    from reconcile_ingest import engine
    return engine


def _process(**_):
    return _engine().process_approvals()


def _act(ti, **_):
    actions = ti.xcom_pull(task_ids="process_decisions") or []
    if not actions:
        raise AirflowSkipException("no approved card passed the re-check")
    _engine().reextract(actions)
    return {"business_date": max(a["business_date"] for a in actions)}


with DAG(
    dag_id="approval_worker",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    tags=["reconcile", "phase5"],
) as dag:
    process = PythonOperator(task_id="process_decisions", python_callable=_process)
    act = PythonOperator(task_id="reextract", python_callable=_act)
    rebuild = TriggerDagRunOperator(
        task_id="rebuild_and_verify",
        trigger_dag_id="dbt_build",
        conf={"business_date": "{{ ti.xcom_pull(task_ids='reextract')['business_date'] }}"},
        wait_for_completion=False,
    )
    process >> act >> rebuild
