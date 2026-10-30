"""approval_worker — Phase 5 stage 1: acts on the data owner's decisions.

Every 5 minutes (or `airflow dags trigger approval_worker` right after clicking Approve):
  process_decisions (sa-engine) — expire unanswered cards, escalate rejected ones, and for approved
                                  ones re-check fingerprint, incident state, policy version, kill switch
                                  and attempt cap. Only cards that pass become actions.
  → reextract       (sa-ingest)  — same executor as recovery_engine (skipped when nothing was approved)
  → rebuild_and_verify           — triggers dbt_build; its gate decides, and the engine closes the incident

  slack_sync        (sa-engine)  — Phase 5 stage 4: post new PENDING cards to the approvals channel and
                                  replace finished ones with their outcome (no-op while Slack is off)

The console and the Slack callback never run anything: they only record the decision in
recovery.approval_decisions.
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


def _slack(**_):
    _engine()                                   # puts /opt/reconcile/ingest on the path
    from reconcile_ingest import slack_notify
    return slack_notify.sync()


def _act(ti, **_):
    actions = ti.xcom_pull(task_ids="process_decisions") or []
    if not actions:
        raise AirflowSkipException("no approved card passed the re-check")
    _engine().reextract(actions)
    # one rebuild per approved business date — each date's own build and engine run close its incident
    return [{"business_date": d} for d in sorted({a["business_date"] for a in actions})]


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
    rebuild = TriggerDagRunOperator.partial(
        task_id="rebuild_and_verify",
        trigger_dag_id="dbt_build",
        trigger_run_id="approved__{{ run_id }}__{{ ti.map_index }}",   # unique per approved date
        wait_for_completion=False,
    ).expand(conf=act.output)
    slack = PythonOperator(task_id="slack_sync", python_callable=_slack, trigger_rule="all_done")
    process >> act >> rebuild
    process >> slack
