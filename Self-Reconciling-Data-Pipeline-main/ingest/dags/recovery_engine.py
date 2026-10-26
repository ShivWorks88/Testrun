"""recovery_engine — Phase 4 self-repair, started by dbt_build after every build.

assess_and_decide (sa-engine: incidents, diagnosis, five gates)
  → reextract (only for auto-fixable incidents; confirms raw matches the receipt)
  → rebuild_and_verify (triggers dbt_build; its gate publishes, and the next engine run closes the incident)

Kill switch:  kill_switch in policy/policy.yml (mounted read-only); one switch for engine and console
Fake agent:   test project only — `airflow variables set fake_agent_mode valid|injected|timeout|…` sends
              unknown incidents to the fake Diagnosis agent (Phase 5 stage 2); delete the variable to stop.
Real agent:   `airflow variables set agent_provider vertex` asks Gemini (as sa-agents) — only when policy.yml
              allows it (Security confirmation, or synthetic_trial_ref in the test project).
"""
from __future__ import annotations

import sys

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, Param, Variable

try:
    from airflow.sdk.exceptions import AirflowSkipException
except ImportError:  # older layout
    from airflow.exceptions import AirflowSkipException


def _engine():
    if "/opt/reconcile/ingest" not in sys.path:
        sys.path.insert(0, "/opt/reconcile/ingest")
    from reconcile_ingest import engine
    return engine


def _decide(params, **_):
    mode = Variable.get("fake_agent_mode", default=None) or None   # test-only knob; unset = no agent
    provider = Variable.get("agent_provider", default=None) or None   # "vertex" = real agent, if policy allows
    # rules, attempt cap and kill switch come from policy.yml
    return _engine().evaluate(params["business_date"], fake_agent_mode=mode, agent_provider=provider)


def _act(ti, **_):
    actions = ti.xcom_pull(task_ids="assess_and_decide") or []
    if not actions:
        raise AirflowSkipException("nothing to re-extract")
    return _engine().reextract(actions)


with DAG(
    dag_id="recovery_engine",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    params={"business_date": Param("2026-10-14", type="string")},
    tags=["reconcile", "phase4", "phase5"],
) as dag:
    decide = PythonOperator(task_id="assess_and_decide", python_callable=_decide)
    act = PythonOperator(task_id="reextract", python_callable=_act)
    rebuild = TriggerDagRunOperator(
        task_id="rebuild_and_verify",
        trigger_dag_id="dbt_build",
        conf={"business_date": "{{ params.business_date }}"},
        wait_for_completion=False,
    )
    decide >> act >> rebuild
