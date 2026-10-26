# agents/

Settings for the AI agents used by the recovery engine.

## What is used now (Phase 5, stages 2–3)

| File | Purpose |
|---|---|
| `registry.yml` | Pins each agent: model, region, prompt version, thinking budget, timeout. Mounted read-only in Airflow at `/opt/reconcile/agents`. CI (`scripts/check_policy.py`) fails on a retired model or a region other than `asia-south1`. |

| Agent | Model | Status |
|---|---|---|
| Diagnosis | `gemini-3.5-flash`, `asia-south1`, prompt `diag-v2` | Synthetic-data trial, recommend-only |
| Communication | `gemini-3.5-flash-lite`, `asia-south1` | Not built yet (registry entry only) |

The agent code lives in `ingest/reconcile_ingest/`:

| File | Purpose |
|---|---|
| `agent_gateway.py` | A1 gate (is an agent allowed?), masked payload (counts only), fake agent or Vertex AI call as `sa-agents`, response schema, 20 s limit, output validation, provenance |
| `engine.py` | Hands `fallback_unknown` incidents to the gateway; writes `recovery.agent_calls` (`AGENT_CALL_SCHEMA`) |
| `agent_access_check.py` | Proves `sa-agents` can read only `agent_context` (9 of 9) |
| `agent_eval.py` | Release gate on real Gemini, using `ingest/tests/agent_eval_cases.json` |
| `agent_suite.py` | Live pipeline cases (fake modes and `vertex`); run with `scripts/agent_suite.sh <case\|all\|list>` |

Other pieces:

- `policy/policy.yml`, `agents:` section: `enabled`, `residency_confirmed`, `synthetic_trial_ref`, `recommend_only`, kill switch
- `infra/agents.tf`: `agent_context` masked views, `sa-agents` read access, Vertex AI access
- `ingest/dags/recovery_engine.py`: reads the Airflow variables `fake_agent_mode` and `agent_provider`
- `ingest/tests/test_agent_gateway.py` and `agent_golden.json`: 30 golden cases in CI

## How it works

1. A rule explains the incident → the engine acts on the rule; no agent is asked.
2. No rule explains it (`fallback_unknown`) → the gateway sends a masked summary to the agent.
3. The agent returns one action from the catalog, or `escalate`, with confidence and evidence.
4. Code validates the answer (JSON shape, action allowed, never-auto list, evidence, scope, confidence, 20 s limit). Anything rejected → escalated to a person.
5. Every call is recorded in `recovery.agent_calls` (model, prompt version, policy version, validation, latency).

This is a personal practice project on a GCP free-trial account, using synthetic data only. The real agent runs only in the test project, when the Airflow variable `agent_provider=vertex` is set, and as **recommend-only** (owner decision `POC-SYNTHETIC-ONLY`). It never runs an action and never creates an approval card.

## Quick commands

    $G "cd /opt/reconcile/ingest && python -m reconcile_ingest.agent_access_check"   # sa-agents access, 9 of 9
    $G "cd /opt/reconcile/ingest && python -m reconcile_ingest.agent_eval"           # release gate on real Gemini
    scripts/agent_suite.sh list                                                       # fake-agent and vertex cases

## Removed for now

The ADK package `reconcile_agents` (schemas, Diagnosis and Communication agents as a separate Cloud Run service) was removed on 2026-10-26. Its schemas and Diagnosis logic now live in `agent_gateway.py`. Its Communication prompt will be reused when that agent is built.

To bring it back:

    git checkout parked-reconcile-agents -- agents/agents/reconcile_agents