# agents/

Settings and code for the AI agents used by the recovery engine. Agents advise; the engine decides.

## What is here

| Path | Purpose |
|---|---|
| `registry.yml` | Pins each agent: model, region (`asia-south1` only), prompt version, budgets, timeout; `service_url` of the Cloud Run service (stage 3b); the Communication agent's `fallback_model`. Mounted read-only in Airflow at `/opt/reconcile/agents`. CI (`scripts/check_policy.py`) fails on retired models, a non-Mumbai region or a service URL outside asia-south1. |
| `reconcile_agents/` | The Diagnosis agent as an **ADK** agent (stage 3b): `agent.py` (`root_agent`, output schema = the proposal the gateway validates, **no tools**) and `prompts/diag-v2.txt` (the same prompt as the REST path — a unit test keeps them equal). |
| `Dockerfile`, `requirements.txt`, `.gcloudignore` | The private Cloud Run image: `adk api_server`, no web UI. Built by `scripts/deploy_agent_service.sh`. |

| Agent | Model | How it is called | Status |
|---|---|---|---|
| Diagnosis | `gemini-3.5-flash`, `asia-south1`, `diag-v2` | `agent_provider=vertex` (REST) or `agent_provider=adk` (Cloud Run) | Synthetic-data trial, recommend-only |
| Communication | `gemini-3.5-flash-lite` (fallback `gemini-3.5-flash`), `comms-v1` | when an approval card is written and a real agent is allowed | Wording only; summary dropped if it adds any number, ID, link or e-mail |

The engine-side code lives in `ingest/reconcile_ingest/`:

| File | Purpose |
|---|---|
| `agent_gateway.py` | A1 gate, masked payload (counts only), fake / vertex / adk call, Model Armor screening, 20 s limit, validation, provenance; `communicate()` for card summaries |
| `model_armor.py` | Screens the untrusted text before a call and the answer after it (template `reconcile-agents` in asia-south1); `setup`, `status`, `test` |
| `engine.py` | Hands `fallback_unknown` incidents to the gateway; adds the card summary; writes `recovery.agent_calls` |
| `agent_access_check.py` | Proves `sa-agents` can read only `agent_context` (9 of 9) |
| `agent_eval.py` | Release gate on the real model (`--provider vertex` or `adk`) |
| `agent_suite.py` | Live pipeline cases: fake modes, `vertex`, `adk` (`scripts/agent_suite.sh <case>`) |
| `phi_scan.py` | T26: no patient data in agent calls, approval cards or incident notes |

## How it works

1. A rule explains the incident → the engine acts on the rule; no agent is asked.
2. No rule explains it (`fallback_unknown`) → the gateway builds a masked summary (counts and check names).
3. Model Armor screens the untrusted log/check text. Injection or sensitive data → rejected, the model is never called.
4. The Diagnosis agent (REST or ADK on Cloud Run) returns one catalog action or `escalate`, with confidence and evidence.
5. Model Armor screens the answer; then the engine validates it (JSON shape, action allowed, never-auto list, evidence, scope, confidence, 20 s limit). Anything rejected → escalated to a person.
6. Every call is recorded in `recovery.agent_calls` (agent, provider, model, prompt and policy version, validation, latency).

Personal practice project on a GCP free-trial account, synthetic data only. Real agents run only in the test project, only when the Airflow variable `agent_provider` is `vertex` or `adk`, and **recommend-only** (owner decision `POC-SYNTHETIC-ONLY`).

## Quick commands

    $G "cd /opt/reconcile/ingest && python -m reconcile_ingest.model_armor test"        # harmless → clean, injected → blocked
    $G "cd /opt/reconcile/ingest && python -m reconcile_ingest.agent_access_check"      # 9 of 9
    $G "cd /opt/reconcile/ingest && python -m reconcile_ingest.agent_eval"              # release gate (vertex)
    $G "cd /opt/reconcile/ingest && python -m reconcile_ingest.agent_eval --provider adk"
    scripts/agent_suite.sh vertex                                                     # or: adk
    scripts/deploy_agent_service.sh                                                   # build + deploy the ADK service

The earlier ADK draft is still in git history under the tag `parked-reconcile-agents`; the package here replaces it.
