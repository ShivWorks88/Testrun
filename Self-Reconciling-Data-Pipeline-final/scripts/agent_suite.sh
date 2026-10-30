#!/usr/bin/env bash
# Phase 5 stage 2 — live checks with the FAKE Diagnosis agent (test project only). No model is called.
#   scripts/agent_suite.sh list
#   scripts/agent_suite.sh injected
#   scripts/agent_suite.sh all          # about 25 min; summary at the end; log saved under logs/
set -euo pipefail
case_name="${1:-list}"
mkdir -p logs
docker compose exec -T airflow bash -c \
  "cd /opt/reconcile/ingest && python -m reconcile_ingest.agent_suite ${case_name}" 2>&1 \
  | tee "logs/agent_suite_${case_name}_$(date +%Y%m%d_%H%M).log"
