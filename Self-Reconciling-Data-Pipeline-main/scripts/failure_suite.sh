#!/usr/bin/env bash
# Failure suite (test project only): run one case or all six from Git Bash.
#   scripts/failure_suite.sh list
#   scripts/failure_suite.sh build_error
#   scripts/failure_suite.sh all          # about 35-45 min; prints a summary table at the end
# Each case injects a fault, lets the pipeline and engine react, checks the decision, then cleans up.
set -euo pipefail
case_name="${1:-list}"
docker compose exec -T airflow bash -c \
  "cd /opt/reconcile/ingest && python -m reconcile_ingest.suite ${case_name}" 2>&1 \
  | tee "failure_suite_${case_name}_$(date +%Y%m%d_%H%M).log"
