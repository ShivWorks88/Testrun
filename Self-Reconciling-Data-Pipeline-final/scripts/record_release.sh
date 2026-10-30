#!/usr/bin/env bash
# Phase 6 pkg 2 — record a dbt release (which models changed) so the engine can tell a regression from bad data.
# Run from Git Bash in the repo folder after you deploy dbt changes (e.g. after merging to main):
#   scripts/record_release.sh <from-ref>          # e.g. scripts/record_release.sh HEAD~1   or a tag/sha
# Models = the .sql files under dbt/reconcile/models changed between <from-ref> and HEAD.
set -euo pipefail
FROM="${1:?usage: scripts/record_release.sh <from-ref>  (e.g. HEAD~1)}"
SHA=$(git rev-parse --short HEAD)
MODELS=$(git diff --name-only "${FROM}" HEAD -- dbt/reconcile/models | grep '\.sql$' | xargs -r -n1 basename | sed 's/\.sql$//' | sort -u | tr '\n' ' ')
if [ -z "${MODELS// /}" ]; then
  echo "no dbt model changed between ${FROM} and ${SHA} — nothing to record"
  exit 0
fi
echo "release ${SHA}: ${MODELS}"
docker compose exec -T airflow bash -c \
  "cd /opt/reconcile/ingest && python -m reconcile_ingest.releases record --sha ${SHA} --note 'git ${FROM}..${SHA}' --models ${MODELS}"
