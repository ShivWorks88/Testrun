#!/usr/bin/env bash
# Copy the POC's evidence (incident journal, cards, decisions, agent calls, gate decisions, breakers, releases,
# remaps) to the archive bucket BEFORE a destroy + rebuild, so the history survives. Read-only on BigQuery.
#   scripts/export_evidence.sh            → gs://<archive bucket>/evidence/<UTC timestamp>/<table>-*.json
set -euo pipefail
PROJECT="${GCP_PROJECT_ID:-$(grep -E '^GCP_PROJECT_ID=' .env | cut -d= -f2)}"
BUCKET="${ARCHIVE_BUCKET:-$(grep -E '^ARCHIVE_BUCKET=' .env | cut -d= -f2)}"
[ -n "$PROJECT" ] && [ -n "$BUCKET" ] || { echo "set GCP_PROJECT_ID and ARCHIVE_BUCKET (or have them in .env)"; exit 1; }
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
DEST="gs://${BUCKET#gs://}/evidence/$STAMP"
for t in recovery.incidents recovery.approvals recovery.approval_decisions recovery.agent_calls \
         recovery.slack_posts publication.publication_state publication.check_results \
         ops.breakers ops.releases ops.patient_remaps ops.batches ops.control_totals; do
  if bq --project_id="$PROJECT" show --format=none "$PROJECT:$t" >/dev/null 2>&1; then
    bq --project_id="$PROJECT" --location=asia-south1 extract --destination_format NEWLINE_DELIMITED_JSON \
       "$PROJECT:$t" "$DEST/${t/./_}-*.json" >/dev/null && echo "  ✅ $t"
  else
    echo "  · $t (not there, skipped)"
  fi
done
echo "evidence → $DEST   (counts and engine notes only; no patient rows)"
