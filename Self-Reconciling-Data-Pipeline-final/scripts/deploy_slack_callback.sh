#!/usr/bin/env bash
# Phase 5 stage 4 — deploy the Slack callback (the ONLY public endpoint) to Cloud Run in asia-south1.
# Run from Git Bash in the repo folder, as the operator, after:
#   1) make tf-apply with slack_enabled = true   (creates the two secrets and who may read them)
#   2) both secret values added (runbook: Slack setup)
#   SLACK_CHANNEL_ID=C0123ABCDEF SLACK_APPROVERS=you@example.com scripts/deploy_slack_callback.sh
# Settings go through a temporary env-vars YAML file (works on Windows, where gcloud runs via cmd.exe).
set -euo pipefail
PROJECT="${GCP_PROJECT_ID:-reconcile-poc-29056}"
REGION="asia-south1"
SERVICE="reconcile-slack-callback"
: "${SLACK_CHANNEL_ID:?set SLACK_CHANNEL_ID (C…) first}"
: "${SLACK_APPROVERS:?set SLACK_APPROVERS (comma-separated e-mails) first}"

ENV_FILE="slack/.env.deploy.yaml"
trap 'rm -f "${ENV_FILE}"' EXIT
cat > "${ENV_FILE}" <<YAML
GCP_PROJECT_ID: "${PROJECT}"
SLACK_CHANNEL_ID: "${SLACK_CHANNEL_ID}"
SLACK_APPROVERS: "${SLACK_APPROVERS}"
YAML

gcloud run deploy "${SERVICE}" \
  --project="${PROJECT}" --region="${REGION}" \
  --source=slack \
  --service-account="sa-api@${PROJECT}.iam.gserviceaccount.com" \
  --no-allow-unauthenticated \
  --min-instances=0 --max-instances=2 --cpu=1 --memory=512Mi --timeout=30 \
  --env-vars-file="${ENV_FILE}" \
  --set-secrets="SLACK_SIGNING_SECRET=slack-signing-secret:latest,SLACK_BOT_TOKEN=slack-bot-token:latest" \
  --labels=purpose=slack-approvals \
  --quiet

URL="https://${SERVICE}-$(gcloud projects describe "${PROJECT}" --format='value(projectNumber)').${REGION}.run.app"
echo
echo "Deployed: ${URL}"
echo "Next: set slack_callback_deployed = true in infra/terraform.tfvars, then make tf-plan && make tf-apply"
echo "      (makes ONLY this service public; every request still needs a valid Slack signature)"
echo "Slack app Interactivity Request URL:  ${URL}/slack/interactions"
