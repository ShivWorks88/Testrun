#!/usr/bin/env bash
# Phase 5 stage 3b — build and deploy the Diagnosis agent (ADK) as a PRIVATE Cloud Run service.
# Run from Git Bash in the repo folder, as the operator (Bhavna's laptop), AFTER `make tf-apply` with
#   agents_residency_ref set (model access) — and again later with agents_service_deployed = true (invoker).
#   scripts/deploy_agent_service.sh            # build + deploy, then print the URL
# The service runs as sa-agents, in asia-south1 only, with NO unauthenticated access and no web UI.
set -euo pipefail
PROJECT="${GCP_PROJECT_ID:-reconcile-poc-29056}"
REGION="asia-south1"
SERVICE="reconcile-diagnosis"
SA="sa-agents@${PROJECT}.iam.gserviceaccount.com"

gcloud run deploy "${SERVICE}" \
  --project="${PROJECT}" --region="${REGION}" \
  --source=agents \
  --service-account="${SA}" \
  --no-allow-unauthenticated \
  --ingress=all \
  --min-instances=0 --max-instances=2 --cpu=1 --memory=1Gi --timeout=60 \
  --set-env-vars="GOOGLE_GENAI_USE_VERTEXAI=TRUE,GOOGLE_CLOUD_PROJECT=${PROJECT},GOOGLE_CLOUD_LOCATION=${REGION}" \
  --labels=purpose=reconcile-agents \
  --quiet

URL=$(gcloud run services describe "${SERVICE}" --project="${PROJECT}" --region="${REGION}" --format='value(status.url)')
echo
echo "Deployed: ${URL}"
echo "Next: put it in agents/registry.yml  →  diagnosis.service_url: ${URL}"
echo "      then set agents_service_deployed = true in infra/terraform.tfvars and run: make tf-plan && make tf-apply"
echo "Check it is private (expect 403):  curl -s -o /dev/null -w '%{http_code}\n' ${URL}/list-apps"
