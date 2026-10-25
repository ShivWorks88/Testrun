#!/usr/bin/env bash
# Build the console + API image with Cloud Build and deploy it to Cloud Run (private).
#   - runs as sa-api (read-only on ops/analytics, publish-only on engine-requests)
#   - no public access: open it with `make console` (gcloud run services proxy)
#   - reaches Cloud SQL over the private network (Direct VPC egress)
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a

SHA=$(git rev-parse --short HEAD 2>/dev/null || echo dev)
IMAGE="${ARTIFACT_REPO}/console:${SHA}"
SERVICE=reconcile-console

echo "==> Building ${IMAGE} with Cloud Build (as ${SA_BUILD})"
gcloud builds submit . \
  --project "${GCP_PROJECT}" --region "${GCP_REGION}" \
  --config cloudbuild.yaml \
  --service-account "projects/${GCP_PROJECT}/serviceAccounts/${SA_BUILD}" \
  --default-buckets-behavior=regional-user-owned-bucket \
  --substitutions "_IMAGE=${IMAGE},_GIT_SHA=${SHA}"

ENVFILE=$(mktemp)
command -v cygpath >/dev/null 2>&1 && ENVFILE="$(cygpath -m "$ENVFILE")"
cat > "${ENVFILE}" <<EOF
ENV: cloud
GCP_PROJECT: "${GCP_PROJECT}"
GCP_REGION: "${GCP_REGION}"
ARCHIVE_BUCKET: "${ARCHIVE_BUCKET}"
BQ_MAX_BYTES_BILLED: "${BQ_MAX_BYTES_BILLED}"
DB_HOST: "${DB_HOST}"
DB_NAME: reconcile_ops
DB_USER: reconcile_app
ROLE_MAP: "${OPERATOR_EMAIL}=viewer,engineer,data_owner"
DEFAULT_ROLES: viewer
GIT_SHA: "${SHA}"
BUILT_AT: "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
EOF

echo "==> Deploying ${SERVICE} to Cloud Run (${GCP_REGION})"
gcloud run deploy "${SERVICE}" \
  --project "${GCP_PROJECT}" --region "${GCP_REGION}" \
  --image "${IMAGE}" \
  --service-account "${SA_API}" \
  --no-allow-unauthenticated \
  --network "${VPC_NETWORK}" --subnet "${VPC_SUBNET}" --vpc-egress private-ranges-only \
  --env-vars-file "${ENVFILE}" \
  --set-secrets "DB_PASSWORD=${DB_PASSWORD_SECRET}:latest" \
  --cpu 1 --memory 512Mi --min-instances 0 --max-instances 2 \
  --labels app=reconcile-poc,env=test
rm -f "${ENVFILE}"

echo "==> Health check through an authenticated proxy"
URL=$(gcloud run services describe "${SERVICE}" --project "${GCP_PROJECT}" --region "${GCP_REGION}" --format 'value(status.url)')
TOKEN=$(gcloud auth print-identity-token)
curl -fsS -H "Authorization: Bearer ${TOKEN}" "${URL}/api/health" && echo
echo "Done. Open the console with: make console   (then http://localhost:8081)"
