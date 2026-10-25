#!/usr/bin/env bash
# Write .env from Terraform outputs, plus local settings. Re-run after every terraform apply.
set -euo pipefail
cd "$(dirname "$0")/.."

terraform -chdir=infra output -raw dotenv > .env.tmp
OPERATOR=$(grep -E '^\s*operator_email' infra/terraform.tfvars | sed -E 's/.*=\s*"([^"]+)".*/\1/')
cat >> .env.tmp <<EOF
OPERATOR_EMAIL=${OPERATOR}
FACILITY_TZ=Asia/Kolkata
# Failure injectors are allowed in this TEST project only.
ALLOW_INJECTIONS=true
# Synthetic source replicas (docker-compose Postgres)
SOURCE_DSN=postgresql://reconcile:reconcile@localhost:5433/source_replica
EOF
mv .env.tmp .env
echo "wrote .env"
grep -E '^(GCP_PROJECT|ARCHIVE_BUCKET|SA_INGEST|DB_HOST)=' .env
