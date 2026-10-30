#!/usr/bin/env bash
# Phase 6 hardening pass — run from the repo root on the operator laptop (Git Bash). Read-only.
#   1. secrets and dump files in git (tracked files AND history)
#   2. Cloud Run: only the Slack callback may be public
#   3. Secret Manager: one enabled version per Slack secret (old ones disabled)
#   4. no user-managed service-account keys (the POC uses impersonation only)
#   5. the in-container checks: policy guardrails, T26, journal, test leftovers, Airflow toggles
set -uo pipefail
export GIT_PAGER=cat
PROJECT="${GCP_PROJECT_ID:-$(grep -E '^GCP_PROJECT_ID=' .env 2>/dev/null | cut -d= -f2 | tr -d '"\r')}"
[ -n "$PROJECT" ] || PROJECT="$(gcloud config get-value project 2>/dev/null | tr -d '\r')"
REGION=asia-south1
fail=0; warn=0
ok()   { echo "  ✅ $*"; }
bad()  { echo "  ❌ $*"; fail=$((fail+1)); }
note() { echo "  ⚠️  $*"; warn=$((warn+1)); }
PAT='xox[abprs]-[0-9A-Za-z-]{10,}|-----BEGIN [A-Z ]*PRIVATE KEY-----|"private_key_id"|AIza[0-9A-Za-z_-]{35}|ya29\.[0-9A-Za-z_-]{20,}'

echo "1 · secrets and dump files in git"
hits=$(git grep -I -l -E "$PAT" -- . ':!*.lock' 2>/dev/null | wc -l | tr -d ' ')
[ "$hits" = 0 ] && ok "no secret patterns in tracked files" || bad "$hits tracked file(s) match a secret pattern: git grep -I -l -E '<pattern>'"
forbidden=$(git ls-files | grep -E '(^|/)\.env$|\.tfstate|terraform\.tfvars$|_files\.txt$|dbt_tests\.txt$|\.json\.key$' | wc -l | tr -d ' ')
[ "$forbidden" = 0 ] && ok "no .env, tfstate, tfvars or dump files tracked" || bad "$forbidden forbidden file(s) tracked: git ls-files | grep -E '\.env|tfstate|tfvars|_files\.txt'"
hist=$(git log --all -p --no-color 2>/dev/null | grep -c -E "$PAT")
[ "$hist" = 0 ] && ok "no secret patterns anywhere in git history" || bad "$hist line(s) in git history match a secret pattern — rotate that secret (history rewrite optional)"
untracked=$(ls *_files.txt dbt_tests.txt console_files.txt 2>/dev/null | wc -l | tr -d ' ')
[ "$untracked" = 0 ] && ok "no helper dump files in the repo folder" || note "$untracked helper dump file(s) in the repo folder — delete them"

echo "2 · Cloud Run exposure"
if [ -n "$PROJECT" ]; then
  for svc in $(gcloud run services list --project "$PROJECT" --region $REGION --format='value(metadata.name)' 2>/dev/null | tr -d '\r'); do
    public=$(gcloud run services get-iam-policy "$svc" --project "$PROJECT" --region $REGION --format=json 2>/dev/null | grep -c '"allUsers"')
    if [ "$public" != 0 ] && [ "$svc" != "reconcile-slack-callback" ]; then bad "$svc is public (allUsers)"
    elif [ "$public" != 0 ]; then ok "$svc public by design (Slack signs every request)"
    else ok "$svc private (ID token required)"; fi
  done
else note "no project (set GCP_PROJECT_ID or gcloud config set project) — skipped"; fi

echo "3 · Secret Manager"
for s in slack-bot-token slack-signing-secret; do
  n=$(gcloud secrets versions list "$s" --project "$PROJECT" --filter='state=ENABLED' --format='value(name)' 2>/dev/null | wc -l | tr -d ' ')
  if [ "$n" = 1 ]; then ok "$s: 1 enabled version"
  elif [ "$n" = 0 ]; then note "$s: no enabled version (Slack off?)"
  else bad "$s: $n enabled versions — disable the old ones"; fi
done
note "confirm by hand: the Slack tokens shown in a screenshot on 27 Oct were rotated (api.slack.com → OAuth & Permissions → Revoke / reinstall)"

echo "4 · service-account keys"
keys=0
for sa in $(gcloud iam service-accounts list --project "$PROJECT" --format='value(email)' 2>/dev/null); do
  k=$(gcloud iam service-accounts keys list --iam-account "$sa" --managed-by=user --format='value(name)' 2>/dev/null | wc -l | tr -d ' ')
  keys=$((keys + k))
done
[ "$keys" = 0 ] && ok "no user-managed keys (impersonation only)" || bad "$keys user-managed key(s) — delete them"

echo "5 · inside the container"
docker compose exec -T airflow bash -c "cd /opt/reconcile/ingest && python -m reconcile_ingest.hardening" || fail=$((fail+1))

echo
echo "laptop checks: $fail failed, $warn warning(s)"
[ "$fail" = 0 ]
