# Phase 5 stage 4 — Slack approvals.
#
#   - two secrets in Secret Manager, stored in asia-south1 only; their VALUES are added by hand with
#     `gcloud secrets versions add` (never in Terraform, never in git)
#   - sa-engine may read the bot token (to post and update cards); sa-api may read both (the callback
#     checks the signing secret, and asks Slack who clicked)
#   - the callback service runs as sa-api, which may only INSERT into recovery.approval_decisions
#   - it is the ONE public endpoint: allUsers may invoke it, and every request must carry a valid Slack
#     signature that is less than 5 minutes old (slack/slack_core.py)
# Created only when slack_enabled = true in terraform.tfvars.

variable "slack_enabled" {
  description = "true once the Slack workspace exists (Phase 5 stage 4)"
  type        = bool
  default     = false
}

variable "slack_callback_deployed" {
  description = "true after scripts/deploy_slack_callback.sh created the reconcile-slack-callback service"
  type        = bool
  default     = false
}

locals {
  slack_secrets = var.slack_enabled ? toset(["slack-bot-token", "slack-signing-secret"]) : toset([])
}

resource "google_project_service" "slack_apis" {
  count              = var.slack_enabled ? 1 : 0
  project            = var.project_id
  service            = "secretmanager.googleapis.com"
  disable_on_destroy = false
}

resource "google_secret_manager_secret" "slack" {
  for_each  = local.slack_secrets
  project   = var.project_id
  secret_id = each.key
  labels    = { purpose = "slack-approvals" }
  replication {
    user_managed {
      replicas {
        location = "asia-south1"
      }
    }
  }
  depends_on = [google_project_service.slack_apis]
}

resource "google_secret_manager_secret_iam_member" "api_reads_slack" {
  for_each  = local.slack_secrets
  project   = var.project_id
  secret_id = google_secret_manager_secret.slack[each.key].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.sa["api"].email}"
}

resource "google_secret_manager_secret_iam_member" "engine_reads_slack_token" {
  count     = var.slack_enabled ? 1 : 0
  project   = var.project_id
  secret_id = google_secret_manager_secret.slack["slack-bot-token"].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.sa["engine"].email}"
}

resource "google_cloud_run_v2_service_iam_member" "slack_callback_public" {
  count    = var.slack_callback_deployed ? 1 : 0
  project  = var.project_id
  location = "asia-south1"
  name     = "reconcile-slack-callback"
  role     = "roles/run.invoker"
  member   = "allUsers"
}
