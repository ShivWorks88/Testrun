# Phase 6 — the read-only "Ask the pipeline" assistant on the Operations screen.
#
#   The console API (sa-api) gathers the context itself (it already reads ops, publication and recovery),
#   screens the question and the answer with Model Armor, calls the `reconcile_assistant` app on the private
#   reconcile-diagnosis Cloud Run service, and records one row per question in recovery.agent_calls.
#   So sa-api needs exactly three more things, all switched on by assistant_enabled = true:
#     - invoke that one Cloud Run service                    (roles/run.invoker on the service)
#     - use Model Armor                                      (roles/modelarmor.user)
#     - append its provenance rows to recovery.agent_calls   (table-level dataEditor on that one table)
#   It still cannot read raw data, approve a card or run anything.

variable "assistant_enabled" {
  description = "true after the agents service was redeployed with the reconcile_assistant app"
  type        = bool
  default     = false
}

resource "google_cloud_run_v2_service_iam_member" "api_invokes_assistant" {
  count    = var.assistant_enabled && var.agents_service_deployed ? 1 : 0
  project  = var.project_id
  location = "asia-south1"
  name     = "reconcile-diagnosis"
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.sa["api"].email}"
}

resource "google_project_iam_member" "api_model_armor" {
  count   = var.assistant_enabled ? 1 : 0
  project = var.project_id
  role    = "roles/modelarmor.user"
  member  = "serviceAccount:${google_service_account.sa["api"].email}"
}

resource "google_bigquery_table_iam_member" "api_records_assistant_calls" {
  count      = var.assistant_enabled ? 1 : 0
  project    = var.project_id
  dataset_id = "recovery"
  table_id   = "agent_calls"
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.sa["api"].email}"
}
