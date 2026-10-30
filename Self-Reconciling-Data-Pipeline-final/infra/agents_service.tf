# Phase 5 rest (27 Oct) — Model Armor and the private Cloud Run service for the Diagnosis agent (stage 3b).
#
#   - sa-agents may use Model Armor (screening every real-agent call) — only while model access is ON
#   - the operator may deploy a Cloud Run service that RUNS AS sa-agents (iam.serviceAccountUser)
#   - APIs for building and running the service: Cloud Run, Cloud Build, Artifact Registry
#   - sa-engine may INVOKE that one service (roles/run.invoker on it, nothing else) — created only after the
#     service exists: set agents_service_deployed = true in terraform.tfvars after scripts/deploy_agent_service.sh

variable "agents_service_deployed" {
  description = "true after scripts/deploy_agent_service.sh created the reconcile-diagnosis Cloud Run service"
  type        = bool
  default     = false
}

resource "google_project_service" "agents_service_apis" {
  for_each           = toset(["run.googleapis.com", "cloudbuild.googleapis.com", "artifactregistry.googleapis.com"])
  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}

resource "google_project_iam_member" "agents_model_armor" {
  count   = local.agents_model_access ? 1 : 0
  project = var.project_id
  role    = "roles/modelarmor.user"
  member  = local.agents_sa
}

resource "google_service_account_iam_member" "operator_acts_as_agents" {
  service_account_id = google_service_account.sa["agents"].name
  role               = "roles/iam.serviceAccountUser"
  member             = "user:${var.operator_email}"
}

resource "google_cloud_run_v2_service_iam_member" "engine_invokes_diagnosis" {
  count    = var.agents_service_deployed ? 1 : 0
  project  = var.project_id
  location = "asia-south1"
  name     = "reconcile-diagnosis"
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.sa["engine"].email}"
}
