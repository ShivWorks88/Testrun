# Phase 5 stage 3 — infrastructure for the real Diagnosis / Communication agents.
#
# Built now, safe now:
#   - Vertex AI and Model Armor APIs switched on (no cost until something is called)
#   - dataset agent_context with MASKED views: counts, check names and statuses only — no patient ids,
#     no clinical values, no batch ids, no evidence_restricted
#   - sa-agents may read agent_context only (it is NOT given raw, audit, ops, recovery or evidence_restricted)
# Built now, OFF until the written confirmation exists (the Terraform half of the residency gate):
#   - permission for sa-agents to call models (roles/aiplatform.user) is created only when
#     var.agents_residency_ref is set, e.g. "SEC-2026-114 (India processing, BAA, no training)".
#     Set it in terraform.tfvars (never committed) only after Security confirms in writing.

variable "agents_residency_ref" {
  description = "Reference of the written residency/BAA/no-training confirmation. Empty = agents cannot call models."
  type        = string
  default     = ""
}

locals {
  agents_model_access = trimspace(var.agents_residency_ref) != ""
  agents_sa           = "serviceAccount:${google_service_account.sa["agents"].email}"
}

resource "google_project_service" "agents_apis" {
  for_each           = toset(["aiplatform.googleapis.com", "modelarmor.googleapis.com"])
  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}

# ── masked context the agents may read ───────────────────────────────────────
resource "google_bigquery_dataset" "agent_context" {
  project                    = var.project_id
  dataset_id                 = "agent_context"
  location                   = var.region
  description                = "Masked, counts-only views for the Phase 5 agents. No identifiers, no clinical values."
  delete_contents_on_destroy = true
  labels                     = { purpose = "agent-context" }
}

resource "google_bigquery_table" "incident_context" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.agent_context.dataset_id
  table_id            = "incident_context"
  deletion_protection = false
  description         = "Latest state of each incident with counts only (no notes, no batch ids, no values)."
  view {
    use_legacy_sql = false
    query          = <<-SQL
      select incident_id, business_date, dataset, failure_type, status, attempt, rule_id, severity, route,
             policy_version, `at` as updated_at,
             safe_cast(json_value(evidence, '$.raw_rows')      as int64) as raw_rows,
             safe_cast(json_value(evidence, '$.receipt_rows')  as int64) as receipt_rows,
             safe_cast(json_value(evidence, '$.raw_keys')      as int64) as raw_keys,
             safe_cast(json_value(evidence, '$.receipt_keys')  as int64) as receipt_keys,
             safe_cast(json_value(evidence, '$.missing_rows')  as int64) as missing_rows,
             safe_cast(json_value(evidence, '$.missing_keys')  as int64) as missing_keys,
             safe_cast(json_value(evidence, '$.failures')      as int64) as failing_rows
      from `${var.project_id}.recovery.incidents`
      qualify row_number() over (partition by incident_id order by `at` desc) = 1
    SQL
  }
  depends_on = [google_bigquery_dataset.recovery]
}

resource "google_bigquery_table" "check_context" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.agent_context.dataset_id
  table_id            = "check_context"
  deletion_protection = false
  description         = "dbt check outcomes per gate run: names, statuses and failing-row counts only."
  view {
    use_legacy_sql = false
    query          = <<-SQL
      select gate_run_id, business_date, resource_type, name, status, failures, policy_outcome, attached_to,
             recorded_at
      from `${var.project_id}.publication.check_results`
    SQL
  }
  depends_on = [google_bigquery_dataset.publication]
}

# The views may read their source tables on the agents' behalf ("authorized views"),
# so sa-agents never needs access to recovery or publication themselves.
resource "google_bigquery_dataset_access" "authorize_incident_context" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.recovery.dataset_id
  view {
    project_id = var.project_id
    dataset_id = google_bigquery_dataset.agent_context.dataset_id
    table_id   = google_bigquery_table.incident_context.table_id
  }
}

resource "google_bigquery_dataset_access" "authorize_check_context" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.publication.dataset_id
  view {
    project_id = var.project_id
    dataset_id = google_bigquery_dataset.agent_context.dataset_id
    table_id   = google_bigquery_table.check_context.table_id
  }
}

resource "google_bigquery_dataset_iam_member" "agents_read_context" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.agent_context.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = local.agents_sa
}

resource "google_project_iam_member" "agents_run_queries" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = local.agents_sa
}

# ── model access: OFF until the written confirmation is referenced ──────────
resource "google_project_iam_member" "agents_call_models" {
  count   = local.agents_model_access ? 1 : 0
  project = var.project_id
  role    = "roles/aiplatform.user"
  member  = local.agents_sa
}

output "agents_model_access" {
  value = local.agents_model_access ? "ON (${var.agents_residency_ref})" : "OFF (no residency reference)"
}
