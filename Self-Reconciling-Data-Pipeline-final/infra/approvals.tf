# Phase 5 stage 1 — approvals.
# The engine (sa-engine) writes approval cards to recovery.approvals (it already edits the dataset).
# The console (sa-api) may write ONE thing: a decision row in recovery.approval_decisions.
# It gets dataEditor on that single table only — it still cannot touch incidents, cards or analytics,
# and it never runs anything: the approval_worker DAG re-checks the decision and acts as sa-engine/sa-ingest.
resource "google_bigquery_table" "approval_decisions" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.recovery.dataset_id
  table_id            = "approval_decisions"
  description         = "Data-owner decisions on engine approval cards (append-only; first decision wins)"
  deletion_protection = false
  labels              = { purpose = "approvals" }

  schema = jsonencode([
    { name = "decision_id", type = "STRING", mode = "REQUIRED" },
    { name = "approval_id", type = "STRING", mode = "REQUIRED" },
    { name = "decision", type = "STRING", mode = "REQUIRED", description = "APPROVE or REJECT" },
    { name = "fingerprint", type = "STRING", mode = "REQUIRED", description = "fingerprint of the card the person saw" },
    { name = "reason", type = "STRING", mode = "NULLABLE" },
    { name = "decided_by", type = "STRING", mode = "REQUIRED", description = "authenticated email" },
    { name = "roles", type = "STRING", mode = "NULLABLE" },
    { name = "decided_at", type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}

resource "google_bigquery_table_iam_member" "api_writes_decisions" {
  project    = var.project_id
  dataset_id = google_bigquery_table.approval_decisions.dataset_id
  table_id   = google_bigquery_table.approval_decisions.table_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.sa["api"].email}"
}
