# Phase 4 — the recovery engine (sa-engine): reads the evidence, keeps its own incident log.
# It can read receipts, raw, audit and the gate's records, and can never write analytics.
resource "google_bigquery_dataset" "recovery" {
  project                    = var.project_id
  dataset_id                 = "recovery"
  location                   = var.region
  description                = "Recovery engine: incident log with gates, evidence (counts only) and actions"
  delete_contents_on_destroy = true
  labels                     = { purpose = "recovery-engine" }
  depends_on                 = [google_project_service.enabled]
}

resource "google_bigquery_dataset_iam_member" "engine_writes_recovery" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.recovery.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.sa["engine"].email}"
}

resource "google_bigquery_dataset_iam_member" "engine_reads" {
  for_each   = toset(["ops", "raw", "audit"])
  project    = var.project_id
  dataset_id = each.key
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.sa["engine"].email}"
}

resource "google_bigquery_dataset_iam_member" "engine_reads_publication" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.publication.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.sa["engine"].email}"
}

resource "google_project_iam_member" "engine_runs_queries" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.sa["engine"].email}"
}
