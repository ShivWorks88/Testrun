# Phase 3 — a dataset only the publish identity writes: gate decisions, check results, snapshots.
# Kept separate from `ops` so sa-publish can never edit ingest receipts or batch records.
resource "google_bigquery_dataset" "publication" {
  project                    = var.project_id
  dataset_id                 = "publication"
  location                   = var.region
  description                = "Publication gate: decisions, check results and pre-publish snapshots (written by sa-publish only)"
  delete_contents_on_destroy = true
  labels                     = { purpose = "publication-gate" }
  depends_on                 = [google_project_service.enabled]
}

resource "google_bigquery_dataset_iam_member" "publish_writes_publication" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.publication.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.sa["publish"].email}"
}
