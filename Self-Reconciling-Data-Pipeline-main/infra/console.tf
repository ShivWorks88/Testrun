# Console screens for Phases 2–4: sa-api may READ the gate's records and the incident journal.
# (It already reads ops; it never reads raw or evidence_restricted, and writes nothing here.)
resource "google_bigquery_dataset_iam_member" "api_reads_gate_and_incidents" {
  for_each   = toset(["publication", "recovery"])
  project    = var.project_id
  dataset_id = each.key
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.sa["api"].email}"
  depends_on = [google_bigquery_dataset.publication, google_bigquery_dataset.recovery]
}
