# One service account per component. Permissions enforce the design even if code is wrong:
# only `publish` can write analytics; only `ingest` can write raw, the archive and control totals.
#
#                raw     audit   analytics quarantine ops          evidence  archive
#   ingest       Write   -       -         -          3 tables (W) -         create+read
#   dbt          Read    Write   -         Write      Read         -         -
#   valuerecon   Read    Read    -         -          -            Write     -        (+KMS in Phase 2)
#   publish      -       Read    Write     -          Read         -         -
#   engine       -       Read    -         -          Write        -         -
#   executor     Read    Write   -         Write      Read         -         -
#   agents       -       -       -         -          -            -         -        (masked views, Phase 5)
#   api          -       -       Read      -          Read         -         metadata

locals {
  components = {
    ingest     = "Ingest: extract, control totals, archive, raw load"
    dbt        = "dbt build into audit"
    valuerecon = "Value-level reconciliation (only identity with the recon key, Phase 2)"
    publish    = "Publish: clone audit to analytics"
    engine     = "Decision engine"
    executor   = "Recovery executor (catalog actions)"
    agents     = "Advisory AI agents (read masked views only)"
    api        = "Console API (Cloud Run)"
  }

  dataset_grants = [
    { sa = "ingest", ds = "raw", role = "roles/bigquery.dataEditor" },
    { sa = "dbt", ds = "raw", role = "roles/bigquery.dataViewer" },
    { sa = "dbt", ds = "ops", role = "roles/bigquery.dataViewer" },
    { sa = "dbt", ds = "audit", role = "roles/bigquery.dataEditor" },
    { sa = "dbt", ds = "quarantine", role = "roles/bigquery.dataEditor" },
    { sa = "valuerecon", ds = "raw", role = "roles/bigquery.dataViewer" },
    { sa = "valuerecon", ds = "audit", role = "roles/bigquery.dataViewer" },
    { sa = "valuerecon", ds = "evidence_restricted", role = "roles/bigquery.dataEditor" },
    { sa = "publish", ds = "audit", role = "roles/bigquery.dataViewer" },
    { sa = "publish", ds = "ops", role = "roles/bigquery.dataViewer" },
    { sa = "publish", ds = "analytics", role = "roles/bigquery.dataEditor" },
    { sa = "engine", ds = "audit", role = "roles/bigquery.dataViewer" },
    { sa = "engine", ds = "ops", role = "roles/bigquery.dataEditor" },
    { sa = "executor", ds = "raw", role = "roles/bigquery.dataViewer" },
    { sa = "executor", ds = "ops", role = "roles/bigquery.dataViewer" },
    { sa = "executor", ds = "audit", role = "roles/bigquery.dataEditor" },
    { sa = "executor", ds = "quarantine", role = "roles/bigquery.dataEditor" },
    { sa = "api", ds = "analytics", role = "roles/bigquery.dataViewer" },
    { sa = "api", ds = "ops", role = "roles/bigquery.dataViewer" },
  ]

  # Everything that runs queries needs jobUser at project level (jobs are project resources).
  job_users = ["ingest", "dbt", "valuerecon", "publish", "engine", "executor", "api"]
}

resource "google_service_account" "sa" {
  for_each     = local.components
  account_id   = "sa-${each.key}"
  display_name = "Reconcile: ${each.value}"
  depends_on   = [google_project_service.enabled]
}

resource "google_bigquery_dataset_iam_member" "grants" {
  for_each   = { for g in local.dataset_grants : "${g.sa}-${g.ds}" => g }
  dataset_id = google_bigquery_dataset.ds[each.value.ds].dataset_id
  role       = each.value.role
  member     = "serviceAccount:${google_service_account.sa[each.value.sa].email}"
}

resource "google_project_iam_member" "job_user" {
  for_each = toset(local.job_users)
  project  = var.project_id
  role     = "roles/bigquery.jobUser"
  member   = "serviceAccount:${google_service_account.sa[each.value].email}"
}

# Ingest writes three ops tables only, not the whole ops dataset.
resource "google_bigquery_table_iam_member" "ingest_ops" {
  for_each   = local.ingest_ops_tables
  dataset_id = google_bigquery_dataset.ds["ops"].dataset_id
  table_id   = each.value
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.sa["ingest"].email}"
}

# ---------------------------------------------------------------- archive bucket
resource "google_storage_bucket_iam_member" "ingest_archive" {
  for_each = toset(["roles/storage.objectCreator", "roles/storage.objectViewer"])
  bucket   = google_storage_bucket.archive.name
  role     = each.value
  member   = "serviceAccount:${google_service_account.sa["ingest"].email}"
}

resource "google_storage_bucket_iam_member" "api_archive_meta" {
  bucket = google_storage_bucket.archive.name
  role   = "roles/storage.legacyBucketReader" # bucket metadata + object names, no object contents
  member = "serviceAccount:${google_service_account.sa["api"].email}"
}

# ---------------------------------------------------------------- Pub/Sub
resource "google_pubsub_topic_iam_member" "ingest_run_complete" {
  topic  = google_pubsub_topic.main["run-complete"].name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.sa["ingest"].email}"
}

resource "google_pubsub_topic_iam_member" "api_engine_requests" {
  topic  = google_pubsub_topic.main["engine-requests"].name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.sa["api"].email}"
}

resource "google_pubsub_subscription_iam_member" "engine_consumes" {
  for_each     = toset(local.topics)
  subscription = google_pubsub_subscription.engine[each.value].name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${google_service_account.sa["engine"].email}"
}

resource "google_pubsub_topic_iam_member" "engine_notifications" {
  topic  = google_pubsub_topic.main["notifications"].name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.sa["engine"].email}"
}

# ---------------------------------------------------------------- console API: read-only views of the environment
resource "google_project_iam_member" "api_viewers" {
  for_each = toset([
    "roles/bigquery.metadataViewer", # dataset ACLs for the identity matrix
    "roles/pubsub.viewer",
    "roles/run.viewer",
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.sa["api"].email}"
}

resource "google_secret_manager_secret_iam_member" "api_db_password" {
  count     = var.enable_cloud_sql ? 1 : 0
  secret_id = google_secret_manager_secret.db_password[0].id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.sa["api"].email}"
}

# ---------------------------------------------------------------- operator: impersonation instead of key files
resource "google_service_account_iam_member" "operator_impersonates" {
  for_each           = local.components
  service_account_id = google_service_account.sa[each.key].name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "user:${var.operator_email}"
}

resource "google_service_account_iam_member" "operator_acts_as_api" {
  service_account_id = google_service_account.sa["api"].name
  role               = "roles/iam.serviceAccountUser"
  member             = "user:${var.operator_email}"
}
