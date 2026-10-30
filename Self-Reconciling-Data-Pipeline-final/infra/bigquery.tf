# Six warehouse datasets (architecture v4):
#   raw                  written by ingest only; mirrors the immutable archive
#   audit                dbt builds here first (Write-Audit-Publish)
#   analytics            written by the publish identity only
#   quarantine           rows moved out of marts
#   ops                  control totals, batches, source health (no clinical values)
#   evidence_restricted  mismatch keys and field names only

locals {
  datasets = {
    raw                 = "Raw extracts loaded by ingest. Mirrors the immutable archive."
    audit               = "dbt builds and tests here before anything is published."
    analytics           = "Verified data only. Written by the publish service account."
    quarantine          = "Rows moved out of marts by the quarantine_rows action."
    ops                 = "Pipeline metadata: control totals, batches, source health."
    evidence_restricted = "Reconciliation evidence: record keys and field names only."
  }
}

resource "google_bigquery_dataset" "ds" {
  for_each                   = local.datasets
  dataset_id                 = each.key
  description                = each.value
  location                   = var.region
  delete_contents_on_destroy = !var.deletion_protection
  depends_on                 = [google_project_service.enabled]
}

# ---------------------------------------------------------------- ops tables

resource "google_bigquery_table" "control_totals" {
  dataset_id          = google_bigquery_dataset.ds["ops"].dataset_id
  table_id            = "control_totals"
  description         = "Totals each extract computes about itself, in the same snapshot as its rows. Exactly one row per dataset, business date and batch."
  deletion_protection = var.deletion_protection

  time_partitioning {
    type  = "DAY"
    field = "business_date"
  }
  clustering = ["dataset"]

  schema = jsonencode([
    { name = "dataset", type = "STRING", mode = "REQUIRED" },
    { name = "source_system", type = "STRING", mode = "REQUIRED" },
    { name = "business_date", type = "DATE", mode = "REQUIRED" },
    { name = "batch_id", type = "STRING", mode = "REQUIRED" },
    { name = "row_count", type = "INT64", mode = "REQUIRED" },
    { name = "distinct_keys", type = "INT64", mode = "REQUIRED" },
    { name = "null_amount_count", type = "INT64", mode = "NULLABLE", description = "Rows where the amount (or critical) column is null" },
    { name = "amount_total", type = "NUMERIC", mode = "NULLABLE" },
    { name = "key_column", type = "STRING", mode = "REQUIRED" },
    { name = "amount_column", type = "STRING", mode = "NULLABLE" },
    { name = "snapshot_ts", type = "TIMESTAMP", mode = "REQUIRED", description = "Source transaction timestamp of the repeatable-read snapshot" },
    { name = "extracted_at", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "archive_uri", type = "STRING", mode = "REQUIRED" },
    { name = "extractor_version", type = "STRING", mode = "REQUIRED" },
  ])
}

resource "google_bigquery_table" "batches" {
  dataset_id          = google_bigquery_dataset.ds["ops"].dataset_id
  table_id            = "batches"
  description         = "Append-only log of every extract and replay outcome."
  deletion_protection = var.deletion_protection

  time_partitioning {
    type  = "DAY"
    field = "business_date"
  }
  clustering = ["dataset", "batch_id"]

  schema = jsonencode([
    { name = "batch_id", type = "STRING", mode = "REQUIRED" },
    { name = "dataset", type = "STRING", mode = "REQUIRED" },
    { name = "source_system", type = "STRING", mode = "REQUIRED" },
    { name = "business_date", type = "DATE", mode = "REQUIRED" },
    { name = "mode", type = "STRING", mode = "REQUIRED", description = "extract | replay" },
    { name = "status", type = "STRING", mode = "REQUIRED", description = "LOADED | FAILED" },
    { name = "rows_extracted", type = "INT64", mode = "NULLABLE" },
    { name = "rows_loaded", type = "INT64", mode = "NULLABLE" },
    { name = "control_totals_written", type = "BOOL", mode = "NULLABLE" },
    { name = "archive_uri", type = "STRING", mode = "NULLABLE" },
    { name = "injections", type = "STRING", mode = "REPEATED" },
    { name = "started_at", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "finished_at", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "error", type = "STRING", mode = "NULLABLE" },
    { name = "orchestrator_run_id", type = "STRING", mode = "NULLABLE" },
  ])
}

resource "google_bigquery_table" "source_health" {
  dataset_id          = google_bigquery_dataset.ds["ops"].dataset_id
  table_id            = "source_health"
  description         = "Source probes written at the start of every ingest run."
  deletion_protection = var.deletion_protection

  time_partitioning {
    type  = "DAY"
    field = "probed_at"
  }
  clustering = ["source_system"]

  schema = jsonencode([
    { name = "source_system", type = "STRING", mode = "REQUIRED" },
    { name = "probed_at", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "reachable", type = "BOOL", mode = "REQUIRED" },
    { name = "latency_ms", type = "FLOAT64", mode = "NULLABLE" },
    { name = "replica_lag_seconds", type = "FLOAT64", mode = "NULLABLE" },
    { name = "archive_cutoff_date", type = "DATE", mode = "NULLABLE" },
    { name = "error", type = "STRING", mode = "NULLABLE" },
  ])
}

locals {
  ingest_ops_tables = {
    control_totals = google_bigquery_table.control_totals.table_id
    batches        = google_bigquery_table.batches.table_id
    source_health  = google_bigquery_table.source_health.table_id
  }
}
