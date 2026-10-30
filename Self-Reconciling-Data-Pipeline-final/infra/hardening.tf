# Phase 6 hardening: an audit-log sink and an uptime check on the one public endpoint.
# (The budget alert already exists in budget.tf from Phase 0.) Both are opt-in through terraform.tfvars, so a plain
# `make tf-plan` changes nothing until you set:
#   audit_log_sink_enabled   = true                     # admin + data-access logs → BigQuery dataset audit_logs
#   uptime_check_enabled     = true                     # Slack callback /healthz every 5 min, email when down

variable "uptime_check_enabled" {
  description = "Uptime check + email alert on the public Slack callback (needs slack_callback_deployed = true)"
  type        = bool
  default     = false
}

variable "audit_log_sink_enabled" {
  description = "Route Cloud Audit Logs (admin activity + BigQuery data access) to BigQuery dataset audit_logs"
  type        = bool
  default     = false
}

locals {
  uptime = var.uptime_check_enabled && var.slack_callback_deployed
}

resource "google_project_service" "hardening_apis" {
  for_each           = toset(compact([local.uptime ? "monitoring.googleapis.com" : ""]))
  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}

resource "google_bigquery_dataset" "audit_logs" {
  count                       = var.audit_log_sink_enabled ? 1 : 0
  project                     = var.project_id
  dataset_id                  = "audit_logs"
  location                    = "asia-south1"
  description                 = "Cloud Audit Logs (admin activity + BigQuery data access), written by a log sink"
  default_table_expiration_ms = 30 * 24 * 3600 * 1000
  delete_contents_on_destroy  = true
}

resource "google_logging_project_sink" "audit" {
  count                  = var.audit_log_sink_enabled ? 1 : 0
  project                = var.project_id
  name                   = "reconcile-audit-to-bq"
  destination            = "bigquery.googleapis.com/projects/${var.project_id}/datasets/audit_logs"
  filter                 = "logName:\"cloudaudit.googleapis.com\" AND (protoPayload.serviceName=\"bigquery.googleapis.com\" OR logName:\"activity\")"
  unique_writer_identity = true
  bigquery_options {
    use_partitioned_tables = true
  }
  depends_on = [google_bigquery_dataset.audit_logs]
}

resource "google_bigquery_dataset_iam_member" "audit_sink_writer" {
  count      = var.audit_log_sink_enabled ? 1 : 0
  project    = var.project_id
  dataset_id = "audit_logs"
  role       = "roles/bigquery.dataEditor"
  member     = google_logging_project_sink.audit[0].writer_identity
}

# The Slack callback is the only public endpoint: check it every 5 minutes from India-near regions and
# email the operator when it is down for 10 minutes.
data "google_cloud_run_v2_service" "slack_callback" {
  count    = local.uptime ? 1 : 0
  project  = var.project_id
  location = "asia-south1"
  name     = "reconcile-slack-callback"
}

resource "google_monitoring_uptime_check_config" "slack_callback" {
  count        = local.uptime ? 1 : 0
  project      = var.project_id
  display_name = "reconcile-slack-callback /healthz"
  timeout      = "10s"
  period       = "300s"
  http_check {
    path         = "/healthz"
    port         = 443
    use_ssl      = true
    validate_ssl = true
  }
  monitored_resource {
    type = "uptime_url"
    labels = {
      project_id = var.project_id
      host       = trimprefix(data.google_cloud_run_v2_service.slack_callback[0].uri, "https://")
    }
  }
  depends_on = [google_project_service.hardening_apis]
}

resource "google_monitoring_notification_channel" "operator_email" {
  count        = local.uptime ? 1 : 0
  project      = var.project_id
  display_name = "POC operator"
  type         = "email"
  labels       = { email_address = var.operator_email }
}

resource "google_monitoring_alert_policy" "slack_callback_down" {
  count        = local.uptime ? 1 : 0
  project      = var.project_id
  display_name = "Slack callback down"
  combiner     = "OR"
  conditions {
    display_name = "uptime check failing"
    condition_threshold {
      filter          = "metric.type=\"monitoring.googleapis.com/uptime_check/check_passed\" AND resource.type=\"uptime_url\" AND metric.label.check_id=\"${google_monitoring_uptime_check_config.slack_callback[0].uptime_check_id}\""
      comparison      = "COMPARISON_GT"
      threshold_value = 1
      duration        = "600s"
      aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_NEXT_OLDER"
        cross_series_reducer = "REDUCE_COUNT_FALSE"
        group_by_fields      = ["resource.label.host"]
      }
    }
  }
  notification_channels = [google_monitoring_notification_channel.operator_email[0].id]
}
