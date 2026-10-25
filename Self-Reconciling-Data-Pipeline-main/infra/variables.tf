variable "project_id" {
  description = "GCP project that hosts the POC (data, control plane and agents share it on a free-trial account)."
  type        = string
}

variable "region" {
  description = "Region for every resource. asia-south1 (Mumbai) keeps data in India."
  type        = string
  default     = "asia-south1"
}

variable "env" {
  description = "Environment label."
  type        = string
  default     = "test"
}

variable "operator_email" {
  description = "Your Google account. Gets permission to impersonate the component service accounts (no key files)."
  type        = string
}

# ---------------------------------------------------------------- raw archive
variable "archive_retention_days" {
  description = "Objects in the raw archive cannot be deleted or overwritten for this many days."
  type        = number
  default     = 7
}

variable "lock_archive_retention" {
  description = <<-EOT
    Bucket Lock. IRREVERSIBLE: once true and applied, the retention period can never be
    reduced or removed, and the bucket cannot be deleted until every object has aged out.
    Leave false for the trial; see docs/RUNBOOK.md before setting it.
  EOT
  type        = bool
  default     = false
}

# ---------------------------------------------------------------- ops state
variable "enable_cloud_sql" {
  description = "Create the Cloud SQL ops-state instance (about $10/month at db-f1-micro)."
  type        = bool
  default     = true
}

variable "cloudsql_tier" {
  description = "Cloud SQL machine tier. db-f1-micro is shared-core and cheapest."
  type        = string
  default     = "db-f1-micro"
}

variable "deletion_protection" {
  description = "Protect Cloud SQL and BigQuery from terraform destroy. False keeps POC teardown easy."
  type        = bool
  default     = false
}

# ---------------------------------------------------------------- cost guard
variable "billing_account_id" {
  description = "Optional. When set, a budget with email alerts at 50/90/100% is created. Format: 000000-000000-000000."
  type        = string
  default     = ""
}

variable "budget_amount" {
  description = "Monthly budget in your billing account's currency (INR for most Indian accounts)."
  type        = number
  default     = 2000
}

variable "bigquery_max_bytes_billed" {
  description = "Per-query cap used by ingest and the API (bytes). Exposed as an output for the apps."
  type        = number
  default     = 1073741824 # 1 GiB
}
