output "project_id" { value = var.project_id }
output "region" { value = var.region }
output "archive_bucket" { value = google_storage_bucket.archive.name }
output "artifact_repo" { value = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.images.repository_id}" }
output "network" { value = google_compute_network.vpc.name }
output "subnet" { value = google_compute_subnetwork.main.name }
output "bigquery_max_bytes_billed" { value = var.bigquery_max_bytes_billed }

output "service_accounts" {
  value = merge(
    { for k, sa in google_service_account.sa : k => sa.email },
    { build = google_service_account.build.email },
  )
}

output "cloudsql_private_ip" {
  value = var.enable_cloud_sql ? google_sql_database_instance.state[0].private_ip_address : null
}

output "cloudsql_connection_name" {
  value = var.enable_cloud_sql ? google_sql_database_instance.state[0].connection_name : null
}

output "db_password_secret" {
  value = var.enable_cloud_sql ? google_secret_manager_secret.db_password[0].secret_id : null
}

# Paste-ready block for the repo's .env (make env writes it for you).
output "dotenv" {
  value = <<-EOT
    GCP_PROJECT=${var.project_id}
    GCP_REGION=${var.region}
    ARCHIVE_BUCKET=${google_storage_bucket.archive.name}
    ARTIFACT_REPO=${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.images.repository_id}
    VPC_NETWORK=${google_compute_network.vpc.name}
    VPC_SUBNET=${google_compute_subnetwork.main.name}
    SA_INGEST=${google_service_account.sa["ingest"].email}
    SA_DBT=${google_service_account.sa["dbt"].email}
    SA_PUBLISH=${google_service_account.sa["publish"].email}
    SA_API=${google_service_account.sa["api"].email}
    SA_VALUERECON=${google_service_account.sa["valuerecon"].email}
    KMS_RECON_KEY=gcp-kms://${google_kms_crypto_key.recon_digest.id}
    SA_BUILD=${google_service_account.build.email}
    DB_HOST=${var.enable_cloud_sql ? google_sql_database_instance.state[0].private_ip_address : ""}
    DB_PASSWORD_SECRET=${var.enable_cloud_sql ? google_secret_manager_secret.db_password[0].secret_id : ""}
    BQ_MAX_BYTES_BILLED=${var.bigquery_max_bytes_billed}
  EOT
}
