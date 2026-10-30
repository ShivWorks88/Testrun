# Ops state for the engine and console (incidents, publication state, locks, journal).
# Phase 0 creates it; Phase 2+ fills it. Private IP only.

resource "random_password" "app" {
  count   = var.enable_cloud_sql ? 1 : 0
  length  = 32
  special = false
}

resource "google_sql_database_instance" "state" {
  count               = var.enable_cloud_sql ? 1 : 0
  name                = "reconcile-state"
  region              = var.region
  database_version    = "POSTGRES_16"
  deletion_protection = var.deletion_protection

  settings {
    edition           = "ENTERPRISE" # PG16 defaults to Enterprise Plus, which has no shared-core tiers
    tier              = var.cloudsql_tier
    availability_type = "ZONAL" # HA ("REGIONAL") doubles the cost; enable it after the POC
    disk_size         = 10
    disk_autoresize   = true

    deletion_protection_enabled = var.deletion_protection

    ip_configuration {
      ipv4_enabled    = false
      private_network = google_compute_network.vpc.id
      ssl_mode        = "ENCRYPTED_ONLY"
    }

    backup_configuration {
      enabled                        = true
      point_in_time_recovery_enabled = false
    }

    user_labels = {
      app = "reconcile-poc"
      env = var.env
    }
  }

  depends_on = [google_service_networking_connection.psa]
}

resource "google_sql_database" "ops" {
  count    = var.enable_cloud_sql ? 1 : 0
  name     = "reconcile_ops"
  instance = google_sql_database_instance.state[0].name
}

resource "google_sql_user" "app" {
  count    = var.enable_cloud_sql ? 1 : 0
  name     = "reconcile_app"
  instance = google_sql_database_instance.state[0].name
  password = random_password.app[0].result
}

resource "google_secret_manager_secret" "db_password" {
  count     = var.enable_cloud_sql ? 1 : 0
  secret_id = "reconcile-db-password"
  replication {
    user_managed {
      replicas {
        location = var.region
      }
    }
  }
  depends_on = [google_project_service.enabled]
}

resource "google_secret_manager_secret_version" "db_password" {
  count       = var.enable_cloud_sql ? 1 : 0
  secret      = google_secret_manager_secret.db_password[0].id
  secret_data = random_password.app[0].result
}
