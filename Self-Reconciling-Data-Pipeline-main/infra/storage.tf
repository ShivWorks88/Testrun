# Immutable raw archive: every extract is kept here so a defect found later can be
# fixed by replaying archived files, even after the source has archived the records.

resource "google_storage_bucket" "archive" {
  name                        = "${var.project_id}-reconcile-raw-archive"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = !var.deletion_protection # only removes objects whose retention has expired

  retention_policy {
    retention_period = var.archive_retention_days * 86400
    is_locked        = var.lock_archive_retention
  }

  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition { age = 90 }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  depends_on = [google_project_service.enabled]
}
