resource "google_artifact_registry_repository" "images" {
  location      = var.region
  repository_id = "reconcile"
  format        = "DOCKER"
  description   = "Container images for the console/API, engine and jobs."
  depends_on    = [google_project_service.enabled]
}

# Dedicated identity for Cloud Build, so builds never run as the broad default compute account.
resource "google_service_account" "build" {
  account_id   = "sa-build"
  display_name = "Reconcile: Cloud Build"
}

resource "google_project_iam_member" "build" {
  for_each = toset([
    "roles/logging.logWriter",
    "roles/storage.objectViewer", # read the uploaded source archive
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.build.email}"
}

resource "google_artifact_registry_repository_iam_member" "build_writer" {
  location   = google_artifact_registry_repository.images.location
  repository = google_artifact_registry_repository.images.name
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${google_service_account.build.email}"
}

resource "google_service_account_iam_member" "operator_uses_build" {
  service_account_id = google_service_account.build.name
  role               = "roles/iam.serviceAccountUser"
  member             = "user:${var.operator_email}"
}
