# Private network: Cloud SQL has no public IP; Cloud Run reaches it through Direct VPC egress.

resource "google_compute_network" "vpc" {
  name                    = "reconcile-vpc"
  auto_create_subnetworks = false
  depends_on              = [google_project_service.enabled]
}

resource "google_compute_subnetwork" "main" {
  name                     = "reconcile-${var.region}"
  ip_cidr_range            = "10.10.0.0/24"
  region                   = var.region
  network                  = google_compute_network.vpc.id
  private_ip_google_access = true
}

# Private services access range for Cloud SQL.
resource "google_compute_global_address" "psa" {
  name          = "reconcile-psa-range"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 20
  network       = google_compute_network.vpc.id
}

resource "google_service_networking_connection" "psa" {
  network                 = google_compute_network.vpc.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.psa.name]
}
