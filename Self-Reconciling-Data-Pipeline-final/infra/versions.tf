terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # POC: local state. For a shared environment, move state to a GCS bucket:
  # backend "gcs" { bucket = "<project>-tfstate" prefix = "reconcile-poc" }
}

provider "google" {
  project = var.project_id
  region  = var.region

  # Needed for APIs (billing budgets) that require a quota project with user credentials.
  user_project_override = true
  billing_project       = var.project_id

  default_labels = {
    app        = "reconcile-poc"
    env        = var.env
    managed_by = "terraform"
  }
}
