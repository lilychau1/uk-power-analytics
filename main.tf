terraform {
  required_version = ">= 1.0"
  backend "local" {} # Can change from "local" to "gcs" (for google) or "s3" (for aws), if you would like to preserve your tf-state online
  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}

locals {
  envs = { for tuple in regexall("(.*)=(.*)", file(".env")) : tuple[0] => sensitive(tuple[1]) }
}

provider "google" {
  credentials = file(var.credentials)
  project     = local.envs["GCP_PROJECT_ID"]
  region      = var.region
  // credentials = file(var.credentials)  # Use this if you do not want to set env-var GOOGLE_APPLICATION_CREDENTIALS
}

resource "google_service_account" "airflow_service_account" {
  account_id   = var.airflow_service_account_id
  display_name = "Service Account for Airflow"
  project = local.envs["GCP_PROJECT_ID"]
}

resource "google_project_iam_member" "airflow_sa_bigquery_role" {
  project = local.envs["GCP_PROJECT_ID"]
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.airflow_service_account.email}"
}

resource "google_project_iam_member" "airflow_sa_storage_role" {
  project = local.envs["GCP_PROJECT_ID"]
  role    = "roles/storage.objectCreator"
  member  = "serviceAccount:${google_service_account.airflow_service_account.email}"
}

resource "google_project_iam_binding" "airflow_sa_object_viewer_role" {
  project = local.envs["GCP_PROJECT_ID"]
  role    = "roles/storage.objectViewer"

  members = [
    "serviceAccount:${google_service_account.airflow_service_account.email}",
  ]
}
resource "google_project_iam_binding" "airflow_sa_storage_insights_collector_role" {
  project = local.envs["GCP_PROJECT_ID"]
  role    = "roles/storage.insightsCollectorService"

  members = [
    "serviceAccount:${google_service_account.airflow_service_account.email}",
  ]
}
resource "google_project_iam_member" "airflow_sa_grant_iam_service_account_key_creator" {
  project = local.envs["GCP_PROJECT_ID"]

  role   = "roles/iam.serviceAccountKeyAdmin"
  member = "serviceAccount:${google_service_account.airflow_service_account.email}"
}

resource "google_compute_instance" "uk_power_analytics_vm" {
  name         = var.vm_instance
  machine_type = var.machine_type
  zone         = var.region

  service_account {
    email  = "${var.airflow_service_account_id}@${local.envs["GCP_PROJECT_ID"]}.iam.gserviceaccount.com"
    scopes = [
      "userinfo-email",
      "compute-ro",
      "storage-full",
      "https://www.googleapis.com/auth/iam",
      "https://www.googleapis.com/auth/cloud-platform"
    ]
  }

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-lts"  # Ubuntu 22.04 LTS image
    }
  }

  network_interface {
    network = "default"

    access_config {
      // Ephemeral public IP
    }
  }

  metadata = {
    GCP_PROJECT_ID = local.envs["GCP_PROJECT_ID"]
    AIRFLOW_UID = 501
    _PIP_ADDITIONAL_REQUIREMENTS = ""
  }
  
  metadata_startup_script = templatefile("./start_up_script.sh", {
    GCS_BUCKET_SUFFIX            = var.gcs_bucket_class
    AIRFLOW_SERVICE_ACCOUNT_ID   = var.airflow_service_account_id
  })
}


# Data Lake Bucket
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "uk-power-analytics-bucket" {
  name          = "${local.envs["GCP_PROJECT_ID"]}-${var.gcs_bucket_class}" # Concatenating DL bucket & Project name for unique naming
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1 // days
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }

}

resource "google_bigquery_dataset" "uk_power_analytics_dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}
