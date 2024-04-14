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

resource "google_compute_firewall" "allow-ssh" {
  name    = "allow-ssh"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["0.0.0.0/0"]  # Adjust as needed to restrict access
}

resource "google_compute_instance" "uk_power_analytics_vm" {
  name         = var.vm_instance
  machine_type = var.machine_type
  zone         = var.region
  # tags         = google_compute_firewall.allow_ssh.target_tags

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
    _AIRFLOW_WWW_USER_USERNAME = "airflow"
    _AIRFLOW_WWW_USER_PASSWORD = "airflow"
  }
  
  metadata_startup_script = "${file("./start_up_script.sh")}"

}


# Data Lake Bucket
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "terraform-test-bucket" {
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

resource "google_bigquery_dataset" "power_dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}
