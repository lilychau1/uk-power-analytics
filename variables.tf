variable "credentials" {
  description = "My Credentials"
  default     = "./keys/my-creds.json"
}

variable "region" {
  description = "Region"
  default     = "europe-west2-a"
}

variable "location" {
  description = "Project Location"
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "uk_power_analytics"
}

variable "gcs_bucket_class" {
  description = "My Storage Bucket Name"
  default     = "uk-power-analytics"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "vm_instance" {
  description = "Name of VM Instance"
  default     = "uk-power-analytics-vm"
}

variable "machine_type" {
  description = "VM machine type"
  default     = "e2-standard-4"
}