# locals aka constants:
locals {
  data_lake_bucket = "ttc_data_lake"
}

# passed at run-time, variables without the `default` prop are mandatory args (ie. will be asked by the CLI)
variable "project" {
  description = "Your GCP Project ID" 
  default = "ttc-data-analytics"
}

variable "credentials" {
  description = "json file created for service account key"
  default = "~/.google/credentials/ttc-data-analytics-key.json"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "us-east4"
  type = string
}

variable "zone" {
  description = "Your project zone"
  default     = "us-east4-a"
  type        = string
}

variable "vm_image" {
  description = "Image for you VM"
  default     = "ubuntu-os-cloud/ubuntu-2004-lts"
  type        = string
}

variable "network" {
  description = "Network for your instance/cluster"
  default     = "default"
  type        = string
}

# Not needed for now
# variable "bucket_name" {
#   description = "The name of the GCS bucket. Must be globally unique."
#   default = ""
# }

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "ttc_delays_data"
}

# NOTE: airflow can also create tables using BigQuery operators, so we leave that to it
# destroying a dataset will also remove all the tables so we don't have to worry about that