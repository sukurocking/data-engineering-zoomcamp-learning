locals {
  data_lake_bucket = "dtc-data-lake"
}

variable "project" {
  description = "Your GCP Project ID"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location"
  default = "asia-south1"
  type = string
}

# Not needed for now
variable "bucket_name" {
  description = "The name of the Google Cloud Storage Bucket. Must be globally unique"
  default = ""
  type = string
}

variable "storage_class" {
  description = "Storage class of the bucket"
  type = string
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "trips_data_all"
}

variable "TABLE_NAME" {
  description = "BigQuery Table"
  type = string
  default = "ny_trips"
}