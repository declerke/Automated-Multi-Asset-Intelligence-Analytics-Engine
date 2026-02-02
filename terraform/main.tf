terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project     = var.project_id
  region      = "europe-west4"  
  credentials = var.credentials_file != "" ? file(var.credentials_file) : null
}

resource "google_storage_bucket" "raw" {
  name                        = "${var.project_id}-crypto-raw"
  location                    = "EU"         
  force_destroy               = true
  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "processed" {
  name                        = "${var.project_id}-crypto-processed"
  location                    = "EU"
  force_destroy               = true
  uniform_bucket_level_access = true
}

resource "google_bigquery_dataset" "crypto" {
  dataset_id                 = "crypto_market"
  location                   = "EU"
  delete_contents_on_destroy = true
}