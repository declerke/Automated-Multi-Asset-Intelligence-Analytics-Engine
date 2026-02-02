output "raw_bucket_name" {
  value = google_storage_bucket.raw.name
}

output "processed_bucket_name" {
  value = google_storage_bucket.processed.name
}

output "bigquery_dataset_id" {
  value = google_bigquery_dataset.crypto.dataset_id
}
