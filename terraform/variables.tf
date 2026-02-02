variable "project_id" {
  type        = string
  description = "GCP Project ID"
}

variable "credentials_file" {
  type        = string
  description = "Path to service account JSON key"
  default     = ""
}