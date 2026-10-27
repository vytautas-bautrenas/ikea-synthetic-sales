variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "location" {
  description = "GCP location for GCS and BigQuery"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "prod"
}

variable "credentials" {
  description = "Path to Terraform service account key JSON"
  type        = string
}