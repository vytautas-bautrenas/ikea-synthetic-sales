terraform {
  required_version = ">= 1.3"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.0"
    }
  }
}

provider "google" {
  project     = var.project_id
  region      = var.location
  credentials = file(var.credentials)
}


# Service Account

resource "google_service_account" "pipeline_sa" {
  account_id   = "${var.project_id}-sa"
  display_name = "${var.project_id} service account"
}

resource "google_project_iam_member" "sa_bq_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_service_account_key" "pipeline_sa_key" {
  service_account_id = google_service_account.pipeline_sa.name
}

resource "local_file" "gcp_sa_key_file" {
  content  = base64decode(google_service_account_key.pipeline_sa_key.private_key)
  filename = "${path.module}/../secrets/gcp-sa-key.json"
}


# Datasets

resource "google_bigquery_dataset" "ingest" {
  dataset_id                 = "ingest"
  location                   = var.location
  description                = "Raw ingested source data landing tables"
  delete_contents_on_destroy = true

  labels = {
    project = var.project_id
    env     = var.environment
  }
}

resource "google_bigquery_dataset" "transform" {
  dataset_id                 = "transform"
  location                   = var.location
  description                = "Intermediate dbt transformation models"
  delete_contents_on_destroy = true

  labels = {
    project = var.project_id
    env     = var.environment
  }
}

resource "google_bigquery_dataset" "serve" {
  dataset_id                 = "serve"
  location                   = var.location
  description                = "Final dimensional models, facts, and aggregate mart tables"
  delete_contents_on_destroy = true

  labels = {
    project = var.project_id
    env     = var.environment
  }
}

resource "google_bigquery_dataset" "audit" {
  dataset_id                 = "audit"
  location                   = var.location
  description                = "Audit models and input-vs-output validation metrics"
  delete_contents_on_destroy = true

  labels = {
    project = var.project_id
    env     = var.environment
  }
}


# Printout

output "pipeline_sa_email" {
  description = "Pipeline service account email"
  value       = google_service_account.pipeline_sa.email
}

output "sa_key_b64" {
  description = "Pipeline service account key"
  value       = google_service_account_key.pipeline_sa_key.private_key
  sensitive   = true
}
