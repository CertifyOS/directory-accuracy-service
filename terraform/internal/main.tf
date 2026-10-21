# directory-accuracy-service, internal environment. Copied from file-ingestion-service's stack with
# its recorded lessons applied (docs/decisions.md names each). The project is hardcoded: gcloud's
# ambient project on an engineer's machine is production, and this stack must never land there.
provider "google" {
  project = local.google_project
}

terraform {
  required_version = ">= 1.9.0"

  backend "gcs" {
    bucket = "certifyos-tf-state-development"
    prefix = "directory-accuracy-internal/state"
  }

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.38.0"
    }
  }
}

locals {
  env            = var.env
  app_name       = "directory-accuracy-service"
  google_project = "certifyos-development"
  project_number = "106861691435"
  region         = "us-central1"
  hostname       = "directory-accuracy.${var.env}.certifyos.com"
  labels = {
    environment = var.env
    service     = "directory-accuracy"
  }
}
