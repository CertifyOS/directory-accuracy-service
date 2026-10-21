# Docker repository both instance templates pull from. Images are pushed by `make push-image`
# from an engineer's machine until the Workload Identity binding for CI exists (ask A10).
resource "google_artifact_registry_repository" "images" {
  location      = local.region
  repository_id = "directory-accuracy"
  format        = "DOCKER"
  labels        = local.labels
}

locals {
  registry   = "${local.region}-docker.pkg.dev"
  image_repo = "${local.registry}/${local.google_project}/${google_artifact_registry_repository.images.repository_id}"
}
