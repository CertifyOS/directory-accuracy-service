# Two roles, one image: the api group serves HTTP behind the load balancer; the worker group runs
# the same image with the `worker` profile, which turns on the JobRunr job server and dashboard.
# Containers start from a startup script on Container-Optimized OS (startup.sh.tftpl); the
# container-vm module's gce-container-declaration metadata is rejected by the Compute API since
# Google discontinued the container startup agent (file-ingestion finding 58).
#
# Env vars are exactly the ${...} keys application.properties reads. MONGODB_URI is not set: the
# prod profile reads it from Secret Manager. API_LAYER_CLIENT_SECRET and SENTRY_DSN are fetched by
# the startup script (secrets.tf explains why).
locals {
  env_common = {
    JAVA_OPTS_APPEND            = "-Dquarkus.http.host=0.0.0.0 -Djava.util.logging.manager=org.jboss.logmanager.LogManager -XX:MaxRAMPercentage=70"
    AUTH0_ISSUER                = var.auth0_issuer
    AUTH0_AUDIENCE              = var.auth0_audience
    DAL_URL                     = var.dal_url
    DAL_IAP_CLIENT_ID           = var.dal_iap_client_id
    EGRESS_URL                  = var.egress_url
    EGRESS_IAP_CLIENT_ID        = var.egress_iap_client_id
    API_LAYER_URL               = var.api_layer_url
    API_LAYER_IAP_AUDIENCE      = var.api_layer_iap_audience
    API_LAYER_CLIENT_ID         = var.api_layer_client_id
    VENDOR_BUCKET               = var.vendor_bucket
    PUBSUB_PUSH_SERVICE_ACCOUNT = local.push_enabled ? google_service_account.push.email : ""
    PUBSUB_PUSH_AUDIENCE        = local.push_enabled ? local.push_endpoint : ""
    PERMISSIONS_ENFORCE         = var.permissions_enforce ? "true" : "false"
    SENTRY_ENVIRONMENT          = var.sentry_environment
  }

  templates = {
    api = {
      machine_type    = "e2-small"
      tag             = "directory-accuracy-api"
      service_account = google_service_account.api.email
      env             = merge({ QUARKUS_PROFILE = "prod,api" }, local.env_common)
    }
    worker = {
      machine_type    = "e2-standard-2"
      tag             = "directory-accuracy-worker"
      service_account = google_service_account.worker.email
      env             = merge({ QUARKUS_PROFILE = "prod,worker" }, local.env_common)
    }
  }

  zones = ["${local.region}-a", "${local.region}-b", "${local.region}-c"]
}

data "google_compute_image" "cos" {
  family  = "cos-stable"
  project = "cos-cloud"
}

resource "google_compute_instance_template" "role" {
  for_each = local.templates

  name_prefix  = "directory-accuracy-${each.key}-"
  machine_type = each.value.machine_type
  tags         = [each.value.tag]
  labels       = local.labels

  disk {
    source_image = data.google_compute_image.cos.self_link
    auto_delete  = true
    boot         = true
    disk_size_gb = 10
    disk_type    = "pd-balanced"
  }

  # No access_config: no external IP. Egress goes through `default`'s existing Cloud NAT.
  network_interface {
    subnetwork = data.google_compute_subnetwork.svc.id
  }

  service_account {
    email  = each.value.service_account
    scopes = ["cloud-platform"]
  }

  shielded_instance_config {
    enable_secure_boot          = true
    enable_vtpm                 = true
    enable_integrity_monitoring = true
  }

  metadata = {
    startup-script = templatefile("${path.module}/startup.sh.tftpl", {
      image    = var.image
      registry = local.registry
      project  = local.google_project
      # Values are URLs, ids and flags; none contains a single quote.
      env_flags = join(" \\\n  ", [for k, v in each.value.env : "-e ${k}='${v}'"])
    })
    google-logging-enabled    = "true"
    google-monitoring-enabled = "true"
    enable-oslogin            = "true"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "google_compute_health_check" "live" {
  name                = "directory-accuracy-live"
  check_interval_sec  = 10
  timeout_sec         = 5
  healthy_threshold   = 2
  unhealthy_threshold = 3

  http_health_check {
    port         = 8080
    request_path = "/q/health/live"
  }
}

# Regional groups over three zones. A fixed max_surge/max_unavailable on a regional group must be
# 0 or at least the number of zones, so 3 is the smallest surge that works.
resource "google_compute_region_instance_group_manager" "api" {
  name                      = "directory-accuracy-api"
  region                    = local.region
  base_instance_name        = "directory-accuracy-api"
  distribution_policy_zones = local.zones
  target_size               = 2

  version {
    instance_template = google_compute_instance_template.role["api"].id
  }

  named_port {
    name = "http"
    port = 8080
  }

  auto_healing_policies {
    health_check      = google_compute_health_check.live.id
    initial_delay_sec = 120
  }

  update_policy {
    type                  = "PROACTIVE"
    minimal_action        = "REPLACE"
    max_surge_fixed       = 3
    max_unavailable_fixed = 0
  }
}

# One worker, OPPORTUNISTIC so a `terraform apply` never replaces it mid job; `make rollout-worker`
# drives the replacement.
resource "google_compute_region_instance_group_manager" "worker" {
  name                      = "directory-accuracy-worker"
  region                    = local.region
  base_instance_name        = "directory-accuracy-worker"
  distribution_policy_zones = local.zones
  target_size               = 1

  version {
    instance_template = google_compute_instance_template.role["worker"].id
  }

  auto_healing_policies {
    health_check      = google_compute_health_check.live.id
    initial_delay_sec = 120
  }

  update_policy {
    type                  = "OPPORTUNISTIC"
    minimal_action        = "REPLACE"
    max_surge_fixed       = 3
    max_unavailable_fixed = 0
  }
}
