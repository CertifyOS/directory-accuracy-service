# The VMs live in the project's existing `default` VPC: the MongoDB Atlas cluster is peered to
# `default` only, its IP access list is 10.128.0.0/9 (the auto-mode subnets of `default`), and
# us-central1 already has a Cloud NAT (cf-fetcher-router), so this stack creates no router, NAT
# or subnet (file-ingestion finding 51; re-checked 2026-10-05).
data "google_compute_network" "vpc" {
  name = "default"
}

data "google_compute_subnetwork" "svc" {
  name   = "default"
  region = local.region
}

locals {
  vm_tags = ["directory-accuracy-api", "directory-accuracy-worker"]
}

# Google's health-check and load-balancer source ranges.
resource "google_compute_firewall" "hc" {
  name          = "directory-accuracy-allow-hc"
  network       = data.google_compute_network.vpc.name
  direction     = "INGRESS"
  source_ranges = ["35.191.0.0/16", "130.211.0.0/22"]
  target_tags   = local.vm_tags

  allow {
    protocol = "tcp"
    ports    = ["8080"]
  }
}

# IAP TCP forwarding range, for `gcloud compute ssh --tunnel-through-iap` and the dashboard tunnel.
resource "google_compute_firewall" "iap_ssh" {
  name          = "directory-accuracy-allow-iap-ssh"
  network       = data.google_compute_network.vpc.name
  direction     = "INGRESS"
  source_ranges = ["35.235.240.0/20"]
  target_tags   = local.vm_tags

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

# Priority 65000 beats `default`'s own default-allow-* rules (65534) for these tags only, so the
# two allows above are the only ingress these VMs accept. The JobRunr dashboard on the worker's
# port 8000 is therefore reachable only through an IAP SSH tunnel (make tunnel-dashboard).
resource "google_compute_firewall" "deny_rest" {
  name          = "directory-accuracy-deny-ingress"
  network       = data.google_compute_network.vpc.name
  direction     = "INGRESS"
  priority      = 65000
  source_ranges = ["0.0.0.0/0"]
  target_tags   = local.vm_tags

  deny {
    protocol = "all"
  }
}
