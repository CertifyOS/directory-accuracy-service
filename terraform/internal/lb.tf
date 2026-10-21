# External HTTPS load balancer in front of the api group only. The worker group has no external
# IP and no forwarding rule points at it.
resource "google_compute_global_address" "lb" {
  name = "directory-accuracy-lb"
}

# Goes ACTIVE only after the DNS record resolves to the address above. certifyos.com DNS is Route
# 53, written by DevOps (ask A5): directory-accuracy.internal.certifyos.com A <lb_ip>. Until then the
# LB answers with a TLS error and the smoke script reaches it by IP with the Host header.
resource "google_compute_managed_ssl_certificate" "cert" {
  name = "directory-accuracy-${var.env}"

  managed {
    domains = [local.hostname]
  }
}

resource "google_compute_backend_service" "api" {
  name                            = "directory-accuracy-api"
  protocol                        = "HTTP"
  port_name                       = "http"
  timeout_sec                     = 60
  health_checks                   = [google_compute_health_check.live.id]
  load_balancing_scheme           = "EXTERNAL_MANAGED"
  connection_draining_timeout_sec = 300

  backend {
    group          = google_compute_region_instance_group_manager.api.instance_group
    balancing_mode = "UTILIZATION"
  }

  log_config {
    enable      = true
    sample_rate = 1.0
  }
}

resource "google_compute_url_map" "m" {
  name            = "directory-accuracy"
  default_service = google_compute_backend_service.api.id
}

resource "google_compute_target_https_proxy" "p" {
  name             = "directory-accuracy"
  url_map          = google_compute_url_map.m.id
  ssl_certificates = [google_compute_managed_ssl_certificate.cert.id]
}

resource "google_compute_global_forwarding_rule" "f" {
  name                  = "directory-accuracy-https"
  target                = google_compute_target_https_proxy.p.id
  port_range            = "443"
  ip_address            = google_compute_global_address.lb.address
  load_balancing_scheme = "EXTERNAL_MANAGED"
}
