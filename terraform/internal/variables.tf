variable "env" {
  description = "Environment name, used in resource naming and the hostname (directory-accuracy.<env>.certifyos.com)."
  type        = string
  default     = "internal"
}

variable "image" {
  description = <<-EOT
    Container image both instance templates run. No default on purpose: the deployed value lives
    in image.auto.tfvars (committed), which `make deploy` rewrites before applying, so a plain
    `terraform plan` afterwards is clean instead of wanting to roll the templates back.
  EOT
  type        = string
}

variable "auth0_issuer" {
  description = "Auth0 issuer the api group validates platform tokens against. Internal uses the staging tenant, as file-ingestion and api-layer internal do."
  type        = string
  default     = "https://auth-stg.certifyos.com/"
}

variable "auth0_audience" {
  description = "Auth0 API audience for internal."
  type        = string
  default     = "https://ng-api-stg.certifyos.com/"
}

variable "dal_url" {
  description = "DAL base URL for internal."
  type        = string
  default     = "https://dal.internal.certifyos.com"
}

variable "dal_iap_client_id" {
  description = <<-EOT
    OAuth client id of the IAP in front of dal-service-internal, the audience of the Google ID
    token this service sends. Read on 2026-10-05 with
    `gcloud compute backend-services describe dal-service-internal --global --format='value(iap.oauth2ClientId)'`.
  EOT
  type        = string
  default     = "106861691435-corh1vme14hp4lla45uco7nv2s8cu670.apps.googleusercontent.com"
}

variable "egress_url" {
  description = "Egress service base URL for internal (core-dal-egress-practitioner-async)."
  type        = string
  default     = "https://practitioner-egress-service.internal.certifyos.com"
}

variable "egress_iap_client_id" {
  description = <<-EOT
    Audience of the Google ID token sent to egress. On 2026-10-05 the internal egress backend
    (practitioner-egress-internal-be, behind the mcp-server-lb URL map) has IAP disabled, so the
    token is sent and ignored; the shared platform OAuth client id is used so the mint succeeds and
    the value is already right if IAP is turned on with that client (decisions.md finding 65).
  EOT
  type        = string
  default     = "106861691435-corh1vme14hp4lla45uco7nv2s8cu670.apps.googleusercontent.com"
}

variable "api_layer_url" {
  description = "api-layer base URL for internal."
  type        = string
  default     = "https://api-service.internal.certifyos.com"
}

variable "api_layer_iap_audience" {
  description = <<-EOT
    api-service-internal is behind IAP with a Google-managed OAuth client (no client id), so the
    ID token audience is the backend service itself: /projects/<number>/global/backendServices/<id>.
    Read on 2026-10-05 (`gcloud compute backend-services describe api-service-internal --global
    --format='value(id)'`). The token travels in Proxy-Authorization so Authorization keeps the
    Auth0 bearer api-layer reads. Empty disables the IAP header.
  EOT
  type        = string
  default     = "/projects/106861691435/global/backendServices/7022850158602384579"
}

variable "api_layer_client_id" {
  description = "Auth0 machine-to-machine client id for api-layer (ask A1). Empty until it exists; the secret travels through Secret Manager, never through this file."
  type        = string
  default     = ""
}

variable "vendor_bucket" {
  description = <<-EOT
    Bucket egress copies the finished file to and FinishJob verifies it in. The platform team's
    Candor SFTP bucket already exists in this project with the from/ and to/ prefixes the design
    names; the worker's read grant on from/ is ask A4 and is not managed here.
  EOT
  type        = string
  default     = "certifyos-development-sftp-candor-health"
}

variable "egress_events_topic" {
  description = <<-EOT
    Name of the egress-owned completion topic (egress ask 5). Null until egress creates it; the
    push subscription and the two push settings on the api group exist only when this is set.
  EOT
  type        = string
  default     = null
}

variable "permissions_enforce" {
  description = "Per-action permission checks on operator endpoints (ask A8). False until the DAL rows exist."
  type        = bool
  default     = false
}

variable "sentry_environment" {
  description = "Sentry environment tag. Sentry itself turns on only when the directory-accuracy-sentry-dsn secret has a version."
  type        = string
  default     = "internal"
}
