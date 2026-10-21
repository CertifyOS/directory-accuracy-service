# Two service accounts, one per role, so the IAP and bucket grants can name exactly who calls what:
# the api group looks users up in the DAL and receives Pub/Sub pushes; the worker group calls the
# DAL, egress, api-layer and the vendor bucket. A third account is what Pub/Sub pushes as.
resource "google_service_account" "api" {
  account_id   = "directory-accuracy-api"
  display_name = "directory-accuracy-service api VMs"
}

resource "google_service_account" "worker" {
  account_id   = "directory-accuracy-worker"
  display_name = "directory-accuracy-service worker VMs"
}

resource "google_service_account" "push" {
  account_id   = "directory-accuracy-pubsub-push"
  display_name = "Identity Pub/Sub pushes egress completion events with"
}

locals {
  api_sa    = "serviceAccount:${google_service_account.api.email}"
  worker_sa = "serviceAccount:${google_service_account.worker.email}"
  vm_sas    = { api = local.api_sa, worker = local.worker_sa }
  project_roles = toset([
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
    "roles/artifactregistry.reader",
  ])
  vm_project_bindings = {
    for pair in setproduct(keys(local.vm_sas), local.project_roles) :
    "${pair[0]}:${pair[1]}" => { member = local.vm_sas[pair[0]], role = pair[1] }
  }
}

resource "google_project_iam_member" "vm" {
  for_each = local.vm_project_bindings
  project  = local.google_project
  role     = each.value.role
  member   = each.value.member
}

# Bindings this stack cannot create (the applying account holds roles/editor, which has no
# setIamPolicy on service accounts, Pub/Sub resources, IAP backends or other teams' buckets).
# Each is a DevOps ask in the plan's section 8 and in docs/runbook.md; grant by hand, then
# codify in the certifyos-pulumi component that replaces this stack.
#
#   A2  roles/iap.httpsResourceAccessor on dal-service-internal for directory-accuracy-api@ and
#       directory-accuracy-worker@; on api-service-internal for both (api-layer is behind IAP with
#       a Google-managed client); on the egress backend if IAP is ever enabled there.
#   A3  roles/iam.serviceAccountTokenCreator on directory-accuracy-pubsub-push@ for the Pub/Sub
#       service agent service-106861691435@gcp-sa-pubsub.iam.gserviceaccount.com (OIDC pushes);
#       roles/pubsub.publisher on vendor-export-egress-events-dlq and roles/pubsub.subscriber
#       on vendor-export-egress-events for that same agent (dead lettering);
#       roles/pubsub.subscriber on the egress-owned topic's subscription for directory-accuracy-api@.
#   A4  storage.objects.get (roles/storage.objectViewer) on gs://<vendor_bucket>/from/ for
#       directory-accuracy-worker@.
