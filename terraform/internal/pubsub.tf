# The completion topic belongs to egress (egress ask 5); this stack owns only its subscription
# and the dead-letter pair. Until the topic exists, var.egress_events_topic is null, the
# subscription has count 0, and the api group's push settings stay empty, so the push endpoint
# answers 401 PUSH_NOT_CONFIGURED to everything.
locals {
  push_enabled  = var.egress_events_topic != null
  push_endpoint = "https://${local.hostname}/internal/vendor-exports/egress-events"
}

resource "google_pubsub_topic" "dlq" {
  name   = "vendor-export-egress-events-dlq"
  labels = local.labels
}

resource "google_pubsub_subscription" "dlq_drain" {
  name                       = "vendor-export-egress-events-dlq-sub"
  topic                      = google_pubsub_topic.dlq.name
  message_retention_duration = "604800s"
  labels                     = local.labels
}

resource "google_pubsub_subscription" "egress_events" {
  count = local.push_enabled ? 1 : 0

  name                       = "vendor-export-egress-events"
  topic                      = "projects/${local.google_project}/topics/${var.egress_events_topic}"
  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"
  filter                     = "attributes.initiator = \"directory-accuracy-worker\""
  labels                     = local.labels

  push_config {
    push_endpoint = local.push_endpoint

    oidc_token {
      service_account_email = google_service_account.push.email
      audience              = local.push_endpoint
    }
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dlq.id
    max_delivery_attempts = 5
  }
}

# IAM on Pub/Sub resources is not manageable from the applying account (see iam.tf, A3).
