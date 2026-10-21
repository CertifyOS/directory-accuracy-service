# Secret containers only, never values: a value applied through Terraform lands in plaintext in
# the state file. Add versions out of band:
#   gcloud secrets versions add directory-accuracy-mongodb-uri            --data-file=-   (required before first boot)
#   gcloud secrets versions add directory-accuracy-apilayer-client-secret --data-file=-   (ask A1; optional)
#   gcloud secrets versions add directory-accuracy-sentry-dsn             --data-file=-   (optional)
# The Mongo URI is read by the prod profile through the Secret Manager config source and the boot
# fails without it, which is right for a required value. The two optional ones are fetched by the
# startup script into environment variables and are simply empty while absent (decisions.md 66).
locals {
  secrets = {
    mongo    = "directory-accuracy-mongodb-uri"
    apilayer = "directory-accuracy-apilayer-client-secret"
    sentry   = "directory-accuracy-sentry-dsn"
  }
  secret_readers = {
    for pair in setproduct(keys(local.secrets), keys(local.vm_sas)) :
    "${pair[0]}:${pair[1]}" => { secret = pair[0], member = local.vm_sas[pair[1]] }
  }
}

resource "google_secret_manager_secret" "s" {
  for_each  = local.secrets
  secret_id = each.value
  labels    = local.labels

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_iam_member" "read" {
  for_each  = local.secret_readers
  secret_id = google_secret_manager_secret.s[each.value.secret].id
  role      = "roles/secretmanager.secretAccessor"
  member    = each.value.member
}
