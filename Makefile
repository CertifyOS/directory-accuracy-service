# directory-accuracy-service
#
# Developer verbs, then the internal deploy flow (terraform/internal). Each deploy step is one short
# command so a runner that kills long processes never leaves an apply half done. Production deploys
# happen only through CI once the Workload Identity binding exists (ask A10).

PROJECT  := certifyos-development
REGION   := us-central1
REPO     := $(REGION)-docker.pkg.dev/$(PROJECT)/directory-accuracy
TAG      := $(shell git rev-parse --short=12 HEAD)
IMAGE    := $(REPO)/directory-accuracy-service:$(TAG)
ROLE     ?= api
TF_DIR   := terraform/internal
TF       := terraform -chdir=$(TF_DIR)

ifeq ($(OS),Windows_NT)
    GRADLEW := gradlew.bat
else
    GRADLEW := ./gradlew
endif

default: help

.PHONY: help
help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "} {printf "\033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: bootstrap
bootstrap: ## One-time setup: git hooks and a local .env
	$(GRADLEW) installGitHooks
	@test -f .env || cp .env.example .env
	@echo "Fill the (required) values in .env, then: make dev"

.PHONY: dev
dev: ## Run locally in dev mode; ROLE=api (default) or ROLE=worker
	QUARKUS_PROFILE=dev,$(ROLE) $(GRADLEW) quarkusDev -Dquarkus.console.basic=true

.PHONY: fmt
fmt: ## Apply Spotless formatting
	$(GRADLEW) spotlessApply

.PHONY: lint
lint: ## Spotless check, Checkstyle and SpotBugs on every source set (no tests)
	$(GRADLEW) spotlessCheck checkstyleMain checkstyleTest checkstyleIntegrationTest spotbugsMain spotbugsTest spotbugsIntegrationTest

.PHONY: compile
compile: ## Compile main, test and integrationTest sources without running anything
	$(GRADLEW) compileJava compileTestJava compileIntegrationTestJava

.PHONY: test
test: ## Unit tests (plain JUnit, ArchUnit)
	$(GRADLEW) test

.PHONY: check
check: ## Everything CI runs (lint, unit tests, integration tests on Testcontainers)
	$(GRADLEW) check

.PHONY: openapi
openapi: ## Regenerate openapi/openapi.yaml from the code (commit the result; CI verifies it)
	$(GRADLEW) quarkusBuild
	cp build/openapi/openapi.yaml openapi/openapi.yaml

.PHONY: build-image
build-image: ## Fast-jar, then the linux/amd64 image tagged with the current commit
	$(GRADLEW) quarkusBuild
	docker build --platform linux/amd64 -f src/main/docker/Dockerfile.jvm -t $(IMAGE) .
	@echo "$(IMAGE)"

.PHONY: push-image
push-image: build-image ## Push the image (one-time first: gcloud auth configure-docker us-central1-docker.pkg.dev -q)
	docker push $(IMAGE)
	@echo "pushed $(IMAGE)"

# ---- terraform/internal ------------------------------------------------------------------------

.PHONY: tf-init
tf-init: ## terraform init against the GCS state bucket
	$(TF) init -input=false

.PHONY: tf-validate
tf-validate: ## terraform fmt -check and validate
	$(TF) fmt -check -recursive
	$(TF) validate

.PHONY: tf-plan
tf-plan: ## Full plan (read-only)
	$(TF) plan -input=false

.PHONY: bootstrap-infra
bootstrap-infra: ## First apply: registry and secret containers only, so the image can be pushed and the Mongo URI added before the VMs exist
	$(TF) apply -input=false -target=google_artifact_registry_repository.images -target='google_secret_manager_secret.s'

.PHONY: tf-apply
tf-apply: ## Full apply (interactive approval). Run after bootstrap-infra, the Mongo secret version, and push-image.
	$(TF) apply -input=false

.PHONY: create-template
create-template: ## New instance templates for both roles pointing at IMAGE (no group change)
	printf '# Written by `make create-template`. See the Makefile.\nimage = "%s"\n' "$(IMAGE)" > $(TF_DIR)/image.auto.tfvars
	$(TF) apply -input=false -auto-approve -target='google_compute_instance_template.role["api"]' -target='google_compute_instance_template.role["worker"]'

.PHONY: deploy
deploy: push-image ## Push, write image.auto.tfvars, apply templates and both groups (api rolls PROACTIVE; worker waits for rollout-worker)
	printf '# Written by `make deploy`. See the Makefile.\nimage = "%s"\n' "$(IMAGE)" > $(TF_DIR)/image.auto.tfvars
	bash -o pipefail -c "$(TF) apply -input=false -auto-approve -compact-warnings -no-color \
	  -target='google_compute_instance_template.role[\"api\"]' -target='google_compute_instance_template.role[\"worker\"]' \
	  -target=google_compute_region_instance_group_manager.api -target=google_compute_region_instance_group_manager.worker \
	  2>&1 | grep -E 'Plan:|Apply complete|Error|Creating\.\.\.|Creation complete|Modifying\.\.\.|Modifications complete|Destroying\.\.\.|Destruction complete'"
	@echo "deployed $(IMAGE); api group rolling. Worker: make rollout-worker."

.PHONY: rollout-api
rollout-api: ## Proactive rolling update of the api group to the current api template
	gcloud compute instance-groups managed rolling-action start-update directory-accuracy-api --region $(REGION) --project $(PROJECT) \
	  --version template=$$($(TF) output -raw api_template) --max-surge 3 --max-unavailable 0 --type proactive
	gcloud compute instance-groups managed wait-until directory-accuracy-api --stable --region $(REGION) --project $(PROJECT) --timeout 600

.PHONY: rollout-worker
rollout-worker: ## Proactive rolling update of the worker group (check jobrunr_jobs has no PROCESSING document first)
	gcloud compute instance-groups managed rolling-action start-update directory-accuracy-worker --region $(REGION) --project $(PROJECT) \
	  --version template=$$($(TF) output -raw worker_template) --max-surge 3 --max-unavailable 0 --type proactive
	gcloud compute instance-groups managed wait-until directory-accuracy-worker --stable --region $(REGION) --project $(PROJECT) --timeout 600

rollback-%: ## make rollback-api PREVIOUS_TEMPLATE=<template self link> (or rollback-worker)
	gcloud compute instance-groups managed rolling-action start-update directory-accuracy-$* --region $(REGION) --project $(PROJECT) \
	  --version template=$(PREVIOUS_TEMPLATE) --max-surge 3 --max-unavailable 0 --type proactive

.PHONY: tunnel-dashboard
tunnel-dashboard: ## IAP tunnel to the worker's JobRunr dashboard: then open http://localhost:8000
	$(eval WORKER := $(shell gcloud compute instance-groups managed list-instances directory-accuracy-worker --region $(REGION) --project $(PROJECT) --format='value(instance.basename(),instance.scope(zones).segment(0))' | head -1))
	gcloud compute start-iap-tunnel $(word 1,$(WORKER)) 8000 --local-host-port=localhost:8000 --zone $(word 2,$(WORKER)) --project $(PROJECT)

.PHONY: smoke
smoke: ## Live smoke test (TOKEN_FILE=... for the authenticated cases; LB_IP=... until DNS exists)
	scripts/smoke-api.sh
