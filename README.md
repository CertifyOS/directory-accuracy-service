# directory-accuracy-service

The directory accuracy service: vendor export and vendor ingestion as modules of one service. Phase 1 runs a schedule per tenant that names a saved filter (api-layer's filter module), asks the egress service to select the filter's practitioners and build the vendor file, and records what was sent and what egress counted. Phase 2 ingests the vendor's response in the same service.

- Design: Confluence "CP-39602 - Design: Directory accuracy vendor export".
- Working rules: `AGENTS.md` (the one guidance file; every rule there is enforced by a lint or a test).
- Decisions: `docs/adr/` (settled) and `docs/decisions.md` (running log, numbered findings with evidence).
- Operations: `docs/runbook.md`. Hand-over state: `HANDOFF.md`.

## Shape

One image, two roles chosen by Quarkus profile. `prod,api` serves HTTP behind the load balancer: operator endpoints under `/v1/vendor-exports`, the Pub/Sub push endpoint under `/internal`, health and metrics under `/q`. `prod,worker` runs the JobRunr job server and dashboard and takes no inbound traffic. Both roles share one MongoDB database, `directory_accuracy`, which holds the seven service collections and JobRunr's five `jobrunr_` collections.

```
export lane        tick (06:00 UTC) or run-now -> RequestEgressJob -> completion event or DeadlineCheckJob -> FinishJob
batch states       SCHEDULED -> EGRESS_REQUESTED -> EGRESS_COMPLETED -> DELIVERED   (EMPTY, FAILED, SUPERSEDED)
foundations        config  persistence  audit  auth  http  clients  metrics
```

Operators work through `/v1/vendor-exports`: schedules (create with a saved filter id, enable, disable, run-now), batches (read, list, events, retry, supersede) and the tick. The procedures are in `docs/runbook.md` section 7; the contract is `openapi/openapi.yaml`.

## Prerequisites

- Java 21. Gradle comes with the wrapper.
- Docker, for the integration tests (Testcontainers MongoDB replica set) and the image build.
- For local runs: a MongoDB replica set and the values marked required in `.env.example`.
- For the integration tests: nothing beyond Docker; WireMock stands in for the DAL, api-layer and egress, and the vendor bucket is mocked.
- For deploys: `gcloud` authenticated to `certifyos-development`, Terraform 1.9 or newer.

## First run

```shell
make bootstrap                 # installs the git hooks, copies .env.example to .env
docker run -d --name mongo -p 27017:27017 mongo:7.0 --replSet rs0
docker exec mongo mongosh --quiet --eval 'rs.initiate()'
make dev                       # api role on :8080
make dev ROLE=worker           # worker role: JobRunr server, dashboard on :8000
```

Fill the required values in `.env` first. The DAL, egress and api-layer URLs can stay at their internal defaults: nothing calls them until an operator request or a job needs to, and a local run has no IAP access anyway. What a local run touches for real: only the MongoDB you point it at. The tick writes `EXPORT_TICK_COMPLETED` events there; nothing leaves the machine.

Local requests need a platform token only when the OIDC tenant is on, which it is not outside the `prod` profile. The tenant-membership filter still runs and still calls the DAL, so an operator call from a laptop ends in `503 DAL_UNAVAILABLE` unless `DAL_URL` points at something that answers `/users/by-email`.

## Everyday

```shell
make compile     # compile every source set, run nothing
make lint        # Spotless check, Checkstyle, SpotBugs
make fmt         # apply Spotless
make test        # unit tests: plain JUnit, ArchUnit, SmallRye config
make check       # everything CI runs, integration tests included (needs Docker)
make openapi     # regenerate openapi/openapi.yaml after an endpoint change, then commit it
```

CI runs `./gradlew check` on every push and pull request, a gitleaks scan, and an oasdiff breaking-change check of `openapi/openapi.yaml` against `main`. `main` is protected: CI green, one approval, code-owner review.

## Tests

- `src/test`: unit tests. ArchUnit enforces the package rules and the playbook rules (constructor injection, one logger, typed contracts, every endpoint documented and permission-annotated).
- `src/integrationTest`: Quarkus wiring on a Testcontainers MongoDB replica set, two profiles only: `ApiTestProfile` (`test,api`) and `WorkerTestProfile` (`test,worker`). WireMock stands in for the DAL, egress and api-layer. The Google token services are mocked where a test crosses them.
- `./gradlew check` fails when `openapi/openapi.yaml` differs from what the code produces.

## Build and deploy

```shell
make build-image   # fast-jar, then the linux/amd64 image tagged with the current commit
make push-image    # to Artifact Registry (one-time: gcloud auth configure-docker us-central1-docker.pkg.dev -q)
make deploy        # push, write terraform/internal/image.auto.tfvars, apply templates and groups
```

`terraform/internal/` owns the service shape in `certifyos-development`; the deploy flow changes only the image. First-time order, rollouts, rollback, the hand grants and the pending hook-ups are in `docs/runbook.md`.

## Layout

```
src/main/java/com/certifyos/directory_accuracy/
  config/       DirectoryAccuracyConfig (closed mapping), RoleProfileCheck, MongoIndexes, Clocks
  persistence/  Collections, Transactions, Documents, Ids, AlreadyExistsException
  audit/        AuditEvent, AuditEventType (24 types), AuditDetailContract (required detail per type), AuditRepository, Actors
  auth/         UserContextFilter, PermissionFilter, DalUserClient, Google ID tokens, push token verifier
  http/         Problem (RFC 9457), Problems, ProblemException, ProblemMappers
  clients/      EgressClient, ApiLayerClient, ApiLayerTokenService, VendorBucket (GCS metadata), readiness
  metrics/      DirectoryAccuracyMetrics (the design's names)
  export/
    schedule/   ScheduleResource and ScheduleService, Cadence, TemplateProvisioner (+ the mappings CSV)
    batch/      ExportBatch and its repository, BatchLifecycle (birth, pin, request, quota wait),
                BatchCompletion (completed, empty, reuse refused, failed, delivered), BatchOperations (retry, supersede),
                DestinationNames
    jobs/       TickJob, RequestEgressJob, DeadlineCheckJob, FinishJob, ReconcilerJob, JobEnqueuer, JobIds
    events/     EgressEventResource -> EgressEventHandler (the design's handler table)
    api/        VendorExportsResource (batches), ExportOpsResource (tick), BatchViews
terraform/internal/   the internal environment
scripts/smoke-api.sh  live smoke test
openapi/openapi.yaml  the committed contract
docs/                 adr/, decisions.md, runbook.md
```
