# HANDOFF

State of `directory-accuracy-service` at the end of the export lane subtask (CP-39605), and how work on it is done. Read `AGENTS.md` first; this file says where things stand, that one says how to work.

## Where things stand (2026-10-06, end of CP-40438)

- The scaffolding (CP-39604) is merged: PR #1 on `main` at `4e1e220`, findings 1 to 68.
- The export lane (CP-39605) is merged: PRs #2 to #9 on `main` at `13b72d8`, CI green there; findings 69 to 90 hold the gate evidence and finding 89 the three end-to-end event sequences.
- Observability and audit for the export leg (CP-40438) is built as four stacked pull requests against `main`, to merge in order: #10 the audit detail contract, the file MD5, the schedule version and the `AUDIT` log line; #11 the audit read endpoints; #12 the metrics, no-TTL and append-only proofs; #13 this documentation and the alert table. Findings 91 to 94.
- Every endpoint in the design's section 3 plus the two audit reads is in `openapi/openapi.yaml` with problem+json responses; every audit event type carries the design's detail fields by construction (`AuditDetailContract`); every metric name is asserted after the whole-lane run.
- Deployment is still held (finding 67): `terraform/internal/` is the validated reference shape for the Pulumi component; no image is pushed. `docs/runbook.md` section 9 is the first live verification once the deployment and the egress changes exist.

## What is deliberately not done here

- Nothing in the lane is a stub any more. What remains is outside this repository: the egress changes the design asks for, the Auth0 machine client, the IAP grants, the bucket read grant, the Pub/Sub topic and bindings, the Pulumi deployment (which also builds alerts E1 to E11 and a dashboard from runbook section 10, by Dev's decision of 2026-10-06 not to keep GCP resource definitions in this repository), the design-doc edits (ask A9, grown by `PRIOR_ATTEMPT` as a completion source, `userId directory-accuracy-worker`, the tick's 200, the file date being the first request's date, `scheduleVersion` and `md5` on two events, the aligned cancel-event keys).
- The mappings CSV ships with the open columns the design leaves open (`practitioner_phone`, `telehealth_url`, the group join, `Y/N` rendering) as documented placeholders (finding 69); a resolved vendor contract is a v2 of the CSV and a new template version, no code change.
- Phase 2, ingesting the vendor's response, is the next subtask under CP-39602 (CP-40438) and starts from `acknowledgedAt` and `inboundBatchId` on the batch document, which this lane only defines.

## What the next subtask starts from

- Audit events are built through `AuditEvent.forBatch` or `AuditEvent.of` and checked against `AuditDetailContract` at build; a new event type gets its row in the contract first, and a writer that forgets a key fails its own integration test. Every write logs one `AUDIT` line; alerts count those lines.
- Lifecycle writes live in three classes, each one transaction with its events: `BatchLifecycle` (birth, template pin, egress request, quota wait), `BatchCompletion` (egress completed, empty, reuse refused, failed at egress, delivered), `BatchOperations` (retry, supersede). Jobs call them; nothing else moves a batch. Add a transition there, never in a job or a resource.
- Jobs are the three batch handlers in `export/jobs/` (request, deadline check, finish), each `loadExpecting` its state first, so any job can be re-enqueued safely; `JobIds` makes ids deterministic and `JobEnqueuer` is the only enqueue point. `JobLogContext.currentJob()` is how a handler reads the JobRunr context (it throws if read the framework's way outside a worker, finding 78).
- The vendor bucket is `VendorBucket` (metadata only); `GcsVendorBucket` builds its client on first use. Tests mock the interface.
- Integration tests share one application per profile: use emails and tenants no other class uses, and `WireMockUpstreams.resetRequests()` before counting (finding 72). `EgressFixtures` builds batches in any state with the egress details the request job would have written. `ExportLaneIT` is the pattern for an end-to-end scenario on the worker role.
- Constructor injection is capped at seven parameters by Checkstyle; when a class needs an eighth, split by phase (findings 77d, 82) or route a read through a class that already holds the dependency (`BatchJobSupport.config()`, `BatchLifecycle.scheduleOf`).
- Nothing in `config/`, `persistence/`, `auth/`, `metrics/` or `terraform/` changed for the lane. `http/` gained `ProblemException` and two mappers (finding 71g), `clients/` gained the template list call and `VendorBucket`; both are recorded.

## Pending outside this repository

Listed with owners in `docs/runbook.md` sections 4 and 5, with the live verification in section 9: egress changes, Auth0 machine client, IAP grants on the DAL and api-layer backends, service-account and Pub/Sub bindings, vendor bucket read, DNS record, dedicated Atlas user, egress completion topic, DAL permission rows, Workload Identity for CI, Pulumi deployment, design-doc edits (ask A9). Also: `@suhasini-certify` needs write access on this repository for the CODEOWNERS entry to count.

## How work is done here

- One step at a time, with a gate: every step ends with CI green and a numbered finding in `docs/decisions.md` that states what was proven and what was not. Nothing moves to the next step without that.
- Nothing is assumed from memory about a framework: check the jar (`javap`), the source of the sibling service, or a running test. Findings 50, 53, 56, 61, 78 and 84 are each a case where the assumption was wrong and the check found it.
- The new-service playbook (Claude Doc "New Service Playbook: Lessons from api-layer and DAL") is the standard; its rules are enforced by ArchUnit and Checkstyle, not remembered.
- Locally: `make compile` and `make lint`, plus the unit test classes a change touches. The full suite runs in CI; integration tests need Docker. Never push with a failing local compile (finding 84 records the one time it happened).
- External writes (Jira, Confluence, access grants, `terraform apply`) are drafted or planned here and run by the engineer.
- Every deviation from the plan or the design is written down the day it happens, with the reason, in `docs/decisions.md`; a settled, hard-to-reverse choice becomes an ADR.
