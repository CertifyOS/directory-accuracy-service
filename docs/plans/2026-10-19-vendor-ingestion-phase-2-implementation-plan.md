# Vendor ingestion (phase 2) implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A vendor response file landing in `to/<tenantId>/` becomes one reviewable document per field the vendor wants changed, a reviewer's decision becomes an audited state, and an approved value reaches the Golden record under `candor:<tenantId>` (slice fields through the DAL, junction and location fields in place through the api-layer), with the outcome read back from survivorship's event and every step answerable for seven years.

**Architecture:** A new `ingestion/` lane beside `export/` in the same Quarkus image and MongoDB database. Three Pub/Sub push endpoints on the `api` role (arrival, completion, ov-generated) claim or finish work with guarded writes; the JobRunr jobs on the `worker` role (request ingestion, deadline check, stage rows, release, read-back deadline, reconciler) do the external calls. Parsing is delegated to file-ingestion-service; Golden-record slice writes go through the DAL `/batch` endpoint as whole-slice `UPSERT`s after a read-merge; in-place edits go through the api-layer endpoints the tenant UI uses. Every state change is a guarded update inside a Mongo transaction with its audit event; nothing outside MongoDB runs inside `Transactions.run`.

**Tech stack:** Java 21, Quarkus 3.33.3, JobRunr 8.8.2, MongoDB (blocking driver, transactions), Cloud Storage client 2.45.0 (server-side copy), Micrometer, ArchUnit, Testcontainers, WireMock. One new dependency: `commons-codec` (the Soundex the matching layer uses, Task 7.2).

**Spec:** `docs/design/vendor-ingestion.md`, refreshed on 2026-10-19 from Confluence "CP-40437 - Design: Directory accuracy vendor ingestion" (page 2302345225, published 2026-10-19). Design decisions are cited as D1 to D22, Rollout asks as "ask n" (1 to 20); this plan's own decisions are P1 to P14 below. Where the two disagree, the plan says so and lists the design-doc edit at the end.

**Supersedes:** `docs/plans/2026-10-18-vendor-ingestion-phase-2-implementation-plan.md`, written against the 2026-10-18 Slack copy of the design. The 2026-10-19 Confluence version changed the plan in six places: confirmations and rejects are counts on the inbound entry, not collections (D11); staging does no identity lookup and has no suppression (D16, "Identity lookup at staging"); rejected values are one row per distinct value with a hashed key (D16); release has no ledger, release or lock collection (D18), writes location slices (D21) and in-place edits (D22), and reads back on `mdm.slice.ov_generated` (D19); the audit trail gains `vendor_review_events` and a per-event table; metrics and alerts are named in the design.

**Written:** 2026-10-19. Starts from `main` at `034f7ac` (end of CP-40438, findings 1 to 95).

## Global constraints

Every task's requirements include these. Each is enforced by a lint, an ArchUnit rule or a test named in `AGENTS.md`.

- Packages: `ingestion/` is a feature slice beside `export/`; shared foundations (`config/`, `persistence/`, `audit/`, `auth/`, `http/`, `clients/`, `metrics/`) never import it (`ArchitectureTest.sharedPackagesDoNotDependOnFeatures`, extended in Step 0). `ingestion/` may import `export/batch` and `export/jobs`; `export/` never imports `ingestion/` (`exportDoesNotDependOnIngestion`, new).
- Constructor injection only, at most 7 constructor parameters, 500 lines per main file, 1,000 per test file, no single-letter locals, `org.jboss.logging.Logger` with printf style, one injected `ObjectMapper`, no `Thread.sleep`, no `Executors`.
- Records for DTOs; no `JsonNode` in `..api..`, `..events..`, `..clients..` (`typedContracts`): the DAL's `contributionMap` and `contributingSlices` arrive as `Map<String, Object>` and `List<Map<String, Object>>`.
- Every endpoint carries `@Operation`; every method in `ingestion.api` carries `@RequiresPermission`; `openapi/openapi.yaml` is regenerated with `make openapi` in every step that adds a path.
- Every repository method takes the tenant or an id that embeds it; every list query carries a limit.
- Every state change is a guarded update (compare-and-set on the stored state) inside `Transactions.run`, with its audit event written in the same transaction through `AuditRepository.write(session, event)`. Enqueues, publishes and HTTP calls happen after the transaction returns. The one exception the design grants is the staging bulk upsert (D10: idempotent by `_id`, no transaction).
- Audit events: a new `AuditEventType` gets its required-keys row in `AuditDetailContract` first, copied from the design's "Per-event contracts" table; detail carries ids, counts and enumerations, never a vendor value, a name, an address or a phone number.
- Logs carry ids and counts, never bodies or PII; every job opens `IngestionLogContext` first (`exportBatchId`, `uploadId`, `releaseId`, `tenantId`, `jobId` on the MDC).
- Metrics only through `metrics/VendorExchangeMetrics`, the design's Observability names only; tags are tenant, vendor, state, reason, source, status, disposition, action, result, outcome, eventType or job, never a practitioner, an NPI, a location or a field value.
- Errors are `ProblemException` rendered by `http/ProblemMappers`; codes are upper snake case and stable.
- Ticket keys only in commits and PRs; `@Disabled` needs a reason with a ticket.
- Branch `feature/CP-40437-step-<n>-<kebab>` from `main`, Conventional Commits, one pull request per step, CI green before merge, a numbered finding in `docs/decisions.md` per step gate (next number: 96).
- Retention: exactly one TTL index in the database, `vendor_rejected_values.expiresAt` (D16, P4); `MongoIndexesIT.noIndexExpiresDocuments` enforces it over every collection.

## Decisions folded into this plan

| ID | Decision | Instead of | Where it lands |
| --- | --- | --- | --- |
| P1 | Reviewer and operator endpoints live under `/v1/vendor-review/…` and `/v1/vendor-ingestion/…`; push endpoints under `/internal/vendor-ingestion/…`. The HTTP policy already authenticates `/v1/*` and permits `/internal/*`. | The design's unversioned `/vendor-review/…` | Steps 2, 5, 7; design-doc edit 1 |
| P2 | One push service account and one custom audience for all four push subscriptions (`vendor-exchange-egress-events`, `vendor-ingestion-arrivals`, `vendor-ingestion-completions`, `vendor-ingestion-ov-generated`), so `PubSubPushAuthFilter` and `GooglePushTokenVerifier` are untouched. | A per-subscription service account (the design's Security section), which needs the filter to know which endpoint it guards | Step 1; ask 3 amended |
| P3 | `InboundFile` and its state machine live in `ingestion/inbound/`; `InboundFileRepository` writes the `inboundFiles[]` array on `vendor_export_batches` with array-element guarded updates and reads the batch through `ExportBatch`, which is unchanged (its `fromDocument` ignores the array). The export batch view does not show inbound files; a new operator read does. | Adding the array to `ExportBatch` and `BatchViews`, which would make `export.api` depend on `ingestion` | Step 1 |
| P4 | Exactly one TTL index exists: `vendor_rejected_values.expiresAt`. `MongoIndexesIT.noIndexExpiresDocuments` is rewritten to iterate every collection `Collections.all()` names and allow only that one. `field_recommendations` gets no TTL in MVP (D13 leaves its lifetime open). | A TTL on anything audited; deciding the field-document lifetime here | Step 0, Step 6 |
| P5 | Staging trusts the echoed `certify_practitioner_id`, `npi` and `certify_location_id` and does no lookup (design, "Identity lookup at staging" lost); a blank id is `MISSING_IDS`. The design's sentence "module 1 no longer keeps a per-NPI export registry" is wrong for this repository (`vendor_export_npis`, `ExportNpiRepository` exist, findings 76 to 78); the decision stands anyway and the sentence is a design-doc edit. | `NOT_IN_EXPORT` against `vendor_export_npis` | Step 3; design-doc edit 4 |
| P6 | Ingestion jobs get their own `IngestionJobEnqueuer`, `InboundJobRequest` and `IngestionFinalFailureFilter`, registered by a second `StartupEvent` observer (`BackgroundJobServer.setJobFilters` appends; verified with `javap` at Gate 0). `JobIds.of(job, key, attempt)` is reused: the upload id is the key for inbound jobs, the release id for release jobs. | Extending `JobEnqueuer` and `BatchFinalFailureFilter` in `export/jobs` | Step 1 |
| P7 | The file-ingestion client authenticates like the api-layer client: the Auth0 machine token from `ApiLayerTokenService` as the bearer, plus a Google ID token in `Proxy-Authorization` when `vendor-exchange.file-ingestion.iap-audience` is set. file-ingestion's `UserContextFilter` (its lines 51 to 84) requires an email claim and a DAL role; a machine token has neither, so ask 21 (new) asks the file-ingestion team to accept this service's identity. Until then register answers 403 and the entry fails with reason `FILE_INGESTION_REFUSED`. | A second token service, or a user account's token | Step 1; ask 21 |
| P8 | The DAL batch, slice, operational-value and tenant-configuration clients share the `dal` REST client key and `DalAuthFilter`, and send `requesting-user-id: vendor-exchange-service` and `requesting-organization-id: <tenantId>` like `auth/DalUserClient`. The ranking guardrail reads `GET /tenants/{tenantId}/configurations` and looks for a rule `type = "sourceRanking"` whose ranking names `candor`. | A survivorship-layer call (it exposes no configuration endpoint) | Step 7 |
| P9 | Read-back verdicts come from the operational value's `contributionMap`: `GET /practitioners/ov/{certifyId}` for practitioner fields, `GET /locations/ov/{certifyId}` for location fields. `WON` when the path's winner is `candor:<tenantId>`, `OUTRANKED` when another source, `NOT_APPLIED` when our slice is absent from `contributingSlices`, `UNKNOWN` when the key is absent or no read succeeds before the deadline. The exact path keys are confirmed against a live operational value at Gate 7 and recorded. | Trusting the DAL's 200 (D19) | Step 7 |
| P10 | Slice releases (`first_name`, `last_name` to `CorePractitioner`; `phone`, `fax` to `CoreLocation` + `CoreEntityAddress` + `LocationEntityAddress`) are Step 7. In-place releases (`accepting_new_patients`, `pcp_at_location`, `specialties`, `practice_name`, `location`) are Step 8, because they need the api-layer service identity (ask 19) and junction ids this service does not hold; until Step 8 merges those fields answer 422 `IN_PLACE_RELEASE_UNSUPPORTED` at request time and stay `APPROVED`. Removals (`INVALID` on `npi` or `location` with no replacement) are never released (ask 14). | One release step for every field | Steps 7, 8 |
| P11 | Audit routing by scope: `AuditEventType.Scope` gains `FIELD`; `AuditRepository.write` sends `FIELD` events to `vendor_review_events` and everything else to `vendor_export_events`. `AuditEvent` gains four optional envelope fields (`uploadId`, `fieldRecommendationId`, `certifyPractitionerId`, `releaseId`) written only when set. File events ride the existing batch trail endpoint; a new field trail endpoint reads `vendor_review_events`. | A second event record and repository in `ingestion/` | Step 0 |
| P12 | Release serialisation per practitioner without a lock collection: the job id is unique per release (`JobIds.of("release", releaseId, 1)`); `ReleaseJob` claims its release's fields (`RELEASE_REQUESTED → RELEASING`) and then counts `RELEASING` fields of the same practitioner with another `releaseId`; a non-zero count un-claims and throws for a JobRunr retry. Each job counts after its own claim, so at least one of two concurrent jobs sees the other. | The design's "same job id per practitioner, later requests run after it": JobRunr ignores an enqueue whose id already exists in storage, so the second release would never run | Step 7; design-doc edit 7 |
| P13 | The read-back deadline is a scheduled `ReleaseReadBackDeadlineJob` per release at `writtenAt + releaseReadBackDeadlineMinutes` (deterministic id, deleted by the ov-generated handler on success), the pattern of `DeadlineCheckJob` and `IngestionDeadlineCheckJob`. The reconciler covers inbound entries only. | The reconciler scanning `RELEASING` fields, which needs an index over `updatedAt` on a 1.2M-document collection | Step 7; design-doc edit 8 |
| P14 | `releaseWorkerConcurrency` is the JobRunr worker count (`quarkus.jobrunr.background-job-server.worker-count`, 4) and is not a separate key; JobRunr OSS has no per-job-type limiter. | A limiter of our own | Step 0 |

Two consequences found while writing the plan, not covered by a design decision:

- **The first upload acknowledges the export batch.** `ExportBatch.acknowledgedAt`, `inboundBatchId` and the event `EXPORT_BATCH_ACKNOWLEDGED` exist since CP-40438 for this moment. The arrival handler writes them in the claim transaction when the entry is `RECEIVED` (Task 2.2); `inboundBatchId` holds the `uploadId`; the event's required keys become `uploadId`, `vendorBatchId`, `daysToResponse` as the design's table says (today `inboundBatchId`, finding 91; recorded as a contract change).
- **Mark-consumed is a courtesy, not a gate.** `POST /v1/ingestion-complete/{id}` is idempotent on file-ingestion's side; `StageRowsJob` calls it after `STAGED` is committed and logs any failure once, so a flaky file-ingestion never fails a staged entry.

## Steps, pull requests and estimates

One engineer, days of focused work. Each step ends with CI green, `make openapi` committed where a path changed, and one finding in `docs/decisions.md`.

| Step | Pull request title | Design steps | Est. |
| --- | --- | --- | --- |
| 0 | Ingestion foundations: config, collections, indexes, audit scope and types, permissions, metric names, lint rules, clients | all | 1.5 days |
| 1 | Inbound file registry, archive, request-ingestion job, deadline check | 3, 4 (fallback) | 3 days |
| 2 | Arrival handler: match, claim, acknowledge, unmatched uploads, operator reads | 1, 2 | 2 days |
| 3 | StageRowsJob: wide rows, field recommendations, counts on the entry, checkpoint | 5 | 3 days |
| 4 | Completion handler, inbound reconciler, final-failure filter | 4 | 2 days |
| 5 | Review API: list, summary, practitioner and NPI views, decision, bulk, field trail | 6 | 3 days |
| 6 | Rejected-value memory | 7 | 1 day |
| 7 | Release of slice fields: endpoints, ranking guard, release job, ov-generated handler, read-back deadline | 8 | 5 days |
| 8 | In-place releases through the api-layer | 8 | 4 days |
| 9 | Whole-lane proof, gauges, runbook (alerts A1 to A16), hand-over | all | 2 days |

Total about 26.5 working days. Steps 5 and 6 can run in parallel with Step 4 by a second engineer; Step 8 can run in parallel with Step 9 and can slip without blocking the lane (P10). Nothing else is parallel.

## Review Focus

Inputs the design implies but no single section tests, most likely to bite first. Each has its test in the task named.

1. **Two api instances receive the same arrival message at once.** Expected: one entry `RECEIVED`, one job, both deliveries answered 2xx. Pinned by `InboundFileRepositoryIT.claimUnderConcurrentWritersCreatesOneEntry` (Task 1.2).
2. **A vendor row with a blank `certify_location_id`.** Expected: practitioner-level fields staged, the location part counted `MISSING_IDS`, the row counted once. Pinned by `StageRowsJobIT.blankLocationIdStagesPractitionerFieldsOnly` (Task 3.4).
3. **A decision request retried after a client timeout, same version.** Expected: 409 with the current document showing the first request's decision; one audit event, not two. Pinned by `ReviewServiceIT.retriedDecisionIs409WithOwnDecisionVisible` (Task 5.2).
4. **Two release jobs for one practitioner run at once.** Expected: one writes the slice, the other backs off and retries, the slice is never written from two merges. Pinned by `ReleaseJobIT.concurrentJobsForOnePractitionerSerialise` (Task 7.4).
5. **StageRowsJob crashes after the bulk write and before the checkpoint.** Expected: the retry redoes the chunk, every document lands on its existing `_id`, disposition counts are not doubled. Pinned by `StageRowsJobIT.retryAfterCrashBeforeCheckpointDoesNotDoubleCount` (Task 3.4).

A sixth, pinned where it belongs: an `mdm.slice.ov_generated` event for a run a roster write triggered reaches the handler despite the subscription filter; expected: acknowledged, nothing changes (`OvGeneratedHandlerIT.notOursIsAcknowledged`, Task 7.3).

## File structure

```
src/main/java/com/certifyos/vendor_exchange/
  config/VendorExchangeConfig.java          + Ingestion ingestion(), FileIngestion fileIngestion()
  config/MongoIndexes.java                  + ingestion indexes, the one TTL index
  persistence/Collections.java              + fieldRecommendations, unmatchedUploads, rejectedValues, reviewEvents, all()
  persistence/Ids.java                      + uploadId, externalRef, fieldRecommendationId, rejectedValueId, releaseId
  audit/AuditEventType.java                 + Scope.FIELD; 13 INBOUND_* types, 7 FIELD_* types
  audit/AuditDetailContract.java            + the design's per-event keys; EXPORT_BATCH_ACKNOWLEDGED keys corrected
  audit/AuditEvent.java                     + uploadId, fieldRecommendationId, certifyPractitionerId, releaseId (optional)
  audit/AuditRepository.java                + routing by scope, trailForField, findForRelease; ALERT_KEYS additions
  audit/Actors.java                         + INGESTION = "system:vendor-ingestion"
  auth/Permission.java                      + REVIEW_READ, REVIEW_DECIDE, REVIEW_RELEASE
  metrics/VendorExchangeMetrics.java        + the design's vendor_ingestion.* names
  clients/ArchiveStore.java, GcsArchiveStore.java, ArchiveSourceMissingException.java
  clients/FileIngestionClient.java, FileIngestionIapFilter.java, FileIngestionRegisterRequest.java,
          FileIngestionRegisterResponse.java, FileIngestionStatus.java, FileIngestionCounts.java,
          FileIngestionRow.java, FileIngestionCell.java
  clients/DalBatchClient.java, DalBatchRequest.java, DalOperation.java, DalOperationResult.java
  clients/DalSliceClient.java, Slice.java                       GET /core/practitioner|locations|entity-addresses/{sourceType}/{crosswalkId}
  clients/DalOperationalValueClient.java, OperationalValue.java GET /practitioners/ov/{id}, /locations/ov/{id}
  clients/DalTenantConfigClient.java, TenantConfigurationPage.java
  clients/ApiLayerClient.java (Step 8)      + PUT /practitioners/{id}/locations, PATCH …/group-practitioner-location-specialties/specialty,
                                              PUT /locations/update, GET /tenant-specialties; request records beside it
  ingestion/inbound/InboundFile.java, InboundFileState.java, InboundNames.java, InboundFileRepository.java,
                    InboundLifecycle.java, UnmatchedUpload.java, UnmatchedUploadRepository.java
  ingestion/events/ArrivalEvent.java, ArrivalHandler.java, ArrivalResource.java
  ingestion/events/CompletionEvent.java, CompletionHandler.java, CompletionResource.java
  ingestion/events/OvGeneratedEvent.java, OvGeneratedHandler.java, OvGeneratedResource.java
  ingestion/jobs/InboundJobRequest.java, IngestionJobEnqueuer.java, InboundJobSupport.java, IngestionLogContext.java
  ingestion/jobs/RequestIngestionJob.java (+Request), IngestionDeadlineCheckJob.java (+Request), StageRowsJob.java (+Request)
  ingestion/jobs/ReleaseJob.java (+Request), ReleaseReadBackDeadlineJob.java (+Request)
  ingestion/jobs/InboundReconcilerJob.java, IngestionFinalFailureFilter.java, IngestionFinalFailureFilterRegistration.java
  ingestion/staging/CandorRecsMapping.java, FieldLevel.java, WideRow.java, RowDisposition.java, ReviewStatus.java,
                    FieldRecommendation.java, FieldRecommendationRepository.java, ChunkBuilder.java, RowStager.java
  ingestion/review/ReviewService.java, ValueNormalizer.java, RejectedValue.java, RejectedValueRepository.java
  ingestion/release/ReleaseService.java, RankingGuard.java, ReleaseLifecycle.java, Crosswalks.java, NameSound.java,
                    SliceMerge.java, ReadBack.java, ReadBackService.java
  ingestion/release/JunctionResolver.java, InPlaceWriter.java (Step 8)
  ingestion/api/VendorReviewResource.java, VendorReleaseResource.java, VendorIngestionOpsResource.java,
                ReviewViews.java, InboundViews.java, Page.java
  ingestion/IngestionStateGauges.java
src/test/java/...                          unit tests beside the class (names, states, mapping, wide row, chunk builder,
                                           normaliser, crosswalks, name sound, slice merge, read back)
src/integrationTest/java/...               ITs per repository, handler, job and resource; IngestionLaneIT on the worker role
src/integrationTest/.../ingestion/InboundFixtures.java    batches in DELIVERED with inbound entries in any state; field documents
src/integrationTest/.../clients/WireMockUpstreams.java    + quarkus.rest-client.file-ingestion.url
src/integrationTest/.../TestConfig.java                   + archive bucket, file-ingestion url, mapping id
gradle/libs.versions.toml                                + commons-codec
docs/decisions.md, docs/runbook.md, HANDOFF.md, README.md, AGENTS.md, openapi/openapi.yaml
```

---

## Step 0: ingestion foundations (1.5 days)

Branch `feature/CP-40437-step-0-foundations`. No behaviour yet: every shared piece the lane needs, each guarded by a test, so Steps 1 to 8 touch only `ingestion/`.

### Task 0.1: Configuration keys and test profile values

**Files:**
- Modify: `config/VendorExchangeConfig.java`, `src/main/resources/application.properties`, `src/integrationTest/.../TestConfig.java`
- Test: `src/test/.../config/VendorExchangeConfigTest.java`

**Interfaces:**
- Produces `VendorExchangeConfig.Ingestion ingestion()`: `String archiveBucket()` (required), `int ingestionDeadlineHours()` 6, `int stageChunkRows()` 500, `int reviewPageLimitMax()` 200, `int rejectedValueTtlMonths()` 12, `int releaseReadBackDeadlineMinutes()` 30, `int releaseBulkMax()` 5000, `int inboundStaleMinutes()` 30, `String completenessSignal()` `"DATA_FILE"`.
- Produces `VendorExchangeConfig.FileIngestion fileIngestion()`: `String url()` (required), `String consumerIdentifier()` `"vendor-ingestion"`, `Optional<String> mappingId()`, `Optional<String> iapAudience()`. One vendor in MVP, so the design's `fileIngestion.mappingId.<vendor>` is the single key `mapping-id`; a second vendor adds a map (recorded).
- Properties: `vendor-exchange.ingestion.archive-bucket=${INGESTION_ARCHIVE_BUCKET}`, `vendor-exchange.file-ingestion.url=${FILE_INGESTION_URL}`, defaults as `${INGESTION_DEADLINE_HOURS:6}`, `${STAGE_CHUNK_ROWS:500}`, `${REVIEW_PAGE_LIMIT_MAX:200}`, `${REJECTED_VALUE_TTL_MONTHS:12}`, `${RELEASE_READBACK_DEADLINE_MINUTES:30}`, `${RELEASE_BULK_MAX:5000}`, `${INBOUND_STALE_MINUTES:30}`, `${COMPLETENESS_SIGNAL:DATA_FILE}`, `vendor-exchange.file-ingestion.mapping-id=${FILE_INGESTION_MAPPING_ID:}`, `vendor-exchange.file-ingestion.iap-audience=${FILE_INGESTION_IAP_AUDIENCE:}`; REST client block `quarkus.rest-client.file-ingestion.url=${vendor-exchange.file-ingestion.url}`, scope Singleton, connect 5000, read 120000 (the NDJSON stream of a large file runs for minutes).

- [ ] **Step 1: Write the failing test** `VendorExchangeConfigTest.ingestionDefaultsMatchTheDesign` (load with only the two required keys; assert 6, 500, 200, 12, 30, 5000, 30, `"DATA_FILE"`, `"vendor-ingestion"`, empty `mappingId`) and `archiveBucketIsRequired` (load without it throws). `load` is the test's existing SmallRye builder helper.
- [ ] **Step 2: Run** `./gradlew test --tests '*VendorExchangeConfigTest*'` → FAIL: no method `ingestion()`.
- [ ] **Step 3: Implement** the two nested interfaces with `@WithDefault` on every value but the two required ones; the properties above; `TestConfig.common()` gains `vendor-exchange.ingestion.archive-bucket=test-archive-bucket`, `vendor-exchange.file-ingestion.url=http://localhost:0`, `vendor-exchange.file-ingestion.mapping-id=map-candor-recs-v1`.
- [ ] **Step 4: Run** the test → PASS; `./gradlew integrationTest --tests '*BootIT*'` → both roles boot.
- [ ] **Step 5: Commit** `feat(config): ingestion and file-ingestion settings`.

### Task 0.2: Collections, ids, indexes, the TTL rule (P4)

**Files:**
- Modify: `persistence/Collections.java`, `persistence/Ids.java`, `config/MongoIndexes.java`
- Test: `src/test/.../persistence/IdsTest.java`, `src/integrationTest/.../config/MongoIndexesIT.java`

**Interfaces:**
- `Collections`: `fieldRecommendations()` → `field_recommendations`, `unmatchedUploads()` → `vendor_unmatched_uploads`, `rejectedValues()` → `vendor_rejected_values`, `reviewEvents()` → `vendor_review_events`, and `List<MongoCollection<Document>> all()` naming every service collection (the TTL test iterates it).
- `Ids`: `uploadId(bucket, objectName, generation)` = `<bucket>|<objectName>|<generation>`; `externalRef(exportBatchId, generation)` = `<exportBatchId>|<generation>`; `fieldRecommendationId(tenantId, source, exportBatchId, certifyPractitionerId, certifyLocationId /*nullable*/, field)` = legs joined with `|`, location `-` when null; `rejectedValueId(tenantId, certifyPractitionerId, certifyLocationId /*nullable*/, field, normalizedValue)` = the four readable legs then `sha256Hex(normalizedValue)` (design, step 7); `releaseId()` = `rel-<uuid v7>`. Every builder refuses a null or blank leg and a leg containing `|` with `IllegalArgumentException`.
- Indexes, named, in `MongoIndexes.createAll()`: on batches `batches_inbound_state_updated` `(inboundFiles.state, inboundFiles.updatedAt)` and `batches_inbound_external_ref` `(inboundFiles.externalRef)` partial `exists`; on `field_recommendations` `field_recs_open_list` `(tenantId, reviewStatus, field, stagedAt, _id)` partial `reviewStatus in [PENDING, SKIPPED]`, `field_recs_batch_counts` `(tenantId, exportBatchId, reviewStatus, field)`, `field_recs_practitioner` `(tenantId, practitioner.certifyPractitionerId, exportBatchId)`, `field_recs_npi` `(tenantId, practitioner.npi, exportBatchId)`, `field_recs_release` `(tenantId, goldenRecordUpdate.releaseId)` partial `exists` (the release read), `field_recs_open_list_any_field` `(tenantId, reviewStatus, stagedAt, _id)` partial `in [PENDING, SKIPPED]` (the list without a `field` filter; a plan addition); on `vendor_rejected_values` `rejected_values_ttl` `(expiresAt)` `expireAfter(0, SECONDS)` and `rejected_values_practitioner` `(tenantId, certifyPractitionerId)`; on `vendor_unmatched_uploads` `unmatched_resolution` `(resolution.kind, receivedAt)`; on `vendor_review_events` `review_events_by_field` `(fieldRecommendationId, _id)`, `review_events_by_release` `(tenantId, releaseId, _id)` partial `exists` (the `_id` leg serves the sort of the release trail read), `review_events_by_tenant` `(tenantId, occurredAt)`. The reconciler index is on `updatedAt`, not the design's `receivedAt`: "without progress" is a question about the last write (design-doc edit 6).

- [ ] **Step 1: Write the failing tests** in `IdsTest`: the design's example `org-xyz|candor|org-xyz-candor-2026-10-001|cert-000123|loc-7781|phone`; the practitioner-level form with `-`; `externalRef("b","1760520000123456")`; `rejectedValueId(...)` ends with the 64-hex SHA-256 of `3036790177`; a leg with `|` throws.
- [ ] **Step 2: Run** `./gradlew test --tests '*IdsTest*'` → FAIL.
- [ ] **Step 3: Implement** the builders, accessors and indexes.
- [ ] **Step 4: Rewrite** `MongoIndexesIT.noIndexExpiresDocuments` to iterate `collections.all()` and allow `expireAfterSeconds` only for `vendor_rejected_values/rejected_values_ttl`, with the comment naming D16 and P4; add `ingestionIndexesExist` asserting each name and key above and `expireAfterSeconds == 0` on the TTL index.
- [ ] **Step 5: Run** `./gradlew test --tests '*IdsTest*' && ./gradlew integrationTest --tests '*MongoIndexesIT*'` → PASS.
- [ ] **Step 6: Commit** `feat(persistence): ingestion collections, ids and indexes; one TTL index allowed`.

### Task 0.3: Audit scope, types, envelope fields, routing (P11); actors; permissions; metric names

**Files:**
- Modify: `audit/AuditEventType.java`, `audit/AuditDetailContract.java`, `audit/AuditEvent.java`, `audit/AuditRepository.java`, `audit/Actors.java`, `auth/Permission.java`, `metrics/VendorExchangeMetrics.java`, `src/test/.../ArchitectureTest.java` (`auditIsAppendOnly` covers `reviewEvents()`)
- Test: `audit/AuditDetailContractTest.java`, `audit/AuditEventTest.java`, `src/integrationTest/.../audit/AuditRepositoryIT.java`, `auth/PermissionFilterTest.java`, `metrics/VendorExchangeMetricsTest.java`

**Interfaces:**
- `AuditEventType.Scope` adds `FIELD`. New types, scope `BATCH` unless noted: `INBOUND_FILE_RECEIVED`, `INBOUND_FILE_DUPLICATE_LOGGED`, `INBOUND_FILE_UNMATCHED` (`SERVICE`), `INBOUND_UPLOAD_ATTACHED`, `INBOUND_UPLOAD_DISCARDED`, `INBOUND_FILE_ARCHIVED`, `INBOUND_FILE_INGESTION_REQUESTED`, `INBOUND_FILE_PARSED`, `INBOUND_FILE_INGESTION_FAILED`, `INBOUND_FILE_STAGED`, `INBOUND_FILE_FAILED`, `INBOUND_FILE_RECONCILED`; scope `FIELD`: `FIELD_APPROVED`, `FIELD_REJECTED`, `FIELD_SKIPPED`, `FIELD_ROLLED_BACK`, `FIELD_RELEASE_REQUESTED`, `FIELD_RELEASED`, `FIELD_RELEASE_FAILED`.
- Required detail keys, copied from the design's "Per-event contracts" table: RECEIVED `objectName, generation, vendorBatchId, eventTime` (`overwroteGeneration` optional); ACKNOWLEDGED `uploadId, vendorBatchId, daysToResponse` (replaces `inboundBatchId`); DUPLICATE_LOGGED `objectName, generation, firstUploadId, firstUploadState`; UNMATCHED `reason, objectName, generation` (`parsedLegs` optional); ATTACHED `exportBatchId, uploadId`; DISCARDED `reason, uploadId`; ARCHIVED `archivePath, bytes, copyResult`; INGESTION_REQUESTED `ingestionId, externalRef, mappingId, registerResponse, deadlineAt`; PARSED `status, populatedRows, validRows, invalidRows, completionSource, waitSeconds`; INGESTION_FAILED `reason, completionSource`; STAGED `dispositions, written, chunks, durationMs, practitionerFieldDisagreements`; FAILED `failedStep, failureReason, lastError, jobAttempts`; RECONCILED `state, ageMinutes, action`; APPROVED, REJECTED, SKIPPED `field, fromStatus, version` (`certifyLocationId`, `comment` optional; REJECTED also `rejectedValueId, rejectionCount`); ROLLED_BACK `reason, version`; RELEASE_REQUESTED `releaseId, bulk`; RELEASED `outcome, winningSource, writePath, resource, crosswalkId, completionSource, readBackSeconds`; RELEASE_FAILED `reason, error, jobAttempts`.
- `AuditEvent` gains nullable components `uploadId`, `fieldRecommendationId`, `certifyPractitionerId`, `releaseId` and builder methods `upload(String)`, `field(String fieldRecommendationId, String certifyPractitionerId)`, `release(String)`; `build()` refuses a `FIELD` event without `fieldRecommendationId`; `toDocument` writes the four only when set; `fromDocument` reads them. `AuditEvent.Builder.batch(String exportBatchId)` (attempt left null) joins the existing `batch(id, attempt)`.
- `AuditRepository.write(session, event)` and `write(event)` choose `collections.reviewEvents()` for `Scope.FIELD`, else `events()`; new reads `trailForField(String fieldRecommendationId, int limit)` (sorted by `_id`) and `findForRelease(String tenantId, String releaseId, int limit)`. `ALERT_KEYS` adds `failureReason`, `outcome`, `status`, `action`, `writePath`. The `AUDIT` log line carries the envelope ids as today and never a detail value outside `ALERT_KEYS`.
- `Actors.INGESTION = "system:vendor-ingestion"` (design: one system actor for handlers, jobs and the reconciler).
- `Permission`: `REVIEW_READ("vendor-review", "read")`, `REVIEW_DECIDE("vendor-review", "decide")`, `REVIEW_RELEASE("vendor-review", "release")`.
- `VendorExchangeMetrics`: one constant and one method per design name. Counters: `arrivalReceived(tenant, vendor)`, `arrivalDuplicateLogged(tenant, vendor)`, `arrivalUnmatched(tenant, reason)`, `arrivalIgnoredEventType(eventType)`, `registerResult(result)` (`201|200|4xx|5xx`), `completionSource(source)`, `completionStatus(status)`, `stageRows(tenant, disposition, long)`, `stageDocuments(tenant, long)`, `stageStream429()`, `reviewDecision(tenant, action)`, `reviewConflict(tenant)`, `releaseJob(tenant, result)`, `releaseFailedReason(reason)`, `releaseOutcome(tenant, outcome)`, `ovEventNotOurs()`, `ingestionJobRetry(job)`, `ingestionJobFinalFailure(job)`. Timers: `archiveDuration(tenant, Duration)`, `stageChunkDuration(Duration)`. Gauges through `registerGauge` (Step 9): `inbound.by_state{state}`, `inbound.age_seconds{state}`, `parse.wait_seconds{tenant}`, `review.pending{tenant, field}`, `review.pending_age_days{tenant}`, `release.readback_seconds{tenant}`. `dlq.depth` is a Pub/Sub platform metric, named in the runbook only.

- [ ] **Step 1: Write the failing tests**: `AuditDetailContractTest.everyIngestionTypeHasRequiredKeys` (each new type non-empty; `ACKNOWLEDGED` requires `uploadId` and no longer `inboundBatchId`); `AuditEventTest.fieldEventNeedsAFieldId` and `envelopeFieldsRoundTrip`; `AuditRepositoryIT.fieldEventsLandInReviewEvents` (a `FIELD_APPROVED` write appears in `vendor_review_events` only, with `fieldRecommendationId`; `trailForField` returns it); `PermissionFilterTest.reviewPermissionsResolve`; `VendorExchangeMetricsTest.ingestionNamesRegister` (each method registers a meter named by its constant with the listed tags, none named `npi`).
- [ ] **Step 2: Run** the five test classes → FAIL on missing symbols.
- [ ] **Step 3: Implement** as listed; `auditIsAppendOnly` in `ArchitectureTest` extends to `reviewEvents()`.
- [ ] **Step 4: Run** `./gradlew test && ./gradlew integrationTest --tests '*Audit*'` → PASS (`AuditLogLineIT`, `AuditReadIT` still green).
- [ ] **Step 5: Commit** `feat(audit,auth,metrics): field-scoped audit events in vendor_review_events, ingestion types and keys, review permissions, metric names`.

### Task 0.4: Lint rules, outbound clients, archive store, WireMock

**Files:**
- Modify: `src/test/.../ArchitectureTest.java`, `src/integrationTest/.../clients/WireMockUpstreams.java`, `src/integrationTest/.../auth/WireMockDal.java`
- Create: the `clients/` files of the file structure except the Step 8 api-layer additions
- Test: `src/integrationTest/.../clients/FileIngestionClientIT.java`, `DalClientsIT.java`; `src/test/.../clients/FileIngestionContractsTest.java`

**Interfaces (produced, used by Steps 1, 3, 7):**
```java
@RegisterRestClient(configKey = "file-ingestion") @RegisterProvider(FileIngestionIapFilter.class) @RegisterProvider(ApiLayerAuthFilter.class)
public interface FileIngestionClient {
  String TENANT_HEADER = "tenant-id"; String IDENTIFIER_HEADER = "identifier";
  @POST @Path("/v1/ingestion") Response register(@HeaderParam(TENANT_HEADER) String tenantId, @HeaderParam(IDENTIFIER_HEADER) String identifier, FileIngestionRegisterRequest body); // 201 new, 200 existing
  @GET @Path("/v1/ingestion/{id}") FileIngestionStatus status(tenantId, identifier, @PathParam("id") String ingestionId);
  @GET @Path("/v1/ingestion-rows/{id}") @Produces("application/x-ndjson") InputStream rows(tenantId, identifier, @PathParam("id") String ingestionId, @QueryParam("after") String after); // 429 → WebApplicationException
  @POST @Path("/v1/ingestion-complete/{id}") Response markConsumed(tenantId, identifier, @PathParam("id") String ingestionId);
}
record FileIngestionRegisterRequest(String mappingId, String filename, String sourceUri, String externalRef, Map<String, String> meta) {}  // sourceUri is ask 9; today's RegisterRequest has no such field
record FileIngestionRegisterResponse(String ingestionId, String status) {}
record FileIngestionStatus(String ingestionId, String status, FileIngestionCounts counts, Instant registeredAt, Instant terminalAt, String invalidReason) {}
record FileIngestionCounts(Long populatedRows, Long validRows, Long invalidRows) {}
record FileIngestionRow(String sheet, Integer row, String status, Map<String, FileIngestionCell> values, List<String> warnings) {}  // RowView.of in file-ingestion
record FileIngestionCell(String value, String status, List<String> problems) {}

@RegisterRestClient(configKey = "dal") @RegisterProvider(DalAuthFilter.class) @ClientHeaderParam(name = "requesting-user-id", value = "vendor-exchange-service")
public interface DalBatchClient { @POST @Path("/batch") Response execute(@HeaderParam("requesting-organization-id") String tenantId, DalBatchRequest body); }
record DalBatchRequest(Boolean transaction, List<DalOperation> operations) {}                       // DAL BatchRequest
record DalOperation(String resource, String alias, String operation, Map<String, Object> body, Map<String, Object> filters, List<String> returning) {} // DAL OperationRequest
record DalOperationResult(String alias, Boolean success, Map<String, Object> result, String error) {}
public interface DalSliceClient {   // same key, filter and user header
  @GET @Path("/core/practitioner/{sourceType}/{crosswalkId}") Slice practitioner(tenantId, @PathParam sourceType, @PathParam crosswalkId);   // 404 → WebApplicationException
  @GET @Path("/core/locations/{sourceType}/{crosswalkId}") Slice location(...);  @GET @Path("/core/entity-addresses/{sourceType}/{crosswalkId}") Slice entityAddress(...); }
record Slice(String certifyId, String sourceType, String crosswalkId, Map<String, Object> data, Instant updatedAt) {}
public interface DalOperationalValueClient {
  @GET @Path("/practitioners/ov/{entityId}") OperationalValue practitioner(@HeaderParam("requesting-organization-id") String tenantId, @PathParam("entityId") String certifyId);
  @GET @Path("/locations/ov/{entityId}") OperationalValue location(tenantId, certifyId); }
record OperationalValue(String certifyId, String tenantId, Map<String, Object> data, List<Map<String, Object>> contributingSlices, Map<String, Object> contributionMap) {}
public interface DalTenantConfigClient { @GET @Path("/tenants/{tenantId}/configurations") TenantConfigurationPage list(tenantId, @PathParam("tenantId") String pathTenantId, @QueryParam("page") int page, @QueryParam("size") int size); }
record TenantConfigurationPage(List<Map<String, Object>> data, Long totalCount) {}

public interface ArchiveStore {
  record Copied(String path, long bytes, boolean alreadyExisted) {}
  Copied copy(String sourceBucket, String sourceObject, long sourceGeneration, String targetBucket, String targetObject); // create-only; 412 → alreadyExisted; missing source → ArchiveSourceMissingException
}
```
`GcsArchiveStore` builds `Storage.CopyRequest.newBuilder().setSource(BlobId.of(bucket, object, generation)).setSourceOptions(BlobSourceOption.generationMatch()).setTarget(BlobId.of(archive, name), BlobTargetOption.doesNotExist()).build()`, `storage.copy(request).getResult()`, maps `StorageException` 412 to `alreadyExisted` (size read with `storage.get`), 404 to `ArchiveSourceMissingException`; client built on first use like `GcsVendorBucket`; no test until the live verification (finding 80e pattern). Every record is `@JsonIgnoreProperties(ignoreUnknown = true)` and copies collections in its compact constructor.

- [ ] **Step 1: Extend ArchitectureTest**: `sharedPackagesDoNotDependOnFeatures` forbids `..vendor_exchange.ingestion..` as well; `operatorEndpointsDeclarePermission` matches `..ingestion.api..` too; add `exportDoesNotDependOnIngestion`. Run `./gradlew test --tests '*ArchitectureTest*'` → PASS.
- [ ] **Step 2: Write the failing ITs**. `FileIngestionClientIT` (api profile, `WireMockUpstreams`, `ApiLayerTokenService` mocked to `"tok"`): `registerNewIs201` (headers `tenant-id`, `identifier`, `Authorization: Bearer tok`; body `equalToJson` of the design's step-3 request), `registerRepeatIs200`, `mappingUnknownIs422`, `rowsStreamYieldsOneRowPerLine` (two NDJSON rows read through `ObjectMapper.readerFor(FileIngestionRow.class).readValues(stream)`; `after` sent), `rowsRefused429Propagates`, `markConsumedIsOk`. `DalClientsIT` (`WireMockDal`): `batchSendsTransactionAndHeaders`, `sliceReadMapsData`, `sliceMissingIs404`, `practitionerAndLocationOvRead`, `tenantConfigurationsPage`. `FileIngestionContractsTest`: `FileIngestionRow` deserialises `RowView`'s shape, unknown properties ignored.
- [ ] **Step 3: Run** → FAIL (no client). **Step 4: Implement**; `WireMockUpstreams.start()` adds `quarkus.rest-client.file-ingestion.url`; `FileIngestionIapFilter` mirrors `ApiLayerIapFilter` with `cfg.fileIngestion().iapAudience()`. **Step 5: Run** the ITs and `make lint` → PASS, clean.
- [ ] **Step 6: Commit** `feat(clients): file-ingestion, DAL batch, slice, operational-value and tenant-config clients, archive store; ingestion lint rules`.

### Gate 0

- [ ] CI green; `make openapi` shows no diff.
- [ ] `docs/decisions.md` finding 96, "CP-40437 Step 0": P2, P4, P6, P7, P8, P11, P14 and the verified facts: `BackgroundJobServer.setJobFilters` appends (`javap -c`), `Storage.CopyRequest` options exist in 2.45.0 (`javap`), file-ingestion's `UserContextFilter` needs an email claim (its lines 55 to 58) and `RegisterRequest` has no `sourceUri` (ask 9 open), DAL `BatchRequest`/`OperationRequest` field names, `EXPORT_BATCH_ACKNOWLEDGED` key change.
- [ ] Merge; next branch from `main`.

---

## Step 1: inbound file registry, archive, request-ingestion job, deadline check (3 days)

Branch `feature/CP-40437-step-1-inbound-registry`. Design step 3 and the fallback of step 4, built before the handler that feeds it. Entries are created by `InboundFixtures` in tests; Step 2 adds the real producer.

### Task 1.1: `InboundFile`, `InboundFileState`, `InboundNames`, fixtures

**Files:**
- Create: `ingestion/inbound/InboundFile.java`, `InboundFileState.java`, `InboundNames.java`, `ingestion/events/ArrivalEvent.java`; `src/integrationTest/.../ingestion/InboundFixtures.java`
- Test: `src/test/.../ingestion/inbound/InboundFileStateTest.java`, `InboundNamesTest.java`, `InboundFileTest.java`, `src/test/.../ingestion/events/ArrivalEventTest.java`

**Interfaces:**
```java
public record ArrivalEvent(String bucketId, String objectId, String objectGeneration, String eventType, Instant eventTime, String overwroteGeneration) {
  public static Optional<ArrivalEvent> fromAttributes(Map<String, String> attributes); // empty when bucketId, objectId or objectGeneration is missing
  public boolean isFinalize(); /* OBJECT_FINALIZE */ public String uploadId(); /* Ids.uploadId(bucketId, objectId, objectGeneration) */ }
public enum InboundFileState { RECEIVED, ARCHIVING, INGESTION_REQUESTED, PARSED, STAGING, STAGED, DUPLICATE_LOGGED, INGESTION_FAILED, FAILED;
  boolean canTransitionTo(InboundFileState next); // RECEIVED→ARCHIVING|FAILED; ARCHIVING→INGESTION_REQUESTED|FAILED; INGESTION_REQUESTED→PARSED|INGESTION_FAILED|FAILED; PARSED→STAGING|FAILED; STAGING→STAGED|FAILED; FAILED→RECEIVED|PARSED (operator retry, later); others terminal
  boolean isTerminal(); static Set<InboundFileState> reconcilable(); /* RECEIVED, ARCHIVING, PARSED, STAGING */ }
public record InboundFile(String uploadId, String generation, String vendorBatchId, String fileDate, InboundFileState state, int attempt, String jobId,
    String archivePath, Long archiveBytes, String externalRef, String ingestionId, Parse parse, Checkpoint checkpoint, Map<String, Long> dispositions, Written written,
    String failedStep, String failureReason, String lastError, String overwroteGeneration, Instant eventTime,
    Instant receivedAt, Instant requestedAt, Instant parsedAt, Instant stagedAt, Instant updatedAt) {
  public record Parse(String status, Long populatedRows, Long validRows, Long invalidRows, String completionSource, String reason) {}
  public record Checkpoint(String after, Long chunks) {} public record Written(Long fieldRecommendations, Long confirmedFields, Long unverifiedFields, Long practitionerFieldDisagreements) {}
  public String bucket(); public String objectName();            // parsed from uploadId (design: "read from it, not stored again")
  public static InboundFile received(ArrivalEvent event, InboundNames.Parsed name, Instant now); public static InboundFile duplicateLogged(ArrivalEvent event, InboundNames.Parsed name, Instant now);
  public Document toDocument(); public static InboundFile fromDocument(Document doc); }
public final class InboundNames {
  public record Parsed(String folderTenant, String fileTenant, String exportBatchRef, String vendorBatchId, String fileDate, String fileName) {}
  public static Optional<Parsed> parse(String objectName);  // to/<t>/<t>_<batchRef>_<vendorBatchId>_<yyyyMMdd>.csv; empty on any mismatch
  public static String archiveObject(String tenantId, String exportBatchId, String generation, String fileName); // <tenantId>/<exportBatchId>/<generation>/<fileName>
  public static final String TO_PREFIX = "to/"; public static final String UNPARSEABLE_NAME = "UNPARSEABLE_NAME", UNKNOWN_BATCH = "UNKNOWN_BATCH", TENANT_MISMATCH = "TENANT_MISMATCH", BATCH_NOT_DELIVERED = "BATCH_NOT_DELIVERED"; }
```
`jobId` is the JobRunr job currently responsible for the entry (the request job from the claim, the deadline check once requested, the stage-rows job once parsed). Dispositions are the design's five: `STAGED`, `DUPLICATE`, `SCHEMA_INVALID`, `MISSING_IDS`, `UNSUPPORTED_ENTITY`.

- [ ] **Step 1: Write the failing tests**: `ArrivalEventTest.readsTheSixAttributes`, `missingGenerationIsEmpty`, `deleteEventIsNotFinalize`; `InboundNamesTest.parsesTheDesignExample` (`to/org-xyz/org-xyz_org-xyz-candor-2026-10-001_CB-2026-10-001_20261015.csv`), `rejectsFromPrefixAndBadDate`, `archiveObjectShape`; `InboundFileStateTest.edgesMatchTheDesign`; `InboundFileTest.roundTripsThroughBson` (every field; absent ones left out; the design's post-staging sample reads back field for field) and `bucketAndObjectComeFromUploadId`.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** `InboundFixtures.deliveredBatchWithEntry(transactions, batches, inboundFiles, tenant, state)` builds a `DELIVERED` batch through `EgressFixtures.batchIn` and pushes one entry in the given state with the fields earlier states would have written. **Step 4: Run** → PASS. **Step 5: Commit** `feat(ingestion): inbound file entry, states and names`.

### Task 1.2: `InboundFileRepository`

**Files:**
- Create: `ingestion/inbound/InboundFileRepository.java`
- Test: `src/integrationTest/.../ingestion/inbound/InboundFileRepositoryIT.java`

**Interfaces:**
```java
@ApplicationScoped public class InboundFileRepository {
  public record Claim(boolean claimed, InboundFileState state, int uploadCount) {}
  public Claim claim(ClientSession s, String exportBatchId, InboundFile entry);   // read the batch in the session; state = inboundFiles empty ? RECEIVED : DUPLICATE_LOGGED; updateOne({_id, "inboundFiles.uploadId": {$ne}}, {$push}); matched 0 → claimed=false
  public record Located(ExportBatch batch, InboundFile entry) {}
  public Optional<Located> findByUploadId(String uploadId); public Optional<Located> findByExternalRef(String externalRef);
  public List<Located> findStale(Set<InboundFileState> states, Instant olderThan, int limit);   // batches_inbound_state_updated, oldest first
  public boolean transition(ClientSession s, String exportBatchId, String uploadId, InboundFileState from, InboundFileState to, Instant now, Bson entryUpdates); // $elemMatch {uploadId, state: from}; $set inboundFiles.$.state, .updatedAt + entryUpdates; refuses a disallowed edge
  public boolean updateInState(ClientSession s, String exportBatchId, String uploadId, InboundFileState inState, Instant now, Bson entryUpdates);
  public static String entry(String field);   // "inboundFiles.$." + field, so callers never spell the positional path
  public Map<String, Long> countByState(); public Optional<Instant> oldestUpdatedAtIn(InboundFileState state); }
```

- [ ] **Step 1: Write the failing ITs** (api profile): `firstClaimIsReceivedSecondUploadIsDuplicateLogged`; `sameUploadIdClaimedTwiceMatchesNothing`; `claimUnderConcurrentWritersCreatesOneEntry` (ten tasks on an injected `ManagedExecutor`, same entry; exactly one `claimed`, array length 1); `transitionIsGuardedByEntryState`; `findByExternalRefUsesTheIndex` (explain plan names `batches_inbound_external_ref`); `findStaleReturnsOldestFirstWithinLimit`.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS. **Step 5: Commit** `feat(ingestion): inbound file repository with conditional claim and guarded array updates`.

### Task 1.3: `InboundLifecycle`

**Files:**
- Create: `ingestion/inbound/InboundLifecycle.java`
- Test: `src/integrationTest/.../ingestion/inbound/InboundLifecycleIT.java`

**Interfaces:** one transaction per method, the audit event in it, nothing else inside. Dependencies: `InboundFileRepository`, `ExportBatchRepository`, `AuditRepository`, `Transactions`, `VendorExchangeMetrics`, `Clock` (6). Events are built with `AuditEvent.forBatch(type, tenantId, "candor", exportBatchId, entry.attempt()).upload(uploadId).actor(Actors.INGESTION)` unless an operator is the actor.
```java
public enum Outcome { RECEIVED, DUPLICATE_LOGGED, ALREADY_CLAIMED }
public Outcome received(ExportBatch batch, InboundFile entry, Instant now);        // claim; RECEIVED: also batch acknowledgedAt=eventTime, inboundBatchId=uploadId by ExportBatchRepository.updateInState(DELIVERED|SUPERSEDED), events INBOUND_FILE_RECEIVED then EXPORT_BATCH_ACKNOWLEDGED(uploadId, vendorBatchId, daysToResponse = whole days deliveredAt→eventTime), metric arrivalReceived; DUPLICATE_LOGGED: INBOUND_FILE_DUPLICATE_LOGGED(firstUploadId, firstUploadState), metric arrivalDuplicateLogged
public void archiving(Located l, Instant now);                                      // RECEIVED→ARCHIVING; no event (the copy has not happened)
public void archived(Located l, ArchiveStore.Copied copy, Instant now);              // updateInState(ARCHIVING): archivePath, archiveBytes; INBOUND_FILE_ARCHIVED(archivePath, bytes, copyResult CREATED|ALREADY_PRESENT)
public void ingestionRequested(Located l, String ingestionId, String externalRef, String mappingId, String registerResponse, UUID deadlineJobId, Instant deadlineAt, Instant now); // ARCHIVING→INGESTION_REQUESTED; jobId = deadline job; INBOUND_FILE_INGESTION_REQUESTED
public boolean parsed(Located l, FileIngestionCounts counts, String status, String completionSource, Instant now);   // INGESTION_REQUESTED→PARSED; parse{…}, parsedAt; INBOUND_FILE_PARSED(waitSeconds = requestedAt→now); metrics completionSource, completionStatus; false when moved
public boolean ingestionFailed(Located l, String reason, String completionSource, Instant now);                     // →INGESTION_FAILED; INBOUND_FILE_INGESTION_FAILED; metric completionStatus("INVALID")
public boolean failed(Located l, InboundFileState from, String failedStep, String failureReason, String lastError, int jobAttempts, Instant now); // →FAILED; INBOUND_FILE_FAILED
```
`received` is the only method that writes batch fields and the entry in one transaction. Step 2 adds `unmatched`, `attach`, `discard`; Step 3 adds `staging`, `chunkCheckpoint`, `staged`.

- [ ] **Step 1: Write the failing ITs**: `receivedAcknowledgesTheBatchOnce` (first upload: entry `RECEIVED`, `acknowledgedAt == eventTime`, `inboundBatchId == uploadId`, two events in insertion order with `daysToResponse` 2 for a delivery two days earlier; second upload: `DUPLICATE_LOGGED`, batch fields unchanged, one event naming the first upload); `archivingThenArchivedThenRequestedWriteTheirEvents` (states and the two events, `jobId` = the deadline job id); `failedRecordsStepReasonAndError`; `parsedOnMovedEntryReturnsFalseAndWritesNothing`.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS, including `AuditLogLineIT`. **Step 5: Commit** `feat(ingestion): inbound lifecycle transactions and acknowledgement`.

### Task 1.4: Job infrastructure, `RequestIngestionJob`, `IngestionDeadlineCheckJob`

**Files:**
- Create: `ingestion/jobs/InboundJobRequest.java`, `IngestionJobEnqueuer.java`, `InboundJobSupport.java`, `IngestionLogContext.java`, `RequestIngestionJob.java`, `RequestIngestionJobRequest.java`, `IngestionDeadlineCheckJob.java`, `IngestionDeadlineCheckJobRequest.java`
- Test: `src/test/.../ingestion/jobs/IngestionLogContextTest.java`; `src/integrationTest/.../ingestion/jobs/RequestIngestionJobIT.java`, `IngestionDeadlineCheckJobIT.java`

**Interfaces:**
```java
public interface InboundJobRequest extends JobRequest { String exportBatchId(); String uploadId(); int attempt(); }
public record RequestIngestionJobRequest(String exportBatchId, String uploadId, int attempt) implements InboundJobRequest {}
public record IngestionDeadlineCheckJobRequest(String exportBatchId, String uploadId, int attempt) implements InboundJobRequest {}
@ApplicationScoped public class IngestionJobEnqueuer {
  public UUID requestIngestion(String exportBatchId, String uploadId, int attempt);     // JobIds.of(RequestIngestionJob.NAME, uploadId, attempt)
  public UUID deadlineCheck(String exportBatchId, String uploadId, int attempt, Instant at); // JobIds.of(IngestionDeadlineCheckJob.NAME, uploadId, attempt), scheduled
  public void delete(UUID jobId); }   // Step 3 adds stageRows(...); Step 7 adds release(tenantId, certifyPractitionerId, releaseId) and readBackDeadline(tenantId, certifyPractitionerId, releaseId, at)
@ApplicationScoped public class InboundJobSupport { public Optional<Located> loadExpecting(String job, String uploadId, Set<InboundFileState> expected); public VendorExchangeConfig config(); public boolean enabled(); }
public final class IngestionLogContext implements AutoCloseable { public static IngestionLogContext open(String exportBatchId, String uploadId, JobContext job); public static IngestionLogContext openRelease(String tenantId, String releaseId, JobContext job); public static void tenant(String t); }
public class RequestIngestionJob implements JobRequestHandler<RequestIngestionJobRequest> { public static final String NAME = "request-ingestion"; }
public class IngestionDeadlineCheckJob implements JobRequestHandler<IngestionDeadlineCheckJobRequest> { public static final String NAME = "ingestion-deadline"; }
```
`RequestIngestionJob.run`: `loadExpecting({RECEIVED, ARCHIVING})`; from `RECEIVED` call `lifecycle.archiving`; copy through `ArchiveStore` (`alreadyExisted` is success); `lifecycle.archived` (skipped when the entry already carries `archivePath`); register with `FileIngestionRegisterRequest(mappingId, fileName, "gs://" + archiveBucket + "/" + object, Ids.externalRef(batchId, generation), meta {exportBatchId, uploadId})`; 201 and 200 both proceed (`registerResult`); `ingestionRequested` with `deadlineAt = now + ingestionDeadlineHours`, then `enqueuer.deadlineCheck`. Errors: 422 → `failed(…, "REGISTER", "MAPPING_UNKNOWN", …)`; 403 → `failed(…, "REGISTER", "FILE_INGESTION_REFUSED", …)` (P7); `ArchiveSourceMissingException` → `failed(…, "ARCHIVE", "SOURCE_MISSING", …)`; a 4xx naming the source and every 5xx or connection error propagate for the JobRunr retry (the final-failure filter in Step 4 writes `SOURCE_UNREADABLE` when the last error was a register 4xx, else `JOB_RETRIES_EXHAUSTED`); mappingId absent → `failed(…, "REGISTER", "MAPPING_UNKNOWN", "not configured")`. `IngestionDeadlineCheckJob.run`: `loadExpecting({INGESTION_REQUESTED})`; one `status` call; `COMPLETED|PARTIAL` → `lifecycle.parsed(…, "DEADLINE")` (the staging enqueue is added in Task 3.5); `INVALID` → `ingestionFailed(…, invalidReason, "DEADLINE")`; anything else → `failed(…, "PARSE", "INGESTION_DID_NOT_FINISH", status, …)`. A disabled service throws, as `DeadlineCheckJob` does (finding 95f).

- [ ] **Step 1: Write the failing tests**: `IngestionLogContextTest` (keys set and removed). `RequestIngestionJobIT` (api profile, run directly; `ArchiveStore` `@InjectMock`; WireMock file-ingestion; `ApiLayerTokenService` mocked): `archivesThenRegistersWithTheDesignBody` (copy called with the generation; register body `equalToJson` of the design's step-3 request; entry `INGESTION_REQUESTED` with `ingestionId`, `externalRef`, `requestedAt`, `jobId` of the deadline check; deadline job `SCHEDULED` at now + 6 h ± 2 min; events `INBOUND_FILE_ARCHIVED` then `INBOUND_FILE_INGESTION_REQUESTED`); `existingArchiveIsSuccess`; `retryFromArchivingSkipsTheCopy`; `repeatRegisterAnswers200AndProceeds`; `mappingUnknownFailsTheEntry`; `refusedRegisterFailsTheEntry` (403 → `FILE_INGESTION_REFUSED`); `sourceMissingFailsTheEntry`; `fileIngestionDownPropagates` (503; entry `ARCHIVING` with the archive kept); `entryNotReceivedIsNoOp`. `IngestionDeadlineCheckJobIT`: `invalidAtDeadlineFailsIngestion`; `stillParsingAtDeadlineFailsEntry`; `entryAlreadyParsedMakesNoStatusCall`.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** `RequestIngestionJob` constructor (6): `InboundJobSupport`, `InboundLifecycle`, `ArchiveStore`, `@RestClient FileIngestionClient`, `IngestionJobEnqueuer`, `Clock`. **Step 4: Run** → PASS. **Step 5: Commit** `feat(ingestion): request-ingestion job archives the exact generation and registers by source URI; deadline check`.

### Gate 1

- [ ] CI green. Finding 97, "CP-40437 Step 1": entry-level `attempt` and retry edges; `ARCHIVING` before the copy; `FILE_INGESTION_REFUSED`; the read timeout; what the deadline job leaves to Step 3.
- [ ] Merge; next branch from `main`.

---

## Step 2: arrival handler, unmatched uploads, operator reads (2 days)

Branch `feature/CP-40437-step-2-arrival`. Design steps 1 and 2. The topic, notifications and subscription are asks 1 to 3; the handler is tested over HTTP with the mocked `PushTokenVerifier`, as `EgressEventResourceIT` does.

### Task 2.1: `UnmatchedUpload` and its repository

**Files:**
- Create: `ingestion/inbound/UnmatchedUpload.java`, `UnmatchedUploadRepository.java`
- Test: `src/integrationTest/.../ingestion/inbound/UnmatchedUploadRepositoryIT.java`

**Interfaces:**
```java
public record UnmatchedUpload(String id /* uploadId */, String folderTenant, InboundNames.Parsed parsed /* nullable */, String reason, Instant receivedAt, Resolution resolution /* nullable */) {
  public record Resolution(String kind /* ATTACHED | DISCARDED */, String exportBatchId, String reason, String actor, Instant at) {} }
@ApplicationScoped public class UnmatchedUploadRepository { public boolean insertIfAbsent(ClientSession s, UnmatchedUpload u); public Optional<UnmatchedUpload> find(String uploadId); public List<UnmatchedUpload> listOpen(String folderTenant /* nullable: all */, int limit); public boolean resolve(ClientSession s, String uploadId, UnmatchedUpload.Resolution r); /* only while unresolved */ public long countOpen(); }
```

- [ ] **Step 1: Write the failing tests**: `UnmatchedUploadRepositoryIT.secondInsertOfSameUploadIsFalse`, `resolveOnlyOnce`, `listOpenExcludesResolved`.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS. **Step 5: Commit** `feat(ingestion): unmatched uploads`.

### Task 2.2: `ArrivalHandler` and `ArrivalResource`

**Files:**
- Create: `ingestion/events/ArrivalHandler.java`, `ArrivalResource.java`
- Modify: `ingestion/inbound/InboundLifecycle.java` (`unmatched`)
- Test: `src/integrationTest/.../ingestion/events/ArrivalHandlerIT.java`, `ArrivalResourceIT.java`

**Interfaces:**
```java
@ApplicationScoped public class ArrivalHandler {
  public enum Outcome { IGNORED_EVENT_TYPE, IGNORED_PREFIX, UNMATCHED, ALREADY_CLAIMED, RECEIVED, DUPLICATE_LOGGED, DISABLED }
  public Outcome handle(PubSubPushMessage.Message message); }
@Path("/internal/vendor-ingestion/arrivals") @PubSubPush @Tag(name = "internal") public class ArrivalResource { @POST @Operation(...) public Response receive(PubSubPushMessage envelope); } // 204 on every outcome
```
Table, in order: attributes unreadable → log, 204 (no metric name exists for it; counted under `arrivalIgnoredEventType("UNREADABLE")`); `eventType != OBJECT_FINALIZE` → `IGNORED_EVENT_TYPE`, metric; `objectId` not under `to/` → `IGNORED_PREFIX`; `InboundNames.parse` empty → unmatched `UNPARSEABLE_NAME`; batch by `exportBatchRef` not found → `UNKNOWN_BATCH`; folder tenant, file tenant, batch tenant disagree → `TENANT_MISMATCH`; batch state not `DELIVERED|SUPERSEDED` or `file == null` → `BATCH_NOT_DELIVERED`; else `lifecycle.received(batch, entry, now)`; on `RECEIVED` → `enqueuer.requestIngestion(batch.id(), uploadId, 1)` after the commit, then `updateInState(RECEIVED)` writing `jobId` (a second small transaction, as the export tick records its job id); on `ALREADY_CLAIMED` with the entry `RECEIVED` and no `jobId` → enqueue now (design: "redelivery after a crash between commit and enqueue"). Unmatched is one transaction in `InboundLifecycle.unmatched(UnmatchedUpload, Instant)`: `insertIfAbsent` and `INBOUND_FILE_UNMATCHED` (service scope, actor `INGESTION`, `upload(uploadId)`), metric `arrivalUnmatched(folderTenant or "unknown", reason)`. A `SUPERSEDED` batch is matched (design: "the echo binds the file to the cycle that asked"). Only `completenessSignal = DATA_FILE` is built; the constructor refuses any other value with `IllegalStateException`, so a `MANIFEST` deployment fails at boot instead of claiming the wrong object.

- [ ] **Step 1: Write the failing ITs**. `ArrivalHandlerIT` (api profile): `finalizeUnderToClaimsAndEnqueues` (entry `RECEIVED` with `jobId`; job `ENQUEUED` under `JobIds.of("request-ingestion", uploadId, 1)`; `acknowledgedAt` set; metric `arrivals.received` 1); `secondGenerationIsDuplicateLogged` (no job; `INBOUND_FILE_DUPLICATE_LOGGED` with `firstUploadId`); `redeliveryOfClaimedEventIsNoOp`; `redeliveryOfReceivedWithoutJobEnqueues`; `fromPrefixIsIgnored`; `deleteEventIsIgnored`; `badNameIsUnmatched`; `unknownBatchIsUnmatched`; `tenantMismatchIsUnmatched` (folder `to/org-b/`, file named `org-a`); `batchNotDeliveredIsUnmatched`; `supersededBatchIsMatched`; `disabledServiceAcknowledgesWithoutWriting`. `ArrivalResourceIT`: `verifiedPushIs204`, `missingTokenIs401Problem`, `rejectedTokenIs401Problem`, `envelopeWithoutAttributesIs204`.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** Handler constructor (7): `ExportBatchRepository`, `InboundFileRepository`, `InboundLifecycle`, `UnmatchedUploadRepository`, `IngestionJobEnqueuer`, `VendorExchangeConfig`, `Clock`. **Step 4: Run** → PASS; `make openapi`. **Step 5: Commit** `feat(ingestion): arrival handler claims the first upload, logs the rest, records unmatched files`.

### Task 2.3: Operator reads and unmatched resolution

**Files:**
- Create: `ingestion/api/VendorIngestionOpsResource.java`, `ingestion/api/InboundViews.java`, `ingestion/api/Page.java`
- Modify: `ingestion/inbound/InboundLifecycle.java` (`attach`, `discard`)
- Test: `src/integrationTest/.../ingestion/api/VendorIngestionOpsResourceIT.java`

**Interfaces:**
- `GET /v1/vendor-ingestion/batches/{exportBatchId}/inbound-files` → `Items<InboundFileView>` (`@RequiresPermission(Permission.READ)`; tenant-scoped through `BatchOperations.require`; the view hides `jobId`).
- `GET /v1/vendor-ingestion/unmatched-uploads?limit=` → `Items<UnmatchedUploadView>` (`READ`; rows whose `folderTenant` is the caller's tenant; limit 1 to 200, default 50).
- `POST /v1/vendor-ingestion/unmatched-uploads/{uploadId}/attach` body `{exportBatchId, reason}` (`MANAGE`): the batch must be the caller's and `DELIVERED|SUPERSEDED`, else 409 `ATTACH_NOT_ALLOWED`; one transaction: the claim path with the operator as actor, the row resolved `ATTACHED`, `INBOUND_UPLOAD_ATTACHED`; enqueue after; 202 `{uploadId, state, jobId}`.
- `POST /v1/vendor-ingestion/unmatched-uploads/{uploadId}/discard` body `{reason}` (`MANAGE`): resolves `DISCARDED` with `INBOUND_UPLOAD_DISCARDED`; 200. Both refuse a resolved row with 409 `ALREADY_RESOLVED` and a blank reason with 400 `REASON_REQUIRED`.
- `Page<T>(List<T> items, String next)` is the keyset envelope Step 5 uses; `Items<T>` from `export.api` is reused for plain lists.

- [ ] **Step 1: Write the failing IT** (api profile, `@TestSecurity` + `WireMockDal.stubMember`): `inboundFilesAreTenantScoped` (own batch lists the entry without `jobId`; another tenant's batch 404 `BATCH_NOT_FOUND`); `unmatchedListShowsOnlyOwnTenantFolder`; `attachClaimsAndEnqueues` (row `ATTACHED` with the operator email; entry `RECEIVED`; job enqueued; event actor is the email); `attachToAnotherTenantsBatchIs404`; `discardTwiceIs409`.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS; `make openapi`. **Step 5: Commit** `feat(ingestion): inbound file and unmatched upload reads; attach and discard`.

### Gate 2

- [ ] CI green; oasdiff accepts five new paths. Finding 98, "CP-40437 Step 2": the table as built, `SUPERSEDED` matched, the two-transaction job id write, `DATA_FILE` only.
- [ ] Merge.

---

## Step 3: StageRowsJob (3 days)

Branch `feature/CP-40437-step-3-stage-rows`. Design step 5, D10, D11, D15. One collection written (`field_recommendations`); confirmations, unverified cells and rejected rows are counts on the inbound entry; no lookup of any kind (P5).

### Task 3.1: `CandorRecsMapping`, `FieldLevel`, `WideRow`

**Files:**
- Create: `ingestion/staging/CandorRecsMapping.java`, `FieldLevel.java`, `WideRow.java`, `src/main/resources/mappings/candor-recs-v1.json` (the 59 columns of the September 2026 sample with `required` on `certify_practitioner_id`, `npi` and `entity_type`; the body of ask 7's `POST /v1/mappings`, versioned here as the design asks)
- Test: `src/test/.../ingestion/staging/CandorRecsMappingTest.java`, `WideRowTest.java`

**Interfaces:**
```java
public enum FieldLevel { PRACTITIONER, LOCATION }
public final class CandorRecsMapping {
  public static final String MAPPING_NAME = "candor-recs-v1";
  public static final String PRACTITIONER_ID = "certify_practitioner_id", NPI = "npi", LOCATION_ID = "certify_location_id", ENTITY_TYPE = "entity_type", ENTITY_PRACTITIONER = "PRACTITIONER";
  public static final List<String> PRACTITIONER_FIELDS = List.of("npi", "first_name", "last_name", "specialties");
  public static final List<String> LOCATION_FIELDS = List.of("location", "practice_name", "phone", "fax", "accepting_new_patients", "pcp_at_location");
  public static final List<String> LOCATION_LEGS = List.of("address_line1", "address_line2", "city", "state", "zip");   // the sent columns that make up `location`
  public static FieldLevel levelOf(String field);   // unknown field throws
  public static String column(String field, Suffix s); public enum Suffix { STATUS("_verification_status"), VALUE("_candor_value"), REASON("_verification_reason"), CONFIDENCE("_verification_confidence"), EVIDENCE("_verification_evidence"); } }
public record WideRow(String sheet, int row, boolean schemaValid, List<String> problems, String entityType, String certifyPractitionerId, String npi, String certifyLocationId, Map<String, FieldCell> cells) {
  public record FieldCell(String field, FieldLevel level, String vendorStatus /* INVALID|VALID|UNKNOWN|INCONCLUSIVE|"" */, String sentValue, String vendorValue, List<String> reasons, String confidence, String evidenceJson) {}
  public static WideRow from(FileIngestionRow row);   // values[<column>].value; schemaValid = !"INVALID".equals(row.status()); sentValue of `location` = the five legs joined with "|"
  public boolean hasLocationId(); public boolean isPractitioner(); }
```
The ten field names, the sheet name `Physicians Directory Accuracy` and the suffixes are the September 2026 sample's (design, step 5). `reasons` splits the reason cell on `;`.

- [ ] **Step 1: Write the failing tests**: `CandorRecsMappingTest.levelsAndColumns`; `WideRowTest.readsOneDesignRow` (practitioner `cert-000123`, npi `1003002890`, location `loc-7781`, `practice_name` cell `INVALID` / `Greenwood Village` / `["direct_outreach"]` / `VERY HIGH`; `phone` cell `VALID`; `location` sent value is the joined legs); `blankLocationIdHasNoLocationId`; `schemaInvalidRowKeepsProblems`; `nonPractitionerRowIsNotPractitioner`.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS. **Step 5: Commit** `feat(ingestion): the candor-recs-v1 wide row`.

### Task 3.2: `ReviewStatus`, `RowDisposition`, `FieldRecommendation`, repository

**Files:**
- Create: `ingestion/staging/ReviewStatus.java`, `RowDisposition.java`, `FieldRecommendation.java`, `FieldRecommendationRepository.java`
- Test: `src/test/.../ingestion/staging/ReviewStatusTest.java`, `FieldRecommendationTest.java`; `src/integrationTest/.../ingestion/staging/FieldRecommendationRepositoryIT.java`

**Interfaces:**
```java
public enum ReviewStatus { PENDING, SKIPPED, APPROVED, REJECTED, RELEASE_REQUESTED, RELEASING, RELEASED, RELEASE_FAILED;
  public enum Action { APPROVE, REJECT, SKIP, ROLLBACK, RELEASE }
  public boolean allows(Action a);  // APPROVE, REJECT from PENDING|SKIPPED; SKIP from PENDING; ROLLBACK from APPROVED; RELEASE from APPROVED|RELEASE_FAILED (an operator re-release after a fix; design-doc edit 9)
  public static Set<ReviewStatus> open(); /* PENDING, SKIPPED */ }
public enum RowDisposition { STAGED, DUPLICATE, SCHEMA_INVALID, MISSING_IDS, UNSUPPORTED_ENTITY }
public record FieldRecommendation(String id, String tenantId, String sourceType /* "vendor" */, String source /* "candor" */, String exportBatchId,
    Practitioner practitioner, Location location /* null for practitioner-level fields; that is what tells the levels apart */, String field,
    String sentValue, String vendorValue, String vendorStatus, List<String> vendorReason, String vendorConfidence, List<Map<String, Object>> vendorEvidence,
    ReviewStatus reviewStatus, List<HistoryEntry> reviewHistory, long version, GoldenRecordUpdate goldenRecordUpdate /* nullable */, SourceFile sourceFile, Instant stagedAt, Instant updatedAt) {
  public record Practitioner(String certifyPractitionerId, String npi) {} public record Location(String certifyLocationId) {} public record HistoryEntry(String action, String by, Instant at, String comment) {}
  public record GoldenRecordUpdate(String releaseId, String requestedBy, Instant requestedAt, String writePath /* SLICE | IN_PLACE */, String resource, String crosswalkId, Instant writtenAt, String deadlineJobId, String outcome, String winningSource, Instant releasedAt, String error) {}
  public record SourceFile(String uploadId, String sheet, int row) {}
  public FieldLevel level(); public static FieldRecommendation pending(WideRow row, WideRow.FieldCell cell, ExportBatch batch, InboundFile entry, ObjectMapper mapper, Instant now); // id from Ids.fieldRecommendationId; evidence parsed from the cell's JSON, or [{"evidence": "<text>"}] when it is not JSON
  public Document toDocument(); public static FieldRecommendation fromDocument(Document d); }
@ApplicationScoped public class FieldRecommendationRepository {
  public record Upserted(int attempted, Set<String> insertedIds) {}
  public Upserted upsertAll(List<FieldRecommendation> docs);  // one unordered bulkWrite of updateOne({_id}, {$setOnInsert: <every field>, $set: {updatedAt}}, upsert); insertedIds from BulkWriteResult.getUpserts(); no session (D10)
  public Optional<FieldRecommendation> find(String tenantId, String id);
  public record ListQuery(String tenantId, String field, Set<ReviewStatus> statuses, String npi, String exportBatchId, String after /* "<stagedAt epoch ms>|<_id>" */, int limit) {}
  public List<FieldRecommendation> list(ListQuery q);                                      // newest staged first, keyset on (stagedAt desc, _id desc); fetches limit + 1
  public Map<String, Map<ReviewStatus, Long>> summary(String tenantId, String exportBatchId); // one $group
  public List<FieldRecommendation> forPractitioner(String tenantId, String certifyPractitionerId, String exportBatchId /* nullable: latest batch */, int limit);
  public List<FieldRecommendation> forNpi(String tenantId, String npi, String exportBatchId /* nullable */, int limit);
  public List<FieldRecommendation> forRelease(String tenantId, String releaseId, int limit);
  public boolean decide(ClientSession s, String tenantId, String id, long expectedVersion, Set<ReviewStatus> from, ReviewStatus to, Bson updates, Instant now); // filter {_id, tenantId, version, reviewStatus in from}; $set reviewStatus, updatedAt; $inc version; $push reviewHistory
  public long moveAll(ClientSession s, String tenantId, String certifyPractitionerId, String releaseId, ReviewStatus from, ReviewStatus to, Bson updates, Instant now); // updateMany, Step 7
  public long countReleasing(String tenantId, String certifyPractitionerId, String exceptReleaseId);  // Step 7, P12
  public Map<String, Long> countByStatus(); public Map<String, Long> countPendingByField(String tenantId); public Optional<Instant> oldestPendingStagedAt(String tenantId); }
```
The stored document is the design's sample field for field, so the list can return it as is. A document carries no practitioner name or address (design: read from the practitioner record at request time).

- [ ] **Step 1: Write the failing tests**: `ReviewStatusTest.movesMatchTheDesign`; `FieldRecommendationTest.pendingFromTheDesignRowMatchesTheSample` (`_id`, `location.certifyLocationId loc-7781`, `sentValue AdventHealth`, `vendorValue Greenwood Village`, `vendorEvidence` one object with `reason`, `evidence`, `value`, `version 1`, `reviewHistory []`, `goldenRecordUpdate null`, `sourceFile.row 913`), `practitionerLevelFieldHasNullLocation`, `roundTripsThroughBson`; `FieldRecommendationRepositoryIT.upsertTwiceInsertsOnce` (second call inserts nothing; a decision written in between survives), `listPagesByKeysetWithoutSkippingDecidedRows` (seven PENDING rows, limit 3; decide row 2 between pages; pages 3, 3, 1, no repeat, no gap), `listUsesThePartialIndex` (explain names `field_recs_open_list`), `summaryCountsFieldByStatus`, `decideIsGuardedByVersionAndStatus`, `forNpiWithoutBatchTakesTheLatest`.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS. **Step 5: Commit** `feat(ingestion): field recommendations, the review state machine and the repository`.

### Task 3.3: `ChunkBuilder`

**Files:**
- Create: `ingestion/staging/ChunkBuilder.java`
- Test: `src/test/.../ingestion/staging/ChunkBuilderTest.java`

**Interfaces:**
```java
public final class ChunkBuilder {
  public record Built(List<FieldRecommendation> recommendations, Map<String, Set<String>> documentIdsByRow /* "<sheet>:<row>" → ids */, Map<RowDisposition, Long> dispositions, long confirmedFields, long unverifiedFields, long practitionerFieldDisagreements, int rowsSeen, String lastCursor) {}
  public static Built build(List<WideRow> rows, ExportBatch batch, InboundFile entry, ObjectMapper mapper, Instant now);
  public static Map<RowDisposition, Long> settle(Built built, Set<String> insertedIds); }  // a row with ≥1 recommendation none of which was inserted → DUPLICATE; others keep their disposition
```
Rules, in order per row, all local (design step 5): `!schemaValid` → `SCHEMA_INVALID`; `entityType != PRACTITIONER` → `UNSUPPORTED_ENTITY`; `certifyPractitionerId` or `npi` blank → `MISSING_IDS`; location id blank → practitioner-level cells processed, location-level cells counted under `MISSING_IDS` for the row, the row's disposition `MISSING_IDS`; else `STAGED`. Per cell: `INVALID` → one recommendation (`PENDING`); `VALID` → `confirmedFields + 1`; `UNKNOWN`, `INCONCLUSIVE`, blank → `unverifiedFields + 1`. Practitioner-level recommendations of one practitioner across its location rows collapse to one document; a later row disagreeing on `vendorValue` keeps the first and adds one to `practitionerFieldDisagreements` (logged with the row numbers, never the values). `lastCursor` = `<sheet>:<row>` of the last row.

- [ ] **Step 1: Write the failing tests**: `onePractitionerThreeLocationsWritesPractitionerFieldsOnce`; `disagreeingRowsCountOnce`; `blankLocationIdStagesPractitionerCellsOnly`; `nonPractitionerRowIsUnsupported`; `schemaInvalidRowIsCounted`; `validAndUnknownAreCountedNotWritten`; `settleMarksRowsWhoseDocumentsAllExistedAsDuplicate`; `dispositionsSumToRowsSeen`.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS. **Step 5: Commit** `feat(ingestion): chunk builder turns wide rows into idempotent documents and counts`.

### Task 3.4: `RowStager` and `StageRowsJob`

**Files:**
- Create: `ingestion/staging/RowStager.java`, `ingestion/jobs/StageRowsJob.java`, `StageRowsJobRequest.java`
- Modify: `ingestion/jobs/IngestionJobEnqueuer.java` (`stageRows`), `ingestion/inbound/InboundLifecycle.java` (`staging`, `chunkCheckpoint`, `staged`)
- Test: `src/integrationTest/.../ingestion/jobs/StageRowsJobIT.java`

**Interfaces:**
```java
@ApplicationScoped public class RowStager {
  public record Progress(String after, Map<RowDisposition, Long> dispositions, long recommendations, long confirmedFields, long unverifiedFields, long disagreements) {}
  public Progress stageChunk(Located l, List<FileIngestionRow> chunk, Instant now); }  // build; upsertAll; settle; lifecycle.chunkCheckpoint(l, progress); metrics stageRows per disposition, stageDocuments, stageChunkDuration
public record StageRowsJobRequest(String exportBatchId, String uploadId, int attempt) implements InboundJobRequest {}
public class StageRowsJob implements JobRequestHandler<StageRowsJobRequest> { public static final String NAME = "stage-rows"; }
// InboundLifecycle additions
public boolean staging(Located l, Instant now);                       // PARSED→STAGING; no event
public void chunkCheckpoint(Located l, RowStager.Progress p, Instant now);  // updateInState(STAGING): $inc dispositions.<d>, written.*, checkpoint.chunks; $set checkpoint.after; no event
public void staged(Located l, long durationMs, Instant now);          // STAGING→STAGED; stagedAt; INBOUND_FILE_STAGED(dispositions, written, chunks, durationMs, practitionerFieldDisagreements)
```
`StageRowsJob.run`: `loadExpecting({PARSED, STAGING})`; from `PARSED` call `lifecycle.staging`; open `FileIngestionClient.rows(tenant, identifier, ingestionId, entry.checkpoint().after())`, read `FileIngestionRow` values with `ObjectMapper.readerFor(FileIngestionRow.class).readValues(stream)`, collect `stageChunkRows` rows, `stager.stageChunk`, repeat; end of stream: `sum(dispositions)` against `parse.populatedRows` → mismatch `lifecycle.failed(l, STAGING, "STAGE", "COUNT_MISMATCH", "<sum> vs <populated>", attempts)`; else `lifecycle.staged`, then `markConsumed` best effort (one warning on failure). A 429 propagates (JobRunr retry, metric `stageStream429`); a 404 or 410 on the rows → `failed(…, "ROWS_EXPIRED", …)`.

- [ ] **Step 1: Write the failing IT** (api profile, run directly, WireMock file-ingestion serving NDJSON): `stagesTheDesignFile` (five rows for two practitioners, one with two locations; entry `STAGED`; `dispositions {STAGED: 5}`; `written.confirmedFields` and `unverifiedFields` as counted; the sample document present `PENDING`; `INBOUND_FILE_STAGED` carries the counts; mark-consumed called once); `chunksAndCheckpoints` (two 2-row chunks through `RowStager` directly: `checkpoint.after` is `Physicians Directory Accuracy:2` then `:4`, counts accumulate, `chunks` 2); `retryAfterCrashBeforeCheckpointDoesNotDoubleCount` (stream dropped after row 3 inside chunk 2: job throws, entry `STAGING`, checkpoint after row 2; second run resumes with `after=…:2`, document count unchanged, dispositions sum to 5); `blankLocationIdStagesPractitionerFieldsOnly`; `secondUploadOfSameRowsIsDuplicate` (stage twice: `DUPLICATE` 5 on the second entry); `countMismatchFailsTheEntry`; `rowsExpiredFailsTheEntry`; `tooManyStreamsPropagates`.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** `StageRowsJob` constructor (6): `InboundJobSupport`, `InboundLifecycle`, `RowStager`, `@RestClient FileIngestionClient`, `ObjectMapper`, `Clock`. **Step 4: Run** → PASS. **Step 5: Commit** `feat(ingestion): stage-rows job streams file-ingestion rows into idempotent chunks with a checkpoint`.

### Task 3.5: Deadline check enqueues staging

**Files:**
- Modify: `ingestion/jobs/IngestionDeadlineCheckJob.java`
- Test: `src/integrationTest/.../ingestion/jobs/IngestionDeadlineCheckJobIT.java`

- [ ] **Step 1: Add the failing test** `completedAtDeadlineParsesAndEnqueuesStaging` (`PARSED`, `completionSource DEADLINE`, stage-rows job `ENQUEUED` under `JobIds.of("stage-rows", uploadId, attempt)`, `jobId` updated).
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS. **Step 5: Commit** `feat(ingestion): deadline check hands a parsed file to staging`.

### Gate 3

- [ ] CI green. Finding 99, "CP-40437 Step 3": D11 as built (counts, not collections), P5, the `DUPLICATE` rule through `BulkWriteResult.getUpserts()`, the evidence parse fallback, what a disagreement logs.
- [ ] Merge.

---

## Step 4: completion handler, reconciler, final-failure filter (2 days)

Branch `feature/CP-40437-step-4-completion`. Design step 4 by event, plus the two safety nets the export lane has: an entry written but never enqueued, and a job whose retries ran out.

### Task 4.1: `CompletionEvent`, `CompletionHandler`, `CompletionResource`

**Files:**
- Create: `ingestion/events/CompletionEvent.java`, `CompletionHandler.java`, `CompletionResource.java`
- Test: `src/integrationTest/.../ingestion/events/CompletionHandlerIT.java`, `CompletionResourceIT.java`

**Interfaces:**
```java
@JsonIgnoreProperties(ignoreUnknown = true) public record CompletionEvent(String ingestionId, String tenantId, String identifier, String status, FileIngestionCounts counts, String reason, String externalRef, Map<String, String> meta, String rows) {}
@ApplicationScoped public class CompletionHandler {
  public enum Outcome { REJECTED_UNPARSEABLE, UNKNOWN_REF, REJECTED_TENANT_MISMATCH, STATE_PASSED, PARSED, INGESTION_FAILED, IGNORED_STATUS, DISABLED }
  public Outcome handle(PubSubPushMessage.Message message); }
@Path("/internal/vendor-ingestion/completions") @PubSubPush public class CompletionResource { @POST public Response receive(PubSubPushMessage envelope); }
```
Table: body (base64 `data`) unparseable → log, 204; `externalRef` null or `findByExternalRef` empty → `UNKNOWN_REF`, 204; `event.tenantId != batch.tenantId` → `REJECTED_TENANT_MISMATCH`; entry not `INGESTION_REQUESTED` → `STATE_PASSED`; `COMPLETED|PARTIAL` → `lifecycle.parsed(l, counts, status, "EVENT")`, delete the deadline job (`entry.jobId()`), `enqueuer.stageRows(...)` and store that job id with `updateInState(PARSED)`; `INVALID` → `lifecycle.ingestionFailed(l, reason, "EVENT")`, delete the deadline job; any other status → `IGNORED_STATUS`.

- [ ] **Step 1: Write the failing ITs**: `CompletionHandlerIT`: `completedParsesEnqueuesAndDeletesDeadline`; `partialIsParsed`; `invalidFailsIngestion` (`INGESTION_FAILED`, `parse.reason STRUCTURE_REJECTED`, metric `completion.status{INVALID}`); `unknownRefIsAcknowledged`; `tenantMismatchIsRejected`; `duplicateEventIsNoOp` (one event); `unparseableIsAcknowledged`. `CompletionResourceIT`: the three token cases and 204.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS; `make openapi`. **Step 5: Commit** `feat(ingestion): completion handler moves a parsed file to staging`.

### Task 4.2: `InboundReconcilerJob`, `IngestionFinalFailureFilter`

**Files:**
- Create: `ingestion/jobs/InboundReconcilerJob.java`, `IngestionFinalFailureFilter.java`, `IngestionFinalFailureFilterRegistration.java`
- Test: `src/test/.../ingestion/jobs/InboundReconcilerJobTest.java`; `src/integrationTest/.../ingestion/jobs/InboundReconcilerJobIT.java`, `IngestionJobRunrGateIT.java`

**Interfaces:**
```java
@ApplicationScoped public class InboundReconcilerJob { public static final String RECURRING_ID = "vendor-ingestion-reconciler"; public static final String CRON = "*/10 * * * *"; public static final String REENQUEUED_PREFIX = "INGESTION_RECONCILER_REENQUEUED";
  @Recurring(id = RECURRING_ID, cron = CRON, zoneId = "UTC") public void reconcile(); public List<Reenqueued> run(); public record Reenqueued(String uploadId, InboundFileState state, String job, UUID jobId) {} }
public class IngestionFinalFailureFilter implements ApplyStateFilter {}  // inbound jobs → entry FAILED; ReleaseJob (Step 7) → fields RELEASE_FAILED reason EXHAUSTED
```
`run`: `findStale(reconcilable(), now - inboundStaleMinutes, 500)`; `RECEIVED|ARCHIVING` → `requestIngestion`, `PARSED|STAGING` → `stageRows`, each under the entry's attempt (a deterministic id that already exists is not enqueued twice); writes `INBOUND_FILE_RECONCILED(state, ageMinutes, action RE_ENQUEUED)` as a standalone fact and one warning line `INGESTION_RECONCILER_REENQUEUED batch=… upload=… state=… since=… job=… jobId=…`. `INGESTION_REQUESTED` is never touched (the deadline job owns it). The filter mirrors `BatchFinalFailureFilter`: on `FAILED` after JobRunr's retry filter, read the `InboundJobRequest`, `lifecycle.failed(l, state, <step by job>, reason, "<type>: <message>", attempts)` unless terminal, where `reason` is `SOURCE_UNREADABLE` when the last exception was a register 4xx and `JOB_RETRIES_EXHAUSTED` otherwise; metrics `ingestionJobRetry`, `ingestionJobFinalFailure`. Registration observes `StartupEvent` and calls `setJobFilters(List.of(filter))` (appends, P6).

- [ ] **Step 1: Write the failing tests**: `InboundReconcilerJobTest.prefixAndCronArePinned`; `InboundReconcilerJobIT.staleEntriesAreReenqueuedByState` (RECEIVED, ARCHIVING, PARSED, STAGING entries 31 minutes old plus one INGESTION_REQUESTED: four jobs under deterministic ids, the fifth untouched, four `INBOUND_FILE_RECONCILED`); `IngestionJobRunrGateIT` (worker profile, retries 1): a `RequestIngestionJob` whose archive store always throws reaches `FAILED` and the entry reads `FAILED` with `JOB_RETRIES_EXHAUSTED` and `INBOUND_FILE_FAILED` within 60 s.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS; `WorkerRoleBootIT` sees both recurring jobs. **Step 5: Commit** `feat(ingestion): inbound reconciler every ten minutes and the final-failure filter`.

### Gate 4

- [ ] CI green. Finding 100, "CP-40437 Step 4". Merge.

---

## Step 5: review API (3 days)

Branch `feature/CP-40437-step-5-review-api`. Design step 6, D12, D13. Paths under `/v1/vendor-review` (P1). The tenant comes from the caller's `UserContext`, never from the query.

### Task 5.1: `ReviewViews` and the read endpoints

**Files:**
- Create: `ingestion/api/VendorReviewResource.java`, `ingestion/api/ReviewViews.java`
- Test: `src/integrationTest/.../ingestion/api/VendorReviewResourceIT.java`

**Interfaces:**
- `GET /v1/vendor-review/fields?field=&reviewStatus=&npi=&exportBatchId=&after=&limit=` → `Page<FieldView>` (`REVIEW_READ`). `reviewStatus` defaults to `PENDING`, repeatable; `limit` 10 to `reviewPageLimitMax`, default 50, else 400 `INVALID_REQUEST`; `after` opaque from the previous page's `next`; unknown `field` → 400 `FIELD_UNKNOWN`.
- `GET /v1/vendor-review/fields/{id}` → `FieldView` (404 `FIELD_NOT_FOUND` for another tenant's or a missing document).
- `GET /v1/vendor-review/fields/{id}/events` → `Items<AuditEventView>` from `AuditRepository.trailForField` (the field's `FIELD_*` events in insertion order; `AuditViews` from `export.api` is reused).
- `GET /v1/vendor-review/fields/summary?exportBatchId=` → `{ "fields": { "<field>": { "<status>": n } } }`. With `exportBatchId`, the response also carries `exportBatchState` from `BatchOperations.require`, so a `SUPERSEDED` cycle shows on the review screen (design, edge cases).
- `GET /v1/vendor-review/practitioners/{certifyPractitionerId}?exportBatchId=` and `GET /v1/vendor-review/npis/{npi}?exportBatchId=` → `{ practitioner, fields: [FieldView] }`; without `exportBatchId` the latest batch for the tenant; 404 when none.
- `FieldView` is the stored document as is, `version` included.

- [ ] **Step 1: Write the failing IT** (api profile, `@TestSecurity`, `WireMockDal.stubMember`): `listDefaultsToPendingNewestFirst`; `listPagesWithNext` (fifteen docs, `limit=10`: two pages, `next` null on the last; `limit=5` is 400); `listFiltersByFieldAndNpi`; `anotherTenantsDocumentIs404`; `summaryCounts`; `practitionerViewGroups`; `npiViewWithoutBatchTakesTheLatest`; `fieldTrailListsItsEvents`; `unknownFieldIs400`.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS; `make openapi`. **Step 5: Commit** `feat(review): list, read, trail, summary, practitioner and NPI views`.

### Task 5.2: `ReviewService` and the decision endpoints

**Files:**
- Create: `ingestion/review/ReviewService.java`
- Modify: `ingestion/api/VendorReviewResource.java`
- Test: `src/integrationTest/.../ingestion/review/ReviewServiceIT.java`, `VendorReviewResourceIT.java` (decision cases)

**Interfaces:**
```java
@ApplicationScoped public class ReviewService {
  public record DecisionRequest(String id, ReviewStatus.Action action, String comment, Long version) {}
  public record DecisionResult(String id, int status /* 200, 400, 404, 409 */, String code, FieldRecommendation document /* current, on 200 and 409 */) {}
  public FieldRecommendation decide(String tenantId, DecisionRequest r, String actor);         // throws ProblemException; one transaction: repository.decide + FIELD_* event (+ the Step 6 rejected-value upsert)
  public List<DecisionResult> decideAll(String tenantId, List<DecisionRequest> rs, String actor); // per item, up to reviewPageLimitMax; never throws for an item
}
```
Rules: `version` required (400 `VERSION_REQUIRED`); `REJECT` needs a non-blank comment (400 `COMMENT_REQUIRED`); the document must exist in the tenant (404 `FIELD_NOT_FOUND`); `reviewStatus.allows(action)` and `version` match, else 409 `DECISION_CONFLICT` whose problem `detail` is JSON of `{id, reviewStatus, version, lastDecisionBy}` (the UI reads the row and sees its own decision); `APPROVE`→`APPROVED`, `REJECT`→`REJECTED`, `SKIP`→`SKIPPED`; history entry `{action, by, at, comment}`; events `FIELD_APPROVED|FIELD_REJECTED|FIELD_SKIPPED` built with `AuditEvent.of(type, tenantId, "candor").batch(exportBatchId).upload(sourceFile.uploadId).field(id, certifyPractitionerId).actor(email)` and detail `field, fromStatus, version` (`certifyLocationId`, `comment` when present); metrics `reviewDecision`, `reviewConflict`.
- `POST /v1/vendor-review/fields/{id}/decision` body `{action, comment, version}` → 200 `FieldView` (`REVIEW_DECIDE`).
- `POST /v1/vendor-review/decisions` body `{decisions: [{id, action, comment, version}]}` → 207 `{results: [{id, status, code, document}]}`; more than `reviewPageLimitMax` items → 400 `INVALID_REQUEST` before anything is written.

- [ ] **Step 1: Write the failing ITs**: `ReviewServiceIT`: `approveFromPendingWritesHistoryAndEvent` (version 1 → 2; `FIELD_APPROVED` in `vendor_review_events` with `fieldRecommendationId`, `fromStatus PENDING`); `rejectNeedsComment`; `skipThenApproveIsAllowedAndFinal`; `skipAfterApproveIs409`; `retriedDecisionIs409WithOwnDecisionVisible` (approve at version 1 twice: the second is 409 whose detail names `APPROVED` and the actor; one event); `twoReviewersOneWins` (two threads, same version; exactly one 200); `bulkIsPerItem` (200, 409, 404 across three items); `anotherTenantsDocumentIs404`. `VendorReviewResourceIT` additions: `decisionIs200WithDocument`, `bulkIs207`, `decidePermissionEnforcedWhenFlagOn` (nested `@TestProfile` with `vendor-exchange.permissions.enforce=true`; caller with `vendor-review: [read]` → 403 `PERMISSION_DENIED`).
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS; `make openapi`. **Step 5: Commit** `feat(review): guarded decisions, bulk decisions, conflict answers`.

### Gate 5

- [ ] CI green; oasdiff accepts eight paths. Finding 101, "CP-40437 Step 5": P1; the 409 `detail` shape; the 10-row floor; the field trail endpoint. Merge.

---

## Step 6: rejected-value memory (1 day)

Branch `feature/CP-40437-step-6-rejected-values`. Design step 7, D16: written on every `REJECT`, one row per distinct normalised value, read by nothing in MVP.

### Task 6.1: `ValueNormalizer`

**Files:**
- Create: `ingestion/review/ValueNormalizer.java`
- Test: `src/test/.../ingestion/review/ValueNormalizerTest.java`

**Interfaces:** `public final class ValueNormalizer { public static final int VERSION = 1; public static String normalize(String field, String raw); }` with the design's version-1 table: `phone`, `fax` digits only, a leading `1` dropped from 11 digits; `first_name`, `last_name`, `practice_name` trimmed, upper-cased, punctuation removed, spaces collapsed; `specialties` entries split on `;` or `,`, trimmed, upper-cased, sorted, joined with `;`; `accepting_new_patients`, `pcp_at_location` → `true`/`false` (`Y`, `YES`, `TRUE`, `1` → `true`); `location` → `address_line1|city|state|zip` with lines and city trimmed, upper-cased, punctuation removed, `SUITE→STE`, `STREET→ST`, `AVENUE→AVE`, state two letters, zip first five digits (the design's table drops `address_line2`; `location` values arrive as the five legs joined with `|`, Task 3.1); `npi` digits. Null or blank → `""`.

- [ ] **Step 1: Write the failing tests**: one case per rule, including `(303) 679-0177` → `3036790177`, `1-303-679-0177` → `3036790177`, `5351 S. Roslyn St, Suite 200|Greenwood Village|co|80111-1234` legs → `5351 S ROSLYN ST STE 200|GREENWOOD VILLAGE|CO|80111`, `Cardiology; internal medicine` → `CARDIOLOGY;INTERNAL MEDICINE`, `Y` → `true`.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS. **Step 5: Commit** `feat(review): value normaliser version 1`.

### Task 6.2: `RejectedValue`, repository, write on reject

**Files:**
- Create: `ingestion/review/RejectedValue.java`, `RejectedValueRepository.java`
- Modify: `ingestion/review/ReviewService.java`
- Test: `src/integrationTest/.../ingestion/review/RejectedValueRepositoryIT.java`, `ReviewServiceIT.java` (reject cases)

**Interfaces:**
```java
public record RejectedValue(String id, String tenantId, String certifyPractitionerId, String certifyLocationId /* nullable */, String field, String normalizedValue, int normalizationVersion, String rawValue,
    long rejectionCount, Instant firstRejectedAt, Instant lastRejectedAt, List<Rejection> rejections, Instant expiresAt) {
  public record Rejection(String exportBatchId, String by, Instant at, String reason) {} }
@ApplicationScoped public class RejectedValueRepository {
  public record Upserted(String id, long rejectionCount) {}
  public Upserted upsert(ClientSession s, FieldRecommendation f, String actor, String reason, Instant now, int ttlMonths);  // updateOne by _id, upsert: $setOnInsert identity, normalizedValue, normalizationVersion, rawValue, firstRejectedAt; $inc rejectionCount; $set lastRejectedAt, expiresAt = now + ttlMonths; $push rejections (keep the last 20 with $slice)
  public Optional<RejectedValue> find(String tenantId, String id); public List<RejectedValue> forPractitioner(String tenantId, String certifyPractitionerId, int limit); }
```
`ReviewService.decide` on `REJECT` calls `upsert` inside the decision transaction and adds `rejectedValueId`, `rejectionCount` to the `FIELD_REJECTED` detail. The TTL index exists since Step 0. Nothing reads the collection in MVP (D16); `forPractitioner` serves the report the design names and has no endpoint yet.

- [ ] **Step 1: Write the failing tests**: `RejectedValueRepositoryIT.secondRejectionOfSameValueIsOneRowWithCountTwo` (October and December rejections: one row, `rejectionCount 2`, `lastRejectedAt` moved, `expiresAt` 12 months after the second, two `rejections[]`), `differentValueIsASecondRow`; `ReviewServiceIT.rejectWritesTheRejectedValue` (row id `org-xyz|cert-000123|loc-7781|phone|<sha256 of 3036790177>`, `normalizationVersion 1`; event detail carries `rejectedValueId` and `rejectionCount 1`), `rejectRollsBackWhenTheMemoryWriteFails` (repository mocked to throw: field stays `PENDING`, no event).
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS. **Step 5: Commit** `feat(review): rejected-value memory, one row per distinct value, twelve-month TTL`.

### Gate 6

- [ ] CI green; `MongoIndexesIT` shows the one TTL index. Finding 102, "CP-40437 Step 6". Merge.

---

## Step 7: release of slice fields (5 days)

Branch `feature/CP-40437-step-7-release`. Design step 8, D17 to D22, P8 to P13. In tests the DAL, its operational values and survivorship's event are WireMock and HTTP pushes; the live `core_sources` row, the ranking entry, the ov-generated subscription and the matching change are asks 11, 12, 13, 18. Fields in scope: `first_name`, `last_name` (practitioner slice), `phone`, `fax` (location slice). Every other field answers 422 `IN_PLACE_RELEASE_UNSUPPORTED` until Step 8 (P10).

### Task 7.1: `Crosswalks`, `NameSound`, `SliceMerge`, `ReadBack`

**Files:**
- Create: `ingestion/release/Crosswalks.java`, `NameSound.java`, `SliceMerge.java`, `ReadBack.java`
- Modify: `gradle/libs.versions.toml` (`commons-codec = "1.17.1"`, library `commons-codec`), `build.gradle.kts`
- Test: `src/test/.../ingestion/release/CrosswalksTest.java`, `NameSoundTest.java`, `SliceMergeTest.java`, `ReadBackTest.java`

**Interfaces:**
```java
public final class Crosswalks {
  public static final String SOURCE_PREFIX = "candor:"; public static String sourceType(String tenantId);   // "candor:" + tenantId
  public static String practitioner(String npi);                                                           // the NPI (EntityCrosswalkGenerator.generateCorePractitionerCrosswalk)
  public static String normalize(String s);           // [^a-zA-Z0-9] → "_", collapse "_+", trim leading and trailing "_", upper-case (LocationHashService.normalizeString)
  public static String addressHash(Map<String, Object> address);  // SHA-256 hex of normalize(value) + "|" for each non-blank of addressLine1, addressLine2, city, state, zip, addressType in that order; null when none (LocationHashService.createAddressHash)
  public static String location(String name, Map<String, Object> address); }  // URLEncoder.encode(normalize(name or "UnknownLocation"), UTF_8) + "_" + (addressHash or "no_address") (formatLocationCrosswalkId)
public final class NameSound { public static boolean same(String a, String b); }  // Soundex.US_ENGLISH over letters only; blank vs non-blank differs; both blank same
public final class SliceMerge {
  public record PractitionerSlice(Map<String, Object> data) {} public record LocationSlices(Map<String, Object> location, Map<String, Object> address, String locationCrosswalk, String addressCrosswalk) {}
  public static PractitionerSlice practitioner(OperationalValue golden, Slice own /* nullable */, Map<String, String> fieldValues);   // seed {npi, firstName, lastName} from golden.data; overlay own.data; overlay first_name→firstName, last_name→lastName
  public static LocationSlices location(OperationalValue goldenLocation, Map<String, Object> goldenServiceAddress, Slice ownLocation /* nullable */, Map<String, String> fieldValues); // seed {name, contacts[]} from golden; overlay own; replace the phone or fax entry in contacts[] (type "phone"|"fax"); address = the Golden service address unchanged
  public static Map<String, String> fieldToDataPath(); }   // first_name→firstName, last_name→lastName, phone→contacts.phone, fax→contacts.fax
public final class ReadBack {
  public enum Outcome { WON, OUTRANKED, NOT_APPLIED, UNKNOWN } public record Verdict(Outcome outcome, String winningSource) {}
  public static Map<String, Verdict> evaluate(OperationalValue ov, String sourceType, Map<String, String> fieldRecommendationIdToField); }
```
`evaluate`: no entry of `contributingSlices` whose `sourceType` equals ours → every field `NOT_APPLIED`; else per field the `contributionMap` winner at the field's data path (both `firstName` and `data.firstName` key shapes accepted until Gate 7 pins one): ours → `WON`, another → `OUTRANKED` with that source, absent → `UNKNOWN`. The exact `contacts[]` shape and how survivorship picks it (ask 17) are confirmed at Gate 7; until then the location slice carries every Golden contact entry with the corrected one replaced.

- [ ] **Step 1: Write the failing tests**: `CrosswalksTest.recipesMatchTheApiLayer` (`normalize("5351 S. Roslyn St, Suite 200")` → `5351_S_ROSLYN_ST_SUITE_200`; a fixed address's hash equals a value computed once from the api-layer's `LocationHashService` and pasted into the test; `location("Greenwood Village Clinic", address)` → `GREENWOOD_VILLAGE_CLINIC_<hash>`; a null address → `_no_address` suffix); `NameSoundTest` (`Bob`/`Robert` differ, `Jon`/`John` same, blank cases); `SliceMergeTest.practitionerSeedOverlayRelease` (own slice carries last month's `lastName`; this release's `firstName` lands; `npi` always present), `locationReplacesOneContactKeepsTheRest`; `ReadBackTest.wonOutrankedNotAppliedUnknown`.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS; `make lint`. **Step 5: Commit** `feat(release): crosswalk recipes, name sound guard, slice merge and read-back verdicts`.

### Task 7.2: `RankingGuard`, `ReleaseService`, `ReleaseLifecycle`, endpoints

**Files:**
- Create: `ingestion/release/RankingGuard.java`, `ReleaseService.java`, `ReleaseLifecycle.java`, `ingestion/api/VendorReleaseResource.java`
- Modify: `ingestion/jobs/IngestionJobEnqueuer.java` (`release(String tenantId, String certifyPractitionerId, String releaseId)` → `JobIds.of("release", releaseId, 1)` with `ReleaseJobRequest(tenantId, certifyPractitionerId, releaseId)`; `readBackDeadline(String tenantId, String certifyPractitionerId, String releaseId, Instant at)` → `JobIds.of("release-readback", releaseId, 1)` with `ReleaseReadBackDeadlineJobRequest(tenantId, certifyPractitionerId, releaseId)`)
- Test: `src/integrationTest/.../ingestion/release/ReleaseServiceIT.java`, `src/integrationTest/.../ingestion/api/VendorReleaseResourceIT.java`

**Interfaces:**
```java
@ApplicationScoped public class RankingGuard { public boolean ranksCandor(String tenantId); }   // DalTenantConfigClient pages; true when a rule {type: "sourceRanking"} names "candor"; cached per tenant for auth.cacheTtl; DAL unreachable → ProblemException 503 DAL_UNAVAILABLE
@ApplicationScoped public class ReleaseService {
  public record Requested(String releaseId, String certifyPractitionerId, List<String> fieldRecommendationIds) {}
  public record Skipped(String id, String code) {}
  public Requested request(String tenantId, String fieldRecommendationId, long version, String actor, boolean bulk);            // guards, then one transaction: APPROVED|RELEASE_FAILED→RELEASE_REQUESTED with goldenRecordUpdate{releaseId, requestedBy, requestedAt, writePath SLICE, resource}, FIELD_RELEASE_REQUESTED(releaseId, bulk); enqueue release(tenantId, certifyPractitionerId, releaseId) after
  public record Bulk(List<Requested> releases, List<Skipped> skipped) {}
  public Bulk requestAll(String tenantId, List<String> ids, Map<String, Long> versions, String actor);                        // grouped per practitioner into one releaseId each; a stale or unsupported item is skipped with its code
  public Bulk requestBatch(String tenantId, String exportBatchId, String actor);                                              // every APPROVED slice field of the batch, at most releaseBulkMax
  public FieldRecommendation rollback(String tenantId, String fieldRecommendationId, long version, String reason, String actor); // APPROVED→PENDING, history, FIELD_ROLLED_BACK(reason, version); 409 ROLLBACK_NOT_ALLOWED from RELEASE_REQUESTED onward
  public List<FieldRecommendation> release(String tenantId, String releaseId); }                                               // 404 RELEASE_NOT_FOUND when empty
@ApplicationScoped public class ReleaseLifecycle {
  public record Claimed(List<FieldRecommendation> fields) {}
  public Optional<Claimed> claim(String tenantId, String certifyPractitionerId, String releaseId, Instant now);  // one transaction: moveAll RELEASE_REQUESTED→RELEASING for the releaseId; then countReleasing(tenant, practitioner, exceptReleaseId) > 0 → moveAll back and throw ReleaseInProgressException (P12); empty claim → Optional.empty (already done or rolled back)
  public void written(String tenantId, String releaseId, String resource, String crosswalkId, Instant writtenAt, UUID deadlineJobId); // $set goldenRecordUpdate.resource, .crosswalkId, .writtenAt, .deadlineJobId on the RELEASING fields; no event
  public void released(String tenantId, String certifyId, FieldLevel level, Map<String, ReadBack.Verdict> verdicts, String completionSource, Instant now); // every RELEASING field of (tenant, certifyId, level) → RELEASED with outcome, winningSource, releasedAt; FIELD_RELEASED per field; metrics releaseOutcome, releaseJob(RELEASED)
  public void failed(String tenantId, String releaseId, String reason, String error, boolean backToApproved, int jobAttempts, Instant now);
  public VendorExchangeConfig config(); } // fields → APPROVED (RANKING_MISSING, UNKNOWN_ENTITY, NAME_SOUND_CHANGED, DAL_REJECTED) or RELEASE_FAILED (EXHAUSTED); FIELD_RELEASE_FAILED per field; metrics releaseJob(RELEASE_FAILED), releaseFailedReason
```
Guards in `request`, before any write: field exists in the tenant (404 `FIELD_NOT_FOUND`); `reviewStatus.allows(RELEASE)` and `version` match (409 `RELEASE_NOT_ALLOWED`); field in `{first_name, last_name, phone, fax}` else 422 `IN_PLACE_RELEASE_UNSUPPORTED` (P10); `field == npi` → 422 `REMOVAL_NOT_RELEASED` (ask 14); `rankingGuard.ranksCandor(tenant)` else 422 `RANKING_MISSING`. An open release for the practitioner (fields in `RELEASE_REQUESTED` with one `releaseId`) is joined; otherwise a new id. Endpoints (`REVIEW_RELEASE` unless noted): `POST /v1/vendor-review/fields/{id}/release` body `{version}` → 202 `{releaseId}`; `POST /v1/vendor-review/releases` body `{ids, versions}` or `{exportBatchId, all: true}` → 202 `{releases: [{releaseId, certifyPractitionerId, fieldRecommendationIds}], skipped: [{id, code}]}`; `GET /v1/vendor-review/releases/{releaseId}` → `{releaseId, fields: [FieldView]}` (`REVIEW_READ`); `POST /v1/vendor-review/fields/{id}/rollback` body `{version, reason}` → 200 `FieldView`.

- [ ] **Step 1: Write the failing ITs**: `ReleaseServiceIT`: `requestMovesFieldAndEnqueues` (field `RELEASE_REQUESTED` with `goldenRecordUpdate.releaseId`; job `ENQUEUED` under `JobIds.of("release", releaseId, 1)`; `FIELD_RELEASE_REQUESTED` in `vendor_review_events` with `releaseId` on the envelope); `secondFieldOfSamePractitionerJoinsTheOpenRelease`; `inPlaceFieldIs422`; `rankingMissingIs422AndWritesNothing`; `rollbackFromApprovedThenRefusedOnceRequested`; `requestBatchTakesEverySliceField`; `claimBacksOffWhenAnotherReleaseIsRunning` (seed a `RELEASING` field of the practitioner under another release id: `claim` throws and leaves this release's fields `RELEASE_REQUESTED`). `VendorReleaseResourceIT`: the four endpoints' happy paths, 404 across tenants, `permissionEnforcedWhenFlagOn` for `REVIEW_RELEASE`.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** `ReleaseService` constructor (7): `FieldRecommendationRepository`, `AuditRepository`, `Transactions`, `IngestionJobEnqueuer`, `RankingGuard`, `VendorExchangeConfig`, `Clock`. **Step 4: Run** → PASS; `make openapi`. **Step 5: Commit** `feat(release): release requests, bulk release, rollback, release reads, ranking guard`.

### Task 7.3: `ReadBackService`, `OvGeneratedHandler`, `OvGeneratedResource`, `ReleaseReadBackDeadlineJob`

**Files:**
- Create: `ingestion/release/ReadBackService.java`, `ingestion/events/OvGeneratedEvent.java`, `OvGeneratedHandler.java`, `OvGeneratedResource.java`, `ingestion/jobs/ReleaseReadBackDeadlineJob.java`, `ReleaseReadBackDeadlineJobRequest.java`
- Test: `src/integrationTest/.../ingestion/events/OvGeneratedHandlerIT.java`, `OvGeneratedResourceIT.java`, `src/integrationTest/.../ingestion/jobs/ReleaseReadBackDeadlineJobIT.java`

**Interfaces:**
```java
@JsonIgnoreProperties(ignoreUnknown = true) public record OvGeneratedEvent(String eventId, String serviceSource, Payload payload) {   // survivorship's MDMEnvelope, base64 in the push data
  @JsonIgnoreProperties(ignoreUnknown = true) public record Payload(String certifyId, String tenantId, String entityType, List<Crosswalk> triggeringCrosswalks) {}
  @JsonIgnoreProperties(ignoreUnknown = true) public record Crosswalk(String certifyId, String crosswalkId, String sourceType, String updatedAt) {}
  public boolean triggeredByCandor(); public FieldLevel level(); }   // entityType practitioner → PRACTITIONER, location → LOCATION, else null
@ApplicationScoped public class ReadBackService {
  public enum Result { RELEASED, NOTHING_RELEASING, UNKNOWN_MARKED }
  public Result readAndFinish(String tenantId, String certifyId, FieldLevel level, String completionSource, Instant now); } // find RELEASING fields; read the OV by level; ReadBack.evaluate; lifecycle.released; delete each field's deadline job; a read failure propagates
@ApplicationScoped public class OvGeneratedHandler { public enum Outcome { REJECTED_UNPARSEABLE, NOT_OURS, UNKNOWN_ENTITY_TYPE, NOTHING_RELEASING, RELEASED, DISABLED } public Outcome handle(PubSubPushMessage.Message message); }
@Path("/internal/vendor-ingestion/ov-generated") @PubSubPush public class OvGeneratedResource { @POST public Response receive(PubSubPushMessage envelope); }  // 204 on every outcome; 5xx only on an exception before the write
public record ReleaseReadBackDeadlineJobRequest(String tenantId, String certifyPractitionerId, String releaseId) implements JobRequest {}
public class ReleaseReadBackDeadlineJob implements JobRequestHandler<ReleaseReadBackDeadlineJobRequest> { public static final String NAME = "release-readback"; }  // fields of the release still RELEASING: readAndFinish(…, "DEADLINE"); a read failure past the deadline → released with every field UNKNOWN and a warning
```
Handler table: `data` unparseable → 204; `!triggeredByCandor()` → `NOT_OURS`, metric `ovEventNotOurs`; level null → `UNKNOWN_ENTITY_TYPE`; `readAndFinish(payload.tenantId, payload.certifyId, level, "EVENT")`. `FIELD_RELEASED` detail: `outcome, winningSource, writePath SLICE, resource, crosswalkId, completionSource, readBackSeconds` (`writtenAt` → now).

- [ ] **Step 1: Write the failing ITs**: `OvGeneratedHandlerIT` (WireMock DAL OV): `candorTriggeredEventReleasesTheFields` (two `RELEASING` fields for the practitioner; OV with `contributionMap.firstName = candor:org-xyz`, `lastName = roster:org-xyz`; fields `RELEASED WON` and `RELEASED OUTRANKED roster:org-xyz`; two `FIELD_RELEASED`; deadline job deleted); `notOursIsAcknowledged` (`triggeringSourceTypes` roster only: nothing changes, metric 1); `locationEventReadsTheLocationOv`; `nothingReleasingIsAcknowledged`; `unparseableIsAcknowledged`. `OvGeneratedResourceIT`: token cases, 204. `ReleaseReadBackDeadlineJobIT`: `deadlineReadsOnceAndReleases` (`completionSource DEADLINE`); `deadlineReadFailureMarksUnknown` (OV 503 → fields `RELEASED` outcome `UNKNOWN`, warning).
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS; `make openapi`. **Step 5: Commit** `feat(release): read-back on mdm.slice.ov_generated with a scheduled deadline`.

### Task 7.4: `ReleaseJob`

**Files:**
- Create: `ingestion/jobs/ReleaseJob.java`, `ReleaseJobRequest.java`
- Modify: `ingestion/jobs/IngestionFinalFailureFilter.java` (`ReleaseJob` → `lifecycle.failed(…, "EXHAUSTED", …, false, attempts)`)
- Test: `src/integrationTest/.../ingestion/jobs/ReleaseJobIT.java`

**Interfaces:**
```java
public record ReleaseJobRequest(String tenantId, String certifyPractitionerId, String releaseId) implements JobRequest {}
public class ReleaseJob implements JobRequestHandler<ReleaseJobRequest> { public static final String NAME = "release"; }
```
`run`, in the design's phase order: `IngestionLogContext.openRelease`; `lifecycle.claim` (empty → exit; `ReleaseInProgressException` → propagate for retry); `rankingGuard.ranksCandor` again (false → `failed(…, "RANKING_MISSING", …, true)`); resolve the Golden practitioner `ov.practitioner(tenant, certifyPractitionerId)` and, for location fields, `ov.location(tenant, certifyLocationId)` (404 → `failed(…, "UNKNOWN_ENTITY", …, true)`); name guard for `first_name`/`last_name` against the Golden names (`!NameSound.same` → `failed(…, "NAME_SOUND_CHANGED", …, true)`); read own slices by `DalSliceClient` (404 → null); merge with `SliceMerge`; one `DalBatchClient.execute(tenant, DalBatchRequest(true, ops))` where each op is `DalOperation(resource, alias, "UPSERT", body {sourceType, crosswalkId, data}, filters {}, returning [])` for `CorePractitioner`, and for a location release `CoreLocation`, `CoreEntityAddress` plus a `LocationEntityAddress` op `{addressType: "service"}` linking the two on first write; 4xx → `failed(…, "DAL_REJECTED", body, true)`; 5xx or `ProcessingException` → propagate; success → `lifecycle.written(...)` carrying the deterministic deadline job id `JobIds.of("release-readback", releaseId, 1)`, then, after the transaction, `enqueuer.readBackDeadline(tenantId, certifyPractitionerId, releaseId, now + releaseReadBackDeadlineMinutes)`. A retry that finds the fields `RELEASING` with a `writtenAt` skips to scheduling the deadline (design: "crash after the DAL write, before the fields are marked"). The read-back itself is the handler's or the deadline job's work (Task 7.3); this job never marks `RELEASED`.

- [ ] **Step 1: Write the failing IT** (api profile, run directly, `WireMockDal` for `/batch`, `/core/...`, `/practitioners/ov/...`, `/locations/ov/...`, `/tenants/.../configurations`): `releasesAPractitionerFieldEndToEnd` (`/batch` body `equalToJson` of `{"transaction":true,"operations":[{"resource":"CorePractitioner","alias":"practitioner","operation":"UPSERT","body":{"sourceType":"candor:org-xyz","crosswalkId":"1003002890","data":{"npi":"1003002890","firstName":"Daniel","lastName":"Smith"}},"filters":{},"returning":[]}]}` with the two DAL headers; fields `RELEASING` with `writtenAt`; deadline job `SCHEDULED` at now + 30 min); `secondReleaseKeepsLastMonthsValue` (own slice read answers last month's `lastName`; the body carries both names); `locationReleaseWritesThreeSlicesAndTheLink` (crosswalk from `Crosswalks.location`; `contacts[]` keeps the Golden fax and replaces the phone); `rankingMissingWritesNothing` (zero `/batch` calls; fields back to `APPROVED`; `FIELD_RELEASE_FAILED RANKING_MISSING`); `unknownEntityWritesNothing`; `nameSoundChangedWritesNothing` (`Bob` → `Robert`); `dalRejectsWithFourHundred` (fields `APPROVED`, `error` holds the DAL body); `dalDownPropagates` (503 → exception, fields still `RELEASING`; second run writes); `retryAfterWriteSchedulesTheDeadlineOnly`; `concurrentJobsForOnePractitionerSerialise` (two releases for one practitioner run on two threads: exactly one `/batch` call completes before the other job's claim succeeds; after both, two `/batch` calls, the second body containing the first's value).
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** `ReleaseJob` constructor (7): `ReleaseLifecycle`, `RankingGuard`, `@RestClient DalSliceClient`, `@RestClient DalOperationalValueClient`, `@RestClient DalBatchClient`, `IngestionJobEnqueuer`, `Clock`; config reaches it through `ReleaseLifecycle.config()`. **Step 4: Run** → PASS. **Step 5: Commit** `feat(release): release job merges and writes the candor slices through the DAL, then schedules the read-back deadline`.

### Gate 7

- [ ] CI green; oasdiff accepts five paths.
- [ ] **Live verification before the finding (read-only, staging):** one `GET /practitioners/ov/{id}` and one `GET /locations/ov/{id}` to pin the `contributionMap` key shape and the `contacts[]` shape (P9, ask 17); one `GET /core/entity-addresses/entity/{entityId}/crosswalk-id` to confirm `Crosswalks.location` against a real crosswalk; `GET /tenants/{t}/configurations` to confirm the `sourceRanking` rule shape (P8). Record each with the request and the relevant response keys (no values) in finding 103, "CP-40437 Step 7", with P9, P12, P13 and the `DalOperation` body shape as accepted by `TransactionalBatchService`.
- [ ] Merge.

---

## Step 8: in-place releases through the api-layer (4 days)

Branch `feature/CP-40437-step-8-in-place`. Design step 8, D22: `accepting_new_patients`, `pcp_at_location` (`PUT /practitioners/{id}/locations`), `specialties` (`PATCH /practitioners/group-practitioner-location-specialties/specialty`), `practice_name` and `location` (`PUT /locations/update`). Needs ask 19 (a service identity on those endpoints) and the junction ids the api-layer requests want, which this service does not hold. Can slip without blocking Step 9 (P10). The most uncertain step: Task 8.1 is a contract pin against a live read before any code.

### Task 8.1: Pin the api-layer contracts and the id resolution

**Files:**
- Create: `src/integrationTest/resources/fixtures/api-layer/group-practitioner-locations.json`, `tenant-specialties.json` (captured shapes, ids replaced by `cert-…`, `tgl-…` placeholders, no names or addresses)
- Modify: `docs/decisions.md` (finding 104, the pinned contracts)

- [ ] **Step 1: Read once, read-only, against staging:** `GET /practitioners/ov/{certifyPractitionerId}/group-practitioner-locations` (DAL) for a practitioner with two locations; `GET /tenant-specialties` (api-layer) for the tenant; the request records of `UpdatePractitionerLocationsRequest(tenantGroupId, locations[UpdatePractitionerLocationRequest(tenantGroupLocationId, …, networks[])])`, `CorrectLocationSpecialtyRequest(groupPractitionerLocationSpecialtyId, newTenantSpecialtyId)` and `LocationUpdateRequest(locationId, crosswalkId, entityAddressId, groupLocationId, tenantGroupLocationId, name, acceptNewPatients, …)` from the api-layer sources, noting which fields are required and which the UI sends for a one-field edit.
- [ ] **Step 2: Write** the two fixtures and finding 104: for the echoed `certifyLocationId`, which row of the group-practitioner-locations response carries `tenantGroupLocationId`, `tenantGroupId`, `groupLocationId`, `locationId`, `entityAddressId` and the specialty link ids; how a vendor specialty string maps to a `tenantSpecialtyId` (exact name match, case-insensitive; no match → `SPECIALTY_UNRESOLVED`); which `acceptNewPatients` encoding the api-layer expects.
- [ ] **Step 3: Commit** `docs(decisions): api-layer in-place contracts pinned for the release step`.

### Task 8.2: `ApiLayerClient` additions, `JunctionResolver`, `InPlaceWriter`

**Files:**
- Modify: `clients/ApiLayerClient.java` (four methods, each with `@HeaderParam("tenant-id")`), request records beside it (`ApiLayerPractitionerLocationsRequest`, `ApiLayerLocationSpecialtyCorrection`, `ApiLayerLocationUpdateRequest`, `ApiLayerTenantSpecialty`)
- Create: `ingestion/release/JunctionResolver.java`, `InPlaceWriter.java`
- Modify: `clients/DalOperationalValueClient.java` (`GET /practitioners/ov/{entityId}/group-practitioner-locations`)
- Test: `src/integrationTest/.../clients/ApiLayerClientIT.java` (new cases), `src/test/.../ingestion/release/JunctionResolverTest.java`, `src/integrationTest/.../ingestion/release/InPlaceWriterIT.java`

**Interfaces:**
```java
@ApplicationScoped public class JunctionResolver {
  public record Junction(String tenantGroupId, String tenantGroupLocationId, String groupLocationId, String locationId, String entityAddressId, Map<String, String> specialtyLinkIdsByTenantSpecialtyId) {}
  public Optional<Junction> resolve(String tenantId, String certifyPractitionerId, String certifyLocationId); }   // from the DAL group-practitioner-locations read; the fixture's field names
@ApplicationScoped public class InPlaceWriter {
  public record Written(String resource, String changeReason) {}
  public Written write(String tenantId, String certifyPractitionerId, JunctionResolver.Junction junction, String field, String value, String releaseId); }  // one api-layer call per field group; changeReason = "directory-accuracy release <releaseId>"; 4xx → ApiLayerRejectedException(body)
```
Field groups: `accepting_new_patients`, `pcp_at_location` → `PUT /practitioners/{id}/locations` with the resolved `tenantGroupId` and the one location entry changed; `specialties` → `PATCH …/group-practitioner-location-specialties/specialty` per entry whose tenant specialty differs; `practice_name`, `location` → `PUT /locations/update` with the resolved ids and the one field changed. Unresolved junction → `UNKNOWN_ENTITY`; unresolved specialty → `SPECIALTY_UNRESOLVED`.

- [ ] **Step 1: Write the failing tests** from the fixtures: `JunctionResolverTest.picksTheRowForTheEchoedLocation`, `missingLocationIsEmpty`; `ApiLayerClientIT.inPlaceCallsCarryTenantAndToken` (three stubs, bodies `equalToJson` of the fixture-derived requests); `InPlaceWriterIT.oneCallPerFieldGroup`, `fourHundredIsRejected`.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS. **Step 5: Commit** `feat(release): api-layer in-place writes and junction resolution`.

### Task 8.3: In-place fields in `ReleaseService` and `ReleaseJob`

**Files:**
- Modify: `ingestion/release/ReleaseService.java` (the 422 for in-place fields becomes `writePath IN_PLACE`), `ingestion/release/ReleaseLifecycle.java` (`releasedInPlace`), `ingestion/jobs/ReleaseJob.java`
- Test: `src/integrationTest/.../ingestion/jobs/ReleaseJobIT.java` (in-place cases), `ReleaseServiceIT.java`

`ReleaseJob`: after the slice phase (or instead of it when the release holds in-place fields only), resolve the junction once per location, `InPlaceWriter.write` per field, read the value back from the junction row at once (`resolve` again) and `lifecycle.releasedInPlace(...)`: field `RELEASED` with `outcome WON`, `writePath IN_PLACE`, `resource`, `completionSource EVENT` is not applicable so `DIRECT`; `FIELD_RELEASED`. An api-layer 4xx fails those fields only (`API_LAYER_REJECTED`, back to `APPROVED`); slice fields of the same release are unaffected (design). `ReleaseService.requestAll` groups by practitioner as before; a release may hold both kinds.

- [ ] **Step 1: Write the failing ITs**: `ReleaseJobIT.inPlaceFieldIsWrittenThroughTheApiLayerAndReleasedAtOnce`; `apiLayerRejectionFailsInPlaceFieldsOnly` (one slice field `RELEASING` awaiting read-back, one in-place field back to `APPROVED`); `unresolvedSpecialtyFails`; `ReleaseServiceIT.inPlaceFieldIsAccepted` (replaces `inPlaceFieldIs422`).
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS. **Step 5: Commit** `feat(release): in-place releases of junction and location fields`.

### Gate 8

- [ ] CI green. Finding 105, "CP-40437 Step 8": the contracts as pinned, `completionSource DIRECT`, what a roster overwrite does to an in-place edit (design: expected). Merge.

---

## Step 9: whole-lane proof, gauges, runbook, hand-over (2 days)

Branch `feature/CP-40437-step-9-lane-docs`.

### Task 9.1: `IngestionLaneIT` and `IngestionStateGauges`

**Files:**
- Create: `src/integrationTest/.../ingestion/IngestionLaneIT.java`, `ingestion/IngestionStateGauges.java`

Worker role, JobRunr executing every job, driven over HTTP like `ExportLaneIT`: a `DELIVERED` batch from `EgressFixtures`; the arrival push lands; `RequestIngestionJob` copies (mocked `ArchiveStore`) and registers (WireMock); the completion push lands; `StageRowsJob` streams five rows; a reviewer lists, approves `first_name`, rejects `phone` with a comment, skips `fax`; a second arrival for the batch is `DUPLICATE_LOGGED`; a release of `first_name` writes the slice and the ov-generated push lands → `RELEASED WON`. Scenarios, one tenant each, `@TestMethodOrder`: (1) event path to `STAGED`, review, release to `RELEASED`; (2) completion event withheld, deadline check run directly, `completionSource DEADLINE`; (3) `INVALID` completion → `INGESTION_FAILED`, visible in the inbound-files read; (4) ov-generated event withheld, read-back deadline job run directly, `completionSource DEADLINE`. Each asserts the audit trail in insertion order from both event collections and the design's post-staging entry sample field for field. A fifth test scrapes `/q/metrics` and asserts the design's metric names with the values the scenarios produced, including the gauges, and that no label is named `npi` or holds a ten-digit value.

- [ ] **Step 1: Write the scenarios and the metrics test.** **Step 2: Run** `./gradlew integrationTest --tests '*IngestionLaneIT*'` → FAIL on the gauges. **Step 3: Implement** `IngestionStateGauges` (`inbound.by_state`, `inbound.age_seconds`, `review.pending{tenant, field}`, `review.pending_age_days`, `parse.wait_seconds`, `release.readback_seconds`; one aggregation each, cached 60 s, as `ExportStateGauges`). **Step 4: Run** → PASS. **Step 5: Commit** `test(ingestion): whole-lane proof on the worker role; state gauges`.

### Task 9.2: Runbook, README, HANDOFF, AGENTS, OpenAPI, decisions, Jira

**Files:**
- Modify: `docs/runbook.md` (sections 4 and 5: the hook-ups and grants of asks 1 to 23; section 7: operator procedures for unmatched uploads, inbound files, review, release, rollback, each with the exact `curl` the lane test performs; section 8: failure rows from the design's edge-case table; section 10: alerts A1 to A16 with the `AUDIT` line or metric that evaluates each, in the E1 to E11 row format), `README.md` (layout: the `ingestion/` lane and its jobs), `HANDOFF.md` (end state, what the next subtask starts from, the design-doc edits), `AGENTS.md` (one line: `ingestion/` is `inbound/`, `events/`, `jobs/`, `staging/`, `review/`, `release/`, `api/`), `openapi/openapi.yaml` (final `make openapi`), `docs/decisions.md` (finding 106: Step 9 gate and the definition of done)

- [ ] **Step 1: Write the documentation** as listed.
- [ ] **Step 2: Run** `make openapi && make lint && ./gradlew test` → clean; CI runs the suite.
- [ ] **Step 3: Jira**: the implementation-details text for CP-40437 (and the per-step subtasks) summarises the lane; the design-doc edits below go to the design's author.
- [ ] **Step 4: Commit** `docs: ingestion runbook, alerts A1 to A16, hand-over state`.

### Task 9.3 (optional, needs the owner's decision): deployable names

The design names the instance groups `directory-accuracy-api` and `directory-accuracy-worker` (module 1 open question Q10); `terraform/internal/` has `vendor-exchange-api` and `vendor-exchange-worker`, and the hostname, load balancer, tags and secret names carry `vendor-exchange`. Renaming is a deployment change with no code effect (the Java package, Mongo database and metric prefixes stay module-specific by the design's own rule). Do it only when the owner confirms the repository rename, as its own PR touching `terraform/internal/*.tf`, `Makefile`, `docs/runbook.md` section 1 and `README.md`.

### Gate 9 and definition of done

- [ ] Every gate 0 to 8 has a finding; `IngestionLaneIT`'s four scenarios pass in CI with their recorded event sequences in finding 106.
- [ ] Every endpoint of design steps 2, 4, 6 and 8 plus the operator reads is in `openapi/openapi.yaml` with problem+json responses; every new audit event type has its contract row copied from the design; every design metric is asserted after the lane.
- [ ] No foundation package changed beyond Step 0's additions (recorded); `export/` has no dependency on `ingestion/` (ArchUnit).
- [ ] A second engineer can follow runbook section 7 to attach an unmatched file, review a batch and release a field against the integration-test stack: by construction, since the section's calls are the ones `IngestionLaneIT` makes in the same order.

---

## Design-doc edits this plan asks for (for the design's author)

1. Paths carry the `/v1/` prefix (P1); the operator endpoints of Task 2.3 and the field trail endpoint of Task 5.1 are listed.
2. One push audience for the four subscriptions (P2); ask 3 says so.
3. `EXPORT_BATCH_ACKNOWLEDGED` carries `uploadId`; `ExportBatch.inboundBatchId` holds the upload id.
4. "Module 1 no longer keeps a per-NPI export registry" is wrong for this repository; the decision (no lookup, P5) stands.
5. `goldenRecordUpdate` carries `requestedBy`, `requestedAt`, `writePath`, `resource`, `crosswalkId`, `writtenAt`, `deadlineJobId`, `error` as well as the four listed.
6. The reconciler index is `(inboundFiles.state, inboundFiles.updatedAt)`, and the reconciler runs every ten minutes over `RECEIVED`, `ARCHIVING`, `PARSED`, `STAGING`.
7. Release job ids are unique per release; serialisation per practitioner is claim-then-count (P12). JobRunr ignores an enqueue whose id exists in storage, so "later requests enqueue the same job id again and run after it" does not hold.
8. The read-back deadline is a scheduled job per release, deleted on the event (P13); the reconciler does not scan fields.
9. `RELEASE` is allowed from `RELEASE_FAILED` (an operator re-release after a fix); an operator retry endpoint for a `FAILED` inbound entry is a later amendment.
10. `releaseWorkerConcurrency` is the JobRunr worker count (P14).
11. `FILE_INGESTION_REFUSED` as a `FAILED` reason; `MAPPING_UNKNOWN` also covers an unset mapping id; `COUNT_MISMATCH` and `ROWS_EXPIRED` carry `failedStep STAGE`.
12. In-place releases finish with `completionSource DIRECT`; `IN_PLACE_RELEASE_UNSUPPORTED`, `REMOVAL_NOT_RELEASED` and `SPECIALTY_UNRESOLVED` as codes.
13. The `location` normalisation rule names four legs; the staged `location` value carries five (`address_line2` included).

## Rollout asks this plan adds

| # | Ask | Owner | Why |
| --- | --- | --- | --- |
| 21 | file-ingestion accepts this service's machine identity: a DAL user row for the Auth0 machine client's subject with a role in every participating tenant, or a service-principal path in its `UserContextFilter` | file-ingestion team; platform | Its filter needs an email claim and a tenant role; the machine token has neither (P7) |
| 22 | Deployment environment gains `INGESTION_ARCHIVE_BUCKET`, `FILE_INGESTION_URL`, `FILE_INGESTION_MAPPING_ID`, `FILE_INGESTION_IAP_AUDIENCE`, and the push audience of ask 3 reuses `PUBSUB_PUSH_AUDIENCE` | DevOps; this team | The config keys of Task 0.1 and P2 |
| 23 | DAL permission rows for `vendor-review` actions `read`, `decide`, `release` | Platform | `Permission` enum, enforced when `vendor-exchange.permissions.enforce` is on |
| 24 | Decide the deployable rename to `directory-accuracy-api` / `directory-accuracy-worker` (module 1 Q10) and whether the repository is renamed | Service owner | Task 9.3 |
