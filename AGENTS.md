# AGENTS.md — vendor-exchange-service

The one guidance file for coding agents and people. Other tool files point here. Every rule below is enforced by a lint or a test, or says which file enforces it; a rule that cannot be enforced is not listed.

## What this service is

- Directory accuracy vendor exchange: scheduled practitioner export to an accuracy vendor (phase 1), ingestion of the vendor's response (phase 2). Design: Confluence "CP-39602 - Design: Directory accuracy vendor export". Plan: `cos-docs/directory-accuracy/vendor-export/scaffolding-implementation-plan.md`.
- One image, two roles by Quarkus profile: `api` (HTTP behind the load balancer) and `worker` (JobRunr server, no inbound traffic). `RoleProfileCheck` refuses to boot with neither or both.
- Quarkus 3.33.4, JobRunr 8.8.2, MongoDB database `vendor_exchange`, Java 21. Imperative code only (blocking Mongo driver, JobRunr); no Mutiny.

## Layout (ArchitectureTest enforces the dependency rules)

- `config/`, `persistence/`, `audit/`, `auth/`, `http/`, `clients/`, `metrics/` are shared foundations. They never import `export/` or any other feature package. Operator endpoints, including ops actions such as the tick, live in `export/api/`.
- `export/` is the export lane: `schedule/`, `batch/`, `jobs/`, `events/`, `api/`. Phase 2 adds `ingestion/` beside it.
- No `util`, `common` or `helpers` package. A helper lives with the feature that uses it.
- Tests: `src/test` is plain JUnit (logic, ArchUnit, SmallRye config); `src/integrationTest` is Quarkus wiring on Testcontainers MongoDB with exactly two profiles, `ApiTestProfile` and `WorkerTestProfile`.

## Rules that lints enforce

- Constructor injection only; a single constructor needs no `@Inject` (ArchitectureTest `noFieldInjection`).
- One logger: `org.jboss.logging.Logger`, printf style (`infof("%s", x)`), never `{}` (ArchitectureTest, Checkstyle).
- One `ObjectMapper`, injected; no `new ObjectMapper()`; no `Executors.new*`; no `Thread.sleep` (ArchitectureTest).
- Typed contracts: no `JsonNode` in `api`, `events`, `clients` (ArchitectureTest); DTOs are records.
- Every HTTP endpoint carries `@Operation`; every `export/api` endpoint carries `@RequiresPermission` (ArchitectureTest). `openapi/openapi.yaml` must equal what the code produces (`verifyOpenApi` in `check`; regenerate with `make openapi`); the `openapi` CI job refuses breaking changes against `main`.
- 500 lines per main file, 1,000 per test file, 7 constructor parameters (Checkstyle).
- Ticket keys (`CP-nnnnn`) only in commits and PRs, never in source (Checkstyle `BanTicketKeys`). `@Disabled` needs a reason with a ticket (`DisabledNeedsTicket`).
- Single-letter local variables are refused (`LocalVariableName`); `catch (Exception e)` is fine (a parameter).
- Versions only in `gradle/libs.versions.toml`; SNAPSHOT fails the build; no `mavenLocal()`.
- Spotless (Palantir format), Checkstyle, SpotBugs all run in `./gradlew check`, which CI runs. SpotBugs expose-internal-representation rules are off as policy; records copy collections in compact constructors.

## Rules the code and config carry

- Config is one closed `@ConfigMapping`, `VendorExchangeConfig`. Required infrastructure values have no default. Swap-in points are `Optional` and map to `${ENV:}`; empty reads as absent.
- HTTP policy is deny by default; `/q/*` and `/internal/*` are permitted and guarded elsewhere; operator endpoints live under `/v1/` and require a platform token.
- Every batch state change is a compare-and-set on `state` inside a Mongo transaction, with its audit event in the same transaction. Nothing outside MongoDB (an enqueue, a publish) ever runs inside `Transactions.run`.
- Every repository method takes the tenant or an id that embeds it; every list query carries a limit.
- Logs carry ids and counts, never bodies, DTOs, bound values or PII. Job code opens `export/jobs/JobLogContext` first, so every line carries `exportBatchId`, `tenantId` and `jobId` on the MDC.
- Metrics are the design's names only, through `metrics/VendorExchangeMetrics`; tags are tenant, vendor, state, reason, source, status, disposition, action, result, outcome, eventType or job, never a practitioner, an NPI, a location or a field value. Database-backed gauges go through `registerGauge` with a cached supplier (`export/ExportStateGauges`).
- Error responses are RFC 9457 problem+json with a stable `code` (`http/Problem`), rendered only by `http/ProblemMappers` and the auth filters' aborts through `http/Problems`. Resources throw; nothing builds an error body by hand.
- Outbound identity: `auth/GoogleIdTokenService` mints one Google ID token per IAP audience (`DalAuthFilter`, `clients/EgressAuthFilter`); `clients/ApiLayerTokenService` holds the Auth0 machine token (`ApiLayerAuthFilter`) and reports "not configured" on readiness until the client exists. REST client interfaces in `clients/` carry typed records only; the tenant header is a method parameter, never ambient state.
- Inbound auth: the HTTP policy requires a platform token on `/v1/*`; `auth/UserContextFilter` requires `tenant-id` and a role in that tenant (DAL lookup, cached by email); `auth/PermissionFilter` checks `@RequiresPermission` only when `vendor-exchange.permissions.enforce` is true. `/internal/*` is guarded by `auth/PubSubPushAuthFilter` (Google OIDC push token), closed with 401 until the push settings exist.

## Working

- Local: `make bootstrap` once, then `make compile` and `make lint`. Tests run in CI (`make check` runs them locally if you must; the integration tests need Docker).
- Branch `feature/<kebab-title>` from `main`, Conventional Commits (hook), PR with the template, CI green, code-owner review. `main` is protected.
- Record every finding and deviation in `docs/decisions.md` with what was checked; architecture decisions go to `docs/adr/`.
- Deploys: `terraform/internal/` owns the service shape; CI changes only the image tag (Step 8). Never deploy to production from a laptop.
- Never trust a snippet or a framework memory: verify against the jar (`javap`), the source, or a running test before relying on it.
