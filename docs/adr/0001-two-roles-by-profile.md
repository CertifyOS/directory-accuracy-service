# ADR 0001 — One image, two roles by Quarkus profile on two GCE instance groups

Status: Accepted, 2026-10-01. Source: design doc decision D1 and D3.

## Context

The service has two halves with different shapes. The api half is small, request-driven and must sit behind a load balancer. The worker half runs JobRunr jobs of up to ten minutes, has no inbound traffic and must survive deploys without killing a job. Cloud Run bills the always-on worker per instance-second and gives it ten seconds after SIGTERM; a managed instance group drains on a window we set. Nothing else in the organisation runs on GCE; `file-ingestion-service` is the first and proved the topology.

## Decision

- One container image. The role is the active Quarkus profile: `QUARKUS_PROFILE=prod,api` or `prod,worker`. `RoleProfileCheck` throws at startup unless exactly one is active.
- `%worker` is the only profile that enables the JobRunr background server and dashboard. `%api` keys are added only when the api role needs something the worker must not have.
- Two regional managed instance groups from one instance template per role: `directory-accuracy-api` (autoscaled, behind an external HTTPS load balancer) and `directory-accuracy-worker` (fixed size, opportunistic updates, proactive rollout by hand after a drain).
- Two service accounts, one per group, with the least privilege each role needs.

## Consequences

- Role-dependent behaviour must be a runtime property. Build-time-fixed Quarkus keys cannot differ by role; `quarkus.shutdown.delay-enabled` was the first one caught.
- Every integration test picks a profile (`ApiTestProfile` or `WorkerTestProfile`); there is no profile-less boot.
- Deploy order matters: deploy, proactive worker rollout, wait for stable, apply to restore opportunistic updates.
