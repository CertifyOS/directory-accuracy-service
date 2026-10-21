package com.certifyos.directory_accuracy.export.api;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.audit.AuditEvent;
import com.certifyos.directory_accuracy.audit.AuditEventType;
import com.certifyos.directory_accuracy.audit.AuditRepository;
import com.certifyos.directory_accuracy.auth.GoogleIdTokenService;
import com.certifyos.directory_accuracy.auth.UserContextFilter;
import com.certifyos.directory_accuracy.auth.WireMockDal;
import com.certifyos.directory_accuracy.export.batch.BatchState;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.batch.ExportBatchRepository;
import com.certifyos.directory_accuracy.export.batch.ExportNpiRepository;
import com.certifyos.directory_accuracy.export.jobs.EgressFixtures;
import com.certifyos.directory_accuracy.export.jobs.JobIds;
import com.certifyos.directory_accuracy.export.jobs.RequestEgressJob;
import com.certifyos.directory_accuracy.export.jobs.SelectJob;
import com.certifyos.directory_accuracy.export.schedule.Cadence;
import com.certifyos.directory_accuracy.export.schedule.Schedule;
import com.certifyos.directory_accuracy.export.schedule.ScheduleRepository;
import com.certifyos.directory_accuracy.http.Problem;
import com.certifyos.directory_accuracy.persistence.Transactions;
import com.mongodb.client.model.Updates;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.security.TestSecurity;
import io.quarkus.test.security.jwt.Claim;
import io.quarkus.test.security.jwt.JwtSecurity;
import io.restassured.RestAssured;
import io.restassured.specification.RequestSpecification;
import jakarta.inject.Inject;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;
import org.hamcrest.Matchers;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.storage.StorageProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Batch reads, retry and supersede through the endpoint, tenant-scoped. Unique email for this class (finding 72). */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@QuarkusTestResource(WireMockDal.class)
@TestProfile(ApiTestProfile.class)
class BatchResourceIT {

    static final String EMAIL = UserContextFilter.EMAIL_CLAIM;
    static final String TENANT = "org-batch";
    static final String BASE = "/v1/vendor-exports";

    @Inject
    ExportBatchRepository batches;

    @Inject
    ExportNpiRepository npis;

    @Inject
    ScheduleRepository schedules;

    @Inject
    AuditRepository audit;

    @Inject
    Transactions transactions;

    @Inject
    StorageProvider storage;

    @InjectMock
    GoogleIdTokenService google;

    @BeforeEach
    void before() {
        Mockito.when(google.idToken("test-dal-iap-client-id")).thenReturn("dal-id-token");
        WireMockDal.stubMember("batch@certifyos.com", TENANT);
    }

    private static RequestSpecification member() {
        return RestAssured.given().header("tenant-id", TENANT).contentType("application/json");
    }

    private ExportBatch batch(String tenant, String vendor, BatchState state, int npiCount) {
        return EgressFixtures.batchIn(
                transactions,
                batches,
                npis,
                tenant,
                vendor,
                state,
                npiCount,
                Instant.now().minusSeconds(3600));
    }

    private ExportBatch failedAt(String vendor, ExportBatch.FailedStep step) {
        ExportBatch batch = step == ExportBatch.FailedStep.SELECT
                ? EgressFixtures.batchIn(
                        transactions, batches, npis, TENANT, vendor, BatchState.NPIS_SELECTED, 0, Instant.now())
                : batch(TENANT, vendor, BatchState.EGRESS_REQUESTED, 2);
        transactions.run(session -> {
            BatchState from =
                    step == ExportBatch.FailedStep.SELECT ? BatchState.NPIS_SELECTED : BatchState.EGRESS_REQUESTED;
            if (step == ExportBatch.FailedStep.SELECT) {
                // the fixture cannot stop at SCHEDULED with details set; walk back through FAILED instead
                batches.transition(
                        session,
                        batch.id(),
                        from,
                        BatchState.FAILED,
                        Instant.now(),
                        Updates.combine(Updates.set("failedStep", "SELECT"), Updates.set("lastError", "boom")));
                batches.transition(session, batch.id(), BatchState.FAILED, BatchState.SCHEDULED, Instant.now(), null);
                batches.transition(
                        session,
                        batch.id(),
                        BatchState.SCHEDULED,
                        BatchState.FAILED,
                        Instant.now(),
                        Updates.combine(Updates.set("failedStep", "SELECT"), Updates.set("lastError", "boom")));
            } else {
                batches.transition(
                        session,
                        batch.id(),
                        from,
                        BatchState.FAILED,
                        Instant.now(),
                        Updates.combine(
                                Updates.set("failedStep", "EGRESS"), Updates.set("lastError", "egress said no")));
            }
            return null;
        });
        return batches.find(batch.id()).orElseThrow();
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "batch@certifyos.com")})
    void readShowsTheDocumentWithoutInternalJobIds() {
        ExportBatch batch = batch(TENANT, "read", BatchState.EGRESS_REQUESTED, 2);
        member().get(BASE + "/" + batch.id())
                .then()
                .statusCode(200)
                .body("id", Matchers.equalTo(batch.id()))
                .body("state", Matchers.equalTo("EGRESS_REQUESTED"))
                .body("attempt", Matchers.equalTo(1))
                .body("selection.criteria.'data.delegationStatus'.in", Matchers.contains("Direct"))
                .body("egress.correlationId", Matchers.equalTo(batch.egress().correlationId()))
                .body(
                        "egress.destination.objectName",
                        Matchers.equalTo(batch.egress().destination().objectName()))
                .body("egress", Matchers.not(Matchers.hasKey("deadlineJobId")))
                .body("file.name", Matchers.equalTo(batch.file().name()));
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "batch@certifyos.com")})
    void anotherTenantsBatchReadsAsNotFound() {
        ExportBatch theirs = batch("org-batch-other", "candor", BatchState.EGRESS_REQUESTED, 1);
        member().get(BASE + "/" + theirs.id())
                .then()
                .statusCode(404)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("BATCH_NOT_FOUND"));
        member().body("{\"reason\":\"x\"}")
                .post(BASE + "/" + theirs.id() + "/retry")
                .then()
                .statusCode(404);
        member().get(BASE + "/" + theirs.id() + "/npis").then().statusCode(404);
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "batch@certifyos.com")})
    void listIsTenantScopedAndNarrowsByPeriod() {
        ExportBatch mine = batch(TENANT, "list", BatchState.EGRESS_REQUESTED, 1);
        ExportBatch theirs = batch("org-batch-list-other", "candor", BatchState.EGRESS_REQUESTED, 1);
        member().get(BASE + "?period=2026-10")
                .then()
                .statusCode(200)
                .body("items.id", Matchers.hasItem(mine.id()))
                .body("items.id", Matchers.not(Matchers.hasItem(theirs.id())));
        member().get(BASE + "?period=2025-01")
                .then()
                .statusCode(200)
                .body("items.id", Matchers.not(Matchers.hasItem(mine.id())));
        member().get(BASE + "?period=october").then().statusCode(400).body("code", Matchers.equalTo("PERIOD_INVALID"));
        member().get(BASE + "?tenantId=org-batch-list-other")
                .then()
                .statusCode(403)
                .body("code", Matchers.equalTo("TENANT_MISMATCH"));
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "batch@certifyos.com")})
    void npisAreKeysetPagedByNpi() {
        ExportBatch batch = batch(TENANT, "npis", BatchState.EGRESS_REQUESTED, 5);
        String first = member().get(BASE + "/" + batch.id() + "/npis?limit=2")
                .then()
                .statusCode(200)
                .body("items.size()", Matchers.equalTo(2))
                .body("items[0].npi", Matchers.equalTo("1000000000"))
                .body("items[0].certifyPractitionerId", Matchers.equalTo("p-0"))
                .body("nextAfter", Matchers.equalTo("1000000001"))
                .extract()
                .path("nextAfter");
        String second = member().get(BASE + "/" + batch.id() + "/npis?limit=2&after=" + first)
                .then()
                .statusCode(200)
                .body("items.npi", Matchers.contains("1000000002", "1000000003"))
                .extract()
                .path("nextAfter");
        member().get(BASE + "/" + batch.id() + "/npis?limit=2&after=" + second)
                .then()
                .statusCode(200)
                .body("items.npi", Matchers.contains("1000000004"))
                .body("nextAfter", Matchers.nullValue());
        member().get(BASE + "/" + batch.id() + "/npis?limit=0")
                .then()
                .statusCode(400)
                .body("code", Matchers.equalTo("INVALID_REQUEST"));
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "batch@certifyos.com")})
    void retryFromFailedSelectGoesBackToScheduledWithASelectJobAtAttemptTwo() {
        ExportBatch failed = failedAt("retry-sel", ExportBatch.FailedStep.SELECT);
        UUID expectedJob = JobIds.of(SelectJob.NAME, failed.id(), 2);

        member().body("{\"reason\":\"api-layer is back\"}")
                .post(BASE + "/" + failed.id() + "/retry")
                .then()
                .statusCode(202)
                .body("exportBatchId", Matchers.equalTo(failed.id()))
                .body("attempt", Matchers.equalTo(2))
                .body("jobId", Matchers.equalTo(expectedJob.toString()));

        ExportBatch after = batches.find(failed.id()).orElseThrow();
        Assertions.assertEquals(BatchState.SCHEDULED, after.state());
        Assertions.assertEquals(2, after.attempt());
        Assertions.assertNull(after.failedStep());
        Assertions.assertNull(after.lastError());
        Assertions.assertEquals(
                StateName.ENQUEUED, storage.getJobById(expectedJob).getState());
        AuditEvent event = audit.findForBatch(failed.id(), 20).stream()
                .filter(found -> found.type() == AuditEventType.EXPORT_RETRY_REQUESTED)
                .findFirst()
                .orElseThrow();
        Assertions.assertEquals("batch@certifyos.com", event.actor());
        Assertions.assertEquals("FAILED", event.detail().get("fromState"));
        Assertions.assertEquals("SELECT", event.detail().get("failedStep"));
        Assertions.assertEquals(2, event.detail().get("newAttempt"));
        Assertions.assertEquals("api-layer is back", event.detail().get("reason"));
        Assertions.assertEquals(2, event.attempt());
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "batch@certifyos.com")})
    void retryFromFailedEgressGoesBackToNpisSelectedWithAnEgressRequestJob() {
        ExportBatch failed = failedAt("retry-egr", ExportBatch.FailedStep.EGRESS);
        UUID expectedJob = JobIds.of(RequestEgressJob.NAME, failed.id(), 2);

        member().body("{\"reason\":\"egress fixed\"}")
                .post(BASE + "/" + failed.id() + "/retry")
                .then()
                .statusCode(202)
                .body("attempt", Matchers.equalTo(2))
                .body("jobId", Matchers.equalTo(expectedJob.toString()));

        ExportBatch after = batches.find(failed.id()).orElseThrow();
        Assertions.assertEquals(BatchState.NPIS_SELECTED, after.state());
        Assertions.assertEquals(
                failed.egress().correlationId(),
                after.egress().correlationId(),
                "the prior correlation stays for the cancel");
        Assertions.assertEquals(
                StateName.ENQUEUED, storage.getJobById(expectedJob).getState());
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "batch@certifyos.com")})
    void retryIsRefusedOutsideFailedAndWithoutAReason() {
        ExportBatch running = batch(TENANT, "retry-no", BatchState.EGRESS_REQUESTED, 1);
        member().body("{\"reason\":\"x\"}")
                .post(BASE + "/" + running.id() + "/retry")
                .then()
                .statusCode(409)
                .body("code", Matchers.equalTo("RETRY_NOT_ALLOWED"));
        ExportBatch failed = failedAt("retry-noreason", ExportBatch.FailedStep.EGRESS);
        member().body("{}")
                .post(BASE + "/" + failed.id() + "/retry")
                .then()
                .statusCode(400)
                .body("code", Matchers.equalTo("REASON_REQUIRED"));
        Assertions.assertEquals(
                BatchState.FAILED, batches.find(failed.id()).orElseThrow().state());
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "batch@certifyos.com")})
    void supersedeFromDeliveredCreatesSeqTwoMarksTheOldAndMovesTheSchedule() {
        Schedule schedule = Schedule.create(
                TENANT,
                "candor",
                Cadence.monthly(1),
                ZoneId.of("UTC"),
                EgressFixtures.CRITERIA,
                "tpl-1",
                Instant.now().plusSeconds(86400),
                "user:ops",
                Instant.now());
        ExportBatch delivered = batch(TENANT, "candor", BatchState.EGRESS_COMPLETED, 2);
        transactions.run(session -> {
            schedules.insert(session, schedule);
            batches.transition(
                    session,
                    delivered.id(),
                    BatchState.EGRESS_COMPLETED,
                    BatchState.DELIVERED,
                    Instant.now(),
                    Updates.set("deliveredAt", Instant.now()));
            return null;
        });
        String newId = TENANT + "-candor-2026-10-002";
        UUID expectedJob = JobIds.of(SelectJob.NAME, newId, 1);

        member().body("{\"reason\":\"wrong template version\"}")
                .post(BASE + "/" + delivered.id() + "/supersede")
                .then()
                .statusCode(201)
                .body("exportBatchId", Matchers.equalTo(newId))
                .body("jobId", Matchers.equalTo(expectedJob.toString()));

        ExportBatch old = batches.find(delivered.id()).orElseThrow();
        Assertions.assertEquals(BatchState.SUPERSEDED, old.state());
        Assertions.assertEquals(newId, old.supersededBy());
        ExportBatch replacement = batches.find(newId).orElseThrow();
        Assertions.assertEquals(BatchState.SCHEDULED, replacement.state());
        Assertions.assertEquals(2, replacement.seq());
        Assertions.assertEquals(1, replacement.attempt());
        Assertions.assertEquals("2026-10", replacement.period());
        Schedule after = schedules.find(TENANT, "candor").orElseThrow();
        Assertions.assertEquals(newId, after.lastBatchId());
        Assertions.assertEquals(schedule.version() + 1, after.version());
        Assertions.assertEquals(
                schedule.nextDueAt().truncatedTo(java.time.temporal.ChronoUnit.MILLIS),
                after.nextDueAt(),
                "supersede never moves the next run");
        Assertions.assertEquals(
                StateName.ENQUEUED, storage.getJobById(expectedJob).getState());

        List<AuditEvent> oldTrail = audit.findForBatch(delivered.id(), 20);
        AuditEvent superseded = oldTrail.stream()
                .filter(found -> found.type() == AuditEventType.EXPORT_BATCH_SUPERSEDED)
                .findFirst()
                .orElseThrow();
        Assertions.assertEquals("batch@certifyos.com", superseded.actor());
        Assertions.assertEquals(newId, superseded.detail().get("supersededBy"));
        AuditEvent scheduled = audit.findForBatch(newId, 20).stream()
                .filter(found -> found.type() == AuditEventType.EXPORT_BATCH_SCHEDULED)
                .findFirst()
                .orElseThrow();
        Assertions.assertEquals("SUPERSEDE", scheduled.detail().get("trigger"));
        Assertions.assertEquals("batch@certifyos.com", scheduled.actor());

        member().body("{\"reason\":\"again\"}")
                .post(BASE + "/" + delivered.id() + "/supersede")
                .then()
                .statusCode(409)
                .body("code", Matchers.equalTo("SUPERSEDE_NOT_ALLOWED"));
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "batch@certifyos.com")})
    void supersedeIsRefusedOutsideDelivered() {
        ExportBatch failed = failedAt("sup-no", ExportBatch.FailedStep.EGRESS);
        member().body("{\"reason\":\"x\"}")
                .post(BASE + "/" + failed.id() + "/supersede")
                .then()
                .statusCode(409)
                .body("code", Matchers.equalTo("SUPERSEDE_NOT_ALLOWED"));
    }
}
