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
import com.certifyos.directory_accuracy.export.jobs.EgressFixtures;
import com.certifyos.directory_accuracy.export.schedule.Cadence;
import com.certifyos.directory_accuracy.export.schedule.Schedule;
import com.certifyos.directory_accuracy.export.schedule.ScheduleRepository;
import com.certifyos.directory_accuracy.http.Problem;
import com.certifyos.directory_accuracy.persistence.Transactions;
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
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** The audit read endpoints: a batch's trail in write order, a schedule's own events, tenant scoping. */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@QuarkusTestResource(WireMockDal.class)
@TestProfile(ApiTestProfile.class)
class AuditReadIT {

    static final String EMAIL = UserContextFilter.EMAIL_CLAIM;
    static final String TENANT = "org-audit";

    @Inject
    ExportBatchRepository batches;

    @Inject
    ScheduleRepository schedules;

    @Inject
    AuditRepository audit;

    @Inject
    Transactions transactions;

    @InjectMock
    GoogleIdTokenService google;

    @BeforeEach
    void before() {
        Mockito.when(google.idToken("test-dal-iap-client-id")).thenReturn("dal-id-token");
        WireMockDal.stubMember("auditread@certifyos.com", TENANT);
    }

    private static RequestSpecification member() {
        return RestAssured.given().header("tenant-id", TENANT);
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "auditread@certifyos.com")})
    void aBatchTrailComesBackInWriteOrderWithTheEnvelope() {
        ExportBatch batch = EgressFixtures.batchIn(
                transactions,
                batches,
                TENANT,
                "trail",
                BatchState.EGRESS_REQUESTED,
                1,
                Instant.now().minusSeconds(60));
        Instant same = Instant.parse("2026-10-01T07:00:00Z");
        // Three events with one occurredAt, as one transaction writes them: write order must win.
        audit.write(AuditEvent.forBatch(AuditEventType.EXPORT_BATCH_SCHEDULED, TENANT, "trail", batch.id(), 1)
                .actor("auditread@certifyos.com")
                .jobId("11111111-1111-1111-1111-111111111111")
                .occurredAt(same)
                .detail("period", "2026-10")
                .detail("seq", 1)
                .detail("cadence", "monthly")
                .detail("trigger", "MANUAL")
                .detail("nextDueAt", same)
                .detail("scheduleVersion", 1L)
                .build());
        audit.write(AuditEvent.forBatch(AuditEventType.EXPORT_EGRESS_REQUESTED, TENANT, "trail", batch.id(), 1)
                .occurredAt(same)
                .detail("egressCorrelationId", batch.id() + "-r1")
                .detail("templateId", "tpl-1")
                .detail("filterId", "flt-direct")
                .detail("reuseSelectionFrom", null)
                .detail("egressResponse", "202")
                .detail("deadlineAt", same.plusSeconds(21600))
                .build());
        audit.write(AuditEvent.forBatch(AuditEventType.EXPORT_RECONCILED, TENANT, "trail", batch.id(), 1)
                .occurredAt(same)
                .detail("selected", 1L)
                .detail("inFile", 1L)
                .detail("match", true)
                .build());

        member().get("/v1/vendor-exports/" + batch.id() + "/events")
                .then()
                .statusCode(200)
                .body("items.size()", Matchers.equalTo(3))
                .body(
                        "items.type",
                        Matchers.contains("EXPORT_BATCH_SCHEDULED", "EXPORT_EGRESS_REQUESTED", "EXPORT_RECONCILED"))
                .body("items[0].id", Matchers.startsWith("ev-"))
                .body("items[0].tenantId", Matchers.equalTo(TENANT))
                .body("items[0].vendor", Matchers.equalTo("trail"))
                .body("items[0].exportBatchId", Matchers.equalTo(batch.id()))
                .body("items[0].attempt", Matchers.equalTo(1))
                .body("items[0].actor", Matchers.equalTo("auditread@certifyos.com"))
                .body("items[0].jobId", Matchers.equalTo("11111111-1111-1111-1111-111111111111"))
                .body("items[0].occurredAt", Matchers.equalTo("2026-10-01T07:00:00Z"))
                .body("items[0].detail.trigger", Matchers.equalTo("MANUAL"))
                .body("items[1].actor", Matchers.equalTo("system:vendor-export"))
                .body("items[1].jobId", Matchers.nullValue())
                .body("items[2].detail.selected", Matchers.equalTo(1));
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "auditread@certifyos.com")})
    void anotherTenantsBatchTrailIsNotFound() {
        ExportBatch theirs = EgressFixtures.batchIn(
                transactions, batches, "org-audit-other", "candor", BatchState.EGRESS_REQUESTED, 1, Instant.now());
        member().get("/v1/vendor-exports/" + theirs.id() + "/events")
                .then()
                .statusCode(404)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("BATCH_NOT_FOUND"));
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "auditread@certifyos.com")})
    void aScheduleTrailHoldsItsOwnEventsNewestFirstAndNoBatchEvent() {
        Schedule schedule = Schedule.create(
                TENANT,
                "sched",
                Cadence.monthly(1),
                ZoneId.of("UTC"),
                EgressFixtures.FILTER_ID,
                "tpl-1",
                Instant.now().plusSeconds(86400),
                "auditread@certifyos.com",
                Instant.now());
        transactions.run(session -> {
            schedules.insert(session, schedule);
            return null;
        });
        audit.write(AuditEvent.of(AuditEventType.SCHEDULE_CREATED, TENANT, "sched")
                .actor("auditread@certifyos.com")
                .occurredAt(Instant.parse("2026-10-01T06:00:00Z"))
                .detail("cadence", "monthly")
                .detail("timezone", "UTC")
                .detail("selection", "s")
                .detail("egressTemplateId", "tpl-1")
                .detail("nextDueAt", "n")
                .build());
        audit.write(AuditEvent.of(AuditEventType.SCHEDULE_RUN_NOW, TENANT, "sched")
                .actor("auditread@certifyos.com")
                .occurredAt(Instant.parse("2026-10-01T06:05:00Z"))
                .detail("period", "2026-10")
                .detail("exportBatchId", TENANT + "-sched-2026-10-001")
                .build());
        audit.write(AuditEvent.forBatch(
                        AuditEventType.EXPORT_BATCH_EMPTY, TENANT, "sched", TENANT + "-sched-2026-10-001", 1)
                .occurredAt(Instant.parse("2026-10-01T06:06:00Z"))
                .detail("criteria", "c")
                .build());

        member().get("/v1/vendor-exports/schedules/" + TENANT + "/sched/events")
                .then()
                .statusCode(200)
                .body("items.type", Matchers.contains("SCHEDULE_RUN_NOW", "SCHEDULE_CREATED"))
                .body("items[0].exportBatchId", Matchers.nullValue())
                .body("items[0].detail.exportBatchId", Matchers.equalTo(TENANT + "-sched-2026-10-001"));
        member().get("/v1/vendor-exports/schedules/" + TENANT + "/nobody/events")
                .then()
                .statusCode(404)
                .body("code", Matchers.equalTo("SCHEDULE_NOT_FOUND"));
        RestAssured.given()
                .header("tenant-id", TENANT)
                .get("/v1/vendor-exports/schedules/org-audit-other/sched/events")
                .then()
                .statusCode(403)
                .body("code", Matchers.equalTo("TENANT_MISMATCH"));
    }
}
