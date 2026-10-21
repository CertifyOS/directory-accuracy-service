package com.certifyos.directory_accuracy.audit;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.export.batch.BatchState;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.batch.ExportBatchRepository;
import com.certifyos.directory_accuracy.persistence.Collections;
import com.certifyos.directory_accuracy.persistence.Transactions;
import com.mongodb.client.model.Filters;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Instant;
import java.time.YearMonth;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** The three commit rules: with the transition, standalone, and standalone idempotent on a message id. */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(ApiTestProfile.class)
public class AuditRepositoryIT {

    static final Instant NOW = Instant.parse("2026-10-01T06:00:12Z");
    static final String FILTER_ID = "flt-direct";

    @Inject
    AuditRepository audit;

    @Inject
    ExportBatchRepository batches;

    @Inject
    Transactions transactions;

    @Inject
    Collections collections;

    private ExportBatch insertBatch(String tenantId) {
        ExportBatch batch = ExportBatch.scheduled(tenantId, "candor", YearMonth.of(2026, 10), 1, FILTER_ID, NOW);
        transactions.run(session -> {
            batches.insert(session, batch);
            audit.write(
                    session,
                    AuditEvent.forBatch(AuditEventType.EXPORT_BATCH_SCHEDULED, tenantId, "candor", batch.id(), 1)
                            .occurredAt(NOW)
                            .detail("trigger", "TICK")
                            .detail("period", "2026-10")
                            .detail("seq", 1)
                            .detail("cadence", "monthly")
                            .detail("nextDueAt", NOW)
                            .detail("scheduleVersion", 1L)
                            .build());
            return null;
        });
        return batch;
    }

    @Test
    void eventCommitsWithItsTransition() {
        ExportBatch batch = insertBatch("aud-commit");
        transactions.run(session -> {
            boolean moved = batches.transition(
                    session, batch.id(), BatchState.SCHEDULED, BatchState.EGRESS_REQUESTED, NOW.plusSeconds(30), null);
            Assertions.assertTrue(moved);
            audit.write(
                    session,
                    AuditEvent.forBatch(AuditEventType.EXPORT_EGRESS_REQUESTED, "aud-commit", "candor", batch.id(), 1)
                            .occurredAt(NOW.plusSeconds(30))
                            .detail("egressCorrelationId", batch.id() + "-r1")
                            .detail("templateId", "tpl-1")
                            .detail("filterId", FILTER_ID)
                            .detail("reuseSelectionFrom", null)
                            .detail("egressResponse", "202")
                            .detail("deadlineAt", NOW.plusSeconds(21600))
                            .build());
            return null;
        });

        List<AuditEvent> trail = audit.findForBatch(batch.id(), 10);
        Assertions.assertEquals(
                List.of(AuditEventType.EXPORT_BATCH_SCHEDULED, AuditEventType.EXPORT_EGRESS_REQUESTED),
                trail.stream().map(AuditEvent::type).toList());
        Assertions.assertEquals(FILTER_ID, trail.get(1).detail().get("filterId"));
    }

    @Test
    void eventRollsBackWithItsTransition() {
        ExportBatch batch = insertBatch("aud-rollback");
        Assertions.assertThrows(
                IllegalStateException.class,
                () -> transactions.run(session -> {
                    batches.transition(
                            session, batch.id(), BatchState.SCHEDULED, BatchState.FAILED, NOW.plusSeconds(30), null);
                    audit.write(
                            session,
                            AuditEvent.forBatch(
                                            AuditEventType.EXPORT_BATCH_FAILED, "aud-rollback", "candor", batch.id(), 1)
                                    .occurredAt(NOW.plusSeconds(30))
                                    .detail("failedStep", "EGRESS")
                                    .detail("cause", "EGRESS_REFUSED")
                                    .detail("lastError", "403 SELECTION_SOURCE_DISABLED: off")
                                    .build());
                    throw new IllegalStateException("crash before commit");
                }));

        Assertions.assertEquals(
                BatchState.SCHEDULED, batches.find(batch.id()).orElseThrow().state(), "state rolled back");
        List<AuditEventType> types = audit.findForBatch(batch.id(), 10).stream()
                .map(AuditEvent::type)
                .toList();
        Assertions.assertEquals(List.of(AuditEventType.EXPORT_BATCH_SCHEDULED), types, "event rolled back with it");
    }

    @Test
    void receivedEventIsIdempotentOnMessageIdOnly() {
        ExportBatch batch = insertBatch("aud-idem");
        AuditEvent first = received(batch, "msg-1", NOW.plusSeconds(10));
        AuditEvent duplicate = received(batch, "msg-1", NOW.plusSeconds(11));
        AuditEvent other = received(batch, "msg-2", NOW.plusSeconds(12));

        Assertions.assertTrue(audit.writeIdempotent(first));
        Assertions.assertFalse(audit.writeIdempotent(duplicate), "same messageId is recorded once");
        Assertions.assertTrue(audit.writeIdempotent(other));

        // The partial index covers EXPORT_EVENT_RECEIVED only: another type may carry the same messageId.
        audit.write(AuditEvent.forBatch(AuditEventType.EXPORT_EVENT_MISSED, "aud-idem", "candor", batch.id(), 1)
                .occurredAt(NOW.plusSeconds(13))
                .detail("messageId", "msg-1")
                .detail("check", 1)
                .detail("phaseFound", "COMPLETED")
                .detail("hoursSinceRequest", 6)
                .build());

        List<AuditEvent> trail = audit.findForBatch(batch.id(), 10);
        Assertions.assertEquals(
                List.of(
                        AuditEventType.EXPORT_BATCH_SCHEDULED,
                        AuditEventType.EXPORT_EVENT_RECEIVED,
                        AuditEventType.EXPORT_EVENT_RECEIVED,
                        AuditEventType.EXPORT_EVENT_MISSED),
                trail.stream().map(AuditEvent::type).toList());
        Assertions.assertEquals("msg-1", trail.get(1).detail().get("messageId"));
        Assertions.assertEquals("msg-2", trail.get(2).detail().get("messageId"));
    }

    private static AuditEvent received(ExportBatch batch, String messageId, Instant when) {
        return AuditEvent.forBatch(AuditEventType.EXPORT_EVENT_RECEIVED, batch.tenantId(), "candor", batch.id(), 1)
                .actor(Actors.EGRESS_EVENT)
                .occurredAt(when)
                .detail("messageId", messageId)
                .detail("phase", "COMPLETED")
                .detail("publishTime", when.toString())
                .detail("outputUri", null)
                .detail("totalRecords", null)
                .detail("totalRows", null)
                .detail("failureReason", null)
                .detail("selectionDistinct", null)
                .build();
    }

    @Test
    void fieldEventsLandInReviewEvents() {
        String batchId = "aud-field-candor-2026-10-001";
        String fieldId = "aud-field|candor|" + batchId + "|cert-000123|loc-7781|phone";
        AuditEvent approved = fieldEvent(AuditEventType.FIELD_APPROVED, fieldId, NOW)
                .detail("field", "phone")
                .detail("fromStatus", "PENDING")
                .detail("version", 1L)
                .build();
        transactions.run(session -> {
            audit.write(session, approved);
            return null;
        });
        AuditEvent requested = fieldEvent(AuditEventType.FIELD_RELEASE_REQUESTED, fieldId, NOW.plusSeconds(5))
                .release("aud-field-rel-1")
                .detail("releaseId", "aud-field-rel-1")
                .detail("bulk", false)
                .build();
        audit.write(requested);

        Assertions.assertEquals(0, collections.events().countDocuments(Filters.eq("fieldRecommendationId", fieldId)));
        Assertions.assertEquals(0, collections.events().countDocuments(Filters.eq("tenantId", "aud-field")));
        Document stored =
                collections.reviewEvents().find(Filters.eq("id", approved.id())).first();
        Assertions.assertNotNull(stored, "the field event is in vendor_review_events");
        Assertions.assertEquals(fieldId, stored.getString("fieldRecommendationId"));
        Assertions.assertEquals("cert-000123", stored.getString("certifyPractitionerId"));

        List<AuditEvent> trail = audit.trailForField("aud-field", fieldId, 10);
        Assertions.assertEquals(List.of(approved, requested), trail);
        Assertions.assertEquals(
                List.of(), audit.trailForField("other-tenant", fieldId, 10), "another tenant reads no field trail");
        Assertions.assertEquals(List.of(requested), audit.findForRelease("aud-field", "aud-field-rel-1", 10));
        Assertions.assertEquals(List.of(), audit.findForRelease("other-tenant", "aud-field-rel-1", 10));
        Assertions.assertEquals(List.of(), audit.findForBatch(batchId, 10), "batch trail reads export events only");
    }

    private static AuditEvent.Builder fieldEvent(AuditEventType type, String fieldId, Instant when) {
        return AuditEvent.of(type, "aud-field", "candor")
                .actor("user:reviewer-7")
                .batch("aud-field-candor-2026-10-001")
                .upload("vendor-sftp|to/aud-field/f.csv|1760520000123456")
                .field(fieldId, "cert-000123")
                .occurredAt(when);
    }

    @Test
    void scheduleTrailIsNewestFirstAndSpansBatches() {
        ExportBatch batch = insertBatch("aud-sched");
        audit.write(AuditEvent.of(AuditEventType.SCHEDULE_CREATED, "aud-sched", "candor")
                .actor("user:ops-1")
                .occurredAt(NOW.minusSeconds(600))
                .detail("cadence", "monthly")
                .detail("timezone", "UTC")
                .detail("filterId", "flt-1")
                .detail("egressTemplateId", "tpl")
                .detail("nextDueAt", NOW)
                .build());

        List<AuditEvent> trail = audit.findForSchedule("aud-sched", "candor", 10);
        Assertions.assertEquals(
                List.of(AuditEventType.EXPORT_BATCH_SCHEDULED, AuditEventType.SCHEDULE_CREATED),
                trail.stream().map(AuditEvent::type).toList());
        Assertions.assertEquals(batch.id(), trail.get(0).exportBatchId());
        Assertions.assertNull(trail.get(1).exportBatchId());
        Assertions.assertEquals("user:ops-1", trail.get(1).actor());
    }
}
