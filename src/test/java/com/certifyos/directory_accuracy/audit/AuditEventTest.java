package com.certifyos.directory_accuracy.audit;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class AuditEventTest {

    static final Instant WHEN = Instant.parse("2026-10-01T07:12:09Z");

    static final String FIELD_ID = "org-xyz|candor|org-xyz-candor-2026-10-001|cert-000123|loc-7781|phone";

    @Test
    void catalogueHasTheDesignsFortyThreeTypes() {
        Assertions.assertEquals(43, AuditEventType.values().length, "24 export types and 19 ingestion types");
        Assertions.assertEquals(AuditEventType.Scope.SCHEDULE, AuditEventType.SCHEDULE_CREATED.scope());
        Assertions.assertEquals(AuditEventType.Scope.BATCH, AuditEventType.EXPORT_BATCH_DELIVERED.scope());
        Assertions.assertEquals(AuditEventType.Scope.SERVICE, AuditEventType.EXPORT_TICK_COMPLETED.scope());
        Assertions.assertEquals(AuditEventType.Scope.SERVICE, AuditEventType.EXPORT_EVENT_REJECTED.scope());
        Assertions.assertEquals(AuditEventType.Scope.BATCH, AuditEventType.INBOUND_FILE_STAGED.scope());
        Assertions.assertEquals(AuditEventType.Scope.SERVICE, AuditEventType.INBOUND_FILE_UNMATCHED.scope());
        Assertions.assertEquals(
                AuditEventType.Scope.SERVICE,
                AuditEventType.INBOUND_UPLOAD_DISCARDED.scope(),
                "a discarded unmatched upload has no export batch");
        Assertions.assertEquals(
                AuditEventType.Scope.BATCH,
                AuditEventType.INBOUND_UPLOAD_ATTACHED.scope(),
                "an attached upload names its batch");
        Assertions.assertEquals(AuditEventType.Scope.FIELD, AuditEventType.FIELD_APPROVED.scope());
        Assertions.assertEquals(AuditEventType.Scope.FIELD, AuditEventType.FIELD_RELEASE_FAILED.scope());
    }

    @Test
    void fieldEventNeedsAFieldId() {
        IllegalArgumentException refused = Assertions.assertThrows(
                IllegalArgumentException.class, () -> AuditEvent.of(AuditEventType.FIELD_APPROVED, "org-xyz", "candor")
                        .actor("user:reviewer-7")
                        .detail("field", "phone")
                        .detail("fromStatus", "PENDING")
                        .detail("version", 1L)
                        .build());
        Assertions.assertTrue(refused.getMessage().contains("fieldRecommendationId"), refused.getMessage());
        AuditEvent approved = AuditEvent.of(AuditEventType.FIELD_APPROVED, "org-xyz", "candor")
                .actor("user:reviewer-7")
                .field(FIELD_ID, "cert-000123")
                .detail("field", "phone")
                .detail("fromStatus", "PENDING")
                .detail("version", 1L)
                .build();
        Assertions.assertEquals(FIELD_ID, approved.fieldRecommendationId());
    }

    @Test
    void fieldEventNeedsThePractitionerId() {
        IllegalArgumentException refused = Assertions.assertThrows(
                IllegalArgumentException.class, () -> AuditEvent.of(AuditEventType.FIELD_APPROVED, "org-xyz", "candor")
                        .actor("user:reviewer-7")
                        .field(FIELD_ID, null)
                        .detail("field", "phone")
                        .detail("fromStatus", "PENDING")
                        .detail("version", 1L)
                        .build());
        Assertions.assertTrue(refused.getMessage().contains("certifyPractitionerId"), refused.getMessage());
    }

    @Test
    void envelopeFieldsRoundTrip() {
        AuditEvent event = AuditEvent.of(AuditEventType.FIELD_RELEASE_REQUESTED, "org-xyz", "candor")
                .actor("user:reviewer-7")
                .batch("org-xyz-candor-2026-10-001")
                .upload("vendor-sftp|to/org-xyz/f.csv|1760520000123456")
                .field(FIELD_ID, "cert-000123")
                .release("rel-1")
                .occurredAt(WHEN)
                .detail("releaseId", "rel-1")
                .detail("bulk", true)
                .build();
        Assertions.assertNull(event.attempt(), "batch(id) leaves the attempt unset");

        Document doc = event.toDocument();
        Assertions.assertEquals("vendor-sftp|to/org-xyz/f.csv|1760520000123456", doc.getString("uploadId"));
        Assertions.assertEquals(FIELD_ID, doc.getString("fieldRecommendationId"));
        Assertions.assertEquals("cert-000123", doc.getString("certifyPractitionerId"));
        Assertions.assertEquals("rel-1", doc.getString("releaseId"));
        Assertions.assertFalse(doc.containsKey("attempt"));
        Assertions.assertEquals(event, AuditEvent.fromDocument(doc));

        AuditEvent fileEvent = AuditEvent.of(AuditEventType.INBOUND_UPLOAD_DISCARDED, null, null)
                .actor("user:ops-1")
                .upload("vendor-sftp|to/org-xyz/f.csv|1760520000123456")
                .detail("reason", "not ours")
                .detail("uploadId", "vendor-sftp|to/org-xyz/f.csv|1760520000123456")
                .build();
        Document fileDoc = fileEvent.toDocument();
        Assertions.assertFalse(fileDoc.containsKey("exportBatchId"), "a discarded upload names no batch");
        Assertions.assertFalse(fileDoc.containsKey("fieldRecommendationId"), "written only when set");
        Assertions.assertFalse(fileDoc.containsKey("certifyPractitionerId"));
        Assertions.assertFalse(fileDoc.containsKey("releaseId"));
        Assertions.assertEquals(fileEvent, AuditEvent.fromDocument(fileDoc));
    }

    @Test
    void batchEventCarriesTheEnvelopeAndRoundTrips() {
        AuditEvent event = AuditEvent.forBatch(
                        AuditEventType.EXPORT_BATCH_DELIVERED, "org-xyz", "candor", "org-xyz-candor-2026-10-001", 1)
                .jobId("3f9a")
                .occurredAt(WHEN)
                .detail("fileName", "org-xyz_org-xyz-candor-2026-10-001_20261001.csv")
                .detail("rowCount", 1873)
                .detail("deliveredAt", "2026-10-01T07:12:09Z")
                .detail("bytes", 1912044)
                .detail("completionSource", "EVENT")
                .detail("md5", "x")
                .detail("absent", null)
                .build();

        Assertions.assertTrue(event.id().startsWith("ev-"));
        Assertions.assertEquals(Actors.SYSTEM, event.actor(), "system actor by default");
        Assertions.assertEquals(1, event.attempt());
        Assertions.assertFalse(event.detail().containsKey("absent"), "null detail values are left out");

        Document doc = event.toDocument();
        Assertions.assertFalse(doc.containsKey("_id"), "the driver assigns _id");
        Assertions.assertEquals("EXPORT_BATCH_DELIVERED", doc.getString("type"));
        Assertions.assertEquals(1873, doc.get("detail", Document.class).getInteger("rowCount"));
        Assertions.assertEquals(event, AuditEvent.fromDocument(doc));
    }

    @Test
    void scheduleEventHasNoBatchAndServiceEventNeedsNoTenant() {
        AuditEvent schedule = AuditEvent.of(AuditEventType.SCHEDULE_DISABLED, "org-xyz", "candor")
                .actor("user:ops-1")
                .occurredAt(WHEN)
                .detail("reason", "hold")
                .detail("disabledAt", "2026-10-01T00:00:00Z")
                .build();
        Assertions.assertNull(schedule.exportBatchId());
        Assertions.assertFalse(schedule.toDocument().containsKey("exportBatchId"));

        AuditEvent tick = AuditEvent.of(AuditEventType.EXPORT_TICK_COMPLETED, null, null)
                .occurredAt(WHEN)
                .detail("schedulesDue", 0)
                .detail("batchesCreated", java.util.List.of())
                .detail("skippedAlreadyExists", 0)
                .detail("durationMs", 1)
                .build();
        Assertions.assertNull(tick.tenantId());
        Assertions.assertEquals(tick, AuditEvent.fromDocument(tick.toDocument()));
    }

    @Test
    void envelopeRulesAreEnforced() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> AuditEvent.of(AuditEventType.EXPORT_BATCH_FAILED, "org-xyz", "candor")
                        .build(),
                "a batch event without a batch id");
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> AuditEvent.of(AuditEventType.SCHEDULE_CREATED, null, "candor")
                        .build(),
                "a schedule event without a tenant");
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> AuditEvent.of(AuditEventType.SCHEDULE_CREATED, "org-xyz", "candor")
                        .batch("org-xyz-candor-2026-10-001", 1)
                        .build(),
                "a schedule event with a batch id");
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> AuditEvent.of(AuditEventType.SCHEDULE_CREATED, "org-xyz", "candor")
                        .actor(" ")
                        .build(),
                "a blank actor");
    }

    @Test
    void detailIsCopiedAndImmutable() {
        Map<String, Object> detail = new HashMap<>();
        detail.put("count", 1);
        AuditEvent event = AuditEvent.of(AuditEventType.SCHEDULE_CREATED, "org-xyz", "candor")
                .details(detail)
                .detail("cadence", "monthly")
                .detail("timezone", "UTC")
                .detail("selection", "s")
                .detail("egressTemplateId", "tpl")
                .detail("nextDueAt", "n")
                .build();
        detail.put("count", 2);
        Assertions.assertEquals(1, event.detail().get("count"));
        Assertions.assertThrows(
                UnsupportedOperationException.class, () -> event.detail().put("x", 1));
    }
}
