package com.certifyos.directory_accuracy.audit;

import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class AuditDetailContractTest {

    /** The design's per-event contracts for the ingestion lane, optional keys left out. */
    static final Map<AuditEventType, Set<String>> INGESTION_KEYS = Map.ofEntries(
            Map.entry(
                    AuditEventType.INBOUND_FILE_RECEIVED,
                    Set.of("objectName", "generation", "vendorBatchId", "eventTime")),
            Map.entry(AuditEventType.EXPORT_BATCH_ACKNOWLEDGED, Set.of("uploadId", "vendorBatchId", "daysToResponse")),
            Map.entry(
                    AuditEventType.INBOUND_FILE_DUPLICATE_LOGGED,
                    Set.of("objectName", "generation", "firstUploadId", "firstUploadState")),
            Map.entry(AuditEventType.INBOUND_FILE_UNMATCHED, Set.of("reason", "objectName", "generation")),
            Map.entry(AuditEventType.INBOUND_UPLOAD_ATTACHED, Set.of("exportBatchId", "uploadId")),
            Map.entry(AuditEventType.INBOUND_UPLOAD_DISCARDED, Set.of("reason", "uploadId")),
            Map.entry(AuditEventType.INBOUND_FILE_ARCHIVED, Set.of("archivePath", "bytes", "copyResult")),
            Map.entry(
                    AuditEventType.INBOUND_FILE_INGESTION_REQUESTED,
                    Set.of("ingestionId", "externalRef", "mappingId", "registerResponse", "deadlineAt")),
            Map.entry(
                    AuditEventType.INBOUND_FILE_PARSED,
                    Set.of("status", "populatedRows", "validRows", "invalidRows", "completionSource", "waitSeconds")),
            Map.entry(AuditEventType.INBOUND_FILE_INGESTION_FAILED, Set.of("reason", "completionSource")),
            Map.entry(
                    AuditEventType.INBOUND_FILE_STAGED,
                    Set.of("dispositions", "written", "chunks", "durationMs", "practitionerFieldDisagreements")),
            Map.entry(
                    AuditEventType.INBOUND_FILE_FAILED,
                    Set.of("failedStep", "failureReason", "lastError", "jobAttempts")),
            Map.entry(AuditEventType.INBOUND_FILE_RECONCILED, Set.of("state", "ageMinutes", "action")),
            Map.entry(AuditEventType.FIELD_APPROVED, Set.of("field", "fromStatus", "version")),
            Map.entry(
                    AuditEventType.FIELD_REJECTED,
                    Set.of("field", "fromStatus", "version", "rejectedValueId", "rejectionCount")),
            Map.entry(AuditEventType.FIELD_SKIPPED, Set.of("field", "fromStatus", "version")),
            Map.entry(AuditEventType.FIELD_ROLLED_BACK, Set.of("reason", "version")),
            Map.entry(AuditEventType.FIELD_RELEASE_REQUESTED, Set.of("releaseId", "bulk")),
            Map.entry(
                    AuditEventType.FIELD_RELEASED,
                    Set.of(
                            "outcome",
                            "winningSource",
                            "writePath",
                            "resource",
                            "crosswalkId",
                            "completionSource",
                            "readBackSeconds")),
            Map.entry(AuditEventType.FIELD_RELEASE_FAILED, Set.of("reason", "error", "jobAttempts")));

    @Test
    void everyIngestionTypeHasRequiredKeys() {
        Assertions.assertEquals(20, INGESTION_KEYS.size(), "nineteen new types plus the acknowledged change");
        INGESTION_KEYS.forEach((type, keys) ->
                Assertions.assertEquals(keys, AuditDetailContract.requiredKeys(type), type + " required detail keys"));
        Set<String> acknowledged = AuditDetailContract.requiredKeys(AuditEventType.EXPORT_BATCH_ACKNOWLEDGED);
        Assertions.assertTrue(acknowledged.contains("uploadId"), acknowledged.toString());
        Assertions.assertFalse(acknowledged.contains("inboundBatchId"), acknowledged.toString());
    }

    @Test
    void everyEventTypeHasAnEntryFromTheDesignTable() {
        for (AuditEventType type : AuditEventType.values()) {
            Assertions.assertFalse(
                    AuditDetailContract.requiredKeys(type).isEmpty(),
                    type + " has no required detail keys; the design's per-event table lists some for every type");
        }
    }

    @Test
    void aWriterThatForgetsARequiredKeyIsRefusedAtBuild() {
        IllegalArgumentException refused =
                Assertions.assertThrows(IllegalArgumentException.class, () -> AuditEvent.forBatch(
                                AuditEventType.EXPORT_BATCH_FAILED, "org-1", "candor", "org-1-candor-2026-10-001", 1)
                        .detail("failedStep", "EGRESS")
                        .detail("cause", "EGRESS_FAILED")
                        .build());
        Assertions.assertTrue(refused.getMessage().contains("EXPORT_BATCH_FAILED"), refused.getMessage());
        Assertions.assertTrue(refused.getMessage().contains("lastError"), refused.getMessage());
    }

    @Test
    void aKeyNamedWithANullValueCountsAsNamedAndIsNotStored() {
        // An egress failure event has no output URI; the writer still names the field.
        AuditEvent event = AuditEvent.forBatch(
                        AuditEventType.EXPORT_EVENT_RECEIVED, "org-1", "candor", "org-1-candor-2026-10-001", 1)
                .detail("messageId", "m-1")
                .detail("publishTime", "2026-10-01T07:12:01Z")
                .detail("phase", "FAILED")
                .detail("outputUri", null)
                .detail("totalRecords", null)
                .detail("totalRows", null)
                .build();
        Assertions.assertFalse(event.detail().containsKey("outputUri"));
        Assertions.assertEquals("FAILED", event.detail().get("phase"));
    }

    @Test
    void extraKeysAreAllowed() {
        AuditEvent event = AuditEvent.forBatch(
                        AuditEventType.NPIS_REGISTERED, "org-1", "candor", "org-1-candor-2026-10-001", 1)
                .detail("page", 0)
                .detail("count", 2)
                .detail("firstNpi", "1234567893")
                .detail("lastNpi", "2345678918")
                .detail("inserted", 2L)
                .detail("skippedNoNpi", 0)
                .build();
        Assertions.assertEquals(2L, event.detail().get("inserted"));
        Assertions.assertEquals(
                Set.of("page", "count", "firstNpi", "lastNpi"),
                AuditDetailContract.requiredKeys(AuditEventType.NPIS_REGISTERED));
    }

    @Test
    void theAuditLineCarriesIdsAndAlertKeysOnly() {
        AuditEvent event = AuditEvent.forBatch(
                        AuditEventType.EXPORT_BATCH_FAILED, "org-1", "candor", "org-1-candor-2026-10-001", 2)
                .actor("ops@certifyos.com")
                .detail("failedStep", "EGRESS")
                .detail("cause", "FILE_NOT_FOUND")
                .detail("lastError", "no object at gs://b/from/org-1/f.csv")
                .build();
        String line = AuditRepository.line(event);
        Assertions.assertEquals(
                "AUDIT type=EXPORT_BATCH_FAILED tenantId=org-1 vendor=candor exportBatchId=org-1-candor-2026-10-001 attempt=2 "
                        + "actor=ops@certifyos.com cause=FILE_NOT_FOUND",
                line);
        Assertions.assertFalse(line.contains("lastError"), "free text never reaches the line");
    }

    @Test
    void theAuditLineCarriesTheIngestionIdsAndTheNewAlertKeysOnly() {
        AuditEvent event = AuditEvent.of(AuditEventType.FIELD_RELEASED, "org-1", "candor")
                .actor(Actors.INGESTION)
                .batch("org-1-candor-2026-10-001")
                .upload("vendor-sftp|to/org-1/f.csv|1760520000123456")
                .field("org-1|candor|org-1-candor-2026-10-001|cert-1|loc-1|phone", "cert-1")
                .release("rel-1")
                .detail("outcome", "WON")
                .detail("winningSource", "VENDOR")
                .detail("writePath", "SLICE")
                .detail("resource", "phone")
                .detail("crosswalkId", "cw-1")
                .detail("completionSource", "EVENT")
                .detail("readBackSeconds", 12)
                .detail("comment", "free text")
                .build();
        String line = AuditRepository.line(event);
        Assertions.assertEquals(
                "AUDIT type=FIELD_RELEASED tenantId=org-1 vendor=candor exportBatchId=org-1-candor-2026-10-001 "
                        + "attempt=null actor=system:vendor-ingestion uploadId=vendor-sftp|to/org-1/f.csv|1760520000123456 "
                        + "fieldRecommendationId=org-1|candor|org-1-candor-2026-10-001|cert-1|loc-1|phone "
                        + "certifyPractitionerId=cert-1 releaseId=rel-1 completionSource=EVENT outcome=WON writePath=SLICE",
                line);
        Assertions.assertFalse(line.contains("comment"), "free text never reaches the line");
    }

    @Test
    void theAuditLineEchoesAnAlertValueOnlyWhenItIsOneToken() {
        // A deadline check that could not reach egress records the exception text as the outcome.
        AuditEvent unreachable = cancelled("unreachable: connection refused by egress");
        String freeText = AuditRepository.line(unreachable);
        Assertions.assertFalse(freeText.contains("outcome="), freeText);
        Assertions.assertFalse(freeText.contains("connection refused"), freeText);
        Assertions.assertEquals(
                "unreachable: connection refused by egress",
                unreachable.detail().get("outcome"),
                "the value stays in the stored detail");

        Assertions.assertTrue(
                AuditRepository.line(cancelled("CANCELLED")).endsWith(" outcome=CANCELLED"),
                AuditRepository.line(cancelled("CANCELLED")));
        Assertions.assertFalse(AuditRepository.line(cancelled("x".repeat(65))).contains("outcome="), "longer than 64");
        Assertions.assertTrue(AuditRepository.line(cancelled("x".repeat(64))).contains("outcome="), "64 is allowed");
        Assertions.assertFalse(AuditRepository.line(cancelled("a=b")).contains("outcome="), "no key=value inside");
        Assertions.assertTrue(
                AuditRepository.line(cancelled("gs://bucket/a-b_c.1:2")).endsWith(" outcome=gs://bucket/a-b_c.1:2"),
                "letters, digits and _ . : / - are one token");
    }

    private static AuditEvent cancelled(String outcome) {
        return AuditEvent.forBatch(
                        AuditEventType.EXPORT_PRIOR_ATTEMPT_CANCELLED, "org-1", "candor", "org-1-candor-2026-10-001", 2)
                .detail("cancelledCorrelationId", "corr-1")
                .detail("outcome", outcome)
                .build();
    }
}
