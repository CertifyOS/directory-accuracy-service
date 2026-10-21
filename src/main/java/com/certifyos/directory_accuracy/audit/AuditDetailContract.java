package com.certifyos.directory_accuracy.audit;

import java.util.EnumMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * The designs' per-event contracts tables: the detail keys every event type must name. Checked
 * where the event is built, so a writer that forgets a field fails the test that exercises it.
 * A key counts as named when the writer passed it, even with a null value (an egress failure has
 * no output URI); extra keys are allowed. Two keys beyond the design's table come from the ticket:
 * {@code scheduleVersion} on {@code EXPORT_BATCH_SCHEDULED} and {@code md5} on
 * {@code EXPORT_BATCH_DELIVERED}.
 */
public final class AuditDetailContract {

    private static final Map<AuditEventType, Set<String>> REQUIRED = new EnumMap<>(AuditEventType.class);

    static {
        require(AuditEventType.SCHEDULE_CREATED, "cadence", "timezone", "selection", "egressTemplateId", "nextDueAt");
        require(AuditEventType.SCHEDULE_UPDATED, "version", "changes", "nextDueAtBefore", "nextDueAtAfter");
        require(AuditEventType.SCHEDULE_ENABLED, "reason", "catchUp", "nextDueAt");
        require(AuditEventType.SCHEDULE_RUN_NOW, "period", "exportBatchId");
        require(AuditEventType.SCHEDULE_DISABLED, "reason", "disabledAt");
        require(
                AuditEventType.EXPORT_TICK_COMPLETED,
                "schedulesDue",
                "batchesCreated",
                "skippedAlreadyExists",
                "durationMs");
        require(
                AuditEventType.EXPORT_BATCH_SCHEDULED,
                "period",
                "seq",
                "cadence",
                "trigger",
                "nextDueAt",
                "scheduleVersion");
        require(AuditEventType.NPIS_REGISTERED, "page", "count", "firstNpi", "lastNpi");
        require(AuditEventType.EXPORT_SELECTION_COMPLETED, "criteria", "pages", "practitionersSelected", "durationMs");
        require(AuditEventType.EXPORT_BATCH_EMPTY, "criteria");
        require(
                AuditEventType.EXPORT_EGRESS_REQUESTED,
                "egressCorrelationId",
                "templateId",
                "npiCount",
                "egressResponse",
                "deadlineAt");
        require(
                AuditEventType.EXPORT_EVENT_RECEIVED,
                "messageId",
                "publishTime",
                "phase",
                "outputUri",
                "totalRecords",
                "totalRows");
        require(AuditEventType.EXPORT_EVENT_REJECTED, "messageId", "reason");
        require(AuditEventType.EXPORT_EVENT_MISSED, "check", "phaseFound", "hoursSinceRequest");
        require(AuditEventType.EXPORT_EGRESS_STALE, "check", "phaseLastSeen", "hoursWaiting");
        require(AuditEventType.EXPORT_EGRESS_COMPLETED, "outputUri", "totalRecords", "waitSeconds", "completionSource");
        require(AuditEventType.NPIS_RECONCILED, "registered", "inFile", "match");
        require(
                AuditEventType.EXPORT_BATCH_DELIVERED,
                "deliveredAt",
                "fileName",
                "rowCount",
                "bytes",
                "completionSource",
                "md5");
        require(AuditEventType.EXPORT_BATCH_FAILED, "failedStep", "cause", "lastError");
        require(AuditEventType.EXPORT_RETRY_REQUESTED, "fromState", "failedStep", "newAttempt", "reason");
        require(AuditEventType.EXPORT_PRIOR_ATTEMPT_CANCELLED, "cancelledCorrelationId", "outcome");
        require(AuditEventType.EXPORT_PRIOR_ATTEMPT_CANCEL_REJECTED, "cancelledCorrelationId", "outcome");
        require(AuditEventType.EXPORT_BATCH_SUPERSEDED, "supersededBy", "reason");
        require(AuditEventType.EXPORT_BATCH_ACKNOWLEDGED, "uploadId", "vendorBatchId", "daysToResponse");
        requireIngestion();
    }

    /**
     * The ingestion design's rows. Optional keys are not required: {@code overwroteGeneration} on
     * received, {@code parsedLegs} on unmatched, {@code certifyLocationId} and {@code comment} on
     * the three decisions.
     */
    private static void requireIngestion() {
        require(AuditEventType.INBOUND_FILE_RECEIVED, "objectName", "generation", "vendorBatchId", "eventTime");
        require(
                AuditEventType.INBOUND_FILE_DUPLICATE_LOGGED,
                "objectName",
                "generation",
                "firstUploadId",
                "firstUploadState");
        require(AuditEventType.INBOUND_FILE_UNMATCHED, "reason", "objectName", "generation");
        require(AuditEventType.INBOUND_UPLOAD_ATTACHED, "exportBatchId", "uploadId");
        require(AuditEventType.INBOUND_UPLOAD_DISCARDED, "reason", "uploadId");
        require(AuditEventType.INBOUND_FILE_ARCHIVED, "archivePath", "bytes", "copyResult");
        require(
                AuditEventType.INBOUND_FILE_INGESTION_REQUESTED,
                "ingestionId",
                "externalRef",
                "mappingId",
                "registerResponse",
                "deadlineAt");
        require(
                AuditEventType.INBOUND_FILE_PARSED,
                "status",
                "populatedRows",
                "validRows",
                "invalidRows",
                "completionSource",
                "waitSeconds");
        require(AuditEventType.INBOUND_FILE_INGESTION_FAILED, "reason", "completionSource");
        require(
                AuditEventType.INBOUND_FILE_STAGED,
                "dispositions",
                "written",
                "chunks",
                "durationMs",
                "practitionerFieldDisagreements");
        require(AuditEventType.INBOUND_FILE_FAILED, "failedStep", "failureReason", "lastError", "jobAttempts");
        require(AuditEventType.INBOUND_FILE_RECONCILED, "state", "ageMinutes", "action");
        require(AuditEventType.FIELD_APPROVED, "field", "fromStatus", "version");
        require(AuditEventType.FIELD_REJECTED, "field", "fromStatus", "version", "rejectedValueId", "rejectionCount");
        require(AuditEventType.FIELD_SKIPPED, "field", "fromStatus", "version");
        require(AuditEventType.FIELD_ROLLED_BACK, "reason", "version");
        require(AuditEventType.FIELD_RELEASE_REQUESTED, "releaseId", "bulk");
        require(
                AuditEventType.FIELD_RELEASED,
                "outcome",
                "winningSource",
                "writePath",
                "resource",
                "crosswalkId",
                "completionSource",
                "readBackSeconds");
        require(AuditEventType.FIELD_RELEASE_FAILED, "reason", "error", "jobAttempts");
    }

    private AuditDetailContract() {}

    private static void require(AuditEventType type, String... keys) {
        REQUIRED.put(type, Set.of(keys));
    }

    /**
     * The keys an event type must name.
     *
     * @param type the type
     * @return the keys, empty when the design lists none
     */
    public static Set<String> requiredKeys(AuditEventType type) {
        return REQUIRED.getOrDefault(type, Set.of());
    }

    /**
     * Refuses an event whose writer did not name every required key.
     *
     * @param type the type
     * @param named the keys the writer passed, with or without a value
     * @throws IllegalArgumentException naming the type and the missing keys
     */
    public static void check(AuditEventType type, Set<String> named) {
        Set<String> missing = new LinkedHashSet<>(requiredKeys(type));
        missing.removeAll(named);
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException(type + " requires detail " + missing + " (design per-event contracts)");
        }
    }
}
