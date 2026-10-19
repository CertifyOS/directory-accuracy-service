package com.certifyos.vendor_exchange.ingestion.inbound;

import com.certifyos.vendor_exchange.ingestion.events.ArrivalEvent;
import com.certifyos.vendor_exchange.persistence.Documents;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.bson.Document;

/**
 * One entry of {@code inboundFiles[]} on an export batch: one vendor upload (bucket, object and
 * generation), with what each stage of ingestion wrote. Absent fields are left out of the BSON.
 * The bucket and object name are read from {@code uploadId}, not stored again.
 *
 * @param uploadId {@code <bucket>|<objectName>|<generation>}
 * @param generation the object generation
 * @param vendorBatchId the vendor's batch id from the file name
 * @param fileDate the {@code yyyyMMdd} leg of the file name
 * @param state where the entry is in its lifecycle
 * @param attempt the ingestion request attempt, 1 for the first
 * @param jobId the job currently responsible for the entry
 * @param archivePath {@code gs://} path of the archived copy
 * @param archiveBytes size of the archived copy
 * @param externalRef {@code <exportBatchId>|<generation>}, how the ingestion event finds the entry
 * @param ingestionId the ingestion service's id for the request
 * @param parse the parse outcome the ingestion service reported
 * @param checkpoint the row-stage resume point
 * @param dispositions rows per disposition: STAGED, DUPLICATE, SCHEMA_INVALID, MISSING_IDS, UNSUPPORTED_ENTITY
 * @param written the counts staging wrote
 * @param failedStep the step that failed the entry
 * @param failureReason why it failed, as a stable code
 * @param lastError the last error text, without row content
 * @param overwroteGeneration the generation this upload replaced
 * @param eventTime when storage finalized the object
 * @param receivedAt when the entry was claimed
 * @param requestedAt when ingestion was requested
 * @param parsedAt when the parse finished
 * @param stagedAt when staging finished
 * @param updatedAt the last write
 * @param reconciliations how many times the reconciler re-enqueued the entry's job, 0 when never;
 *     each re-enqueue runs under its own job id, so an earlier one that ended never blocks the next
 */
public record InboundFile(
        String uploadId,
        String generation,
        String vendorBatchId,
        String fileDate,
        InboundFileState state,
        int attempt,
        String jobId,
        String archivePath,
        Long archiveBytes,
        String externalRef,
        String ingestionId,
        Parse parse,
        Checkpoint checkpoint,
        Map<String, Long> dispositions,
        Written written,
        String failedStep,
        String failureReason,
        String lastError,
        String overwroteGeneration,
        Instant eventTime,
        Instant receivedAt,
        Instant requestedAt,
        Instant parsedAt,
        Instant stagedAt,
        Instant updatedAt,
        int reconciliations) {

    private static final char SEPARATOR = '|';

    /**
     * The parse outcome.
     *
     * @param status the ingestion service's status, for example {@code PARTIAL}
     * @param populatedRows rows with content
     * @param validRows rows that passed validation
     * @param invalidRows rows that did not
     * @param completionSource {@code EVENT} or {@code DEADLINE}
     * @param reason a stable reason code when the parse did not fully succeed
     */
    public record Parse(
            String status,
            Long populatedRows,
            Long validRows,
            Long invalidRows,
            String completionSource,
            String reason) {
        /** The BSON form, absent fields left out. */
        public Document toDocument() {
            Document doc = new Document();
            Documents.put(doc, "status", status);
            Documents.put(doc, "populatedRows", populatedRows);
            Documents.put(doc, "validRows", validRows);
            Documents.put(doc, "invalidRows", invalidRows);
            Documents.put(doc, "completionSource", completionSource);
            Documents.put(doc, "reason", reason);
            return doc;
        }

        static Parse fromDocument(Document doc) {
            return new Parse(
                    doc.getString("status"),
                    Documents.longValue(doc, "populatedRows"),
                    Documents.longValue(doc, "validRows"),
                    Documents.longValue(doc, "invalidRows"),
                    doc.getString("completionSource"),
                    doc.getString("reason"));
        }
    }

    /**
     * Where the row stage resumes.
     *
     * @param after the last row key written, null before the first chunk
     * @param chunks chunks written so far
     */
    public record Checkpoint(String after, Long chunks) {
        /** The BSON form, absent fields left out. */
        public Document toDocument() {
            Document doc = new Document();
            Documents.put(doc, "after", after);
            Documents.put(doc, "chunks", chunks);
            return doc;
        }

        static Checkpoint fromDocument(Document doc) {
            return new Checkpoint(doc.getString("after"), Documents.longValue(doc, "chunks"));
        }
    }

    /**
     * What staging wrote.
     *
     * @param fieldRecommendations recommendation documents written
     * @param confirmedFields fields the vendor confirmed
     * @param unverifiedFields fields the vendor could not verify
     * @param practitionerFieldDisagreements practitioner fields the vendor disagreed with
     */
    public record Written(
            Long fieldRecommendations,
            Long confirmedFields,
            Long unverifiedFields,
            Long practitionerFieldDisagreements) {
        /** The BSON form, absent fields left out. */
        public Document toDocument() {
            Document doc = new Document();
            Documents.put(doc, "fieldRecommendations", fieldRecommendations);
            Documents.put(doc, "confirmedFields", confirmedFields);
            Documents.put(doc, "unverifiedFields", unverifiedFields);
            Documents.put(doc, "practitionerFieldDisagreements", practitionerFieldDisagreements);
            return doc;
        }

        static Written fromDocument(Document doc) {
            return new Written(
                    Documents.longValue(doc, "fieldRecommendations"),
                    Documents.longValue(doc, "confirmedFields"),
                    Documents.longValue(doc, "unverifiedFields"),
                    Documents.longValue(doc, "practitionerFieldDisagreements"));
        }
    }

    public InboundFile {
        if (dispositions != null && dispositions.values().stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("dispositions must not hold a null count: " + uploadId);
        }
        dispositions = dispositions == null ? Map.of() : Map.copyOf(dispositions);
    }

    /**
     * The storage bucket, the first leg of {@code uploadId}.
     *
     * @return the bucket
     */
    public String bucket() {
        return uploadId.substring(0, firstSeparator());
    }

    /**
     * The object name: everything between the first and last legs of {@code uploadId}.
     *
     * @return the object name
     */
    public String objectName() {
        return uploadId.substring(firstSeparator() + 1, uploadId.lastIndexOf(SEPARATOR));
    }

    private int firstSeparator() {
        int first = uploadId == null ? -1 : uploadId.indexOf(SEPARATOR);
        if (first < 1 || first == uploadId.lastIndexOf(SEPARATOR)) {
            throw new IllegalStateException("uploadId needs three '|' separated legs: " + uploadId);
        }
        return first;
    }

    /**
     * The entry for a first upload of a name.
     *
     * @param event the arrival
     * @param name the parsed object name
     * @param now the claim time
     * @return an entry in {@code RECEIVED}, attempt 1, with no later-stage field set
     */
    public static InboundFile received(ArrivalEvent event, InboundNames.Parsed name, Instant now) {
        return start(event, name, InboundFileState.RECEIVED, now);
    }

    /**
     * The entry for a repeat upload of a name: recorded for the audit trail, never processed.
     *
     * @param event the arrival
     * @param name the parsed object name
     * @param now the claim time
     * @return an entry in {@code DUPLICATE_LOGGED}
     */
    public static InboundFile duplicateLogged(ArrivalEvent event, InboundNames.Parsed name, Instant now) {
        return start(event, name, InboundFileState.DUPLICATE_LOGGED, now);
    }

    private static InboundFile start(
            ArrivalEvent event, InboundNames.Parsed name, InboundFileState state, Instant now) {
        return new InboundFile(
                event.uploadId(),
                event.objectGeneration(),
                name.vendorBatchId(),
                name.fileDate(),
                state,
                1,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                event.overwroteGeneration(),
                event.eventTime(),
                now,
                null,
                null,
                null,
                now,
                0);
    }

    /** The BSON shape, absent fields left out. */
    public Document toDocument() {
        Document doc = new Document();
        Documents.put(doc, "uploadId", uploadId);
        Documents.put(doc, "generation", generation);
        Documents.put(doc, "vendorBatchId", vendorBatchId);
        Documents.put(doc, "fileDate", fileDate);
        Documents.put(doc, "state", state == null ? null : state.name());
        doc.append("attempt", attempt);
        Documents.put(doc, "jobId", jobId);
        Documents.put(doc, "archivePath", archivePath);
        Documents.put(doc, "archiveBytes", archiveBytes);
        Documents.put(doc, "externalRef", externalRef);
        Documents.put(doc, "ingestionId", ingestionId);
        Documents.put(doc, "parse", parse == null ? null : parse.toDocument());
        Documents.put(doc, "checkpoint", checkpoint == null ? null : checkpoint.toDocument());
        if (!dispositions.isEmpty()) {
            doc.append("dispositions", new Document(new LinkedHashMap<>(dispositions)));
        }
        Documents.put(doc, "written", written == null ? null : written.toDocument());
        Documents.put(doc, "failedStep", failedStep);
        Documents.put(doc, "failureReason", failureReason);
        Documents.put(doc, "lastError", lastError);
        Documents.put(doc, "overwroteGeneration", overwroteGeneration);
        Documents.put(doc, "eventTime", eventTime);
        Documents.put(doc, "receivedAt", receivedAt);
        Documents.put(doc, "requestedAt", requestedAt);
        Documents.put(doc, "parsedAt", parsedAt);
        Documents.put(doc, "stagedAt", stagedAt);
        Documents.put(doc, "updatedAt", updatedAt);
        if (reconciliations != 0) {
            doc.append("reconciliations", reconciliations);
        }
        return doc;
    }

    /**
     * Reads the BSON shape.
     *
     * @param doc one {@code inboundFiles} element
     * @return the entry; a document written before {@code attempt} existed reads as 0, and one never
     *     reconciled has 0 {@code reconciliations}
     */
    public static InboundFile fromDocument(Document doc) {
        Integer storedAttempt = Documents.intValue(doc, "attempt");
        Integer storedReconciliations = Documents.intValue(doc, "reconciliations");
        return new InboundFile(
                doc.getString("uploadId"),
                doc.getString("generation"),
                doc.getString("vendorBatchId"),
                doc.getString("fileDate"),
                stateOf(doc),
                storedAttempt == null ? 0 : storedAttempt,
                doc.getString("jobId"),
                doc.getString("archivePath"),
                Documents.longValue(doc, "archiveBytes"),
                doc.getString("externalRef"),
                doc.getString("ingestionId"),
                Documents.nested(doc, "parse").map(Parse::fromDocument).orElse(null),
                Documents.nested(doc, "checkpoint")
                        .map(Checkpoint::fromDocument)
                        .orElse(null),
                dispositionsOf(doc),
                Documents.nested(doc, "written").map(Written::fromDocument).orElse(null),
                doc.getString("failedStep"),
                doc.getString("failureReason"),
                doc.getString("lastError"),
                doc.getString("overwroteGeneration"),
                Documents.instant(doc, "eventTime"),
                Documents.instant(doc, "receivedAt"),
                Documents.instant(doc, "requestedAt"),
                Documents.instant(doc, "parsedAt"),
                Documents.instant(doc, "stagedAt"),
                Documents.instant(doc, "updatedAt"),
                storedReconciliations == null ? 0 : storedReconciliations);
    }

    private static InboundFileState stateOf(Document doc) {
        String stateName = doc.getString("state");
        if (stateName == null) {
            return null;
        }
        try {
            return InboundFileState.valueOf(stateName);
        } catch (IllegalArgumentException exception) {
            throw new IllegalStateException(
                    "unknown inbound file state '" + stateName + "' on " + doc.getString("uploadId"), exception);
        }
    }

    private static Map<String, Long> dispositionsOf(Document doc) {
        Map<String, Long> counts = new LinkedHashMap<>();
        Documents.nested(doc, "dispositions").ifPresent(nested -> {
            for (String key : nested.keySet()) {
                Long count = Documents.longValue(nested, key);
                if (count != null) {
                    counts.put(key, count);
                }
            }
        });
        return counts;
    }
}
