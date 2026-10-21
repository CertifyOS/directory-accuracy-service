package com.certifyos.directory_accuracy.export.batch;

import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria;
import com.certifyos.directory_accuracy.persistence.Documents;
import com.certifyos.directory_accuracy.persistence.Ids;
import java.time.Instant;
import java.time.YearMonth;
import java.util.List;
import org.bson.Document;

/**
 * One export batch per tenant, vendor, period and sequence: the truth every job reads first. The
 * state machine is {@link BatchState}; every transition is a compare-and-set on {@code state} and
 * bumps {@code version}.
 *
 * @param id {@code <tenantId>-<vendor>-<yyyy-MM>-<seq>}
 * @param tenantId the tenant
 * @param vendor the vendor code
 * @param period the month, {@code yyyy-MM}
 * @param seq 1 for the first batch of the period; supersede adds one
 * @param state the lifecycle state
 * @param attempt 1 on creation; operator retry adds one
 * @param selection the criteria copied at creation and the selection progress
 * @param egress the egress request details, null before the request
 * @param file the delivered file, null before delivery
 * @param reconciliation the count check, null before delivery
 * @param failedStep {@code SELECT} or {@code EGRESS} while failed
 * @param lastError the last failure cause while failed
 * @param supersededBy the newer batch id once superseded
 * @param acknowledgedAt set later by the ingestion module
 * @param inboundBatchId set later by the ingestion module
 * @param deliveredAt when the batch reached {@code DELIVERED}
 * @param version compare-and-set token, 1 on insert
 * @param createdAt insert time
 * @param updatedAt last write time
 */
public record ExportBatch(
        String id,
        String tenantId,
        String vendor,
        String period,
        int seq,
        BatchState state,
        int attempt,
        Selection selection,
        EgressDetails egress,
        FileDetails file,
        Reconciliation reconciliation,
        FailedStep failedStep,
        String lastError,
        String supersededBy,
        Instant acknowledgedAt,
        String inboundBatchId,
        Instant deliveredAt,
        long version,
        Instant createdAt,
        Instant updatedAt) {

    /** Where a failed batch stopped; decides which job an operator retry re-enqueues. */
    public enum FailedStep {
        SELECT,
        EGRESS
    }

    /**
     * The criteria copied from the schedule at creation and the select job's progress.
     *
     * @param criteria the copied criteria
     * @param page the last api-layer page fetched while selection runs, null when done
     * @param practitionersSelected the count once selection completed
     * @param completedAt when selection completed
     */
    public record Selection(
            SelectionCriteria criteria, Integer page, Integer practitionersSelected, Instant completedAt) {
        Document toDocument() {
            Document doc = new Document("criteria", criteria.toDocuments());
            Documents.put(doc, "page", page);
            Documents.put(doc, "practitionersSelected", practitionersSelected);
            Documents.put(doc, "completedAt", completedAt);
            return doc;
        }

        static Selection fromDocument(Document doc) {
            List<Document> criteria = doc.getList("criteria", Document.class);
            return new Selection(
                    SelectionCriteria.fromDocuments(criteria),
                    Documents.intValue(doc, "page"),
                    Documents.intValue(doc, "practitionersSelected"),
                    Documents.instant(doc, "completedAt"));
        }
    }

    /**
     * A new batch in {@code SCHEDULED}, attempt 1, version 1.
     *
     * @param tenantId the tenant
     * @param vendor the vendor code
     * @param period the month
     * @param seq the sequence
     * @param criteria the schedule's criteria, copied
     * @param now the creation time
     * @return the batch to insert
     */
    public static ExportBatch scheduled(
            String tenantId, String vendor, YearMonth period, int seq, SelectionCriteria criteria, Instant now) {
        return new ExportBatch(
                Ids.batchId(tenantId, vendor, period, seq),
                tenantId,
                vendor,
                period.toString(),
                seq,
                BatchState.SCHEDULED,
                1,
                new Selection(criteria, null, null, null),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                1L,
                now,
                now);
    }

    /** The BSON document, absent fields left out. */
    public Document toDocument() {
        Document doc = new Document("_id", id)
                .append("tenantId", tenantId)
                .append("vendor", vendor)
                .append("period", period)
                .append("seq", seq)
                .append("state", state.name())
                .append("attempt", attempt)
                .append("selection", selection.toDocument())
                .append("version", version)
                .append("createdAt", createdAt)
                .append("updatedAt", updatedAt);
        Documents.put(doc, "egress", egress == null ? null : egress.toDocument());
        Documents.put(doc, "file", file == null ? null : file.toDocument());
        Documents.put(doc, "reconciliation", reconciliation == null ? null : reconciliation.toDocument());
        Documents.put(doc, "failedStep", failedStep == null ? null : failedStep.name());
        Documents.put(doc, "lastError", lastError);
        Documents.put(doc, "supersededBy", supersededBy);
        Documents.put(doc, "acknowledgedAt", acknowledgedAt);
        Documents.put(doc, "inboundBatchId", inboundBatchId);
        Documents.put(doc, "deliveredAt", deliveredAt);
        return doc;
    }

    /**
     * Reads a stored document.
     *
     * @param doc the document
     * @return the batch
     */
    public static ExportBatch fromDocument(Document doc) {
        String failedStep = doc.getString("failedStep");
        return new ExportBatch(
                doc.getString("_id"),
                doc.getString("tenantId"),
                doc.getString("vendor"),
                doc.getString("period"),
                Documents.intValue(doc, "seq"),
                BatchState.valueOf(doc.getString("state")),
                Documents.intValue(doc, "attempt"),
                Selection.fromDocument(doc.get("selection", Document.class)),
                Documents.nested(doc, "egress").map(EgressDetails::fromDocument).orElse(null),
                Documents.nested(doc, "file").map(FileDetails::fromDocument).orElse(null),
                Documents.nested(doc, "reconciliation")
                        .map(Reconciliation::fromDocument)
                        .orElse(null),
                failedStep == null ? null : FailedStep.valueOf(failedStep),
                doc.getString("lastError"),
                doc.getString("supersededBy"),
                Documents.instant(doc, "acknowledgedAt"),
                doc.getString("inboundBatchId"),
                Documents.instant(doc, "deliveredAt"),
                Documents.longValue(doc, "version"),
                Documents.instant(doc, "createdAt"),
                Documents.instant(doc, "updatedAt"));
    }
}
