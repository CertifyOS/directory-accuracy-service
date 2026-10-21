package com.certifyos.directory_accuracy.export.schedule;

import com.certifyos.directory_accuracy.persistence.Documents;
import com.certifyos.directory_accuracy.persistence.Ids;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import org.bson.Document;

/**
 * One row per tenant and vendor: definition (cadence, timezone, selection, template), enablement,
 * and runtime state ({@code nextDueAt}, last run). Written only through this service. {@code version}
 * increments on every write and every update is compare-and-set on it.
 *
 * @param id {@code <tenantId>|<vendor>}
 * @param tenantId the tenant
 * @param vendor the vendor code
 * @param enabled whether the tick considers this schedule
 * @param cadence when it is due
 * @param timezone the IANA zone the cadence is evaluated in
 * @param selection the practitioner criteria copied onto each batch
 * @param egressTemplateId the tenant's vendor template in egress
 * @param nextDueAt the next due instant, UTC
 * @param lastBatchId the last batch created, or null
 * @param lastRunAt when the last batch was created, or null
 * @param disabledAt present only while disabled
 * @param disabledReason present only while disabled
 * @param version compare-and-set token, 1 on insert
 * @param createdBy operator identity
 * @param createdAt insert time
 * @param updatedBy last writer, operator or {@code system:vendor-export}
 * @param updatedAt last write time
 */
public record Schedule(
        String id,
        String tenantId,
        String vendor,
        boolean enabled,
        Cadence cadence,
        ZoneId timezone,
        SelectionCriteria selection,
        String egressTemplateId,
        Instant nextDueAt,
        String lastBatchId,
        Instant lastRunAt,
        Instant disabledAt,
        String disabledReason,
        long version,
        String createdBy,
        Instant createdAt,
        String updatedBy,
        Instant updatedAt) {

    /**
     * A new, enabled schedule at version 1.
     *
     * @param tenantId the tenant
     * @param vendor the vendor code
     * @param cadence when it is due
     * @param timezone the zone
     * @param selection the criteria
     * @param egressTemplateId the egress template
     * @param nextDueAt the first due instant
     * @param createdBy the operator
     * @param now the insert time
     * @return the schedule to insert
     */
    public static Schedule create(
            String tenantId,
            String vendor,
            Cadence cadence,
            ZoneId timezone,
            SelectionCriteria selection,
            String egressTemplateId,
            Instant nextDueAt,
            String createdBy,
            Instant now) {
        return new Schedule(
                Ids.scheduleId(tenantId, vendor),
                tenantId,
                vendor,
                true,
                cadence,
                timezone,
                selection,
                egressTemplateId,
                nextDueAt,
                null,
                null,
                null,
                null,
                1L,
                createdBy,
                now,
                createdBy,
                now);
    }

    /** The BSON document, with absent optional fields left out. */
    public Document toDocument() {
        Document doc = new Document("_id", id)
                .append("tenantId", tenantId)
                .append("vendor", vendor)
                .append("enabled", enabled)
                .append("cadence", cadence.toDocument())
                .append("timezone", timezone.getId())
                .append("selection", selection.toDocuments())
                .append("egressTemplateId", egressTemplateId)
                .append("nextDueAt", nextDueAt)
                .append("version", version)
                .append("createdBy", createdBy)
                .append("createdAt", createdAt)
                .append("updatedBy", updatedBy)
                .append("updatedAt", updatedAt);
        Documents.put(doc, "lastBatchId", lastBatchId);
        Documents.put(doc, "lastRunAt", lastRunAt);
        Documents.put(doc, "disabledAt", disabledAt);
        Documents.put(doc, "disabledReason", disabledReason);
        return doc;
    }

    /**
     * Reads a stored document.
     *
     * @param doc the document
     * @return the schedule
     */
    public static Schedule fromDocument(Document doc) {
        List<Document> selection = doc.getList("selection", Document.class);
        return new Schedule(
                doc.getString("_id"),
                doc.getString("tenantId"),
                doc.getString("vendor"),
                doc.getBoolean("enabled", false),
                Cadence.fromDocument(doc.get("cadence", Document.class)),
                ZoneId.of(doc.getString("timezone")),
                SelectionCriteria.fromDocuments(selection),
                doc.getString("egressTemplateId"),
                Documents.instant(doc, "nextDueAt"),
                doc.getString("lastBatchId"),
                Documents.instant(doc, "lastRunAt"),
                Documents.instant(doc, "disabledAt"),
                doc.getString("disabledReason"),
                Documents.longValue(doc, "version"),
                doc.getString("createdBy"),
                Documents.instant(doc, "createdAt"),
                doc.getString("updatedBy"),
                Documents.instant(doc, "updatedAt"));
    }
}
