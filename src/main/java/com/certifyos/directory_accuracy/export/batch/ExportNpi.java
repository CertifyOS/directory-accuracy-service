package com.certifyos.directory_accuracy.export.batch;

import com.certifyos.directory_accuracy.persistence.Documents;
import com.certifyos.directory_accuracy.persistence.Ids;
import java.time.Instant;
import org.bson.Document;

/**
 * One practitioner sent in one batch. Written once by the select job and never updated; the
 * reconciliation result lives on the batch. {@code certifyPractitionerId} is stored because the
 * platform keys practitioners by it, not by NPI, and the mapping can change after selection.
 *
 * @param id {@code <exportBatchId>|<npi>}
 * @param exportBatchId the batch
 * @param tenantId the tenant
 * @param npi the ten-digit NPI
 * @param certifyPractitionerId the platform id from the same api-layer page
 * @param registeredAt when the select job wrote it
 */
public record ExportNpi(
        String id,
        String exportBatchId,
        String tenantId,
        String npi,
        String certifyPractitionerId,
        Instant registeredAt) {

    /**
     * A registry row for a selected practitioner.
     *
     * @param exportBatchId the batch
     * @param tenantId the tenant
     * @param npi the NPI
     * @param certifyPractitionerId the platform id
     * @param now the registration time
     * @return the row
     */
    public static ExportNpi of(
            String exportBatchId, String tenantId, String npi, String certifyPractitionerId, Instant now) {
        return new ExportNpi(Ids.npiId(exportBatchId, npi), exportBatchId, tenantId, npi, certifyPractitionerId, now);
    }

    /** The BSON document. */
    public Document toDocument() {
        return new Document("_id", id)
                .append("exportBatchId", exportBatchId)
                .append("tenantId", tenantId)
                .append("npi", npi)
                .append("certifyPractitionerId", certifyPractitionerId)
                .append("registeredAt", registeredAt);
    }

    /**
     * Reads a stored document.
     *
     * @param doc the document
     * @return the row
     */
    public static ExportNpi fromDocument(Document doc) {
        return new ExportNpi(
                doc.getString("_id"),
                doc.getString("exportBatchId"),
                doc.getString("tenantId"),
                doc.getString("npi"),
                doc.getString("certifyPractitionerId"),
                Documents.instant(doc, "registeredAt"));
    }
}
