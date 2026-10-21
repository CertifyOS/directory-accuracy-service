package com.certifyos.directory_accuracy.ingestion.inbound;

import com.certifyos.directory_accuracy.persistence.Documents;
import java.time.Instant;
import java.util.Objects;
import org.bson.Document;

/**
 * A vendor upload that could not be attached to a batch, kept with the reason instead of being
 * discarded. The document id is the upload id. Absent fields are left out of the BSON.
 *
 * @param id {@code <bucket>|<objectName>|<generation>}
 * @param folderTenant the {@code to/<tenant>/} leg of the object name, null when the name has none
 * @param parsed what the object name said, null when it did not parse
 * @param reason one of the unmatched reason constants on {@link InboundNames}
 * @param receivedAt when the arrival was recorded
 * @param resolution how an operator closed it, null while open
 */
public record UnmatchedUpload(
        String id,
        String folderTenant,
        InboundNames.Parsed parsed,
        String reason,
        Instant receivedAt,
        Resolution resolution) {

    public static final String ATTACHED = "ATTACHED";
    public static final String DISCARDED = "DISCARDED";

    public UnmatchedUpload {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(reason, "reason");
        Objects.requireNonNull(receivedAt, "receivedAt");
    }

    /**
     * How an operator closed an unmatched upload.
     *
     * @param kind {@code ATTACHED} or {@code DISCARDED}
     * @param exportBatchId the batch it was attached to, set for {@code ATTACHED}
     * @param reason why it was discarded, set for {@code DISCARDED}
     * @param actor who resolved it
     * @param at when it was resolved
     */
    public record Resolution(String kind, String exportBatchId, String reason, String actor, Instant at) {

        public Resolution {
            if (!ATTACHED.equals(kind) && !DISCARDED.equals(kind)) {
                throw new IllegalArgumentException("resolution kind must be ATTACHED or DISCARDED");
            }
            Objects.requireNonNull(actor, "actor");
            Objects.requireNonNull(at, "at");
        }

        Document toDocument() {
            Document doc = new Document("kind", kind);
            Documents.put(doc, "exportBatchId", exportBatchId);
            Documents.put(doc, "reason", reason);
            return doc.append("actor", actor).append("at", at);
        }

        static Resolution fromDocument(Document doc) {
            return new Resolution(
                    doc.getString("kind"),
                    doc.getString("exportBatchId"),
                    doc.getString("reason"),
                    doc.getString("actor"),
                    Documents.instant(doc, "at"));
        }
    }

    Document toDocument() {
        Document doc = new Document("_id", id);
        Documents.put(doc, "folderTenant", folderTenant);
        Documents.put(doc, "parsed", parsed == null ? null : parsedDocument(parsed));
        doc.append("reason", reason).append("receivedAt", receivedAt);
        Documents.put(doc, "resolution", resolution == null ? null : resolution.toDocument());
        return doc;
    }

    static UnmatchedUpload fromDocument(Document doc) {
        return new UnmatchedUpload(
                doc.getString("_id"),
                doc.getString("folderTenant"),
                Documents.nested(doc, "parsed").map(UnmatchedUpload::parsedFrom).orElse(null),
                doc.getString("reason"),
                Documents.instant(doc, "receivedAt"),
                Documents.nested(doc, "resolution")
                        .map(Resolution::fromDocument)
                        .orElse(null));
    }

    private static Document parsedDocument(InboundNames.Parsed parts) {
        return new Document("folderTenant", parts.folderTenant())
                .append("fileTenant", parts.fileTenant())
                .append("exportBatchRef", parts.exportBatchRef())
                .append("vendorBatchId", parts.vendorBatchId())
                .append("fileDate", parts.fileDate())
                .append("fileName", parts.fileName());
    }

    private static InboundNames.Parsed parsedFrom(Document doc) {
        return new InboundNames.Parsed(
                doc.getString("folderTenant"),
                doc.getString("fileTenant"),
                doc.getString("exportBatchRef"),
                doc.getString("vendorBatchId"),
                doc.getString("fileDate"),
                doc.getString("fileName"));
    }
}
