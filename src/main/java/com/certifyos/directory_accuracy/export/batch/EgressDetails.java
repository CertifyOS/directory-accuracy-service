package com.certifyos.directory_accuracy.export.batch;

import com.certifyos.directory_accuracy.persistence.Documents;
import java.time.Instant;
import java.util.List;
import org.bson.Document;

/**
 * What the batch knows about its egress request: the attempt's correlation id, the template pinned
 * at first request (so every attempt builds a byte-identical file), the destination the file must
 * land at, and how completion was learned.
 *
 * @param correlationId {@code <batchId>-r<attempt>} of the current attempt
 * @param jobReference egress's job reference from its 202 or 409
 * @param templateId the tenant's vendor template
 * @param templateVersion the template row version pinned for this batch
 * @param mappingsCsvUrl the version-specific mappings CSV pinned for this batch
 * @param separator the pinned separator
 * @param outputFormat the pinned output format
 * @param rowExpansionKeys the pinned expansion keys
 * @param destination where egress copies the finished file
 * @param requestedAt when the export request was accepted
 * @param deadlineJobId the pending deadline check, for deletion on completion
 * @param completedAt when egress reported completion
 * @param completionSource {@code EVENT} or {@code DEADLINE}
 * @param fileProducedBy the correlation id of the attempt whose file landed
 */
public record EgressDetails(
        String correlationId,
        String jobReference,
        String templateId,
        Integer templateVersion,
        String mappingsCsvUrl,
        String separator,
        String outputFormat,
        List<String> rowExpansionKeys,
        Destination destination,
        Instant requestedAt,
        String deadlineJobId,
        Instant completedAt,
        CompletionSource completionSource,
        String fileProducedBy) {

    /** How the service learned egress finished. */
    public enum CompletionSource {
        EVENT,
        DEADLINE,
        /** A retry found the prior attempt's complete file already in place. */
        PRIOR_ATTEMPT
    }

    /**
     * Where the finished file lands.
     *
     * @param bucket the vendor bucket
     * @param objectName {@code from/<tenantId>/<tenantId>_<batchId>_<yyyyMMdd>.csv}
     */
    public record Destination(String bucket, String objectName) {
        Document toDocument() {
            return new Document("bucket", bucket).append("objectName", objectName);
        }

        static Destination fromDocument(Document doc) {
            return new Destination(doc.getString("bucket"), doc.getString("objectName"));
        }
    }

    public EgressDetails {
        rowExpansionKeys = rowExpansionKeys == null ? List.of() : List.copyOf(rowExpansionKeys);
    }

    /** The BSON shape, absent fields left out. */
    public Document toDocument() {
        Document doc = new Document();
        Documents.put(doc, "correlationId", correlationId);
        Documents.put(doc, "jobReference", jobReference);
        Documents.put(doc, "templateId", templateId);
        Documents.put(doc, "templateVersion", templateVersion);
        Documents.put(doc, "mappingsCsvUrl", mappingsCsvUrl);
        Documents.put(doc, "separator", separator);
        Documents.put(doc, "outputFormat", outputFormat);
        if (!rowExpansionKeys.isEmpty()) {
            doc.append("rowExpansionKeys", rowExpansionKeys);
        }
        Documents.put(doc, "destination", destination == null ? null : destination.toDocument());
        Documents.put(doc, "requestedAt", requestedAt);
        Documents.put(doc, "deadlineJobId", deadlineJobId);
        Documents.put(doc, "completedAt", completedAt);
        Documents.put(doc, "completionSource", completionSource == null ? null : completionSource.name());
        Documents.put(doc, "fileProducedBy", fileProducedBy);
        return doc;
    }

    /**
     * Reads the BSON shape.
     *
     * @param doc the nested egress document
     * @return the details
     */
    public static EgressDetails fromDocument(Document doc) {
        String source = doc.getString("completionSource");
        return new EgressDetails(
                doc.getString("correlationId"),
                doc.getString("jobReference"),
                doc.getString("templateId"),
                Documents.intValue(doc, "templateVersion"),
                doc.getString("mappingsCsvUrl"),
                doc.getString("separator"),
                doc.getString("outputFormat"),
                Documents.strings(doc, "rowExpansionKeys"),
                Documents.nested(doc, "destination")
                        .map(Destination::fromDocument)
                        .orElse(null),
                Documents.instant(doc, "requestedAt"),
                doc.getString("deadlineJobId"),
                Documents.instant(doc, "completedAt"),
                source == null ? null : CompletionSource.valueOf(source),
                doc.getString("fileProducedBy"));
    }
}
