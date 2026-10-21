package com.certifyos.directory_accuracy.ingestion.staging;

import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFile;
import com.certifyos.directory_accuracy.persistence.Documents;
import com.certifyos.directory_accuracy.persistence.Ids;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.bson.Document;

/**
 * One {@code field_recommendations} document: a field the vendor marked {@code INVALID}, for one
 * practitioner, one location or none, in one batch. The stored shape is the design's sample field for
 * field; absent values are left out of the BSON, so {@code location} and {@code goldenRecordUpdate}
 * read back as null and a later dotted {@code $set} on {@code goldenRecordUpdate} can create it. No
 * practitioner name or address is copied here: the reviewer's screen reads them at request time.
 *
 * @param id {@link Ids#fieldRecommendationId}
 * @param tenantId the tenant
 * @param sourceType {@code vendor}
 * @param source the vendor, for example {@code candor}
 * @param exportBatchId the batch the vendor answered
 * @param practitioner our practitioner id and the NPI we sent
 * @param location our location id, null for practitioner-level fields; that is what tells the levels apart
 * @param field the field name
 * @param sentValue the value we sent
 * @param vendorValue the vendor's value
 * @param vendorStatus the vendor's status, {@code INVALID} when staged
 * @param vendorReason the vendor's reasons
 * @param vendorConfidence the vendor's confidence
 * @param vendorEvidence the vendor's evidence objects
 * @param reviewStatus where the field is in review
 * @param reviewHistory every decision, oldest first
 * @param version compare-and-set token, 1 on insert
 * @param goldenRecordUpdate what the release did, null before a release is requested
 * @param sourceFile the upload and row the field came from
 * @param stagedAt when staging first wrote the document
 * @param updatedAt the last write
 */
public record FieldRecommendation(
        String id,
        String tenantId,
        String sourceType,
        String source,
        String exportBatchId,
        Practitioner practitioner,
        Location location,
        String field,
        String sentValue,
        String vendorValue,
        String vendorStatus,
        List<String> vendorReason,
        String vendorConfidence,
        List<Map<String, Object>> vendorEvidence,
        ReviewStatus reviewStatus,
        List<HistoryEntry> reviewHistory,
        long version,
        GoldenRecordUpdate goldenRecordUpdate,
        SourceFile sourceFile,
        Instant stagedAt,
        Instant updatedAt) {

    /** The {@code sourceType} of every recommendation this lane stages. */
    public static final String SOURCE_TYPE = "vendor";

    private static final TypeReference<List<Map<String, Object>>> EVIDENCE = new TypeReference<>() {};
    private static final String EVIDENCE_TEXT = "evidence";

    /**
     * The practitioner the field belongs to.
     *
     * @param certifyPractitionerId our practitioner id, as the vendor echoed it
     * @param npi the NPI we sent
     */
    public record Practitioner(String certifyPractitionerId, String npi) {
        Document toDocument() {
            Document doc = new Document();
            Documents.put(doc, "certifyPractitionerId", certifyPractitionerId);
            Documents.put(doc, "npi", npi);
            return doc;
        }

        static Practitioner fromDocument(Document doc) {
            return new Practitioner(doc.getString("certifyPractitionerId"), doc.getString("npi"));
        }
    }

    /**
     * The location a location-level field belongs to.
     *
     * @param certifyLocationId our location id, as the vendor echoed it
     */
    public record Location(String certifyLocationId) {
        Document toDocument() {
            return Documents.put(new Document(), "certifyLocationId", certifyLocationId);
        }

        static Location fromDocument(Document doc) {
            return new Location(doc.getString("certifyLocationId"));
        }
    }

    /**
     * One decision.
     *
     * @param action the action, for example {@code APPROVE}
     * @param by who took it
     * @param at when
     * @param comment the reviewer's comment, or null
     */
    public record HistoryEntry(String action, String by, Instant at, String comment) {
        /** The BSON form, absent fields left out; the decision endpoints push it onto {@code reviewHistory}. */
        public Document toDocument() {
            Document doc = new Document();
            Documents.put(doc, "action", action);
            Documents.put(doc, "by", by);
            Documents.put(doc, "at", at);
            Documents.put(doc, "comment", comment);
            return doc;
        }

        static HistoryEntry fromDocument(Document doc) {
            return new HistoryEntry(
                    doc.getString("action"),
                    doc.getString("by"),
                    Documents.instant(doc, "at"),
                    doc.getString("comment"));
        }
    }

    /**
     * What a release did to the golden record. Every component is optional; the release request
     * writes the first five, the release job the rest.
     *
     * @param releaseId groups the fields of one release
     * @param requestedBy who requested it
     * @param requestedAt when
     * @param writePath {@code SLICE} or {@code IN_PLACE}
     * @param resource the DAL resource written
     * @param crosswalkId the crosswalk the write went through
     * @param sliceId the certify id of the slice the write produced, as the DAL answered or read after the
     *     write; the read-back looks for it in the operational value's {@code contributingSlices}
     * @param sentAt when the release job sent the DAL batch; an operational-value event counts for the
     *     write only when its triggering slice changed at or after this time
     * @param writtenAt when the DAL accepted the write
     * @param deadlineJobId the read-back deadline job
     * @param outcome {@code WON}, {@code OUTRANKED}, {@code NOT_APPLIED} or {@code UNKNOWN}
     * @param winningSource the source the golden record shows
     * @param releasedAt when the outcome was recorded
     * @param error a stable error code when the release failed
     */
    public record GoldenRecordUpdate(
            String releaseId,
            String requestedBy,
            Instant requestedAt,
            String writePath,
            String resource,
            String crosswalkId,
            String sliceId,
            Instant sentAt,
            Instant writtenAt,
            String deadlineJobId,
            String outcome,
            String winningSource,
            Instant releasedAt,
            String error) {
        /** The BSON form, only the components that are set. */
        public Document toDocument() {
            Document doc = new Document();
            Documents.put(doc, "releaseId", releaseId);
            Documents.put(doc, "requestedBy", requestedBy);
            Documents.put(doc, "requestedAt", requestedAt);
            Documents.put(doc, "writePath", writePath);
            Documents.put(doc, "resource", resource);
            Documents.put(doc, "crosswalkId", crosswalkId);
            Documents.put(doc, "sliceId", sliceId);
            Documents.put(doc, "sentAt", sentAt);
            Documents.put(doc, "writtenAt", writtenAt);
            Documents.put(doc, "deadlineJobId", deadlineJobId);
            Documents.put(doc, "outcome", outcome);
            Documents.put(doc, "winningSource", winningSource);
            Documents.put(doc, "releasedAt", releasedAt);
            Documents.put(doc, "error", error);
            return doc;
        }

        static GoldenRecordUpdate fromDocument(Document doc) {
            return new GoldenRecordUpdate(
                    doc.getString("releaseId"),
                    doc.getString("requestedBy"),
                    Documents.instant(doc, "requestedAt"),
                    doc.getString("writePath"),
                    doc.getString("resource"),
                    doc.getString("crosswalkId"),
                    doc.getString("sliceId"),
                    Documents.instant(doc, "sentAt"),
                    Documents.instant(doc, "writtenAt"),
                    doc.getString("deadlineJobId"),
                    doc.getString("outcome"),
                    doc.getString("winningSource"),
                    Documents.instant(doc, "releasedAt"),
                    doc.getString("error"));
        }
    }

    /**
     * Where the field came from.
     *
     * @param uploadId the {@code inboundFiles[].uploadId} on the batch
     * @param sheet the sheet
     * @param row the row number
     */
    public record SourceFile(String uploadId, String sheet, int row) {
        Document toDocument() {
            Document doc = new Document();
            Documents.put(doc, "uploadId", uploadId);
            Documents.put(doc, "sheet", sheet);
            return doc.append("row", row);
        }

        static SourceFile fromDocument(Document doc) {
            Integer row = Documents.intValue(doc, "row");
            return new SourceFile(doc.getString("uploadId"), doc.getString("sheet"), row == null ? 0 : row);
        }
    }

    public FieldRecommendation {
        vendorReason = vendorReason == null ? List.of() : List.copyOf(vendorReason);
        reviewHistory = reviewHistory == null ? List.of() : List.copyOf(reviewHistory);
        vendorEvidence = evidenceCopy(vendorEvidence);
    }

    /**
     * The level of the field.
     *
     * @return {@code LOCATION} when the document names a location, else {@code PRACTITIONER}
     */
    public FieldLevel level() {
        return location != null ? FieldLevel.LOCATION : FieldLevel.PRACTITIONER;
    }

    /**
     * The document staging writes for one field the vendor marked {@code INVALID}.
     *
     * @param row the wide row
     * @param cell the field's cell of that row
     * @param batch the batch the upload belongs to; its vendor is the source
     * @param entry the upload
     * @param mapper reads the evidence cell
     * @param now the staging time, both {@code stagedAt} and {@code updatedAt}
     * @return a {@code PENDING} recommendation, version 1, no history and no release; the evidence is
     *     the cell's JSON array of objects, or one {@code {"evidence": <text>}} object when the cell is
     *     anything else, or empty when blank
     * @throws IllegalArgumentException for a location-level cell of a row without a location id
     */
    public static FieldRecommendation pending(
            WideRow row,
            WideRow.FieldCell cell,
            ExportBatch batch,
            InboundFile entry,
            ObjectMapper mapper,
            Instant now) {
        boolean atLocation = cell.level() == FieldLevel.LOCATION;
        if (atLocation && !row.hasLocationId()) {
            throw new IllegalArgumentException("location-level field " + cell.field() + " needs a location id, row "
                    + row.sheet() + ":" + row.row());
        }
        String locationId = atLocation ? row.certifyLocationId() : null;
        return new FieldRecommendation(
                Ids.fieldRecommendationId(
                        batch.tenantId(),
                        batch.vendor(),
                        batch.id(),
                        row.certifyPractitionerId(),
                        locationId,
                        cell.field()),
                batch.tenantId(),
                SOURCE_TYPE,
                batch.vendor(),
                batch.id(),
                new Practitioner(row.certifyPractitionerId(), row.npi()),
                atLocation ? new Location(locationId) : null,
                cell.field(),
                cell.sentValue(),
                cell.vendorValue(),
                cell.vendorStatus(),
                cell.reasons(),
                cell.confidence(),
                evidenceOf(cell.evidenceJson(), mapper),
                ReviewStatus.PENDING,
                List.of(),
                1L,
                null,
                new SourceFile(entry.uploadId(), row.sheet(), row.row()),
                now,
                now);
    }

    /** The BSON shape, in the design sample's order, absent fields left out. */
    public Document toDocument() {
        Document doc = new Document();
        Documents.put(doc, "_id", id);
        Documents.put(doc, "tenantId", tenantId);
        Documents.put(doc, "sourceType", sourceType);
        Documents.put(doc, "source", source);
        Documents.put(doc, "exportBatchId", exportBatchId);
        Documents.put(doc, "practitioner", practitioner == null ? null : practitioner.toDocument());
        Documents.put(doc, "location", location == null ? null : location.toDocument());
        Documents.put(doc, "field", field);
        Documents.put(doc, "sentValue", sentValue);
        Documents.put(doc, "vendorValue", vendorValue);
        Documents.put(doc, "vendorStatus", vendorStatus);
        doc.append("vendorReason", vendorReason);
        Documents.put(doc, "vendorConfidence", vendorConfidence);
        doc.append("vendorEvidence", vendorEvidence.stream().map(Document::new).toList());
        Documents.put(doc, "reviewStatus", reviewStatus == null ? null : reviewStatus.name());
        doc.append(
                "reviewHistory",
                reviewHistory.stream().map(HistoryEntry::toDocument).toList());
        doc.append("version", version);
        Documents.put(doc, "goldenRecordUpdate", goldenRecordUpdate == null ? null : goldenRecordUpdate.toDocument());
        Documents.put(doc, "sourceFile", sourceFile == null ? null : sourceFile.toDocument());
        Documents.put(doc, "stagedAt", stagedAt);
        Documents.put(doc, "updatedAt", updatedAt);
        return doc;
    }

    /**
     * Reads the BSON shape.
     *
     * @param doc one {@code field_recommendations} document
     * @return the recommendation
     * @throws IllegalStateException for an unknown {@code reviewStatus} or a missing {@code version}
     */
    public static FieldRecommendation fromDocument(Document doc) {
        Long version = Documents.longValue(doc, "version");
        if (version == null) {
            throw new IllegalStateException("field recommendation without a version: " + doc.getString("_id"));
        }
        return new FieldRecommendation(
                doc.getString("_id"),
                doc.getString("tenantId"),
                doc.getString("sourceType"),
                doc.getString("source"),
                doc.getString("exportBatchId"),
                Documents.nested(doc, "practitioner")
                        .map(Practitioner::fromDocument)
                        .orElse(null),
                Documents.nested(doc, "location").map(Location::fromDocument).orElse(null),
                doc.getString("field"),
                doc.getString("sentValue"),
                doc.getString("vendorValue"),
                doc.getString("vendorStatus"),
                Documents.strings(doc, "vendorReason"),
                doc.getString("vendorConfidence"),
                new ArrayList<Map<String, Object>>(documents(doc, "vendorEvidence")),
                reviewStatusOf(doc),
                documents(doc, "reviewHistory").stream()
                        .map(HistoryEntry::fromDocument)
                        .toList(),
                version,
                Documents.nested(doc, "goldenRecordUpdate")
                        .map(GoldenRecordUpdate::fromDocument)
                        .orElse(null),
                Documents.nested(doc, "sourceFile")
                        .map(SourceFile::fromDocument)
                        .orElse(null),
                Documents.instant(doc, "stagedAt"),
                Documents.instant(doc, "updatedAt"));
    }

    private static ReviewStatus reviewStatusOf(Document doc) {
        String name = doc.getString("reviewStatus");
        if (name == null) {
            return null;
        }
        try {
            return ReviewStatus.valueOf(name);
        } catch (IllegalArgumentException exception) {
            throw new IllegalStateException(
                    "unknown review status '" + name + "' on " + doc.getString("_id"), exception);
        }
    }

    private static List<Document> documents(Document doc, String key) {
        List<Document> values = doc.getList(key, Document.class);
        return values == null ? List.of() : values;
    }

    /** The evidence cell as a list of objects; anything that is not a JSON array of objects is kept as text. */
    private static List<Map<String, Object>> evidenceOf(String text, ObjectMapper mapper) {
        if (text == null || text.isBlank()) {
            return List.of();
        }
        try {
            List<Map<String, Object>> parsed = mapper.readValue(text, EVIDENCE);
            if (parsed != null && parsed.stream().noneMatch(Objects::isNull)) {
                return parsed;
            }
            return asText(text);
        } catch (JsonProcessingException notObjects) {
            return asText(text);
        }
    }

    private static List<Map<String, Object>> asText(String text) {
        return List.of(Map.of(EVIDENCE_TEXT, text));
    }

    /**
     * An unmodifiable deep copy with plain maps and lists, so a list read from BSON (whose elements are
     * {@link Document}s) equals the one staging built. Values may be null; the vendor's JSON allows it.
     */
    private static List<Map<String, Object>> evidenceCopy(List<Map<String, Object>> evidence) {
        if (evidence == null) {
            return List.of();
        }
        List<Map<String, Object>> copy = new ArrayList<>(evidence.size());
        for (Map<String, Object> item : evidence) {
            copy.add(plainMap(Objects.requireNonNull(item, "evidence item")));
        }
        return Collections.unmodifiableList(copy);
    }

    private static Map<String, Object> plainMap(Map<?, ?> map) {
        Map<String, Object> copy = new LinkedHashMap<>();
        map.forEach((key, value) -> copy.put(String.valueOf(key), plain(value)));
        return Collections.unmodifiableMap(copy);
    }

    /**
     * A plain copy of one evidence value. A number Jackson parsed as {@link BigInteger} (beyond a long)
     * or {@link BigDecimal} has no BSON codec and would fail the whole bulk write, so it is kept as its
     * text.
     */
    private static Object plain(Object value) {
        if (value instanceof BigInteger || value instanceof BigDecimal) {
            return value.toString();
        }
        if (value instanceof Map<?, ?> map) {
            return plainMap(map);
        }
        if (value instanceof List<?> list) {
            List<Object> copy = new ArrayList<>(list.size());
            list.forEach(item -> copy.add(plain(item)));
            return Collections.unmodifiableList(copy);
        }
        return value;
    }
}
