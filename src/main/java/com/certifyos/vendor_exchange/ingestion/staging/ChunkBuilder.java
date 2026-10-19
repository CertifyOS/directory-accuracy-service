package com.certifyos.vendor_exchange.ingestion.staging;

import com.certifyos.vendor_exchange.export.batch.ExportBatch;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFile;
import com.certifyos.vendor_exchange.persistence.Ids;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.jboss.logging.Logger;

/**
 * Turns one chunk of wide rows into the documents staging writes and the counts it adds to the
 * inbound entry (design step 5). Every rule is local to the row, with no lookup (plan P5), in this
 * order: a row file-ingestion marked {@code INVALID} is {@code SCHEMA_INVALID}; a row that is not
 * about a practitioner is {@code UNSUPPORTED_ENTITY}; a row whose practitioner id or NPI is unusable
 * is {@code MISSING_IDS}; a row whose location id is unusable has its practitioner-level cells
 * processed, its location-level cells neither written nor counted, and is {@code MISSING_IDS}; every
 * other row is {@code STAGED}. Ids are trimmed first; an id is unusable when blank or when the id
 * rules refuse it ({@code |}, whitespace, a location of {@code -}), so the row is counted rather than
 * failing the chunk.
 *
 * <p>Per processed cell: {@code INVALID} is one {@code PENDING} recommendation; {@code VALID} adds
 * one confirmed field; anything else ({@code UNKNOWN}, {@code INCONCLUSIVE}, blank) adds one
 * unverified field. Recommendations with one id collapse to the first row's: a practitioner's
 * practitioner-level fields across its location rows, and a location row repeated in the file. A
 * later practitioner-level cell whose vendor value differs from the kept one adds one disagreement
 * and one log line naming the field and the two rows, never the values. Disagreements are counted
 * within one chunk: a practitioner whose location rows straddle a chunk boundary is compared only
 * among the rows of each chunk, so {@code practitionerFieldDisagreements} is a lower bound.
 */
public final class ChunkBuilder {

    private static final Logger LOG = Logger.getLogger(ChunkBuilder.class);
    private static final String INVALID = "INVALID";
    private static final String VALID = "VALID";
    private static final String CURSOR_SEPARATOR = ":";

    /** The field the id screen builds a probe id with; any field name of the mapping would do. */
    private static final String PROBE_FIELD = CandorRecsMapping.NPI;

    private ChunkBuilder() {}

    /**
     * One chunk, built.
     *
     * @param recommendations one document per id, in the order the rows first produced them
     * @param documentIdsByRow {@code <sheet>:<row>} of each {@code STAGED} row that produced at least
     *     one recommendation, to the ids it produced, collapsed ones included; the rows {@link
     *     #settle} may turn into {@code DUPLICATE}
     * @param dispositions rows per disposition, every disposition a key
     * @param confirmedFields cells the vendor marked {@code VALID}
     * @param unverifiedFields cells the vendor marked anything but {@code VALID} or {@code INVALID}
     * @param practitionerFieldDisagreements later practitioner-level cells whose vendor value differs
     *     from the kept one
     * @param rowsSeen rows in the chunk
     * @param lastCursor {@code <sheet>:<row>} of the chunk's last row, null for an empty chunk
     */
    public record Built(
            List<FieldRecommendation> recommendations,
            Map<String, Set<String>> documentIdsByRow,
            Map<RowDisposition, Long> dispositions,
            long confirmedFields,
            long unverifiedFields,
            long practitionerFieldDisagreements,
            int rowsSeen,
            String lastCursor) {
        public Built {
            recommendations = List.copyOf(recommendations);
            Map<String, Set<String>> ids = new LinkedHashMap<>();
            documentIdsByRow.forEach((row, rowIds) -> ids.put(row, Set.copyOf(rowIds)));
            documentIdsByRow = Collections.unmodifiableMap(ids);
            dispositions = Collections.unmodifiableMap(allDispositions(dispositions));
        }
    }

    /**
     * The cursor of a row, what {@code checkpoint.after} holds and the row stream resumes after.
     *
     * @param row the row
     * @return {@code <sheet>:<row>}
     */
    public static String cursorOf(WideRow row) {
        return row.sheet() + CURSOR_SEPARATOR + row.row();
    }

    /**
     * Builds one chunk.
     *
     * @param rows the chunk's rows, in stream order
     * @param batch the batch the upload belongs to
     * @param entry the upload
     * @param mapper reads the evidence cells
     * @param now the staging time of every document
     * @return the documents and counts
     */
    public static Built build(
            List<WideRow> rows, ExportBatch batch, InboundFile entry, ObjectMapper mapper, Instant now) {
        Chunk chunk = new Chunk(batch, entry, mapper, now);
        rows.forEach(chunk::add);
        return chunk.built();
    }

    /**
     * The chunk's dispositions once the bulk write has run: a {@code STAGED} row that produced at
     * least one recommendation, none of which is {@code ours}, becomes {@code DUPLICATE} (every
     * document it would write was already there from another upload). Every other row keeps its
     * disposition.
     *
     * @param built the chunk
     * @param ours the ids this upload wrote: inserted by this bulk write, or found already written
     *     by this upload (an earlier chunk, or this chunk before a crash)
     * @return rows per disposition, every disposition a key
     */
    public static Map<RowDisposition, Long> settle(Built built, Set<String> ours) {
        Map<RowDisposition, Long> settled = allDispositions(built.dispositions());
        for (Set<String> ids : built.documentIdsByRow().values()) {
            if (ids.stream().noneMatch(ours::contains)) {
                settled.merge(RowDisposition.STAGED, -1L, Long::sum);
                settled.merge(RowDisposition.DUPLICATE, 1L, Long::sum);
            }
        }
        return Collections.unmodifiableMap(settled);
    }

    private static Map<RowDisposition, Long> allDispositions(Map<RowDisposition, Long> counts) {
        Map<RowDisposition, Long> all = new EnumMap<>(RowDisposition.class);
        for (RowDisposition disposition : RowDisposition.values()) {
            all.put(disposition, counts.getOrDefault(disposition, 0L));
        }
        return all;
    }

    /** The mutable state of one build. */
    private static final class Chunk {
        private final ExportBatch batch;
        private final InboundFile entry;
        private final ObjectMapper mapper;
        private final Instant now;
        private final Map<String, FieldRecommendation> byId = new LinkedHashMap<>();
        private final Map<String, Set<String>> idsByRow = new LinkedHashMap<>();
        private final Map<RowDisposition, Long> dispositions = new EnumMap<>(RowDisposition.class);
        private long confirmed;
        private long unverified;
        private long disagreements;
        private int seen;
        private String lastCursor;

        Chunk(ExportBatch batch, InboundFile entry, ObjectMapper mapper, Instant now) {
            this.batch = batch;
            this.entry = entry;
            this.mapper = mapper;
            this.now = now;
        }

        void add(WideRow raw) {
            seen++;
            lastCursor = cursorOf(raw);
            if (!raw.schemaValid()) {
                count(RowDisposition.SCHEMA_INVALID);
                return;
            }
            if (!raw.isPractitioner()) {
                count(RowDisposition.UNSUPPORTED_ENTITY);
                return;
            }
            WideRow row = trimmed(raw);
            if (!usable(row.certifyPractitionerId(), null) || isBlank(row.npi())) {
                count(RowDisposition.MISSING_IDS);
                return;
            }
            boolean atLocation =
                    row.certifyLocationId() != null && usable(row.certifyPractitionerId(), row.certifyLocationId());
            Set<String> ids = cells(row, atLocation);
            if (!atLocation) {
                count(RowDisposition.MISSING_IDS);
                return;
            }
            count(RowDisposition.STAGED);
            if (!ids.isEmpty()) {
                idsByRow.computeIfAbsent(cursorOf(row), key -> new LinkedHashSet<>())
                        .addAll(ids);
            }
        }

        /**
         * Counts or writes each cell of a row with usable practitioner ids; location-level cells only
         * when the location id is usable too.
         *
         * @return the ids the row's {@code INVALID} cells are written under
         */
        private Set<String> cells(WideRow row, boolean atLocation) {
            Set<String> ids = new LinkedHashSet<>();
            for (WideRow.FieldCell cell : row.cells().values()) {
                if (cell.level() == FieldLevel.LOCATION && !atLocation) {
                    continue;
                }
                switch (cell.vendorStatus()) {
                    case INVALID -> ids.add(write(row, cell));
                    case VALID -> confirmed++;
                    default -> unverified++;
                }
            }
            return ids;
        }

        /** Builds the cell's recommendation, keeps the first one per id and returns its id. */
        private String write(WideRow row, WideRow.FieldCell cell) {
            FieldRecommendation recommendation = FieldRecommendation.pending(row, cell, batch, entry, mapper, now);
            FieldRecommendation kept = byId.putIfAbsent(recommendation.id(), recommendation);
            if (kept != null && !Objects.equals(kept.vendorValue(), recommendation.vendorValue())) {
                String first = kept.sourceFile().sheet()
                        + CURSOR_SEPARATOR
                        + kept.sourceFile().row();
                if (cell.level() == FieldLevel.PRACTITIONER) {
                    disagreements++;
                    LOG.infof(
                            "%s upload %s: practitioner field %s of row %s disagrees with row %s; the first is kept",
                            batch.id(), entry.uploadId(), cell.field(), cursorOf(row), first);
                } else {
                    LOG.infof(
                            "%s upload %s: location field %s of row %s repeats row %s with another value;"
                                    + " the first is kept",
                            batch.id(), entry.uploadId(), cell.field(), cursorOf(row), first);
                }
            }
            return recommendation.id();
        }

        /** Whether the id rules accept the practitioner id and, when given, the location id. */
        private boolean usable(String certifyPractitionerId, String certifyLocationId) {
            try {
                Ids.fieldRecommendationId(
                        batch.tenantId(),
                        batch.vendor(),
                        batch.id(),
                        certifyPractitionerId,
                        certifyLocationId,
                        PROBE_FIELD);
                return true;
            } catch (IllegalArgumentException refused) {
                return false;
            }
        }

        private void count(RowDisposition disposition) {
            dispositions.merge(disposition, 1L, Long::sum);
        }

        Built built() {
            return new Built(
                    new ArrayList<>(byId.values()),
                    idsByRow,
                    dispositions,
                    confirmed,
                    unverified,
                    disagreements,
                    seen,
                    lastCursor);
        }
    }

    /** The row with its three ids stripped of surrounding whitespace. */
    private static WideRow trimmed(WideRow row) {
        return new WideRow(
                row.sheet(),
                row.row(),
                row.schemaValid(),
                row.problems(),
                row.entityType(),
                strip(row.certifyPractitionerId()),
                strip(row.npi()),
                strip(row.certifyLocationId()),
                row.cells());
    }

    private static String strip(String id) {
        return id == null ? null : id.strip();
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
