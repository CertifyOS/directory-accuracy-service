package com.certifyos.directory_accuracy.ingestion.staging;

import com.certifyos.directory_accuracy.clients.FileIngestionRow;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFileRepository.Located;
import com.certifyos.directory_accuracy.metrics.VendorExchangeMetrics;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.jboss.logging.Logger;

/**
 * Stages one chunk of streamed rows (design step 5, "Batching and idempotency"): build the documents
 * and counts, write the documents in one unordered bulk upsert outside any transaction (D10), settle
 * which rows were duplicates, then add the counts and move the checkpoint in one guarded write. A
 * crash before the checkpoint redoes the chunk on the same ids and counts it once.
 *
 * <p>Duplicates are judged by who wrote a document, not only by whether this bulk write inserted it:
 * an existing document whose {@code sourceFile.uploadId} is this upload was written by this upload
 * (an earlier chunk that saw the practitioner first, or this chunk before a crash), so its row is
 * not a duplicate; only documents another upload wrote make a row {@code DUPLICATE}. The one read
 * this needs runs only when the bulk write found existing documents. A document counts toward {@code
 * written.fieldRecommendations} once: when this chunk inserted it, or when its stored source row is
 * the row of this chunk that produced it (a redone chunk).
 */
@ApplicationScoped
public class RowStager {

    private static final Logger LOG = Logger.getLogger(RowStager.class);

    private final FieldRecommendationRepository recommendations;
    private final StagingLifecycle lifecycle;
    private final ObjectMapper mapper;
    private final VendorExchangeMetrics metrics;
    private final Clock clock;

    public RowStager(
            FieldRecommendationRepository recommendations,
            StagingLifecycle lifecycle,
            ObjectMapper mapper,
            VendorExchangeMetrics metrics,
            Clock clock) {
        this.recommendations = recommendations;
        this.lifecycle = lifecycle;
        this.mapper = mapper;
        this.metrics = metrics;
        this.clock = clock;
    }

    /**
     * What one chunk added to the entry.
     *
     * @param after {@code <sheet>:<row>} of the chunk's last row, the new checkpoint
     * @param dispositions rows per disposition after {@link ChunkBuilder#settle}, every disposition a key
     * @param recommendations documents this chunk wrote
     * @param confirmedFields cells the vendor marked {@code VALID}
     * @param unverifiedFields cells the vendor could not verify
     * @param disagreements practitioner-level cells that disagreed with the kept value within this
     *     chunk (rows of one practitioner in an earlier chunk are not compared)
     */
    public record Progress(
            String after,
            Map<RowDisposition, Long> dispositions,
            long recommendations,
            long confirmedFields,
            long unverifiedFields,
            long disagreements) {
        public Progress {
            Map<RowDisposition, Long> copy = new EnumMap<>(RowDisposition.class);
            copy.putAll(dispositions);
            dispositions = Collections.unmodifiableMap(copy);
        }
    }

    /**
     * Stages one chunk and checkpoints it.
     *
     * @param located the entry, in {@code STAGING}
     * @param from the checkpoint the stream resumed from for this chunk, null for the first chunk
     * @param chunk the chunk's rows, in stream order, at least one
     * @param now the staging time of the chunk's documents and of the checkpoint
     * @return what the chunk added, or empty when the entry had left {@code STAGING} or another run
     *     had moved its checkpoint, and this checkpoint was not written (the documents are,
     *     harmlessly: they are idempotent)
     */
    public Optional<Progress> stageChunk(Located located, String from, List<FileIngestionRow> chunk, Instant now) {
        Instant started = clock.instant();
        String tenantId = located.batch().tenantId();
        String uploadId = located.entry().uploadId();
        List<WideRow> rows = chunk.stream().map(WideRow::from).toList();
        ChunkBuilder.Built built = ChunkBuilder.build(rows, located.batch(), located.entry(), mapper, now);
        FieldRecommendationRepository.Upserted upserted = recommendations.upsertAll(built.recommendations());

        List<String> existing = new ArrayList<>();
        Map<String, FieldRecommendation.SourceFile> builtSources = new HashMap<>();
        for (FieldRecommendation recommendation : built.recommendations()) {
            if (!upserted.insertedIds().contains(recommendation.id())) {
                existing.add(recommendation.id());
                builtSources.put(recommendation.id(), recommendation.sourceFile());
            }
        }
        Set<String> ours = new HashSet<>(upserted.insertedIds());
        long written = upserted.insertedIds().size();
        if (!existing.isEmpty()) {
            Map<String, FieldRecommendation.SourceFile> stored = recommendations.sourceFilesOf(tenantId, existing);
            for (Map.Entry<String, FieldRecommendation.SourceFile> source : stored.entrySet()) {
                if (uploadId.equals(source.getValue().uploadId())) {
                    ours.add(source.getKey());
                    if (source.getValue().equals(builtSources.get(source.getKey()))) {
                        written++;
                    }
                }
            }
        }
        Progress progress = new Progress(
                built.lastCursor(),
                ChunkBuilder.settle(built, ours),
                written,
                built.confirmedFields(),
                built.unverifiedFields(),
                built.practitionerFieldDisagreements());
        if (!lifecycle.chunkCheckpoint(located, from, progress, now)) {
            LOG.infof(
                    "%s upload %s left STAGING or its checkpoint moved past %s; chunk to %s not checkpointed",
                    located.batch().id(), uploadId, from, built.lastCursor());
            return Optional.empty();
        }
        progress.dispositions().forEach((disposition, count) -> {
            if (count > 0) {
                metrics.stageRows(tenantId, disposition.name(), count);
            }
        });
        metrics.stageDocuments(tenantId, written);
        Duration took = Duration.between(started, clock.instant());
        metrics.stageChunkDuration(took);
        LOG.infof(
                "%s upload %s: chunk to %s staged in %d ms: %d rows, %d documents written, %d already present,"
                        + " %d disagreements",
                located.batch().id(),
                uploadId,
                built.lastCursor(),
                took.toMillis(),
                built.rowsSeen(),
                written,
                existing.size(),
                built.practitionerFieldDisagreements());
        return Optional.of(progress);
    }
}
