package com.certifyos.directory_accuracy.ingestion.jobs;

import com.certifyos.directory_accuracy.clients.FileIngestionClient;
import com.certifyos.directory_accuracy.clients.FileIngestionRow;
import com.certifyos.directory_accuracy.export.jobs.JobLogContext;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFile;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFileRepository.Located;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFileState;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundLifecycle;
import com.certifyos.directory_accuracy.ingestion.staging.RowStager;
import com.certifyos.directory_accuracy.ingestion.staging.StagingLifecycle;
import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;
import org.jobrunr.jobs.annotations.Job;
import org.jobrunr.jobs.context.JobContext;
import org.jobrunr.jobs.lambdas.JobRequestHandler;

/**
 * Design step 5: read the parsed rows back from file-ingestion and stage them. From {@code PARSED}
 * the entry moves to {@code STAGING}; the NDJSON row stream is opened after the entry's checkpoint
 * (from the start on a first run) and read one row at a time, never buffered whole; every {@code
 * stageChunkRows} rows go to {@link RowStager}, which writes the chunk and moves the checkpoint. At
 * the end of the stream the entry's dispositions must add up to file-ingestion's {@code
 * populatedRows}, or the entry fails as {@code COUNT_MISMATCH}; otherwise it moves to {@code STAGED}
 * and the ingestion is marked consumed, best effort. A 429 propagates for JobRunr's retry; a 404 or
 * 410 on the rows fails the entry as {@code ROWS_EXPIRED}; every other error propagates, and the
 * retry resumes from the checkpoint.
 */
@ApplicationScoped
public class StageRowsJob implements JobRequestHandler<StageRowsJobRequest> {

    /** Job name used in ids and logs. */
    public static final String NAME = "stage-rows";

    static final String STEP_STAGE = "STAGE";
    static final String ROWS_EXPIRED = "ROWS_EXPIRED";
    static final String COUNT_MISMATCH = "COUNT_MISMATCH";

    private static final Set<InboundFileState> EXPECTED = EnumSet.of(InboundFileState.PARSED, InboundFileState.STAGING);
    private static final int NOT_FOUND = 404;
    private static final int GONE = 410;
    private static final int TOO_MANY_STREAMS = 429;
    private static final Logger LOG = Logger.getLogger(StageRowsJob.class);

    private final InboundJobSupport support;
    private final InboundLifecycle lifecycle;
    private final StagingLifecycle stagingLifecycle;
    private final RowStager stager;
    private final FileIngestionClient fileIngestion;
    private final ObjectMapper mapper;
    private final Clock clock;

    public StageRowsJob(
            InboundJobSupport support,
            InboundLifecycle lifecycle,
            StagingLifecycle stagingLifecycle,
            RowStager stager,
            @RestClient FileIngestionClient fileIngestion,
            ObjectMapper mapper,
            Clock clock) {
        this.support = support;
        this.lifecycle = lifecycle;
        this.stagingLifecycle = stagingLifecycle;
        this.stager = stager;
        this.fileIngestion = fileIngestion;
        this.mapper = mapper;
        this.clock = clock;
    }

    /**
     * Runs the job for one upload.
     *
     * @param request the batch, the upload (expected in {@code PARSED} or {@code STAGING}) and the attempt
     */
    @Override
    @Job(name = "stage-rows %0")
    public void run(StageRowsJobRequest request) {
        JobContext job = JobLogContext.currentJob();
        try (IngestionLogContext ignored = IngestionLogContext.open(request.exportBatchId(), request.uploadId(), job)) {
            if (!support.enabled()) {
                // Fail rather than return: a silent no-op would leave a parsed file unstaged until the
                // reconciler finds it. JobRunr retries with backoff until the switch is back on.
                throw new IllegalStateException("service disabled; stage rows " + request + " deferred to a retry");
            }
            Optional<Located> loaded = support.loadExpecting(NAME, request, EXPECTED);
            if (loaded.isEmpty()) {
                return;
            }
            stage(loaded.get(), InboundJobSupport.jobAttempts(job));
        }
    }

    private void stage(Located located, int attempts) {
        Instant started = clock.instant();
        String after = null;
        if (located.entry().state() == InboundFileState.PARSED) {
            if (!stagingLifecycle.staging(located, started)) {
                return;
            }
        } else if (located.entry().checkpoint() != null) {
            after = located.entry().checkpoint().after();
        }
        Optional<InputStream> rows = open(located, after, attempts);
        if (rows.isEmpty() || !stageAll(located, rows.get(), after)) {
            return;
        }
        finish(located, started, attempts);
    }

    /**
     * Opens the row stream after {@code after}.
     *
     * @return the stream, or empty when the rows have expired and the entry was failed
     */
    private Optional<InputStream> open(Located located, String after, int attempts) {
        try {
            return Optional.of(fileIngestion.rows(
                    located.batch().tenantId(), identifier(), located.entry().ingestionId(), after));
        } catch (WebApplicationException refused) {
            int status =
                    refused.getResponse() == null ? 0 : refused.getResponse().getStatus();
            LOG.infof(
                    "%s upload %s: rows after %s answered %d",
                    located.batch().id(), located.entry().uploadId(), after, status);
            if (status == TOO_MANY_STREAMS) {
                support.metrics().stageStream429();
                throw refused;
            }
            if (status == NOT_FOUND || status == GONE) {
                lifecycle.failed(
                        located,
                        InboundFileState.STAGING,
                        STEP_STAGE,
                        ROWS_EXPIRED,
                        "HTTP " + status,
                        attempts,
                        clock.instant());
                return Optional.empty();
            }
            throw refused;
        }
    }

    /**
     * Reads the stream one row at a time and stages every full chunk, then the last partial one.
     *
     * @return true when the stream ended and every chunk was checkpointed; false when the entry left
     *     {@code STAGING} under the job
     */
    private boolean stageAll(Located located, InputStream body, String after) {
        int chunkRows = support.config().ingestion().stageChunkRows();
        List<FileIngestionRow> chunk = new ArrayList<>(chunkRows);
        String cursor = after;
        try (InputStream stream = body;
                MappingIterator<FileIngestionRow> rows =
                        mapper.readerFor(FileIngestionRow.class).readValues(stream)) {
            while (rows.hasNextValue()) {
                chunk.add(rows.nextValue());
                if (chunk.size() == chunkRows) {
                    Optional<RowStager.Progress> progress = stager.stageChunk(located, cursor, chunk, clock.instant());
                    if (progress.isEmpty()) {
                        return false;
                    }
                    cursor = progress.get().after();
                    chunk = new ArrayList<>(chunkRows);
                }
            }
            return chunk.isEmpty()
                    || stager.stageChunk(located, cursor, chunk, clock.instant())
                            .isPresent();
        } catch (JsonProcessingException unreadable) {
            // Jackson's message can quote the row's text; keep only where it broke.
            throw new IllegalStateException(rowStream(located, cursor) + " has an unreadable row: "
                    + unreadable.getClass().getSimpleName() + where(unreadable.getLocation()));
        } catch (IOException broken) {
            throw new UncheckedIOException(rowStream(located, cursor) + " broke", broken);
        }
    }

    private static String rowStream(Located located, String cursor) {
        return "row stream of " + located.batch().id() + " upload "
                + located.entry().uploadId() + " after " + cursor;
    }

    private static String where(JsonLocation location) {
        return location == null ? "" : " at line " + location.getLineNr() + " column " + location.getColumnNr();
    }

    /** Reconciles the stored counts with the parse, then moves the entry to {@code STAGED}. */
    private void finish(Located located, Instant started, int attempts) {
        Optional<Located> reloaded = support.reload(located);
        if (reloaded.isEmpty() || reloaded.get().entry().state() != InboundFileState.STAGING) {
            LOG.infof(
                    "%s upload %s left STAGING before the end of its stream; not staged",
                    located.batch().id(), located.entry().uploadId());
            return;
        }
        Located current = reloaded.get();
        InboundFile entry = current.entry();
        long rows = entry.dispositions().values().stream()
                .mapToLong(Long::longValue)
                .sum();
        Long populated = entry.parse() == null ? null : entry.parse().populatedRows();
        Instant now = clock.instant();
        if (populated == null || rows != populated) {
            lifecycle.failed(
                    current,
                    InboundFileState.STAGING,
                    STEP_STAGE,
                    COUNT_MISMATCH,
                    rows + " vs " + (populated == null ? "unknown" : populated),
                    attempts,
                    now);
            return;
        }
        if (stagingLifecycle.staged(current, Duration.between(started, now).toMillis(), now)) {
            markConsumed(current);
        }
    }

    /** Tells file-ingestion the rows are consumed; a courtesy, so a failure is one warning and nothing else. */
    private void markConsumed(Located located) {
        String ingestionId = located.entry().ingestionId();
        try (Response response = fileIngestion.markConsumed(located.batch().tenantId(), identifier(), ingestionId)) {
            LOG.infof(
                    "%s upload %s: ingestion %s marked consumed (%d)",
                    located.batch().id(), located.entry().uploadId(), ingestionId, response.getStatus());
        } catch (RuntimeException refused) {
            String status = refused instanceof WebApplicationException answered && answered.getResponse() != null
                    ? "HTTP " + answered.getResponse().getStatus()
                    : refused.getClass().getSimpleName();
            LOG.warnf(
                    "%s upload %s: marking ingestion %s consumed failed (%s); the entry stays STAGED",
                    located.batch().id(), located.entry().uploadId(), ingestionId, status);
        }
    }

    private String identifier() {
        return support.config().fileIngestion().consumerIdentifier();
    }
}
