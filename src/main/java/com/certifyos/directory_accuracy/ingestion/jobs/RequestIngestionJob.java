package com.certifyos.directory_accuracy.ingestion.jobs;

import com.certifyos.directory_accuracy.clients.ArchiveSourceMissingException;
import com.certifyos.directory_accuracy.clients.ArchiveStore;
import com.certifyos.directory_accuracy.clients.FileIngestionClient;
import com.certifyos.directory_accuracy.clients.FileIngestionRegisterRequest;
import com.certifyos.directory_accuracy.clients.FileIngestionRegisterResponse;
import com.certifyos.directory_accuracy.config.VendorExchangeConfig;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.jobs.JobLogContext;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFile;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFileRepository.Located;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFileState;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundLifecycle;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundNames;
import com.certifyos.directory_accuracy.persistence.Ids;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;
import org.jobrunr.jobs.annotations.Job;
import org.jobrunr.jobs.context.JobContext;
import org.jobrunr.jobs.lambdas.JobRequestHandler;

/**
 * Design step 3: archive the exact generation of the vendor's upload, then register the archive
 * location with file-ingestion, schedule the deadline check, then move the entry to {@code
 * INGESTION_REQUESTED} (schedule first, so no committed entry is ever left without its check). Works
 * from {@code RECEIVED} or, on a
 * retry, {@code ARCHIVING}; an entry that already carries {@code archivePath} skips the copy. The
 * archive store, file-ingestion and the enqueuer are called outside every transaction; the lifecycle
 * owns the writes. A register 422 or 403 and a missing source fail the entry; any other 4xx
 * propagates as {@link RegisterClientErrorException}, and every 5xx and connection error as itself,
 * for the JobRunr retry. When the retries run out, the final-failure filter fails the entry.
 */
@ApplicationScoped
public class RequestIngestionJob implements JobRequestHandler<RequestIngestionJobRequest> {

    /** Job name used in ids and logs. */
    public static final String NAME = "request-ingestion";

    static final String STEP_ARCHIVE = "ARCHIVE";
    static final String STEP_REGISTER = "REGISTER";
    static final String MAPPING_UNKNOWN = "MAPPING_UNKNOWN";
    static final String FILE_INGESTION_REFUSED = "FILE_INGESTION_REFUSED";
    static final String SOURCE_MISSING = "SOURCE_MISSING";
    static final String NOT_CONFIGURED = "not configured";

    private static final Set<InboundFileState> EXPECTED =
            EnumSet.of(InboundFileState.RECEIVED, InboundFileState.ARCHIVING);
    private static final int CREATED = 201;
    private static final int OK = 200;
    private static final int CLIENT_ERROR = 400;
    private static final int FORBIDDEN = 403;
    private static final int UNPROCESSABLE = 422;
    private static final int SERVER_ERROR = 500;
    private static final Logger LOG = Logger.getLogger(RequestIngestionJob.class);

    private final InboundJobSupport support;
    private final InboundLifecycle lifecycle;
    private final ArchiveStore archive;
    private final FileIngestionClient fileIngestion;
    private final IngestionJobEnqueuer enqueuer;
    private final Clock clock;

    public RequestIngestionJob(
            InboundJobSupport support,
            InboundLifecycle lifecycle,
            ArchiveStore archive,
            @RestClient FileIngestionClient fileIngestion,
            IngestionJobEnqueuer enqueuer,
            Clock clock) {
        this.support = support;
        this.lifecycle = lifecycle;
        this.archive = archive;
        this.fileIngestion = fileIngestion;
        this.enqueuer = enqueuer;
        this.clock = clock;
    }

    /** What file-ingestion answered: its id for the request and {@code 201} or {@code 200}. */
    private record Registered(String ingestionId, String answer) {}

    /**
     * Runs the job for one upload.
     *
     * @param request the batch, the upload (expected in {@code RECEIVED} or {@code ARCHIVING}) and
     *     the attempt
     */
    @Override
    @Job(name = "request-ingestion %0")
    public void run(RequestIngestionJobRequest request) {
        JobContext job = JobLogContext.currentJob();
        try (IngestionLogContext ignored = IngestionLogContext.open(request.exportBatchId(), request.uploadId(), job)) {
            if (!support.enabled()) {
                // Fail rather than return: a silent no-op would leave the entry where only the
                // reconciler finds it. JobRunr retries with backoff until the switch is back on.
                throw new IllegalStateException(
                        "service disabled; request ingestion " + request + " deferred to a retry");
            }
            Optional<Located> loaded = support.loadExpecting(NAME, request, EXPECTED);
            if (loaded.isEmpty()) {
                return;
            }
            requestIngestion(loaded.get(), InboundJobSupport.jobAttempts(job));
        }
    }

    private void requestIngestion(Located located, int attempts) {
        ExportBatch batch = located.batch();
        InboundFile entry = located.entry();
        if (entry.state() == InboundFileState.RECEIVED && !lifecycle.archiving(located, clock.instant())) {
            return;
        }
        VendorExchangeConfig cfg = support.config();
        String archiveBucket = cfg.ingestion().archiveBucket();
        String fileName = fileName(entry.objectName());
        String archiveObject = InboundNames.archiveObject(batch.tenantId(), batch.id(), entry.generation(), fileName);
        if (entry.archivePath() == null) {
            if (!archive(located, archiveBucket, archiveObject, attempts)) {
                return;
            }
        } else {
            LOG.infof("%s upload %s: archive already recorded, copy skipped", batch.id(), entry.uploadId());
        }
        Optional<String> mappingId = cfg.fileIngestion().mappingId();
        if (mappingId.isEmpty()) {
            lifecycle.failed(
                    located,
                    InboundFileState.ARCHIVING,
                    STEP_REGISTER,
                    MAPPING_UNKNOWN,
                    NOT_CONFIGURED,
                    attempts,
                    clock.instant());
            return;
        }
        FileIngestionRegisterRequest body = new FileIngestionRegisterRequest(
                mappingId.get(),
                fileName,
                "gs://" + archiveBucket + "/" + archiveObject,
                Ids.externalRef(batch.id(), entry.generation()),
                Map.of("exportBatchId", batch.id(), "uploadId", entry.uploadId()));
        Optional<Registered> registered = register(located, body, attempts);
        if (registered.isEmpty()) {
            return;
        }
        Instant now = clock.instant();
        Instant deadlineAt = now.plus(Duration.ofHours(cfg.ingestion().ingestionDeadlineHours()));
        // Schedule before the commit. A worker death between the two leaves a deadline check that,
        // should it fire first, finds the entry still ARCHIVING and throws for a JobRunr retry,
        // while JobRunr reruns this job; the rerun schedules the same id (JobRunr keeps the existing
        // job) and then commits. The other order would leave an INGESTION_REQUESTED entry, which
        // only its deadline check moves, with no check scheduled.
        UUID deadlineJobId = enqueuer.deadlineCheck(batch.id(), entry.uploadId(), entry.attempt(), deadlineAt);
        boolean moved = lifecycle.ingestionRequested(
                located,
                registered.get().ingestionId(),
                body.externalRef(),
                mappingId.get(),
                registered.get().answer(),
                deadlineJobId,
                deadlineAt,
                now);
        if (moved) {
            LOG.infof(
                    "%s upload %s: ingestion %s requested, deadline check %s at %s",
                    batch.id(), entry.uploadId(), registered.get().ingestionId(), deadlineJobId, deadlineAt);
        }
    }

    /**
     * Copies the exact generation into the archive and records it.
     *
     * @return true when the archive is recorded and the job goes on to register
     */
    private boolean archive(Located located, String archiveBucket, String archiveObject, int attempts) {
        ExportBatch batch = located.batch();
        InboundFile entry = located.entry();
        Instant started = clock.instant();
        ArchiveStore.Copied copy;
        try {
            copy = archive.copy(
                    entry.bucket(),
                    entry.objectName(),
                    Long.parseLong(entry.generation()),
                    archiveBucket,
                    archiveObject);
        } catch (ArchiveSourceMissingException missing) {
            lifecycle.failed(
                    located,
                    InboundFileState.ARCHIVING,
                    STEP_ARCHIVE,
                    SOURCE_MISSING,
                    missing.getMessage(),
                    attempts,
                    clock.instant());
            return false;
        }
        Instant copied = clock.instant();
        support.metrics().archiveDuration(batch.tenantId(), Duration.between(started, copied));
        return lifecycle.archived(located, copy, copied);
    }

    /**
     * Registers the archived file. 201 and 200 both proceed; 422 and 403 fail the entry; anything
     * else propagates for the JobRunr retry.
     *
     * @return the answer, or empty when the entry was failed
     */
    private Optional<Registered> register(Located located, FileIngestionRegisterRequest body, int attempts) {
        ExportBatch batch = located.batch();
        String identifier = support.config().fileIngestion().consumerIdentifier();
        try (Response response = fileIngestion.register(batch.tenantId(), identifier, body)) {
            int status = response.getStatus();
            if (status != CREATED && status != OK) {
                throw new IllegalStateException("file-ingestion answered register with HTTP " + status);
            }
            support.metrics().registerResult(String.valueOf(status));
            FileIngestionRegisterResponse registered = response.readEntity(FileIngestionRegisterResponse.class);
            if (registered == null || registered.ingestionId() == null) {
                throw new IllegalStateException("file-ingestion answered register " + status + " without an id");
            }
            LOG.infof(
                    "%s upload %s: register answered %d",
                    batch.id(), located.entry().uploadId(), status);
            return Optional.of(new Registered(registered.ingestionId(), String.valueOf(status)));
        } catch (WebApplicationException refused) {
            int status =
                    refused.getResponse() == null ? 0 : refused.getResponse().getStatus();
            support.metrics().registerResult(status >= SERVER_ERROR ? "5xx" : "4xx");
            LOG.infof(
                    "%s upload %s: register answered %d",
                    batch.id(), located.entry().uploadId(), status);
            String reason = failureReason(refused, status);
            lifecycle.failed(
                    located,
                    InboundFileState.ARCHIVING,
                    STEP_REGISTER,
                    reason,
                    errorText(refused, status),
                    attempts,
                    clock.instant());
            return Optional.empty();
        }
    }

    /**
     * The reason a refused register fails the entry with now: 422 {@code MAPPING_UNKNOWN}, 403
     * {@code FILE_INGESTION_REFUSED}. Any other answer is thrown for the JobRunr retry: another 4xx as
     * {@link RegisterClientErrorException}, so the final-failure filter records {@code
     * SOURCE_UNREADABLE} once the retries run out, and anything else as the refusal itself.
     */
    private static String failureReason(WebApplicationException refused, int status) {
        return switch (status) {
            case UNPROCESSABLE -> MAPPING_UNKNOWN;
            case FORBIDDEN -> FILE_INGESTION_REFUSED;
            default -> {
                if (status >= CLIENT_ERROR && status < SERVER_ERROR) {
                    throw new RegisterClientErrorException(status, refused);
                }
                throw refused;
            }
        };
    }

    /** {@code HTTP <status>: <problem body>} for {@code lastError}; never logged. */
    private static String errorText(WebApplicationException refused, int status) {
        String prefix = "HTTP " + status;
        try {
            String body = refused.getResponse().readEntity(String.class);
            return body == null || body.isBlank() ? prefix : prefix + ": " + body;
        } catch (RuntimeException unreadable) {
            return prefix;
        }
    }

    private static String fileName(String objectName) {
        return objectName.substring(objectName.lastIndexOf('/') + 1);
    }
}
