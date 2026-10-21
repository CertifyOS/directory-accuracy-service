package com.certifyos.directory_accuracy.ingestion.events;

import com.certifyos.directory_accuracy.ingestion.inbound.InboundFile;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFileRepository;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFileRepository.Located;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFileState;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundLifecycle;
import com.certifyos.directory_accuracy.ingestion.jobs.IngestionJobEnqueuer;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.jboss.logging.Logger;

/**
 * The step after a claim commits: hand a {@code RECEIVED} entry that has no job yet to the
 * request-ingestion job, then record the job id on it. Never inside a transaction. The one
 * implementation for both claim paths, the arrival handler and the operator attach. Two callers
 * racing here enqueue the same deterministic id, which JobRunr treats as one job.
 */
@ApplicationScoped
public class IngestionRequests {

    private static final Logger LOG = Logger.getLogger(IngestionRequests.class);

    private final InboundFileRepository inbound;
    private final InboundLifecycle lifecycle;
    private final IngestionJobEnqueuer enqueuer;

    public IngestionRequests(InboundFileRepository inbound, InboundLifecycle lifecycle, IngestionJobEnqueuer enqueuer) {
        this.inbound = inbound;
        this.lifecycle = lifecycle;
        this.enqueuer = enqueuer;
    }

    /**
     * What {@link #requestIfUnowned} found and did.
     *
     * @param located the entry as read before any enqueue
     * @param jobId the job this call enqueued, null when the entry was not {@code RECEIVED} or already had a job
     */
    public record Requested(Located located, UUID jobId) {}

    /**
     * The entry an upload is claimed as, on whichever batch holds it, so a claim path can refuse an
     * upload that already sits on another batch before it claims.
     *
     * @param uploadId the upload
     * @return the entry and its batch, or empty when no batch holds the upload
     */
    public Optional<Located> claimed(String uploadId) {
        return inbound.findByUploadId(uploadId);
    }

    /**
     * Enqueues the request-ingestion job for a claimed upload when its entry is {@code RECEIVED} with
     * no job, then records the job id.
     *
     * @param uploadId the claimed upload
     * @param now the time of the job id write
     * @return the entry and the job enqueued, if any
     * @throws IllegalStateException when the upload is on no batch
     */
    public Requested requestIfUnowned(String uploadId, Instant now) {
        Located located = claimed(uploadId)
                .orElseThrow(() -> new IllegalStateException("upload " + uploadId + " is claimed but not found"));
        return new Requested(located, requestIfUnowned(located, now));
    }

    /**
     * Enqueues the request-ingestion job for an entry the caller has read, when it is {@code
     * RECEIVED} with no job, then records the job id.
     *
     * @param located the entry as the caller read it
     * @param now the time of the job id write
     * @return the job enqueued, or null when the entry needed none
     */
    public UUID requestIfUnowned(Located located, Instant now) {
        InboundFile entry = located.entry();
        if (entry.state() != InboundFileState.RECEIVED || entry.jobId() != null) {
            LOG.infof(
                    "%s upload %s is %s with job %s; nothing to enqueue",
                    located.batch().id(), entry.uploadId(), entry.state(), entry.jobId());
            return null;
        }
        UUID jobId = enqueuer.requestIngestion(located.batch().id(), entry.uploadId(), entry.attempt());
        lifecycle.jobRecorded(located, jobId, now);
        return jobId;
    }
}
