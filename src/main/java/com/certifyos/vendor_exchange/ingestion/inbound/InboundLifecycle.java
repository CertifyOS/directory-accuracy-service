package com.certifyos.vendor_exchange.ingestion.inbound;

import com.certifyos.vendor_exchange.audit.Actors;
import com.certifyos.vendor_exchange.audit.AuditEvent;
import com.certifyos.vendor_exchange.audit.AuditEventType;
import com.certifyos.vendor_exchange.audit.AuditRepository;
import com.certifyos.vendor_exchange.clients.ArchiveStore;
import com.certifyos.vendor_exchange.clients.FileIngestionCounts;
import com.certifyos.vendor_exchange.export.batch.ExportBatch;
import com.certifyos.vendor_exchange.export.jobs.JobLogContext;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileRepository.Located;
import com.certifyos.vendor_exchange.metrics.VendorExchangeMetrics;
import com.certifyos.vendor_exchange.persistence.Ids;
import com.certifyos.vendor_exchange.persistence.Transactions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Updates;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import org.bson.conversions.Bson;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

/**
 * The one place that moves an {@code inboundFiles[]} entry. Each method is one transaction: the
 * guarded write on the entry and its audit event, nothing else (no enqueue, no HTTP). Metrics and
 * logs follow the commit. A guarded write that finds the entry moved returns false and writes
 * nothing, so a stale job or a late event is a no-op its caller can see. Two exceptions: {@link
 * #claimIn} joins the caller's transaction, and {@link #envelope} only builds an event.
 */
@ApplicationScoped
public class InboundLifecycle {

    /** What {@link #received} did with an upload. */
    public enum Outcome {
        /** The batch's first upload: claimed, and the batch acknowledged. */
        RECEIVED,
        /** A later upload for the batch: recorded and never processed. */
        DUPLICATE_LOGGED,
        /** The upload was already on the batch (a redelivered event); nothing written. */
        ALREADY_CLAIMED
    }

    /** {@code copyResult} on {@code INBOUND_FILE_ARCHIVED}. */
    static final String COPY_CREATED = "CREATED";

    static final String COPY_ALREADY_PRESENT = "ALREADY_PRESENT";

    /** The parse status an ingestion failure records. */
    static final String STATUS_INVALID = "INVALID";

    /** The most characters of {@code lastError} kept on the entry and the event. */
    public static final int LAST_ERROR_MAX = 500;

    private static final Logger LOG = Logger.getLogger(InboundLifecycle.class);

    private final InboundFileRepository inbound;
    private final AuditRepository audit;
    private final Transactions transactions;
    private final VendorExchangeMetrics metrics;
    private final InboundClaims claims;

    public InboundLifecycle(
            InboundFileRepository inbound,
            AuditRepository audit,
            Transactions transactions,
            VendorExchangeMetrics metrics,
            InboundClaims claims) {
        this.inbound = inbound;
        this.audit = audit;
        this.transactions = transactions;
        this.metrics = metrics;
        this.claims = claims;
    }

    /**
     * Claims an upload on its batch in its own transaction, with {@link Actors#INGESTION} as the
     * actor ({@link #claimIn}), then records the arrival metric and logs the outcome.
     *
     * @param batch the batch the object name points at, {@code DELIVERED} or {@code SUPERSEDED}
     * @param entry the upload, as {@link InboundFile#received} builds it
     * @param now the time of the write
     * @return what happened
     * @throws IllegalArgumentException when the batch is in another state
     * @throws IllegalStateException when the batch left the state read before the acknowledgement;
     *     the transaction rolled back and the claim with it, so a redelivery starts over
     */
    public Outcome received(ExportBatch batch, InboundFile entry, Instant now) {
        Outcome outcome = transactions.run(session -> claimIn(session, batch, entry, Actors.INGESTION, now));
        switch (outcome) {
            case RECEIVED -> metrics.arrivalReceived(batch.tenantId(), batch.vendor());
            case DUPLICATE_LOGGED -> metrics.arrivalDuplicateLogged(batch.tenantId(), batch.vendor());
            case ALREADY_CLAIMED -> {
                // a redelivered event: nothing was written, nothing to count
            }
        }
        LOG.infof("%s upload %s: %s", batch.id(), entry.uploadId(), outcome);
        return outcome;
    }

    /**
     * Claims an upload on its batch inside the caller's transaction. The first upload of the batch
     * is {@code RECEIVED}: the batch gets {@code acknowledgedAt} (the storage event time, or {@code
     * now} when the event carried none) and {@code inboundBatchId} (the upload id), written in the
     * state the caller read, and {@code INBOUND_FILE_RECEIVED} then {@code EXPORT_BATCH_ACKNOWLEDGED}
     * are written. A later upload is {@code DUPLICATE_LOGGED} with one event naming the first upload.
     * An upload already on the batch writes nothing. No metric and no log line: the caller records
     * them after its commit.
     *
     * @param session the caller's transaction
     * @param batch the batch the object name points at, {@code DELIVERED} or {@code SUPERSEDED}
     * @param entry the upload, as {@link InboundFile#received} builds it
     * @param actor who claims it, {@link Actors#INGESTION} or an operator
     * @param now the time of the write
     * @return what happened
     * @throws IllegalArgumentException when the batch is in another state
     * @throws IllegalStateException when the batch left the state read before the acknowledgement;
     *     the caller's transaction must roll back
     */
    public Outcome claimIn(ClientSession session, ExportBatch batch, InboundFile entry, String actor, Instant now) {
        return claims.claimIn(session, batch, entry, actor, now);
    }

    /**
     * Records the job now responsible for a {@code RECEIVED} entry (the arrival handler, after its
     * enqueue). No event; the state does not change.
     *
     * @param located the entry as the caller read it
     * @param jobId the enqueued job
     * @param now the time of the write
     * @return true when the entry was still {@code RECEIVED}
     */
    public boolean jobRecorded(Located located, UUID jobId, Instant now) {
        boolean recorded = transactions.run(session -> inbound.updateInState(
                session,
                located.batch().id(),
                located.entry().uploadId(),
                InboundFileState.RECEIVED,
                now,
                Updates.set(InboundFileRepository.entry("jobId"), jobId.toString())));
        LOG.infof(
                "%s upload %s: job %s %s",
                located.batch().id(),
                located.entry().uploadId(),
                jobId,
                recorded ? "recorded" : "not recorded, the entry left RECEIVED");
        return recorded;
    }

    /** Whole days from delivery to the vendor's upload, never negative; 0 when delivery has no time. */
    static long daysToResponse(Instant deliveredAt, Instant acknowledgedAt) {
        if (deliveredAt == null) {
            return 0;
        }
        return Math.max(0, ChronoUnit.DAYS.between(deliveredAt, acknowledgedAt));
    }

    /**
     * The archive job starts: {@code RECEIVED} to {@code ARCHIVING}, with the deterministic {@code
     * externalRef} so a completion event finds the entry from the first job write. No event; the
     * copy has not happened.
     *
     * @param located the entry as the job read it
     * @param now the time of the write
     * @return true when the entry moved; false when another writer moved it first
     */
    public boolean archiving(Located located, Instant now) {
        boolean moved = transactions.run(session -> inbound.transition(
                session,
                located.batch().id(),
                located.entry().uploadId(),
                InboundFileState.RECEIVED,
                InboundFileState.ARCHIVING,
                now,
                Updates.set(
                        InboundFileRepository.entry("externalRef"),
                        Ids.externalRef(located.batch().id(), located.entry().generation()))));
        logMove(located, InboundFileState.ARCHIVING, moved);
        return moved;
    }

    /**
     * The archive copy exists: {@code archivePath} and {@code archiveBytes} on the entry, still in
     * {@code ARCHIVING}, with {@code INBOUND_FILE_ARCHIVED}.
     *
     * @param located the entry as the job read it
     * @param copy what the archive store reported
     * @param now the time of the write
     * @return true when the entry was still in {@code ARCHIVING}
     */
    public boolean archived(Located located, ArchiveStore.Copied copy, Instant now) {
        Bson updates = Updates.combine(
                Updates.set(InboundFileRepository.entry("archivePath"), copy.path()),
                Updates.set(InboundFileRepository.entry("archiveBytes"), copy.bytes()));
        AuditEvent event = event(AuditEventType.INBOUND_FILE_ARCHIVED, located, now)
                .detail("archivePath", copy.path())
                .detail("bytes", copy.bytes())
                .detail("copyResult", copy.alreadyExisted() ? COPY_ALREADY_PRESENT : COPY_CREATED)
                .build();
        boolean written = transactions.run(session -> {
            boolean still = inbound.updateInState(
                    session,
                    located.batch().id(),
                    located.entry().uploadId(),
                    InboundFileState.ARCHIVING,
                    now,
                    updates);
            if (still) {
                audit.write(session, event);
            }
            return still;
        });
        if (written) {
            LOG.infof(
                    "%s upload %s archived (%d bytes, already present %s)",
                    located.batch().id(), located.entry().uploadId(), copy.bytes(), copy.alreadyExisted());
        } else {
            LOG.infof(
                    "%s upload %s left ARCHIVING; archive not recorded",
                    located.batch().id(), located.entry().uploadId());
        }
        return written;
    }

    /**
     * File ingestion accepted the registration: {@code ARCHIVING} to {@code INGESTION_REQUESTED} with
     * the external ref, the ingestion id, the request time and the deadline check as the entry's job,
     * plus {@code INBOUND_FILE_INGESTION_REQUESTED}.
     *
     * @param located the entry as the job read it
     * @param ingestionId file ingestion's id for the request
     * @param externalRef {@code <exportBatchId>|<generation>}
     * @param mappingId the mapping the file was registered with
     * @param registerResponse a short description of the register answer, for the event
     * @param deadlineJobId the deadline check that now owns the entry
     * @param deadlineAt when that check runs
     * @param now the time of the write
     * @return true when the entry moved
     */
    public boolean ingestionRequested(
            Located located,
            String ingestionId,
            String externalRef,
            String mappingId,
            String registerResponse,
            UUID deadlineJobId,
            Instant deadlineAt,
            Instant now) {
        Bson updates = Updates.combine(
                Updates.set(InboundFileRepository.entry("externalRef"), externalRef),
                Updates.set(InboundFileRepository.entry("ingestionId"), ingestionId),
                Updates.set(InboundFileRepository.entry("requestedAt"), now),
                Updates.set(InboundFileRepository.entry("jobId"), deadlineJobId.toString()));
        AuditEvent event = event(AuditEventType.INBOUND_FILE_INGESTION_REQUESTED, located, now)
                .detail("ingestionId", ingestionId)
                .detail("externalRef", externalRef)
                .detail("mappingId", mappingId)
                .detail("registerResponse", registerResponse)
                .detail("deadlineAt", deadlineAt)
                .detail("deadlineJobId", deadlineJobId.toString())
                .build();
        boolean moved =
                move(located, InboundFileState.ARCHIVING, InboundFileState.INGESTION_REQUESTED, updates, event, now);
        logMove(located, InboundFileState.INGESTION_REQUESTED, moved);
        return moved;
    }

    /**
     * File ingestion finished parsing: {@code INGESTION_REQUESTED} to {@code PARSED} with the parse
     * outcome and time, and {@code INBOUND_FILE_PARSED}; then the completion metrics.
     *
     * @param located the entry as the caller read it
     * @param counts the row counts file ingestion reported, may be null
     * @param status {@code COMPLETED} or {@code PARTIAL}
     * @param completionSource {@code EVENT} or {@code DEADLINE}
     * @param now the time of the write, which is {@code parsedAt}
     * @return true when the entry moved; false, with nothing written, when it had moved already
     */
    public boolean parsed(
            Located located, FileIngestionCounts counts, String status, String completionSource, Instant now) {
        FileIngestionCounts rows = counts == null ? new FileIngestionCounts(null, null, null) : counts;
        InboundFile.Parse parse = new InboundFile.Parse(
                status, rows.populatedRows(), rows.validRows(), rows.invalidRows(), completionSource, null);
        Bson updates = Updates.combine(
                Updates.set(InboundFileRepository.entry("parse"), parse.toDocument()),
                Updates.set(InboundFileRepository.entry("parsedAt"), now));
        long waitSeconds = waitSeconds(located.entry().requestedAt(), now);
        AuditEvent event = event(AuditEventType.INBOUND_FILE_PARSED, located, now)
                .detail("status", status)
                .detail("populatedRows", rows.populatedRows())
                .detail("validRows", rows.validRows())
                .detail("invalidRows", rows.invalidRows())
                .detail("completionSource", completionSource)
                .detail("waitSeconds", waitSeconds)
                .build();
        boolean moved =
                move(located, InboundFileState.INGESTION_REQUESTED, InboundFileState.PARSED, updates, event, now);
        if (moved) {
            metrics.completionSource(completionSource);
            metrics.completionStatus(status);
            LOG.infof(
                    "%s upload %s parsed %s (%s) after %d s: %d valid, %d invalid",
                    located.batch().id(),
                    located.entry().uploadId(),
                    status,
                    completionSource,
                    waitSeconds,
                    rows.validRows(),
                    rows.invalidRows());
        } else {
            logMove(located, InboundFileState.PARSED, false);
        }
        return moved;
    }

    /** Seconds from the request to now, never negative; 0 when the request time is unknown. */
    static long waitSeconds(Instant requestedAt, Instant now) {
        if (requestedAt == null) {
            return 0;
        }
        return Math.max(0, Duration.between(requestedAt, now).getSeconds());
    }

    /**
     * File ingestion refused the file: {@code INGESTION_REQUESTED} to {@code INGESTION_FAILED} with
     * the parse status {@code INVALID} and the reason, and {@code INBOUND_FILE_INGESTION_FAILED}.
     *
     * @param located the entry as the caller read it
     * @param reason a stable reason code
     * @param completionSource {@code EVENT} or {@code DEADLINE}
     * @param now the time of the write
     * @return true when the entry moved
     */
    public boolean ingestionFailed(Located located, String reason, String completionSource, Instant now) {
        InboundFile.Parse parse = new InboundFile.Parse(STATUS_INVALID, null, null, null, completionSource, reason);
        Bson updates = Updates.set(InboundFileRepository.entry("parse"), parse.toDocument());
        AuditEvent event = event(AuditEventType.INBOUND_FILE_INGESTION_FAILED, located, now)
                .detail("reason", reason)
                .detail("completionSource", completionSource)
                .build();
        boolean moved = move(
                located, InboundFileState.INGESTION_REQUESTED, InboundFileState.INGESTION_FAILED, updates, event, now);
        if (moved) {
            metrics.completionStatus(STATUS_INVALID);
        }
        logMove(located, InboundFileState.INGESTION_FAILED, moved);
        return moved;
    }

    /**
     * A step failed the entry: {@code from} to {@code FAILED} with the step, the reason and the last
     * error (cut to {@value #LAST_ERROR_MAX} characters), and {@code INBOUND_FILE_FAILED}.
     *
     * @param located the entry as the caller read it
     * @param from the state the caller read
     * @param failedStep the step, for example {@code ARCHIVE} or {@code REGISTER}
     * @param failureReason a stable reason code
     * @param lastError what was observed, without row content
     * @param jobAttempts how many times the job ran
     * @param now the time of the write
     * @return true when the entry moved
     * @throws IllegalArgumentException when {@code from} has no edge to {@code FAILED}
     */
    public boolean failed(
            Located located,
            InboundFileState from,
            String failedStep,
            String failureReason,
            String lastError,
            int jobAttempts,
            Instant now) {
        String error = truncate(lastError);
        Bson updates = Updates.combine(
                Updates.set(InboundFileRepository.entry("failedStep"), failedStep),
                Updates.set(InboundFileRepository.entry("failureReason"), failureReason),
                Updates.set(InboundFileRepository.entry("lastError"), error));
        AuditEvent event = event(AuditEventType.INBOUND_FILE_FAILED, located, now)
                .detail("failedStep", failedStep)
                .detail("failureReason", failureReason)
                .detail("lastError", error)
                .detail("jobAttempts", jobAttempts)
                .build();
        boolean moved = move(located, from, InboundFileState.FAILED, updates, event, now);
        if (moved) {
            LOG.warnf(
                    "%s upload %s FAILED at %s (%s) after %d job attempts",
                    located.batch().id(), located.entry().uploadId(), failedStep, failureReason, jobAttempts);
        } else {
            logMove(located, InboundFileState.FAILED, false);
        }
        return moved;
    }

    /** {@code lastError} cut to {@value #LAST_ERROR_MAX} characters; null stays null. */
    public static String truncate(String lastError) {
        if (lastError == null || lastError.length() <= LAST_ERROR_MAX) {
            return lastError;
        }
        return lastError.substring(0, LAST_ERROR_MAX);
    }

    private boolean move(
            Located located, InboundFileState from, InboundFileState to, Bson updates, AuditEvent event, Instant now) {
        return transactions.run(session -> {
            boolean moved = inbound.transition(
                    session, located.batch().id(), located.entry().uploadId(), from, to, now, updates);
            if (moved) {
                audit.write(session, event);
            }
            return moved;
        });
    }

    private static void logMove(Located located, InboundFileState to, boolean moved) {
        if (moved) {
            LOG.infof(
                    "%s upload %s -> %s", located.batch().id(), located.entry().uploadId(), to);
        } else {
            LOG.infof(
                    "%s upload %s already moved; %s not written",
                    located.batch().id(), located.entry().uploadId(), to);
        }
    }

    private static AuditEvent.Builder event(AuditEventType type, Located located, Instant now) {
        return envelope(type, located.batch(), located.entry(), Actors.INGESTION)
                .occurredAt(now);
    }

    /**
     * The envelope every inbound file event shares: tenant, vendor, batch, the entry's attempt, the
     * upload, the actor, and the job that writes it when one runs on this thread (its id is on the
     * MDC under the key {@code IngestionLogContext} and {@code JobLogContext} share; an HTTP path has
     * none). The caller adds {@code occurredAt} and the details; an export event resets the attempt
     * to the batch's.
     *
     * @param type the event type
     * @param batch the batch the entry is on
     * @param entry the entry
     * @param actor who wrote it
     * @return the builder
     */
    public static AuditEvent.Builder envelope(AuditEventType type, ExportBatch batch, InboundFile entry, String actor) {
        AuditEvent.Builder builder = AuditEvent.forBatch(
                        type, batch.tenantId(), batch.vendor(), batch.id(), entry.attempt())
                .upload(entry.uploadId())
                .actor(actor);
        Object jobId = MDC.get(JobLogContext.JOB_ID);
        if (jobId != null) {
            builder.jobId(jobId.toString());
        }
        return builder;
    }
}
