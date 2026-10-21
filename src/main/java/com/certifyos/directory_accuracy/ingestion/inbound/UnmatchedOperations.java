package com.certifyos.directory_accuracy.ingestion.inbound;

import com.certifyos.directory_accuracy.audit.Actors;
import com.certifyos.directory_accuracy.audit.AuditEvent;
import com.certifyos.directory_accuracy.audit.AuditEventType;
import com.certifyos.directory_accuracy.audit.AuditRepository;
import com.certifyos.directory_accuracy.export.batch.BatchOperations;
import com.certifyos.directory_accuracy.export.batch.BatchState;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.http.ProblemException;
import com.certifyos.directory_accuracy.ingestion.events.ArrivalEvent;
import com.certifyos.directory_accuracy.ingestion.events.IngestionRequests;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFileRepository.Located;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundLifecycle.Outcome;
import com.certifyos.directory_accuracy.metrics.VendorExchangeMetrics;
import com.certifyos.directory_accuracy.persistence.Ids;
import com.certifyos.directory_accuracy.persistence.Transactions;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import java.util.UUID;
import org.jboss.logging.Logger;

/**
 * Arrivals the service does not attach to a batch. {@link #unmatched} keeps an upload with its
 * reason in one transaction (the row and {@code INBOUND_FILE_UNMATCHED}); {@link #ignored} counts an
 * event that was not an arrival at all. An operator closes an open row once: {@link #attach} claims
 * it on a delivered batch of the operator's tenant, {@link #discard} drops it. Metrics, logs and the
 * enqueue follow the commit.
 */
@ApplicationScoped
public class UnmatchedOperations {

    /** The tag for an unmatched upload whose name has no folder tenant, or an event with no type. */
    public static final String UNKNOWN = "unknown";

    private static final Logger LOG = Logger.getLogger(UnmatchedOperations.class);
    private static final char SEPARATOR = '|';

    /** The storage event type an attach replays: the object arrived. */
    private static final String FINALIZE = "OBJECT_FINALIZE";

    private final UnmatchedUploadRepository unmatched;
    private final AuditRepository audit;
    private final Transactions transactions;
    private final VendorExchangeMetrics metrics;
    private final BatchOperations batchOperations;
    private final InboundLifecycle lifecycle;
    private final IngestionRequests requests;

    public UnmatchedOperations(
            UnmatchedUploadRepository unmatched,
            AuditRepository audit,
            Transactions transactions,
            VendorExchangeMetrics metrics,
            BatchOperations batchOperations,
            InboundLifecycle lifecycle,
            IngestionRequests requests) {
        this.unmatched = unmatched;
        this.audit = audit;
        this.transactions = transactions;
        this.metrics = metrics;
        this.batchOperations = batchOperations;
        this.lifecycle = lifecycle;
        this.requests = requests;
    }

    /**
     * What {@link #attach} left.
     *
     * @param uploadId the upload
     * @param state the entry's state after the claim
     * @param jobId the request-ingestion job this call enqueued, null when it enqueued none
     */
    public record Attached(String uploadId, InboundFileState state, UUID jobId) {}

    /**
     * Records an upload that matched no batch: the row and {@code INBOUND_FILE_UNMATCHED} in one
     * transaction, then the metric. A service event with no tenant, as {@code EXPORT_EVENT_REJECTED}:
     * the folder an upload was dropped in is what the vendor claims, not a tenant the service
     * verified, so it is a detail rather than the event's tenant. A redelivered event finds the row
     * and writes nothing.
     *
     * @param upload the upload, open, with the reason
     * @param now the time of the write
     * @return true when this call recorded it
     */
    public boolean unmatched(UnmatchedUpload upload, Instant now) {
        AuditEvent event = AuditEvent.of(AuditEventType.INBOUND_FILE_UNMATCHED, null, null)
                .actor(Actors.INGESTION)
                .upload(upload.id())
                .occurredAt(now)
                .detail("reason", upload.reason())
                .detail("objectName", objectName(upload.id()))
                .detail("generation", generation(upload.id()))
                .detail("folderTenant", upload.folderTenant())
                .detail("parsedLegs", legs(upload.parsed()))
                .build();
        boolean inserted = transactions.run(session -> {
            boolean added = unmatched.insertIfAbsent(session, upload);
            if (added) {
                audit.write(session, event);
            }
            return added;
        });
        if (inserted) {
            metrics.arrivalUnmatched(tag(upload.folderTenant()), upload.reason());
            LOG.infof("upload %s unmatched: %s", upload.id(), upload.reason());
        } else {
            LOG.infof("upload %s already recorded as unmatched", upload.id());
        }
        return inserted;
    }

    /**
     * Attaches an open unmatched upload to a delivered batch of the operator's tenant. One
     * transaction holds the claim ({@link InboundLifecycle#claimIn} with the operator as actor), the
     * row resolved {@code ATTACHED} and {@code INBOUND_UPLOAD_ATTACHED}; the arrival metric and the
     * request-ingestion enqueue follow the commit. The claim is replayed from the row: bucket, object
     * and generation from its upload id, its {@code receivedAt} as the event time. An upload that
     * already sits on any batch is refused before the transaction, so the operator discards the row
     * instead; one that a racing delivery claims on the target batch meanwhile ({@code
     * ALREADY_CLAIMED}) still resolves the row; one that finds the batch claimed by another upload is
     * {@code DUPLICATE_LOGGED} and is never processed.
     *
     * @param tenantId the caller's tenant
     * @param uploadId the unmatched upload
     * @param exportBatchId the batch to attach it to
     * @param reason why, required
     * @param actor the operator
     * @param now the time of the write
     * @return the upload, the entry's state and the job this call enqueued
     * @throws ProblemException 404 {@code UNMATCHED_NOT_FOUND} for a missing row or one in another
     *     tenant's folder, 409 {@code TENANT_MISMATCH} for a row whose file names another tenant, 404
     *     {@code BATCH_NOT_FOUND}, 409 {@code ALREADY_RESOLVED}, 409 {@code
     *     ATTACH_NOT_ALLOWED} for a batch that is not delivered or moved under the request, 409 {@code
     *     ALREADY_CLAIMED} for an upload already on a batch, 400 {@code REASON_REQUIRED}, or 400
     *     {@code INVALID_REQUEST} without a batch id
     */
    public Attached attach(
            String tenantId, String uploadId, String exportBatchId, String reason, String actor, Instant now) {
        UnmatchedUpload row = requireSameTenantFile(tenantId, requireOpen(tenantId, uploadId));
        if (exportBatchId == null || exportBatchId.isBlank()) {
            throw ProblemException.badRequest("INVALID_REQUEST", "exportBatchId is required");
        }
        ExportBatch batch = batchOperations.require(tenantId, exportBatchId);
        if ((batch.state() != BatchState.DELIVERED && batch.state() != BatchState.SUPERSEDED) || batch.file() == null) {
            throw ProblemException.conflict(
                    "ATTACH_NOT_ALLOWED",
                    "batch " + batch.id() + " is " + batch.state() + "; only a delivered batch takes an upload");
        }
        String why = requireReason(reason);
        // The friendly early answer, naming the batch; the unique upload index refuses the claim in
        // the transaction too, for a delivery that lands between this read and the commit.
        requests.claimed(uploadId).ifPresent(located -> {
            throw ProblemException.conflict(
                    InboundClaims.ALREADY_CLAIMED,
                    "upload " + uploadId + " is already on batch "
                            + located.batch().id());
        });
        InboundFile entry = InboundFile.received(arrivalOf(row), nameOf(row), now);
        UnmatchedUpload.Resolution resolution =
                new UnmatchedUpload.Resolution(UnmatchedUpload.ATTACHED, batch.id(), why, actor, now);
        AuditEvent attached = InboundLifecycle.envelope(AuditEventType.INBOUND_UPLOAD_ATTACHED, batch, entry, actor)
                .occurredAt(now)
                .detail("exportBatchId", batch.id())
                .detail("uploadId", uploadId)
                .build();
        Outcome outcome;
        try {
            outcome = transactions.run(session -> {
                Outcome claimed = lifecycle.claimIn(session, batch, entry, actor, now);
                if (!unmatched.resolve(session, uploadId, resolution)) {
                    throw alreadyResolved(uploadId);
                }
                audit.write(session, attached);
                return claimed;
            });
        } catch (BatchMovedException exception) {
            throw ProblemException.conflict(
                    "ATTACH_NOT_ALLOWED", "batch " + batch.id() + " changed under this request");
        }
        switch (outcome) {
            case RECEIVED -> metrics.arrivalReceived(batch.tenantId(), batch.vendor());
            case DUPLICATE_LOGGED -> metrics.arrivalDuplicateLogged(batch.tenantId(), batch.vendor());
            case ALREADY_CLAIMED -> {
                // the upload was on the batch already: nothing was claimed, nothing to count
            }
        }
        Located located = requests.claimed(uploadId)
                .orElseThrow(() -> new IllegalStateException("upload " + uploadId + " is attached but not found"));
        InboundFileState state = located.entry().state();
        LOG.infof("upload %s attached to %s by an operator: %s, entry %s", uploadId, batch.id(), outcome, state);
        UUID jobId = null;
        try {
            jobId = requests.requestIfUnowned(located, now);
        } catch (RuntimeException exception) {
            // The attach committed; a 500 would invite a retry that answers ALREADY_RESOLVED. The
            // entry stays RECEIVED with no job, which the reconciler re-enqueues (findStale).
            LOG.errorf(
                    exception,
                    "upload %s attached to %s but the request-ingestion enqueue failed; left for the reconciler",
                    uploadId,
                    batch.id());
        }
        return new Attached(uploadId, state, jobId);
    }

    /**
     * Discards an open unmatched upload of the operator's tenant: the row resolved {@code DISCARDED}
     * and {@code INBOUND_UPLOAD_DISCARDED} in one transaction. A service event, as {@code
     * INBOUND_FILE_UNMATCHED} is.
     *
     * @param tenantId the caller's tenant
     * @param uploadId the unmatched upload
     * @param reason why, required
     * @param actor the operator
     * @param now the time of the write
     * @return the resolved row
     * @throws ProblemException 404 {@code UNMATCHED_NOT_FOUND}, 409 {@code ALREADY_RESOLVED}, or 400
     *     {@code REASON_REQUIRED}
     */
    public UnmatchedUpload discard(String tenantId, String uploadId, String reason, String actor, Instant now) {
        requireOpen(tenantId, uploadId);
        String why = requireReason(reason);
        UnmatchedUpload.Resolution resolution =
                new UnmatchedUpload.Resolution(UnmatchedUpload.DISCARDED, null, why, actor, now);
        AuditEvent discarded = AuditEvent.of(AuditEventType.INBOUND_UPLOAD_DISCARDED, null, null)
                .actor(actor)
                .upload(uploadId)
                .occurredAt(now)
                .detail("reason", why)
                .detail("uploadId", uploadId)
                .build();
        transactions.run(session -> {
            if (!unmatched.resolve(session, uploadId, resolution)) {
                throw alreadyResolved(uploadId);
            }
            audit.write(session, discarded);
            return null;
        });
        LOG.infof("upload %s discarded by an operator", uploadId);
        return unmatched.find(uploadId).orElseThrow();
    }

    /** The open row in the caller's tenant folder; another tenant's row reads as not found. */
    private UnmatchedUpload requireOpen(String tenantId, String uploadId) {
        UnmatchedUpload row = unmatched
                .find(uploadId)
                .filter(found -> tenantId.equals(found.folderTenant()))
                .orElseThrow(() -> ProblemException.notFound("UNMATCHED_NOT_FOUND", "no unmatched upload " + uploadId));
        if (row.resolution() != null) {
            throw alreadyResolved(uploadId);
        }
        return row;
    }

    /** The file names another tenant: tenant disagreement is never ingested into the caller's, 409. */
    private static UnmatchedUpload requireSameTenantFile(String tenantId, UnmatchedUpload row) {
        if (row.parsed() != null && !tenantId.equals(row.parsed().fileTenant())) {
            throw ProblemException.conflict(
                    InboundNames.TENANT_MISMATCH,
                    "upload " + row.id() + " names tenant " + row.parsed().fileTenant()
                            + "; it cannot be attached here");
        }
        return row;
    }

    private static ProblemException alreadyResolved(String uploadId) {
        return ProblemException.conflict("ALREADY_RESOLVED", "unmatched upload " + uploadId + " is already resolved");
    }

    private static String requireReason(String reason) {
        if (reason == null || reason.isBlank()) {
            throw ProblemException.badRequest("REASON_REQUIRED", "reason is required");
        }
        return reason.trim();
    }

    /** The storage event the row came from, as the arrival handler read it. */
    private static ArrivalEvent arrivalOf(UnmatchedUpload row) {
        String uploadId = row.id();
        return new ArrivalEvent(
                uploadId.substring(0, uploadId.indexOf(SEPARATOR)),
                objectName(uploadId),
                generation(uploadId),
                FINALIZE,
                row.receivedAt(),
                null);
    }

    /**
     * What the row's name said. An unparsed name gives no vendor batch id and no file date, which
     * the entry then leaves out; the jobs read the file name from the object name, not from here.
     */
    private static InboundNames.Parsed nameOf(UnmatchedUpload row) {
        if (row.parsed() != null) {
            return row.parsed();
        }
        String objectName = objectName(row.id());
        return new InboundNames.Parsed(
                row.folderTenant(), null, null, null, null, objectName.substring(objectName.lastIndexOf('/') + 1));
    }

    /**
     * Counts a storage event that is not an arrival (a delete, an archive, a metadata update) or
     * whose object identity could not be read.
     *
     * @param eventType the event type, or a fixed tag such as {@code UNREADABLE}; null counts as
     *     {@value #UNKNOWN}
     */
    public void ignored(String eventType) {
        metrics.arrivalIgnoredEventType(tag(eventType));
    }

    private static String tag(String value) {
        return value == null || value.isBlank() ? UNKNOWN : value;
    }

    /** The parsed legs as one short string, null when the name did not parse. */
    static String legs(InboundNames.Parsed parsed) {
        if (parsed == null) {
            return null;
        }
        return "folderTenant=" + parsed.folderTenant() + " fileTenant=" + parsed.fileTenant() + " exportBatchRef="
                + parsed.exportBatchRef() + " vendorBatchId=" + parsed.vendorBatchId() + " fileDate="
                + parsed.fileDate();
    }

    /** The object name: everything between the first and last legs of the upload id, unescaped. */
    private static String objectName(String uploadId) {
        return Ids.objectNameOf(uploadId.substring(uploadId.indexOf(SEPARATOR) + 1, uploadId.lastIndexOf(SEPARATOR)));
    }

    /** The generation: the last leg of the upload id. */
    private static String generation(String uploadId) {
        return uploadId.substring(uploadId.lastIndexOf(SEPARATOR) + 1);
    }
}
