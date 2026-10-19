package com.certifyos.vendor_exchange.ingestion.inbound;

import com.certifyos.vendor_exchange.audit.AuditEventType;
import com.certifyos.vendor_exchange.audit.AuditRepository;
import com.certifyos.vendor_exchange.export.batch.BatchState;
import com.certifyos.vendor_exchange.export.batch.ExportBatch;
import com.certifyos.vendor_exchange.export.batch.ExportBatchRepository;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileRepository.Claim;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundLifecycle.Outcome;
import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Updates;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import org.bson.conversions.Bson;

/**
 * The claim of an upload on its batch, inside the caller's transaction: the claim, the batch
 * acknowledgement for the first upload, and their events. {@link InboundLifecycle} owns the entry
 * points ({@link InboundLifecycle#received} in its own transaction, {@link InboundLifecycle#claimIn}
 * in the caller's); this class holds the body so the lifecycle stays within the file length limit.
 * No metric and no log line here: both follow the caller's commit.
 */
@ApplicationScoped
public class InboundClaims {

    private final InboundFileRepository inbound;
    private final ExportBatchRepository batches;
    private final AuditRepository audit;

    public InboundClaims(InboundFileRepository inbound, ExportBatchRepository batches, AuditRepository audit) {
        this.inbound = inbound;
        this.batches = batches;
        this.audit = audit;
    }

    /** See {@link InboundLifecycle#claimIn}. */
    Outcome claimIn(ClientSession session, ExportBatch batch, InboundFile entry, String actor, Instant now) {
        if (batch.state() != BatchState.DELIVERED && batch.state() != BatchState.SUPERSEDED) {
            throw new IllegalArgumentException(
                    batch.id() + " is " + batch.state() + "; only a delivered batch takes an upload");
        }
        Claim claim = inbound.claim(session, batch.id(), entry);
        if (!claim.claimed()) {
            return Outcome.ALREADY_CLAIMED;
        }
        if (claim.state() == InboundFileState.RECEIVED) {
            acknowledge(session, batch, entry, actor, now);
            return Outcome.RECEIVED;
        }
        logDuplicate(session, batch, entry, actor, now);
        return Outcome.DUPLICATE_LOGGED;
    }

    private void acknowledge(ClientSession session, ExportBatch batch, InboundFile entry, String actor, Instant now) {
        Instant acknowledgedAt = entry.eventTime() == null ? now : entry.eventTime();
        Bson updates = Updates.combine(
                Updates.set("acknowledgedAt", acknowledgedAt), Updates.set("inboundBatchId", entry.uploadId()));
        if (!batches.updateInState(session, batch.id(), batch.state(), now, updates)) {
            throw new IllegalStateException(
                    batch.id() + " is no longer " + batch.state() + "; acknowledgement not written");
        }
        audit.write(
                session,
                InboundLifecycle.envelope(AuditEventType.INBOUND_FILE_RECEIVED, batch, entry, actor)
                        .occurredAt(now)
                        .detail("objectName", entry.objectName())
                        .detail("generation", entry.generation())
                        .detail("vendorBatchId", entry.vendorBatchId())
                        .detail("eventTime", entry.eventTime())
                        .detail("overwroteGeneration", entry.overwroteGeneration())
                        .build());
        // An export event: it carries the batch's export attempt, as every export event does, not
        // the entry's ingestion attempt.
        audit.write(
                session,
                InboundLifecycle.envelope(AuditEventType.EXPORT_BATCH_ACKNOWLEDGED, batch, entry, actor)
                        .batch(batch.id(), batch.attempt())
                        .occurredAt(now)
                        .detail("uploadId", entry.uploadId())
                        .detail("vendorBatchId", entry.vendorBatchId())
                        .detail("daysToResponse", InboundLifecycle.daysToResponse(batch.deliveredAt(), acknowledgedAt))
                        .build());
    }

    private void logDuplicate(ClientSession session, ExportBatch batch, InboundFile entry, String actor, Instant now) {
        InboundFile first = inbound.firstEntry(session, batch.id())
                .orElseThrow(() -> new IllegalStateException(batch.id() + " logged a duplicate with no first entry"));
        audit.write(
                session,
                InboundLifecycle.envelope(AuditEventType.INBOUND_FILE_DUPLICATE_LOGGED, batch, entry, actor)
                        .occurredAt(now)
                        .detail("objectName", entry.objectName())
                        .detail("generation", entry.generation())
                        .detail("firstUploadId", first.uploadId())
                        .detail(
                                "firstUploadState",
                                first.state() == null ? null : first.state().name())
                        .build());
    }
}
