package com.certifyos.directory_accuracy.ingestion.staging;

import com.certifyos.directory_accuracy.audit.Actors;
import com.certifyos.directory_accuracy.audit.AuditEvent;
import com.certifyos.directory_accuracy.audit.AuditEventType;
import com.certifyos.directory_accuracy.audit.AuditRepository;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFile;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFileRepository;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFileRepository.Located;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFileState;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundLifecycle;
import com.certifyos.directory_accuracy.persistence.Transactions;
import com.mongodb.client.model.Updates;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jboss.logging.Logger;

/**
 * The staging writes on an {@code inboundFiles[]} entry, beside {@link InboundLifecycle} (which owns
 * every other move and the failure): the stage job's id, {@code PARSED → STAGING}, one checkpoint per
 * chunk, {@code STAGING → STAGED}. Each method is one transaction holding the guarded entry write and,
 * for {@code STAGED}, its event; none enqueues or calls out. Each returns false, with nothing
 * written, when the entry has left the state the caller read, so a stale job stops.
 */
@ApplicationScoped
public class StagingLifecycle {

    private static final Logger LOG = Logger.getLogger(StagingLifecycle.class);

    private final InboundFileRepository inbound;
    private final AuditRepository audit;
    private final Transactions transactions;

    public StagingLifecycle(InboundFileRepository inbound, AuditRepository audit, Transactions transactions) {
        this.inbound = inbound;
        this.audit = audit;
        this.transactions = transactions;
    }

    /**
     * Records the stage job now responsible for a {@code PARSED} entry (the deadline check, and in
     * step 4 the completion handler, after its enqueue). No event; the state does not change.
     *
     * @param located the entry as the caller read it
     * @param jobId the enqueued stage job
     * @param now the time of the write
     * @return true when the entry was still {@code PARSED}; false when the stage job has already
     *     moved it, which leaves the previous job id in place
     */
    public boolean jobRecorded(Located located, UUID jobId, Instant now) {
        boolean recorded = transactions.run(session -> inbound.updateInState(
                session,
                located.batch().id(),
                located.entry().uploadId(),
                InboundFileState.PARSED,
                now,
                Updates.set(InboundFileRepository.entry("jobId"), jobId.toString())));
        LOG.infof(
                "%s upload %s: stage job %s %s",
                located.batch().id(),
                located.entry().uploadId(),
                jobId,
                recorded ? "recorded" : "not recorded, the entry left PARSED");
        return recorded;
    }

    /**
     * Staging starts: {@code PARSED → STAGING}, with the checkpoint at the start of the stream and
     * every count at zero, so the stored entry carries all five dispositions and every written count.
     * No event; nothing has been staged.
     *
     * @param located the entry as the job read it
     * @param now the time of the write
     * @return true when the entry moved
     */
    public boolean staging(Located located, Instant now) {
        Document zeroDispositions = new Document();
        for (RowDisposition disposition : RowDisposition.values()) {
            zeroDispositions.append(disposition.name(), 0L);
        }
        Bson updates = Updates.combine(
                Updates.set(
                        InboundFileRepository.entry("checkpoint"), new InboundFile.Checkpoint(null, 0L).toDocument()),
                Updates.set(InboundFileRepository.entry("dispositions"), zeroDispositions),
                Updates.set(
                        InboundFileRepository.entry("written"), new InboundFile.Written(0L, 0L, 0L, 0L).toDocument()));
        boolean moved = transactions.run(session -> inbound.transition(
                session,
                located.batch().id(),
                located.entry().uploadId(),
                InboundFileState.PARSED,
                InboundFileState.STAGING,
                now,
                updates));
        LOG.infof(
                "%s upload %s %s",
                located.batch().id(),
                located.entry().uploadId(),
                moved ? "-> STAGING" : "already moved; STAGING not written");
        return moved;
    }

    /**
     * One chunk is written: its counts are added to the entry and the checkpoint moves to its last
     * row, still in {@code STAGING}. Every disposition and every written count is incremented, zeros
     * included. The write is a compare-and-set on the checkpoint the chunk resumed from, so two runs
     * of one upload that overlap (JobRunr re-running a job it thought orphaned) cannot both add the
     * same chunk; the late one matches nothing and stops. No event (design: staging writes no
     * per-chunk or per-field event).
     *
     * @param located the entry as the job read it
     * @param from the checkpoint the chunk resumed from, null for the first chunk
     * @param progress the chunk's counts and last row
     * @param now the time of the write
     * @return true when the entry was still {@code STAGING} at {@code from}
     */
    public boolean chunkCheckpoint(Located located, String from, RowStager.Progress progress, Instant now) {
        List<Bson> updates = new ArrayList<>();
        for (RowDisposition disposition : RowDisposition.values()) {
            updates.add(Updates.inc(
                    InboundFileRepository.entry("dispositions." + disposition.name()),
                    progress.dispositions().getOrDefault(disposition, 0L)));
        }
        updates.add(
                Updates.inc(InboundFileRepository.entry("written.fieldRecommendations"), progress.recommendations()));
        updates.add(Updates.inc(InboundFileRepository.entry("written.confirmedFields"), progress.confirmedFields()));
        updates.add(Updates.inc(InboundFileRepository.entry("written.unverifiedFields"), progress.unverifiedFields()));
        updates.add(Updates.inc(
                InboundFileRepository.entry("written.practitionerFieldDisagreements"), progress.disagreements()));
        updates.add(Updates.inc(InboundFileRepository.entry("checkpoint.chunks"), 1L));
        updates.add(Updates.set(InboundFileRepository.entry("checkpoint.after"), progress.after()));
        return transactions.run(session -> inbound.updateInStateAt(
                session,
                located.batch().id(),
                located.entry().uploadId(),
                InboundFileState.STAGING,
                from,
                now,
                Updates.combine(updates)));
    }

    /**
     * Staging finished and the counts reconciled: {@code STAGING → STAGED} with {@code stagedAt}, and
     * {@code INBOUND_FILE_STAGED} carrying the entry's totals as the caller read them after the last
     * checkpoint.
     *
     * @param located the entry as read after the last checkpoint
     * @param durationMs how long staging took
     * @param now the time of the write, which is {@code stagedAt}
     * @return true when the entry moved
     */
    public boolean staged(Located located, long durationMs, Instant now) {
        InboundFile entry = located.entry();
        InboundFile.Written written =
                entry.written() == null ? new InboundFile.Written(0L, 0L, 0L, 0L) : entry.written();
        Long chunks = entry.checkpoint() == null ? null : entry.checkpoint().chunks();
        AuditEvent event = InboundLifecycle.envelope(
                        AuditEventType.INBOUND_FILE_STAGED, located.batch(), entry, Actors.INGESTION)
                .occurredAt(now)
                .detail("dispositions", new Document(dispositionsOf(entry)))
                .detail("written", written.toDocument())
                .detail("chunks", Objects.requireNonNullElse(chunks, 0L))
                .detail("durationMs", durationMs)
                .detail(
                        "practitionerFieldDisagreements",
                        Objects.requireNonNullElse(written.practitionerFieldDisagreements(), 0L))
                .build();
        boolean moved = transactions.run(session -> {
            boolean still = inbound.transition(
                    session,
                    located.batch().id(),
                    entry.uploadId(),
                    InboundFileState.STAGING,
                    InboundFileState.STAGED,
                    now,
                    Updates.set(InboundFileRepository.entry("stagedAt"), now));
            if (still) {
                audit.write(session, event);
            }
            return still;
        });
        LOG.infof(
                "%s upload %s %s",
                located.batch().id(), entry.uploadId(), moved ? "-> STAGED" : "already moved; STAGED not written");
        return moved;
    }

    /** The entry's dispositions in the enum's order, every disposition present. */
    private static Map<String, Object> dispositionsOf(InboundFile entry) {
        Map<String, Object> counts = new LinkedHashMap<>();
        for (RowDisposition disposition : RowDisposition.values()) {
            counts.put(disposition.name(), entry.dispositions().getOrDefault(disposition.name(), 0L));
        }
        return counts;
    }
}
