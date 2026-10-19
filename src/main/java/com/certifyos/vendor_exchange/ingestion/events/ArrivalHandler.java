package com.certifyos.vendor_exchange.ingestion.events;

import com.certifyos.vendor_exchange.config.VendorExchangeConfig;
import com.certifyos.vendor_exchange.export.batch.BatchState;
import com.certifyos.vendor_exchange.export.batch.ExportBatch;
import com.certifyos.vendor_exchange.export.batch.ExportBatchRepository;
import com.certifyos.vendor_exchange.export.events.PubSubPushMessage;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFile;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileRepository;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileRepository.Located;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileState;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundLifecycle;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundNames;
import com.certifyos.vendor_exchange.ingestion.inbound.UnmatchedOperations;
import com.certifyos.vendor_exchange.ingestion.inbound.UnmatchedUpload;
import com.certifyos.vendor_exchange.ingestion.jobs.IngestionJobEnqueuer;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;
import org.jboss.logging.Logger;

/**
 * The arrival handler table (design steps 1 and 2): a storage notification for the vendor bucket,
 * read from the message attributes only. Every outcome but an unexpected exception acknowledges;
 * an exception before the claim commits reaches the 500 mapper and Pub/Sub redelivers. The claim is
 * one transaction ({@link InboundLifecycle#received}); the enqueue follows its commit, and the job id
 * is recorded in a second small transaction after the enqueue. A crash between the commit and the
 * enqueue leaves a {@code RECEIVED} entry with no job, which the redelivered event finds and enqueues.
 *
 * <p>Created at startup: only the {@code DATA_FILE} completeness signal is built, and a deployment
 * configured for another one fails at boot instead of claiming the wrong object.
 */
@Startup
@ApplicationScoped
public class ArrivalHandler {

    /** The one completeness signal this handler implements. */
    public static final String DATA_FILE = "DATA_FILE";

    /** The event type tag for a message whose object identity could not be read. */
    static final String UNREADABLE = "UNREADABLE";

    /** A folder leg the unmatched row and the metric tag may carry: the tenant leg rules of {@link InboundNames}. */
    private static final Pattern FOLDER = Pattern.compile("[A-Za-z0-9.-]+");

    private static final Logger LOG = Logger.getLogger(ArrivalHandler.class);

    private final ExportBatchRepository batches;
    private final InboundFileRepository inbound;
    private final InboundLifecycle lifecycle;
    private final UnmatchedOperations unmatched;
    private final IngestionJobEnqueuer enqueuer;
    private final VendorExchangeConfig cfg;
    private final Clock clock;

    public ArrivalHandler(
            ExportBatchRepository batches,
            InboundFileRepository inbound,
            InboundLifecycle lifecycle,
            UnmatchedOperations unmatched,
            IngestionJobEnqueuer enqueuer,
            VendorExchangeConfig cfg,
            Clock clock) {
        String signal = cfg.ingestion().completenessSignal();
        if (!DATA_FILE.equals(signal)) {
            throw new IllegalStateException("vendor-exchange.ingestion.completeness-signal is " + signal + "; only "
                    + DATA_FILE + " is implemented");
        }
        this.batches = batches;
        this.inbound = inbound;
        this.lifecycle = lifecycle;
        this.unmatched = unmatched;
        this.enqueuer = enqueuer;
        this.cfg = cfg;
        this.clock = clock;
    }

    /** What the handler did with a delivery, for the log and the tests. */
    public enum Outcome {
        /** Not an arrival (a delete, an archive, a metadata update), or the object identity was unreadable. */
        IGNORED_EVENT_TYPE,
        /** An object outside {@code to/}, such as the export file under {@code from/}. */
        IGNORED_PREFIX,
        /** Recorded in {@code vendor_unmatched_uploads} with a reason. */
        UNMATCHED,
        /** The upload was already on its batch (a redelivered event). */
        ALREADY_CLAIMED,
        /** The batch's first upload, claimed and handed to the request-ingestion job. */
        RECEIVED,
        /** A later upload for the batch, recorded and never processed. */
        DUPLICATE_LOGGED,
        /** The service is switched off; acknowledged with nothing written. */
        DISABLED
    }

    /**
     * Applies one push delivery.
     *
     * @param message the Pub/Sub message; only its attributes are read
     * @return what happened
     */
    public Outcome handle(PubSubPushMessage.Message message) {
        String messageId = message.messageId();
        if (!cfg.enabled()) {
            LOG.infof("arrival %s acknowledged, the service is disabled", messageId);
            return Outcome.DISABLED;
        }
        Optional<ArrivalEvent> read = ArrivalEvent.fromAttributes(message.attributes());
        if (read.isEmpty()) {
            LOG.warnf("arrival %s has no readable object identity, acknowledged", messageId);
            unmatched.ignored(UNREADABLE);
            return Outcome.IGNORED_EVENT_TYPE;
        }
        ArrivalEvent event = read.get();
        if (!event.isFinalize()) {
            LOG.infof("arrival %s is %s, acknowledged", messageId, event.eventType());
            unmatched.ignored(event.eventType());
            return Outcome.IGNORED_EVENT_TYPE;
        }
        if (!event.objectId().startsWith(InboundNames.TO_PREFIX)) {
            LOG.infof(
                    "arrival %s upload %s is outside %s, acknowledged",
                    messageId, event.uploadId(), InboundNames.TO_PREFIX);
            return Outcome.IGNORED_PREFIX;
        }
        return match(event, clock.instant());
    }

    private Outcome match(ArrivalEvent event, Instant now) {
        Optional<InboundNames.Parsed> name = InboundNames.parse(event.objectId());
        if (name.isEmpty()) {
            return unmatched(event, folderTenant(event.objectId()), null, InboundNames.UNPARSEABLE_NAME, now);
        }
        InboundNames.Parsed parsed = name.get();
        Optional<ExportBatch> found = batches.find(parsed.exportBatchRef());
        if (found.isEmpty()) {
            return unmatched(event, parsed.folderTenant(), parsed, InboundNames.UNKNOWN_BATCH, now);
        }
        ExportBatch batch = found.get();
        if (!parsed.folderTenant().equals(parsed.fileTenant())
                || !batch.tenantId().equals(parsed.folderTenant())) {
            return unmatched(event, parsed.folderTenant(), parsed, InboundNames.TENANT_MISMATCH, now);
        }
        if ((batch.state() != BatchState.DELIVERED && batch.state() != BatchState.SUPERSEDED) || batch.file() == null) {
            return unmatched(event, parsed.folderTenant(), parsed, InboundNames.BATCH_NOT_DELIVERED, now);
        }
        return claim(batch, InboundFile.received(event, parsed, now), now);
    }

    private Outcome claim(ExportBatch batch, InboundFile entry, Instant now) {
        InboundLifecycle.Outcome claimed = lifecycle.received(batch, entry, now);
        return switch (claimed) {
            case RECEIVED -> {
                requestIngestion(entry.uploadId(), now);
                yield Outcome.RECEIVED;
            }
            case DUPLICATE_LOGGED -> Outcome.DUPLICATE_LOGGED;
            case ALREADY_CLAIMED -> {
                // A redelivery. When the first delivery crashed between its commit and its enqueue,
                // the entry is RECEIVED with no job; this delivery finishes that work.
                requestIngestion(entry.uploadId(), now);
                yield Outcome.ALREADY_CLAIMED;
            }
        };
    }

    /**
     * Enqueues the request-ingestion job for a {@code RECEIVED} entry that has no job yet, then
     * records the job id on it. Runs after the claim's commit, never inside a transaction. Two
     * deliveries racing here enqueue the same deterministic id, which JobRunr treats as one job.
     */
    private void requestIngestion(String uploadId, Instant now) {
        Located located = inbound.findByUploadId(uploadId)
                .orElseThrow(() -> new IllegalStateException("upload " + uploadId + " is claimed but not found"));
        InboundFile entry = located.entry();
        if (entry.state() != InboundFileState.RECEIVED || entry.jobId() != null) {
            LOG.infof(
                    "%s upload %s is %s with job %s; nothing to enqueue",
                    located.batch().id(), uploadId, entry.state(), entry.jobId());
            return;
        }
        UUID jobId = enqueuer.requestIngestion(located.batch().id(), uploadId, entry.attempt());
        lifecycle.jobRecorded(located, jobId, now);
    }

    private Outcome unmatched(
            ArrivalEvent event, String folderTenant, InboundNames.Parsed parsed, String reason, Instant now) {
        unmatched.unmatched(new UnmatchedUpload(event.uploadId(), folderTenant, parsed, reason, now, null), now);
        return Outcome.UNMATCHED;
    }

    /**
     * The {@code to/<tenant>/} leg of a name that did not parse, when it has one that obeys the
     * tenant leg rules; null otherwise, so a stray name never becomes a metric tag.
     */
    static String folderTenant(String objectName) {
        String rest = objectName.substring(InboundNames.TO_PREFIX.length());
        int slash = rest.indexOf('/');
        if (slash < 1) {
            return null;
        }
        String folder = rest.substring(0, slash);
        if (!FOLDER.matcher(folder).matches() || ".".equals(folder) || "..".equals(folder)) {
            return null;
        }
        return folder;
    }
}
