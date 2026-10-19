package com.certifyos.vendor_exchange.ingestion.inbound;

import com.certifyos.vendor_exchange.audit.Actors;
import com.certifyos.vendor_exchange.audit.AuditEvent;
import com.certifyos.vendor_exchange.audit.AuditEventType;
import com.certifyos.vendor_exchange.audit.AuditRepository;
import com.certifyos.vendor_exchange.metrics.VendorExchangeMetrics;
import com.certifyos.vendor_exchange.persistence.Transactions;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import org.jboss.logging.Logger;

/**
 * Arrivals the service does not attach to a batch. {@link #unmatched} keeps an upload with its
 * reason in one transaction (the row and {@code INBOUND_FILE_UNMATCHED}); {@link #ignored} counts an
 * event that was not an arrival at all. Metrics and logs follow the commit.
 */
@ApplicationScoped
public class UnmatchedOperations {

    /** The tag for an unmatched upload whose name has no folder tenant, or an event with no type. */
    public static final String UNKNOWN = "unknown";

    private static final Logger LOG = Logger.getLogger(UnmatchedOperations.class);
    private static final char SEPARATOR = '|';

    private final UnmatchedUploadRepository unmatched;
    private final AuditRepository audit;
    private final Transactions transactions;
    private final VendorExchangeMetrics metrics;

    public UnmatchedOperations(
            UnmatchedUploadRepository unmatched,
            AuditRepository audit,
            Transactions transactions,
            VendorExchangeMetrics metrics) {
        this.unmatched = unmatched;
        this.audit = audit;
        this.transactions = transactions;
        this.metrics = metrics;
    }

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

    /** The object name: everything between the first and last legs of the upload id. */
    private static String objectName(String uploadId) {
        return uploadId.substring(uploadId.indexOf(SEPARATOR) + 1, uploadId.lastIndexOf(SEPARATOR));
    }

    /** The generation: the last leg of the upload id. */
    private static String generation(String uploadId) {
        return uploadId.substring(uploadId.lastIndexOf(SEPARATOR) + 1);
    }
}
