package com.certifyos.vendor_exchange.ingestion.events;

import com.certifyos.vendor_exchange.config.VendorExchangeConfig;
import com.certifyos.vendor_exchange.export.events.PubSubPushMessage;
import com.certifyos.vendor_exchange.ingestion.release.ReadBackService;
import com.certifyos.vendor_exchange.ingestion.release.ReleaseLifecycle;
import com.certifyos.vendor_exchange.ingestion.staging.FieldLevel;
import com.certifyos.vendor_exchange.metrics.VendorExchangeMetrics;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Clock;
import java.util.Optional;
import org.jboss.logging.Logger;

/**
 * The {@code mdm.slice.ov_generated} handler table (design step 8): survivorship wrote an operational
 * value; when our candor slice triggered the run, read it back and finish the entity's written {@code
 * RELEASING} fields. Every outcome acknowledges, but one: fields that are {@code RELEASING} with no
 * write recorded yet mean the event outran the release job's commit, so the handler throws and the 5xx
 * makes Pub/Sub redeliver. A read failure propagates the same way.
 */
@ApplicationScoped
public class OvGeneratedHandler {

    private static final Logger LOG = Logger.getLogger(OvGeneratedHandler.class);

    private final ReadBackService readBack;
    private final VendorExchangeMetrics metrics;
    private final ObjectMapper mapper;
    private final VendorExchangeConfig cfg;
    private final Clock clock;

    public OvGeneratedHandler(
            ReadBackService readBack,
            VendorExchangeMetrics metrics,
            ObjectMapper mapper,
            VendorExchangeConfig cfg,
            Clock clock) {
        this.readBack = readBack;
        this.metrics = metrics;
        this.mapper = mapper;
        this.cfg = cfg;
        this.clock = clock;
    }

    /** What the handler did with a delivery, for the log and the tests. */
    public enum Outcome {
        /** The data was missing, not base64, or not survivorship's envelope. */
        REJECTED_UNPARSEABLE,
        /** No candor slice triggered the run (a roster or UI write). */
        NOT_OURS,
        /** Neither a practitioner nor a location. */
        UNKNOWN_ENTITY_TYPE,
        /** No field of ours is {@code RELEASING} for the entity. */
        NOTHING_RELEASING,
        /** The entity's written fields are {@code RELEASED} with their verdicts. */
        RELEASED,
        /** The service is switched off; acknowledged with nothing written (the deadline reads later). */
        DISABLED
    }

    /**
     * Applies one push delivery.
     *
     * @param message the Pub/Sub message
     * @return what happened
     * @throws IllegalStateException when the entity's fields are {@code RELEASING} without a recorded
     *     write, so the delivery is retried
     */
    public Outcome handle(PubSubPushMessage.Message message) {
        String messageId = message.messageId();
        if (!cfg.enabled()) {
            LOG.infof("ov-generated %s acknowledged, the service is disabled", messageId);
            return Outcome.DISABLED;
        }
        Optional<OvGeneratedEvent> read = OvGeneratedEvent.fromMessage(message, mapper);
        if (read.isEmpty()) {
            LOG.warnf("ov-generated %s is not a readable operational-value event, acknowledged", messageId);
            return Outcome.REJECTED_UNPARSEABLE;
        }
        OvGeneratedEvent event = read.get();
        if (!event.triggeredByCandor()) {
            metrics.ovEventNotOurs();
            LOG.infof("ov-generated %s: no candor slice triggered it, acknowledged", messageId);
            return Outcome.NOT_OURS;
        }
        FieldLevel level = event.level();
        if (level == null) {
            LOG.infof(
                    "ov-generated %s: entity type %s is not ours, acknowledged",
                    messageId, event.payload().entityType());
            return Outcome.UNKNOWN_ENTITY_TYPE;
        }
        ReadBackService.Result result = readBack.readAndFinish(
                event.payload().tenantId(),
                event.payload().certifyId(),
                level,
                ReleaseLifecycle.EVENT,
                clock.instant());
        return switch (result) {
            case RELEASED, UNKNOWN_MARKED -> Outcome.RELEASED;
            case NOTHING_RELEASING -> Outcome.NOTHING_RELEASING;
            case NOT_WRITTEN -> throw new IllegalStateException("ov-generated " + messageId + ": "
                    + level + " " + event.payload().certifyId()
                    + " has RELEASING fields without a recorded write; redelivery expected");
        };
    }
}
