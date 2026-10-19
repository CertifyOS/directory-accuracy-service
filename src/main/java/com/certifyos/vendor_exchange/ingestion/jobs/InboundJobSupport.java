package com.certifyos.vendor_exchange.ingestion.jobs;

import com.certifyos.vendor_exchange.config.VendorExchangeConfig;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileRepository;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileRepository.Located;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileState;
import com.certifyos.vendor_exchange.metrics.VendorExchangeMetrics;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Optional;
import java.util.Set;
import org.jboss.logging.Logger;
import org.jobrunr.jobs.context.JobContext;

/**
 * What every inbound file job does after the kill switch: load the entry and check it is in a state
 * the job expects. The entry is the truth; a job that finds it gone or moved on exits as a no-op,
 * which makes a duplicate enqueue, a redelivered event and a reconciler re-enqueue harmless. The
 * jobs check {@link #enabled()} themselves first and throw, so JobRunr retries rather than drops
 * the work.
 */
@ApplicationScoped
public class InboundJobSupport {

    private static final Logger LOG = Logger.getLogger(InboundJobSupport.class);

    private final VendorExchangeConfig cfg;
    private final InboundFileRepository inbound;
    private final VendorExchangeMetrics metrics;

    public InboundJobSupport(VendorExchangeConfig cfg, InboundFileRepository inbound, VendorExchangeMetrics metrics) {
        this.cfg = cfg;
        this.inbound = inbound;
        this.metrics = metrics;
    }

    /**
     * Loads the entry a job should work on and puts the tenant on the log context.
     *
     * @param job the job name, for the log line
     * @param uploadId the upload
     * @param attempt the entry attempt the request was made for
     * @param expected the states the job works from
     * @return the entry with its batch, or empty when it is missing, of another attempt or in another
     *     state (each logged once)
     */
    public Optional<Located> loadExpecting(String job, String uploadId, int attempt, Set<InboundFileState> expected) {
        Optional<Located> located = inbound.findByUploadId(uploadId);
        if (located.isEmpty()) {
            LOG.warnf("%s %s: inbound entry not found, exiting as a no-op", job, uploadId);
            return Optional.empty();
        }
        IngestionLogContext.tenant(located.get().batch().tenantId());
        int stored = located.get().entry().attempt();
        if (stored != attempt) {
            LOG.infof(
                    "%s %s upload %s: request is for attempt %d, entry is at attempt %d, exiting as a no-op",
                    job, located.get().batch().id(), uploadId, attempt, stored);
            return Optional.empty();
        }
        InboundFileState state = located.get().entry().state();
        if (!expected.contains(state)) {
            LOG.infof(
                    "%s %s upload %s: state is %s, expected one of %s, exiting as a no-op",
                    job, located.get().batch().id(), uploadId, state, expected);
            return Optional.empty();
        }
        LOG.infof(
                "%s %s upload %s: starting from %s, attempt %d",
                job,
                located.get().batch().id(),
                uploadId,
                state,
                located.get().entry().attempt());
        return located;
    }

    /**
     * How many times the running job has run, this run included: JobRunr's failure count plus one.
     *
     * @param job JobRunr's context, null or {@code JobContext.Null} when the handler is called
     *     directly
     * @return 1 outside a job run
     */
    public static int jobAttempts(JobContext job) {
        if (job == null || job == JobContext.Null) {
            return 1;
        }
        return job.amountOfFailures() + 1;
    }

    /** The service configuration, for the settings a job body reads. */
    public VendorExchangeConfig config() {
        return cfg;
    }

    /** The service metrics, for the counters and timers a job body records. */
    public VendorExchangeMetrics metrics() {
        return metrics;
    }

    /** Whether the service-wide kill switch is on. */
    public boolean enabled() {
        return cfg.enabled();
    }
}
