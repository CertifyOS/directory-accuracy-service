package com.certifyos.directory_accuracy.export.jobs;

import com.certifyos.directory_accuracy.config.VendorExchangeConfig;
import com.certifyos.directory_accuracy.export.batch.BatchState;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.batch.ExportBatchRepository;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Optional;
import org.jboss.logging.Logger;

/**
 * What every batch job does first: honour the service-wide kill switch, load the row, and check
 * it is in the state the job expects. The batch row is the truth; a job that finds the row gone
 * or moved on exits as a no-op, which is what makes a duplicate enqueue, a redelivered event and a
 * reconciler re-enqueue all harmless.
 */
@ApplicationScoped
public class BatchJobSupport {

    private static final Logger LOG = Logger.getLogger(BatchJobSupport.class);

    private final VendorExchangeConfig cfg;
    private final ExportBatchRepository batches;

    public BatchJobSupport(VendorExchangeConfig cfg, ExportBatchRepository batches) {
        this.cfg = cfg;
        this.batches = batches;
    }

    /**
     * Loads the batch a job should work on.
     *
     * @param job the job name, for the log line
     * @param exportBatchId the batch
     * @param expected the state the job expects
     * @return the batch, or empty when the service is disabled, the row is missing, or the state
     *     differs (each logged once)
     */
    public Optional<ExportBatch> loadExpecting(String job, String exportBatchId, BatchState expected) {
        if (!cfg.enabled()) {
            LOG.infof("%s %s: service disabled, exiting as a no-op", job, exportBatchId);
            return Optional.empty();
        }
        Optional<ExportBatch> batch = batches.find(exportBatchId);
        if (batch.isEmpty()) {
            LOG.warnf("%s %s: batch not found, exiting as a no-op", job, exportBatchId);
            return Optional.empty();
        }
        if (batch.get().state() != expected) {
            LOG.infof(
                    "%s %s: state is %s, expected %s, exiting as a no-op",
                    job, exportBatchId, batch.get().state(), expected);
            return Optional.empty();
        }
        JobLogContext.tenant(batch.get().tenantId());
        LOG.infof("%s %s: starting, attempt %d", job, exportBatchId, batch.get().attempt());
        return batch;
    }

    /** The service configuration, for the settings a job body reads. */
    public VendorExchangeConfig config() {
        return cfg;
    }

    /** Whether the service-wide kill switch is on. */
    public boolean enabled() {
        return cfg.enabled();
    }
}
