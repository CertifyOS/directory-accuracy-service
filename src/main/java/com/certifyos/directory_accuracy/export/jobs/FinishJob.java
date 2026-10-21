package com.certifyos.directory_accuracy.export.jobs;

import com.certifyos.directory_accuracy.clients.VendorBucket;
import com.certifyos.directory_accuracy.export.batch.BatchCompletion;
import com.certifyos.directory_accuracy.export.batch.BatchState;
import com.certifyos.directory_accuracy.export.batch.EgressDetails;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.batch.ExportNpiRepository;
import com.certifyos.directory_accuracy.export.batch.FileDetails;
import com.certifyos.directory_accuracy.export.batch.Reconciliation;
import com.certifyos.directory_accuracy.export.schedule.TemplateProvisioner;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.jboss.logging.Logger;
import org.jobrunr.jobs.annotations.Job;
import org.jobrunr.jobs.lambdas.JobRequestHandler;

/**
 * Lifecycle step 6: verify the file is at the destination, reconcile its count against the
 * registry, move the batch to {@code DELIVERED}. Reads object metadata only, never the file. A
 * missing object is an egress failure ({@code FILE_NOT_FOUND}): the fix is to ask egress again
 * and the registered NPIs are reused.
 */
@ApplicationScoped
public class FinishJob implements JobRequestHandler<FinishJobRequest> {

    /** Job name used in ids and logs. */
    public static final String NAME = "finish";

    /** Metadata keys egress sets on the finished object. */
    static final String META_TOTAL_RECORDS = "totalRecords";

    static final String META_TOTAL_ROWS = "totalRows";
    private static final Logger LOG = Logger.getLogger(FinishJob.class);

    private final BatchJobSupport support;
    private final BatchCompletion completion;
    private final ExportNpiRepository npis;
    private final VendorBucket bucket;
    private final Clock clock;

    public FinishJob(
            BatchJobSupport support,
            BatchCompletion completion,
            ExportNpiRepository npis,
            VendorBucket bucket,
            Clock clock) {
        this.support = support;
        this.completion = completion;
        this.npis = npis;
        this.bucket = bucket;
        this.clock = clock;
    }

    /**
     * Runs the job for one batch.
     *
     * @param request the batch, expected in {@code EGRESS_COMPLETED}, and the attempt
     */
    @Override
    @Job(name = "finish %0")
    public void run(FinishJobRequest request) {
        try (JobLogContext ignored = JobLogContext.open(request.exportBatchId(), JobLogContext.currentJob())) {
            Optional<ExportBatch> loaded =
                    support.loadExpecting(NAME, request.exportBatchId(), BatchState.EGRESS_COMPLETED);
            if (loaded.isEmpty()) {
                return;
            }
            finish(loaded.get());
        }
    }

    private void finish(ExportBatch batch) {
        Instant now = clock.instant();
        EgressDetails.Destination destination = batch.egress().destination();
        Optional<VendorBucket.ObjectInfo> object = bucket.head(destination.bucket(), destination.objectName());
        if (object.isEmpty()) {
            completion.failed(
                    batch,
                    BatchState.EGRESS_COMPLETED,
                    BatchCompletion.FILE_NOT_FOUND,
                    "no object at gs://" + destination.bucket() + "/" + destination.objectName(),
                    List.of(),
                    now);
            return;
        }
        VendorBucket.ObjectInfo info = object.get();
        long registered = npis.countForBatch(batch.id());
        Long records = info.metadataNumber(META_TOTAL_RECORDS);
        long inFile = records == null ? 0 : records;
        Long rowCount = info.metadataNumber(META_TOTAL_ROWS);
        Reconciliation reconciliation = Reconciliation.of(registered, inFile, now);
        FileDetails file = new FileDetails(
                batch.file() == null ? null : batch.file().name(),
                batch.file() == null ? null : batch.file().path(),
                rowCount,
                info.size(),
                TemplateProvisioner.SCHEMA_VERSION,
                info.md5());
        if (!info.isComplete()) {
            LOG.warnf(
                    "%s: object %s is not marked complete; delivering on egress's completion signal",
                    batch.id(), info.objectName());
        }
        completion.delivered(batch, file, reconciliation, info.producedBy(), now);
    }
}
