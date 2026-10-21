package com.certifyos.directory_accuracy.export.jobs;

import com.certifyos.directory_accuracy.clients.ApiLayerClient;
import com.certifyos.directory_accuracy.clients.EgressCancelResponse;
import com.certifyos.directory_accuracy.clients.EgressClient;
import com.certifyos.directory_accuracy.clients.EgressExportRequest;
import com.certifyos.directory_accuracy.clients.EgressExportResponse;
import com.certifyos.directory_accuracy.clients.EgressTemplate;
import com.certifyos.directory_accuracy.clients.VendorBucket;
import com.certifyos.directory_accuracy.export.batch.BatchLifecycle;
import com.certifyos.directory_accuracy.export.batch.BatchState;
import com.certifyos.directory_accuracy.export.batch.EgressDetails;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.batch.ExportNpiRepository;
import com.certifyos.directory_accuracy.export.schedule.Schedule;
import com.certifyos.directory_accuracy.persistence.Ids;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.WebApplicationException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;
import org.jobrunr.jobs.annotations.Job;
import org.jobrunr.jobs.lambdas.JobRequestHandler;

/**
 * Lifecycle step 3: cancel a prior attempt on retry, pin the template, ask egress to build and
 * place the file, schedule the deadline check, move the batch to {@code EGRESS_REQUESTED}. Talks
 * to egress, to api-layer once per batch (the template pin) and to the vendor bucket only when a
 * cancel is refused. Egress errors other than 409 propagate, which is what makes JobRunr retry;
 * the pin is written before the first egress call so a retry sends byte-identical values.
 */
@ApplicationScoped
public class RequestEgressJob implements JobRequestHandler<RequestEgressJobRequest> {

    /** Job name used in ids and logs. */
    public static final String NAME = "request-egress";

    private static final Logger LOG = Logger.getLogger(RequestEgressJob.class);

    private final BatchJobSupport support;
    private final BatchLifecycle lifecycle;
    private final ExportNpiRepository npis;
    private final EgressClient egress;
    private final ApiLayerClient apiLayer;
    private final VendorBucket bucket;
    private final Clock clock;

    public RequestEgressJob(
            BatchJobSupport support,
            BatchLifecycle lifecycle,
            ExportNpiRepository npis,
            @RestClient EgressClient egress,
            @RestClient ApiLayerClient apiLayer,
            VendorBucket bucket,
            Clock clock) {
        this.support = support;
        this.lifecycle = lifecycle;
        this.npis = npis;
        this.egress = egress;
        this.apiLayer = apiLayer;
        this.bucket = bucket;
        this.clock = clock;
    }

    /**
     * Runs the job for one batch.
     *
     * @param request the batch, expected in {@code NPIS_SELECTED}, and the attempt
     */
    @Override
    @Job(name = "request-egress %0")
    public void run(RequestEgressJobRequest request) {
        try (JobLogContext ignored = JobLogContext.open(request.exportBatchId(), JobLogContext.currentJob())) {
            Optional<ExportBatch> loaded =
                    support.loadExpecting(NAME, request.exportBatchId(), BatchState.NPIS_SELECTED);
            if (loaded.isEmpty()) {
                return;
            }
            requestEgress(loaded.get());
        }
    }

    private void requestEgress(ExportBatch loaded) {
        ExportBatch batch = loaded;
        if (batch.attempt() > 1 && batch.egress() != null && batch.egress().correlationId() != null) {
            if (cancelPriorAttempt(batch)) {
                return;
            }
        }
        if (batch.egress() == null || batch.egress().templateId() == null) {
            batch = pin(batch);
        } else {
            LOG.infof(
                    "%s keeps the pinned template %s v%s",
                    batch.id(), batch.egress().templateId(), batch.egress().templateVersion());
        }
        EgressDetails pinned = batch.egress();
        String correlationId = Ids.correlationId(batch.id(), batch.attempt());
        List<String> npiFilter = npis.npisForBatch(batch.id());
        EgressExportRequest request = new EgressExportRequest(
                EgressExportRequest.PRACTITIONER,
                batch.tenantId(),
                correlationId,
                pinned.templateId(),
                pinned.mappingsCsvUrl(),
                pinned.separator(),
                pinned.outputFormat(),
                jsonArray(pinned.rowExpansionKeys()),
                npiFilter,
                EgressClient.INITIATOR,
                new EgressExportRequest.Destination(
                        pinned.destination().bucket(), pinned.destination().objectName()));
        Instant now = clock.instant();
        String answer = export(batch, request);
        Instant deadlineAt = now.plus(Duration.ofHours(support.config().egress().deadlineHours()));
        lifecycle.egressRequested(
                batch, correlationId, jobReference(answer, correlationId), npiFilter.size(), answer, deadlineAt, now);
    }

    /**
     * Cancels the prior attempt.
     *
     * @return true when the prior attempt's complete file is already in place and this attempt
     *     has nothing left to ask for
     */
    private boolean cancelPriorAttempt(ExportBatch batch) {
        String prior = batch.egress().correlationId();
        Instant now = clock.instant();
        try {
            EgressCancelResponse cancelled = egress.cancel(batch.tenantId(), batch.tenantId(), prior);
            lifecycle.priorAttemptCancel(batch, prior, true, cancelled.status(), now);
            return false;
        } catch (WebApplicationException refused) {
            int status =
                    refused.getResponse() == null ? 0 : refused.getResponse().getStatus();
            if (status == 409) {
                lifecycle.priorAttemptCancel(batch, prior, false, "409 terminal", now);
                EgressDetails.Destination destination = batch.egress().destination();
                Optional<VendorBucket.ObjectInfo> object = destination == null
                        ? Optional.empty()
                        : bucket.head(destination.bucket(), destination.objectName());
                if (object.isPresent() && object.get().isComplete()) {
                    lifecycle.completedByPriorAttempt(batch, object.get(), now);
                    return true;
                }
                LOG.infof(
                        "%s: prior attempt %s is terminal and left no complete file; requesting again",
                        batch.id(), prior);
                return false;
            }
            if (status == 404) {
                lifecycle.priorAttemptCancel(batch, prior, false, "404 unknown to egress", now);
                return false;
            }
            throw refused;
        }
    }

    private ExportBatch pin(ExportBatch batch) {
        Schedule schedule = lifecycle.scheduleOf(batch);
        EgressTemplate template = apiLayer.getEgressTemplate(batch.tenantId(), schedule.egressTemplateId());
        return lifecycle.pinTemplate(batch, schedule, template, support.config().vendorBucket(), clock.instant());
    }

    /** Sends the request; 202 and 409 are both success, anything else propagates after being counted. */
    private String export(ExportBatch batch, EgressExportRequest request) {
        try {
            EgressExportResponse response = egress.export(batch.tenantId(), request);
            EgressExportResponse.Job job = response.jobReference();
            return "202 " + (job.table() != null ? job.table() : job.correlationId());
        } catch (WebApplicationException refused) {
            int status =
                    refused.getResponse() == null ? 0 : refused.getResponse().getStatus();
            if (status == 409) {
                LOG.infof("%s: egress already holds %s; treated as accepted", batch.id(), request.correlationId());
                return "409 already exists";
            }
            lifecycle.egressRefused(batch, "HTTP_" + status);
            throw refused;
        }
    }

    private static String jobReference(String answer, String correlationId) {
        return answer.startsWith("202 ") ? answer.substring(4) : correlationId;
    }

    static String jsonArray(List<String> values) {
        return values.stream().map(value -> "\"" + value + "\"").collect(Collectors.joining(",", "[", "]"));
    }
}
