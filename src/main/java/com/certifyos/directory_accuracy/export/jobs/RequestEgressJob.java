package com.certifyos.directory_accuracy.export.jobs;

import com.certifyos.directory_accuracy.clients.ApiLayerClient;
import com.certifyos.directory_accuracy.clients.EgressCancelResponse;
import com.certifyos.directory_accuracy.clients.EgressClient;
import com.certifyos.directory_accuracy.clients.EgressError;
import com.certifyos.directory_accuracy.clients.EgressExportRequest;
import com.certifyos.directory_accuracy.clients.EgressExportResponse;
import com.certifyos.directory_accuracy.clients.EgressSelection;
import com.certifyos.directory_accuracy.clients.EgressStatusResponse;
import com.certifyos.directory_accuracy.clients.EgressTemplate;
import com.certifyos.directory_accuracy.clients.VendorBucket;
import com.certifyos.directory_accuracy.export.batch.BatchCompletion;
import com.certifyos.directory_accuracy.export.batch.BatchLifecycle;
import com.certifyos.directory_accuracy.export.batch.BatchState;
import com.certifyos.directory_accuracy.export.batch.EgressDetails;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.schedule.Schedule;
import com.certifyos.directory_accuracy.persistence.Ids;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
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
 * Lifecycle step 3: cancel a prior attempt on retry, pin the template, ask egress to select the
 * saved filter's practitioners and build and place the file, schedule the deadline check, move the
 * batch to {@code EGRESS_REQUESTED}. A first attempt names the filter; a retry names the prior
 * attempt so egress reuses its frozen list, and falls back to the filter when egress says that list
 * cannot be used. Egress's answers:
 *
 * <ul>
 *   <li>{@code 202} and {@code 409} (already held) are success.
 *   <li>{@code 429 SELECTION_QUOTA_EXCEEDED}: a new job after {@code Retry-After}, up to
 *       {@link #MAX_QUOTA_WAITS} times; after that the refusal propagates to JobRunr's retries.
 *   <li>{@code 400} on a reuse whose source is unusable: sent again with the filter id.
 *   <li>{@code 403 SELECTION_SOURCE_DISABLED} and any other {@code 400}: the batch goes
 *       {@code FAILED} at {@code EGRESS} at once, since asking again cannot change the answer.
 *   <li>Anything else propagates, which is what makes JobRunr retry.
 * </ul>
 *
 * The pin is written before the first egress call so a retry sends byte-identical values.
 */
@ApplicationScoped
public class RequestEgressJob implements JobRequestHandler<RequestEgressJobRequest> {

    /** Job name used in ids and logs. */
    public static final String NAME = "request-egress";

    /** Waits for egress's selection quota before the refusal is left to JobRunr's retries. */
    static final int MAX_QUOTA_WAITS = 12;

    /** The answer recorded when egress already holds the correlation id. */
    static final String ALREADY_HELD = "409 already exists";

    /** Egress's fixed {@code Retry-After}, used when the header is missing or unreadable. */
    static final Duration DEFAULT_RETRY_AFTER = Duration.ofSeconds(300);

    private static final Logger LOG = Logger.getLogger(RequestEgressJob.class);

    private final BatchJobSupport support;
    private final BatchLifecycle lifecycle;
    private final BatchCompletion completion;
    private final EgressClient egress;
    private final ApiLayerClient apiLayer;
    private final VendorBucket bucket;
    private final Clock clock;

    public RequestEgressJob(
            BatchJobSupport support,
            BatchLifecycle lifecycle,
            BatchCompletion completion,
            @RestClient EgressClient egress,
            @RestClient ApiLayerClient apiLayer,
            VendorBucket bucket,
            Clock clock) {
        this.support = support;
        this.lifecycle = lifecycle;
        this.completion = completion;
        this.egress = egress;
        this.apiLayer = apiLayer;
        this.bucket = bucket;
        this.clock = clock;
    }

    /**
     * Runs the job for one batch.
     *
     * @param request the batch, expected in {@code SCHEDULED}, the attempt and the quota wait
     */
    @Override
    @Job(name = "request-egress %0")
    public void run(RequestEgressJobRequest request) {
        try (JobLogContext ignored = JobLogContext.open(request.exportBatchId(), JobLogContext.currentJob())) {
            Optional<ExportBatch> loaded = support.loadExpecting(NAME, request.exportBatchId(), BatchState.SCHEDULED);
            if (loaded.isEmpty()) {
                return;
            }
            requestEgress(loaded.get(), request.quotaWait());
        }
    }

    private void requestEgress(ExportBatch loaded, int quotaWait) {
        ExportBatch batch = loaded;
        // The prior attempt's id, read before egressRequested overwrites it: the cancel and the reuse both name it.
        String prior =
                batch.attempt() > 1 && batch.egress() != null ? batch.egress().correlationId() : null;
        if (prior != null && cancelPriorAttempt(batch, prior)) {
            return;
        }
        if (batch.egress() == null || batch.egress().templateId() == null) {
            batch = pin(batch);
        } else {
            LOG.infof(
                    "%s keeps the pinned template %s v%s",
                    batch.id(), batch.egress().templateId(), batch.egress().templateVersion());
        }
        String correlationId = Ids.correlationId(batch.id(), batch.attempt());
        EgressExportRequest.Selection selection = prior != null
                ? EgressExportRequest.Selection.reuse(prior)
                : EgressExportRequest.Selection.savedFilter(batch.selection().filterId());
        Instant now = clock.instant();
        Sent sent = export(batch, request(batch, correlationId, selection), quotaWait);
        if (sent.outcome() == Outcome.REUSE_REFUSED) {
            selection =
                    EgressExportRequest.Selection.savedFilter(batch.selection().filterId());
            sent = export(batch, request(batch, correlationId, selection), quotaWait);
        }
        if (sent.outcome() != Outcome.ACCEPTED) {
            return;
        }
        selection = heldSelection(batch, correlationId, selection, sent);
        Instant deadlineAt = now.plus(Duration.ofHours(support.config().egress().deadlineHours()));
        lifecycle.egressRequested(
                batch,
                correlationId,
                jobReference(sent.answer(), correlationId),
                selection,
                sent.answer(),
                deadlineAt,
                now);
    }

    /**
     * The selection egress holds for this correlation id. Egress answers {@code 409} for a known id
     * before it looks at the selection, so a JobRunr re-run whose first run had its reuse refused and
     * the filter accepted gets {@code 409} for the reuse body. One status call says which it was:
     * a selection with no {@code reusedFrom} was evaluated from the filter.
     */
    private EgressExportRequest.Selection heldSelection(
            ExportBatch batch, String correlationId, EgressExportRequest.Selection sent, Sent answer) {
        if (!ALREADY_HELD.equals(answer.answer()) || sent.reuseSelectionFrom() == null) {
            return sent;
        }
        EgressSelection held = Optional.ofNullable(egress.status(batch.tenantId(), batch.tenantId(), correlationId))
                .map(EgressStatusResponse::selection)
                .orElse(null);
        if (held != null && held.reusedFrom() == null) {
            LOG.infof("%s: egress already holds %s, evaluated from the filter", batch.id(), correlationId);
            return EgressExportRequest.Selection.savedFilter(batch.selection().filterId());
        }
        return sent;
    }

    /** What one export call came to. */
    private enum Outcome {
        /** {@code 202}, or {@code 409} for a request egress already holds. */
        ACCEPTED,
        /** A reuse egress refused because the earlier list cannot be used; send the filter instead. */
        REUSE_REFUSED,
        /** Refused and dealt with here: a quota wait was scheduled or the batch failed. */
        HANDLED
    }

    /** An export call's outcome and, when accepted, egress's answer for the event. */
    private record Sent(Outcome outcome, String answer) {}

    private static EgressExportRequest request(
            ExportBatch batch, String correlationId, EgressExportRequest.Selection selection) {
        EgressDetails pinned = batch.egress();
        return new EgressExportRequest(
                EgressExportRequest.PRACTITIONER,
                batch.tenantId(),
                correlationId,
                pinned.templateId(),
                pinned.mappingsCsvUrl(),
                pinned.separator(),
                pinned.outputFormat(),
                jsonArray(pinned.rowExpansionKeys()),
                selection,
                EgressClient.INITIATOR,
                new EgressExportRequest.Destination(
                        pinned.destination().bucket(), pinned.destination().objectName()));
    }

    /**
     * Cancels the prior attempt.
     *
     * @return true when the prior attempt's complete file is already in place and this attempt
     *     has nothing left to ask for
     */
    private boolean cancelPriorAttempt(ExportBatch batch, String prior) {
        Instant now = clock.instant();
        try {
            EgressCancelResponse cancelled = egress.cancel(batch.tenantId(), batch.tenantId(), prior);
            lifecycle.priorAttemptCancel(batch, prior, true, cancelled.status(), now);
            return false;
        } catch (WebApplicationException refused) {
            int status = status(refused);
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

    /** Sends the request and sorts egress's answer into an {@link Outcome}. */
    private Sent export(ExportBatch batch, EgressExportRequest request, int quotaWait) {
        try {
            EgressExportResponse response = egress.export(batch.tenantId(), request);
            EgressExportResponse.Job job = response.jobReference();
            return new Sent(Outcome.ACCEPTED, "202 " + (job.table() != null ? job.table() : job.correlationId()));
        } catch (WebApplicationException refused) {
            if (status(refused) == 409) {
                LOG.infof("%s: egress already holds %s; treated as accepted", batch.id(), request.correlationId());
                return new Sent(Outcome.ACCEPTED, ALREADY_HELD);
            }
            return refused(batch, request, quotaWait, refused);
        }
    }

    /** An egress refusal other than {@code 409}: wait, fall back, fail the batch, or propagate for JobRunr. */
    private Sent refused(
            ExportBatch batch, EgressExportRequest request, int quotaWait, WebApplicationException refused) {
        int status = status(refused);
        EgressError error = error(refused);
        if (status == 429 && quotaWait < MAX_QUOTA_WAITS) {
            lifecycle.waitForEgressQuota(batch, quotaWait + 1, clock.instant().plus(retryAfter(refused)));
            return new Sent(Outcome.HANDLED, null);
        }
        if (status == 400 && request.selection().reuseSelectionFrom() != null && error.reuseSourceUnusable()) {
            LOG.infof(
                    "%s: egress cannot reuse %s (%s); evaluating the saved filter again",
                    batch.id(), request.selection().reuseSelectionFrom(), error.message());
            return new Sent(Outcome.REUSE_REFUSED, null);
        }
        lifecycle.egressRefused(batch, "HTTP_" + status);
        boolean final400 = status == 400;
        boolean disabled = status == 403 && EgressError.SELECTION_SOURCE_DISABLED.equals(error.error());
        if (!final400 && !disabled) {
            throw refused;
        }
        completion.failed(
                batch,
                BatchState.SCHEDULED,
                BatchCompletion.EGRESS_REFUSED,
                status + " " + error.describe(),
                List.of(),
                clock.instant(),
                false);
        return new Sent(Outcome.HANDLED, null);
    }

    private static int status(WebApplicationException refused) {
        return refused.getResponse() == null ? 0 : refused.getResponse().getStatus();
    }

    /** Egress's error body, or an empty one when the body is missing or not egress's shape. */
    private static EgressError error(WebApplicationException refused) {
        Response response = refused.getResponse();
        if (response == null || !response.hasEntity()) {
            return new EgressError(null, null, null);
        }
        try {
            EgressError body = response.readEntity(EgressError.class);
            return body == null ? new EgressError(null, null, null) : body;
        } catch (RuntimeException unreadable) {
            return new EgressError(null, null, null);
        }
    }

    private static Duration retryAfter(WebApplicationException refused) {
        String header =
                refused.getResponse() == null ? null : refused.getResponse().getHeaderString("Retry-After");
        if (header == null) {
            return DEFAULT_RETRY_AFTER;
        }
        try {
            long seconds = Long.parseLong(header.trim());
            return seconds > 0 ? Duration.ofSeconds(seconds) : DEFAULT_RETRY_AFTER;
        } catch (NumberFormatException notSeconds) {
            return DEFAULT_RETRY_AFTER;
        }
    }

    private static String jobReference(String answer, String correlationId) {
        return answer.startsWith("202 ") ? answer.substring(4) : correlationId;
    }

    static String jsonArray(List<String> values) {
        return values.stream().map(value -> "\"" + value + "\"").collect(Collectors.joining(",", "[", "]"));
    }
}
