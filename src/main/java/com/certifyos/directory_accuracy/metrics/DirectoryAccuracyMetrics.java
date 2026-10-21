package com.certifyos.directory_accuracy.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * The metrics from the export and ingestion designs' Observability sections, one method per name.
 * Tags are the designs' (tenant, vendor, state, reason, source, job, and for ingestion also event
 * type, result, status, disposition, action and outcome), never a practitioner, an NPI, a location
 * or a field value. Telemetry is best effort: nothing here throws into a caller's transaction.
 * Names use dots; the Prometheus endpoint renders them with underscores ({@code
 * vendor_export_tick_duration_seconds}).
 */
@ApplicationScoped
public class DirectoryAccuracyMetrics {

    /** Every metric name in the design, for the endpoint test and the runbook. */
    public static final String TICK_DURATION = "vendor_export.tick.duration";

    public static final String TICK_SCHEDULES_DUE = "vendor_export.tick.schedules_due";
    public static final String SCHEDULES_BY_STATE = "vendor_export.schedules.by_state";
    public static final String BATCHES_CREATED = "vendor_export.batches.created";
    public static final String SELECTION_PRACTITIONERS = "vendor_export.selection.practitioners";
    public static final String SELECTION_PAGES = "vendor_export.selection.pages";
    public static final String EGRESS_WAIT_SECONDS = "vendor_export.egress.wait.seconds";
    public static final String EGRESS_FAILED = "vendor_export.egress.failed";
    public static final String COMPLETION_SOURCE = "vendor_export.completion.source";
    public static final String EVENT_REJECTED = "vendor_export.event.rejected";
    public static final String RECONCILE_MISMATCH = "vendor_export.reconcile.mismatch";
    public static final String BATCHES_BY_STATE = "vendor_export.batches.by_state";
    public static final String JOBS_RETRIES = "vendor_export.jobs.retries";
    public static final String JOBS_FINAL_FAILURES = "vendor_export.jobs.final_failures";

    /**
     * Every ingestion metric name in the design. The gauges ({@code INBOUND_BY_STATE}, {@code
     * INBOUND_AGE_SECONDS}, {@code PARSE_WAIT_SECONDS}, {@code REVIEW_PENDING}, {@code
     * REVIEW_PENDING_AGE_DAYS}, {@code RELEASE_READBACK_SECONDS}) are registered through {@link
     * #registerGauge}. {@code vendor_ingestion.dlq.depth} is a Pub/Sub platform metric, named in the
     * runbook only.
     */
    public static final String INGESTION_ARRIVALS_RECEIVED = "vendor_ingestion.arrivals.received";

    public static final String INGESTION_ARRIVALS_DUPLICATE_LOGGED = "vendor_ingestion.arrivals.duplicate_logged";
    public static final String INGESTION_ARRIVALS_UNMATCHED = "vendor_ingestion.arrivals.unmatched";
    public static final String INGESTION_ARRIVALS_IGNORED_EVENT_TYPE = "vendor_ingestion.arrivals.ignored_event_type";
    public static final String INGESTION_INBOUND_BY_STATE = "vendor_ingestion.inbound.by_state";
    public static final String INGESTION_INBOUND_AGE_SECONDS = "vendor_ingestion.inbound.age_seconds";
    public static final String INGESTION_ARCHIVE_DURATION = "vendor_ingestion.archive.duration";
    public static final String INGESTION_REGISTER_RESULT = "vendor_ingestion.register.result";
    public static final String INGESTION_PARSE_WAIT_SECONDS = "vendor_ingestion.parse.wait_seconds";
    public static final String INGESTION_COMPLETION_SOURCE = "vendor_ingestion.completion.source";
    public static final String INGESTION_COMPLETION_STATUS = "vendor_ingestion.completion.status";
    public static final String INGESTION_STAGE_ROWS = "vendor_ingestion.stage.rows";
    public static final String INGESTION_STAGE_DOCUMENTS = "vendor_ingestion.stage.documents";
    public static final String INGESTION_STAGE_CHUNK_DURATION = "vendor_ingestion.stage.chunk.duration";
    public static final String INGESTION_STAGE_STREAM_429 = "vendor_ingestion.stage.stream_429";
    public static final String INGESTION_REVIEW_PENDING = "vendor_ingestion.review.pending";
    public static final String INGESTION_REVIEW_PENDING_AGE_DAYS = "vendor_ingestion.review.pending_age_days";
    public static final String INGESTION_REVIEW_DECISIONS = "vendor_ingestion.review.decisions";
    public static final String INGESTION_REVIEW_CONFLICTS_409 = "vendor_ingestion.review.conflicts_409";
    public static final String INGESTION_RELEASE_JOBS = "vendor_ingestion.release.jobs";
    public static final String INGESTION_RELEASE_FAILED_REASON = "vendor_ingestion.release.failed_reason";
    public static final String INGESTION_RELEASE_OUTCOME = "vendor_ingestion.release.outcome";
    public static final String INGESTION_RELEASE_READBACK_SECONDS = "vendor_ingestion.release.readback_seconds";
    public static final String INGESTION_OV_EVENTS_NOT_OURS = "vendor_ingestion.ov_events.not_ours";
    public static final String INGESTION_JOBS_RETRIES = "vendor_ingestion.jobs.retries";
    public static final String INGESTION_JOBS_FINAL_FAILURES = "vendor_ingestion.jobs.final_failures";

    private final MeterRegistry registry;
    private final Timer tickDuration;
    private final AtomicLong tickSchedulesDue = new AtomicLong();
    private final Map<String, AtomicLong> gaugeValues = new ConcurrentHashMap<>();

    public DirectoryAccuracyMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.tickDuration = Timer.builder(TICK_DURATION)
                .description("Wall time of one schedule tick")
                .register(registry);
        Gauge.builder(TICK_SCHEDULES_DUE, tickSchedulesDue, AtomicLong::doubleValue)
                .description("Schedules the last tick found due")
                .register(registry);
    }

    /** Records one tick. */
    public void tick(int schedulesDue, Duration duration) {
        tickSchedulesDue.set(schedulesDue);
        tickDuration.record(duration);
    }

    /** A batch was created by the tick or run-now. */
    public void batchCreated(String tenant, String vendor) {
        counter(BATCHES_CREATED, Tags.of("tenant", tenant, "vendor", vendor)).increment();
    }

    /** Practitioners the last selection registered for the tenant and vendor. */
    public void selectionPractitioners(String tenant, String vendor, long count) {
        gauge(SELECTION_PRACTITIONERS, Tags.of("tenant", tenant, "vendor", vendor))
                .set(count);
    }

    /** One api-layer page fetched. */
    public void selectionPage(String tenant) {
        counter(SELECTION_PAGES, Tags.of("tenant", tenant)).increment();
    }

    /** Seconds between the egress request and its completion, for the tenant's last batch. */
    public void egressWaitSeconds(String tenant, long seconds) {
        gauge(EGRESS_WAIT_SECONDS, Tags.of("tenant", tenant)).set(seconds);
    }

    /** An egress attempt failed for a reason ({@code EGRESS_DID_NOT_FINISH}, {@code FILE_NOT_FOUND}, ...). */
    public void egressFailed(String tenant, String reason) {
        counter(EGRESS_FAILED, Tags.of("tenant", tenant, "reason", reason)).increment();
    }

    /** How a completion was learned: {@code EVENT} or {@code DEADLINE}. */
    public void completion(String source) {
        counter(COMPLETION_SOURCE, Tags.of("source", source)).increment();
    }

    /** A push event was rejected for a reason. */
    public void eventRejected(String reason) {
        counter(EVENT_REJECTED, Tags.of("reason", reason)).increment();
    }

    /** Egress's counts disagreed with the registry. */
    public void reconcileMismatch(String tenant) {
        counter(RECONCILE_MISMATCH, Tags.of("tenant", tenant)).increment();
    }

    /** A job attempt failed and will be retried. */
    public void jobRetry(String job) {
        counter(JOBS_RETRIES, Tags.of("job", job)).increment();
    }

    /** A job exhausted its retries. */
    public void jobFinalFailure(String job) {
        counter(JOBS_FINAL_FAILURES, Tags.of("job", job)).increment();
    }

    /** A vendor upload arrived and was claimed for its batch. */
    public void arrivalReceived(String tenant, String vendor) {
        counter(INGESTION_ARRIVALS_RECEIVED, Tags.of("tenant", tenant, "vendor", vendor))
                .increment();
    }

    /** A second upload for a batch was logged and not processed. */
    public void arrivalDuplicateLogged(String tenant, String vendor) {
        counter(INGESTION_ARRIVALS_DUPLICATE_LOGGED, Tags.of("tenant", tenant, "vendor", vendor))
                .increment();
    }

    /** An upload matched no batch, for a reason. */
    public void arrivalUnmatched(String tenant, String reason) {
        counter(INGESTION_ARRIVALS_UNMATCHED, Tags.of("tenant", tenant, "reason", reason))
                .increment();
    }

    /** A storage notification of a type the handler does not act on. */
    public void arrivalIgnoredEventType(String eventType) {
        counter(INGESTION_ARRIVALS_IGNORED_EVENT_TYPE, Tags.of("eventType", eventType))
                .increment();
    }

    /** Wall time of one archive copy. */
    public void archiveDuration(String tenant, Duration duration) {
        registry.timer(INGESTION_ARCHIVE_DURATION, Tags.of("tenant", tenant)).record(duration);
    }

    /** The file-ingestion register answer: {@code 201}, {@code 200}, {@code 4xx} or {@code 5xx}. */
    public void registerResult(String result) {
        counter(INGESTION_REGISTER_RESULT, Tags.of("result", result)).increment();
    }

    /** How a parse completion was learned: {@code EVENT} or {@code DEADLINE}. */
    public void completionSource(String source) {
        counter(INGESTION_COMPLETION_SOURCE, Tags.of("source", source)).increment();
    }

    /** How a parse ended: {@code COMPLETED}, {@code PARTIAL} or {@code INVALID}. */
    public void completionStatus(String status) {
        counter(INGESTION_COMPLETION_STATUS, Tags.of("status", status)).increment();
    }

    /** Rows staged with one disposition. */
    public void stageRows(String tenant, String disposition, long count) {
        counter(INGESTION_STAGE_ROWS, Tags.of("tenant", tenant, "disposition", disposition))
                .increment(count);
    }

    /** Field recommendation documents written by staging. */
    public void stageDocuments(String tenant, long count) {
        counter(INGESTION_STAGE_DOCUMENTS, Tags.of("tenant", tenant)).increment(count);
    }

    /** Wall time of one staging chunk. */
    public void stageChunkDuration(Duration duration) {
        registry.timer(INGESTION_STAGE_CHUNK_DURATION).record(duration);
    }

    /** The row stream answered 429. */
    public void stageStream429() {
        counter(INGESTION_STAGE_STREAM_429, Tags.empty()).increment();
    }

    /** A reviewer decided a field: the action taken. */
    public void reviewDecision(String tenant, String action) {
        counter(INGESTION_REVIEW_DECISIONS, Tags.of("tenant", tenant, "action", action))
                .increment();
    }

    /** A decision lost a version race and was answered 409. */
    public void reviewConflict(String tenant) {
        counter(INGESTION_REVIEW_CONFLICTS_409, Tags.of("tenant", tenant)).increment();
    }

    /** A release job ended: {@code RELEASED} or {@code RELEASE_FAILED}. */
    public void releaseJob(String tenant, String result) {
        counter(INGESTION_RELEASE_JOBS, Tags.of("tenant", tenant, "result", result))
                .increment();
    }

    /** A release failed for a reason. */
    public void releaseFailedReason(String reason) {
        counter(INGESTION_RELEASE_FAILED_REASON, Tags.of("reason", reason)).increment();
    }

    /** What a release did in the directory: {@code WON}, {@code OUTRANKED}, {@code NOT_APPLIED} or {@code UNKNOWN}. */
    public void releaseOutcome(String tenant, String outcome) {
        counter(INGESTION_RELEASE_OUTCOME, Tags.of("tenant", tenant, "outcome", outcome))
                .increment();
    }

    /** A survivorship {@code mdm.slice.ov_generated} event that matched no release of ours. */
    public void ovEventNotOurs() {
        counter(INGESTION_OV_EVENTS_NOT_OURS, Tags.empty()).increment();
    }

    /** An ingestion job attempt failed and will be retried. */
    public void ingestionJobRetry(String job) {
        counter(INGESTION_JOBS_RETRIES, Tags.of("job", job)).increment();
    }

    /** An ingestion job exhausted its retries. */
    public void ingestionJobFinalFailure(String job) {
        counter(INGESTION_JOBS_FINAL_FAILURES, Tags.of("job", job)).increment();
    }

    /**
     * Registers a gauge whose value is read on every scrape. Used by the export lane for the
     * by-state gauges and by the ingestion lane for its gauges, whose values come from the
     * database.
     *
     * @param name the metric name
     * @param tags its tags
     * @param value the supplier, read on scrape
     */
    public void registerGauge(String name, Tags tags, Supplier<Number> value) {
        Gauge.builder(name, value::get).tags(tags).register(registry);
    }

    private Counter counter(String name, Tags tags) {
        return registry.counter(name, tags);
    }

    private AtomicLong gauge(String name, Tags tags) {
        return gaugeValues.computeIfAbsent(name + tags, key -> registry.gauge(name, tags, new AtomicLong()));
    }
}
