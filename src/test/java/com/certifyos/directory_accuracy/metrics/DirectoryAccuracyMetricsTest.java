package com.certifyos.directory_accuracy.metrics;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DirectoryAccuracyMetricsTest {

    /** The fourteen names in the design's Observability section. */
    static final List<String> DESIGN_NAMES = List.of(
            DirectoryAccuracyMetrics.TICK_DURATION,
            DirectoryAccuracyMetrics.TICK_SCHEDULES_DUE,
            DirectoryAccuracyMetrics.SCHEDULES_BY_STATE,
            DirectoryAccuracyMetrics.BATCHES_CREATED,
            DirectoryAccuracyMetrics.SELECTION_PRACTITIONERS,
            DirectoryAccuracyMetrics.SELECTION_PAGES,
            DirectoryAccuracyMetrics.EGRESS_WAIT_SECONDS,
            DirectoryAccuracyMetrics.EGRESS_FAILED,
            DirectoryAccuracyMetrics.COMPLETION_SOURCE,
            DirectoryAccuracyMetrics.EVENT_REJECTED,
            DirectoryAccuracyMetrics.RECONCILE_MISMATCH,
            DirectoryAccuracyMetrics.BATCHES_BY_STATE,
            DirectoryAccuracyMetrics.JOBS_RETRIES,
            DirectoryAccuracyMetrics.JOBS_FINAL_FAILURES);

    private SimpleMeterRegistry registry;
    private DirectoryAccuracyMetrics metrics;

    @BeforeEach
    void before() {
        registry = new SimpleMeterRegistry();
        metrics = new DirectoryAccuracyMetrics(registry);
    }

    private Set<String> names() {
        return registry.getMeters().stream()
                .map(meter -> meter.getId().getName())
                .collect(Collectors.toSet());
    }

    @Test
    void tickMetersExistBeforeAnyTick() {
        Assertions.assertTrue(names().contains(DirectoryAccuracyMetrics.TICK_DURATION));
        Assertions.assertTrue(names().contains(DirectoryAccuracyMetrics.TICK_SCHEDULES_DUE));
        metrics.tick(3, Duration.ofMillis(120));
        Assertions.assertEquals(
                3.0,
                registry.get(DirectoryAccuracyMetrics.TICK_SCHEDULES_DUE)
                        .gauge()
                        .value());
        Assertions.assertEquals(
                1, registry.get(DirectoryAccuracyMetrics.TICK_DURATION).timer().count());
    }

    @Test
    void everyDesignNameIsReachable() {
        metrics.batchCreated("org-a", "candor");
        metrics.selectionPractitioners("org-a", "candor", 1240);
        metrics.selectionPage("org-a");
        metrics.egressWaitSeconds("org-a", 712);
        metrics.egressFailed("org-a", "EGRESS_DID_NOT_FINISH");
        metrics.completion("EVENT");
        metrics.eventRejected("UNKNOWN_CORRELATION");
        metrics.reconcileMismatch("org-a");
        metrics.jobRetry("RequestEgressJob");
        metrics.jobFinalFailure("RequestEgressJob");
        metrics.registerGauge(DirectoryAccuracyMetrics.BATCHES_BY_STATE, Tags.of("state", "SCHEDULED"), () -> 2);
        metrics.registerGauge(DirectoryAccuracyMetrics.SCHEDULES_BY_STATE, Tags.of("state", "ENABLED"), () -> 1);
        Set<String> registered = names();
        for (String name : DESIGN_NAMES) {
            Assertions.assertTrue(registered.contains(name), "missing " + name);
        }
        Assertions.assertEquals(DESIGN_NAMES.size(), registered.size(), "nothing beyond the design's names");
    }

    @Test
    void ingestionNamesAreTheDesigns() {
        Assertions.assertEquals(
                List.of(
                        "vendor_ingestion.arrivals.received",
                        "vendor_ingestion.arrivals.duplicate_logged",
                        "vendor_ingestion.arrivals.unmatched",
                        "vendor_ingestion.arrivals.ignored_event_type",
                        "vendor_ingestion.inbound.by_state",
                        "vendor_ingestion.inbound.age_seconds",
                        "vendor_ingestion.archive.duration",
                        "vendor_ingestion.register.result",
                        "vendor_ingestion.parse.wait_seconds",
                        "vendor_ingestion.completion.source",
                        "vendor_ingestion.completion.status",
                        "vendor_ingestion.stage.rows",
                        "vendor_ingestion.stage.documents",
                        "vendor_ingestion.stage.chunk.duration",
                        "vendor_ingestion.stage.stream_429",
                        "vendor_ingestion.review.pending",
                        "vendor_ingestion.review.pending_age_days",
                        "vendor_ingestion.review.decisions",
                        "vendor_ingestion.review.conflicts_409",
                        "vendor_ingestion.release.jobs",
                        "vendor_ingestion.release.failed_reason",
                        "vendor_ingestion.release.outcome",
                        "vendor_ingestion.release.readback_seconds",
                        "vendor_ingestion.ov_events.not_ours",
                        "vendor_ingestion.jobs.retries",
                        "vendor_ingestion.jobs.final_failures"),
                List.of(
                        DirectoryAccuracyMetrics.INGESTION_ARRIVALS_RECEIVED,
                        DirectoryAccuracyMetrics.INGESTION_ARRIVALS_DUPLICATE_LOGGED,
                        DirectoryAccuracyMetrics.INGESTION_ARRIVALS_UNMATCHED,
                        DirectoryAccuracyMetrics.INGESTION_ARRIVALS_IGNORED_EVENT_TYPE,
                        DirectoryAccuracyMetrics.INGESTION_INBOUND_BY_STATE,
                        DirectoryAccuracyMetrics.INGESTION_INBOUND_AGE_SECONDS,
                        DirectoryAccuracyMetrics.INGESTION_ARCHIVE_DURATION,
                        DirectoryAccuracyMetrics.INGESTION_REGISTER_RESULT,
                        DirectoryAccuracyMetrics.INGESTION_PARSE_WAIT_SECONDS,
                        DirectoryAccuracyMetrics.INGESTION_COMPLETION_SOURCE,
                        DirectoryAccuracyMetrics.INGESTION_COMPLETION_STATUS,
                        DirectoryAccuracyMetrics.INGESTION_STAGE_ROWS,
                        DirectoryAccuracyMetrics.INGESTION_STAGE_DOCUMENTS,
                        DirectoryAccuracyMetrics.INGESTION_STAGE_CHUNK_DURATION,
                        DirectoryAccuracyMetrics.INGESTION_STAGE_STREAM_429,
                        DirectoryAccuracyMetrics.INGESTION_REVIEW_PENDING,
                        DirectoryAccuracyMetrics.INGESTION_REVIEW_PENDING_AGE_DAYS,
                        DirectoryAccuracyMetrics.INGESTION_REVIEW_DECISIONS,
                        DirectoryAccuracyMetrics.INGESTION_REVIEW_CONFLICTS_409,
                        DirectoryAccuracyMetrics.INGESTION_RELEASE_JOBS,
                        DirectoryAccuracyMetrics.INGESTION_RELEASE_FAILED_REASON,
                        DirectoryAccuracyMetrics.INGESTION_RELEASE_OUTCOME,
                        DirectoryAccuracyMetrics.INGESTION_RELEASE_READBACK_SECONDS,
                        DirectoryAccuracyMetrics.INGESTION_OV_EVENTS_NOT_OURS,
                        DirectoryAccuracyMetrics.INGESTION_JOBS_RETRIES,
                        DirectoryAccuracyMetrics.INGESTION_JOBS_FINAL_FAILURES));
    }

    @Test
    void ingestionNamesRegister() {
        Set<String> before = names();
        metrics.arrivalReceived("org-a", "candor");
        metrics.arrivalDuplicateLogged("org-a", "candor");
        metrics.arrivalUnmatched("org-a", "BATCH_NOT_DELIVERED");
        metrics.arrivalIgnoredEventType("OBJECT_DELETE");
        metrics.registerResult("201");
        metrics.completionSource("EVENT");
        metrics.completionStatus("PARTIAL");
        metrics.stageRows("org-a", "STAGED", 40);
        metrics.stageDocuments("org-a", 12);
        metrics.stageStream429();
        metrics.reviewDecision("org-a", "APPROVE");
        metrics.reviewConflict("org-a");
        metrics.releaseJob("org-a", "RELEASED");
        metrics.releaseFailedReason("RANKING_MISSING");
        metrics.releaseOutcome("org-a", "WON");
        metrics.ovEventNotOurs();
        metrics.ingestionJobRetry("StageJob");
        metrics.ingestionJobFinalFailure("StageJob");
        metrics.archiveDuration("org-a", Duration.ofMillis(800));
        metrics.stageChunkDuration(Duration.ofMillis(50));

        Map<String, Set<String>> expected = Map.ofEntries(
                Map.entry(DirectoryAccuracyMetrics.INGESTION_ARRIVALS_RECEIVED, Set.of("tenant", "vendor")),
                Map.entry(DirectoryAccuracyMetrics.INGESTION_ARRIVALS_DUPLICATE_LOGGED, Set.of("tenant", "vendor")),
                Map.entry(DirectoryAccuracyMetrics.INGESTION_ARRIVALS_UNMATCHED, Set.of("tenant", "reason")),
                Map.entry(DirectoryAccuracyMetrics.INGESTION_ARRIVALS_IGNORED_EVENT_TYPE, Set.of("eventType")),
                Map.entry(DirectoryAccuracyMetrics.INGESTION_REGISTER_RESULT, Set.of("result")),
                Map.entry(DirectoryAccuracyMetrics.INGESTION_COMPLETION_SOURCE, Set.of("source")),
                Map.entry(DirectoryAccuracyMetrics.INGESTION_COMPLETION_STATUS, Set.of("status")),
                Map.entry(DirectoryAccuracyMetrics.INGESTION_STAGE_ROWS, Set.of("tenant", "disposition")),
                Map.entry(DirectoryAccuracyMetrics.INGESTION_STAGE_DOCUMENTS, Set.of("tenant")),
                Map.entry(DirectoryAccuracyMetrics.INGESTION_STAGE_STREAM_429, Set.of()),
                Map.entry(DirectoryAccuracyMetrics.INGESTION_REVIEW_DECISIONS, Set.of("tenant", "action")),
                Map.entry(DirectoryAccuracyMetrics.INGESTION_REVIEW_CONFLICTS_409, Set.of("tenant")),
                Map.entry(DirectoryAccuracyMetrics.INGESTION_RELEASE_JOBS, Set.of("tenant", "result")),
                Map.entry(DirectoryAccuracyMetrics.INGESTION_RELEASE_FAILED_REASON, Set.of("reason")),
                Map.entry(DirectoryAccuracyMetrics.INGESTION_RELEASE_OUTCOME, Set.of("tenant", "outcome")),
                Map.entry(DirectoryAccuracyMetrics.INGESTION_OV_EVENTS_NOT_OURS, Set.of()),
                Map.entry(DirectoryAccuracyMetrics.INGESTION_JOBS_RETRIES, Set.of("job")),
                Map.entry(DirectoryAccuracyMetrics.INGESTION_JOBS_FINAL_FAILURES, Set.of("job")),
                Map.entry(DirectoryAccuracyMetrics.INGESTION_ARCHIVE_DURATION, Set.of("tenant")),
                Map.entry(DirectoryAccuracyMetrics.INGESTION_STAGE_CHUNK_DURATION, Set.of()));

        expected.forEach((name, tagKeys) -> {
            List<Meter> found = List.copyOf(registry.find(name).meters());
            Assertions.assertEquals(1, found.size(), "one meter named " + name);
            Set<String> keys =
                    found.get(0).getId().getTags().stream().map(Tag::getKey).collect(Collectors.toSet());
            Assertions.assertEquals(tagKeys, keys, name + " tags");
        });
        Set<String> added =
                names().stream().filter(name -> !before.contains(name)).collect(Collectors.toSet());
        Assertions.assertEquals(expected.keySet(), added, "nothing beyond the design's names");
        for (Meter meter : registry.getMeters()) {
            for (Tag tag : meter.getId().getTags()) {
                Assertions.assertNotEquals("npi", tag.getKey(), meter.getId().getName() + " carries no practitioner");
            }
        }
        Assertions.assertEquals(
                40.0,
                registry.get(DirectoryAccuracyMetrics.INGESTION_STAGE_ROWS)
                        .counter()
                        .count());
        Assertions.assertEquals(
                12.0,
                registry.get(DirectoryAccuracyMetrics.INGESTION_STAGE_DOCUMENTS)
                        .counter()
                        .count());
        Assertions.assertEquals(
                1,
                registry.get(DirectoryAccuracyMetrics.INGESTION_ARCHIVE_DURATION)
                        .timer()
                        .count());
    }

    @Test
    void tagsAreTheDesignsAndGaugesKeepTheirHolder() {
        metrics.batchCreated("org-a", "candor");
        metrics.batchCreated("org-a", "candor");
        Assertions.assertEquals(
                2.0,
                registry.get(DirectoryAccuracyMetrics.BATCHES_CREATED)
                        .tags("tenant", "org-a", "vendor", "candor")
                        .counter()
                        .count());
        metrics.selectionPractitioners("org-a", "candor", 10);
        metrics.selectionPractitioners("org-a", "candor", 12);
        Assertions.assertEquals(
                12.0,
                registry.get(DirectoryAccuracyMetrics.SELECTION_PRACTITIONERS)
                        .tags("tenant", "org-a", "vendor", "candor")
                        .gauge()
                        .value());
        Assertions.assertEquals(
                1,
                registry.find(DirectoryAccuracyMetrics.SELECTION_PRACTITIONERS)
                        .gauges()
                        .size(),
                "one gauge per tag set, re-set in place");
    }
}
