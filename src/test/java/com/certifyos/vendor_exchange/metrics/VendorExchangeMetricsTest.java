package com.certifyos.vendor_exchange.metrics;

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

class VendorExchangeMetricsTest {

    /** The fourteen names in the design's Observability section. */
    static final List<String> DESIGN_NAMES = List.of(
            VendorExchangeMetrics.TICK_DURATION,
            VendorExchangeMetrics.TICK_SCHEDULES_DUE,
            VendorExchangeMetrics.SCHEDULES_BY_STATE,
            VendorExchangeMetrics.BATCHES_CREATED,
            VendorExchangeMetrics.SELECTION_PRACTITIONERS,
            VendorExchangeMetrics.SELECTION_PAGES,
            VendorExchangeMetrics.EGRESS_WAIT_SECONDS,
            VendorExchangeMetrics.EGRESS_FAILED,
            VendorExchangeMetrics.COMPLETION_SOURCE,
            VendorExchangeMetrics.EVENT_REJECTED,
            VendorExchangeMetrics.RECONCILE_MISMATCH,
            VendorExchangeMetrics.BATCHES_BY_STATE,
            VendorExchangeMetrics.JOBS_RETRIES,
            VendorExchangeMetrics.JOBS_FINAL_FAILURES);

    private SimpleMeterRegistry registry;
    private VendorExchangeMetrics metrics;

    @BeforeEach
    void before() {
        registry = new SimpleMeterRegistry();
        metrics = new VendorExchangeMetrics(registry);
    }

    private Set<String> names() {
        return registry.getMeters().stream()
                .map(meter -> meter.getId().getName())
                .collect(Collectors.toSet());
    }

    @Test
    void tickMetersExistBeforeAnyTick() {
        Assertions.assertTrue(names().contains(VendorExchangeMetrics.TICK_DURATION));
        Assertions.assertTrue(names().contains(VendorExchangeMetrics.TICK_SCHEDULES_DUE));
        metrics.tick(3, Duration.ofMillis(120));
        Assertions.assertEquals(
                3.0,
                registry.get(VendorExchangeMetrics.TICK_SCHEDULES_DUE).gauge().value());
        Assertions.assertEquals(
                1, registry.get(VendorExchangeMetrics.TICK_DURATION).timer().count());
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
        metrics.jobRetry("SelectJob");
        metrics.jobFinalFailure("SelectJob");
        metrics.registerGauge(VendorExchangeMetrics.BATCHES_BY_STATE, Tags.of("state", "SCHEDULED"), () -> 2);
        metrics.registerGauge(VendorExchangeMetrics.SCHEDULES_BY_STATE, Tags.of("state", "ENABLED"), () -> 1);
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
                        VendorExchangeMetrics.INGESTION_ARRIVALS_RECEIVED,
                        VendorExchangeMetrics.INGESTION_ARRIVALS_DUPLICATE_LOGGED,
                        VendorExchangeMetrics.INGESTION_ARRIVALS_UNMATCHED,
                        VendorExchangeMetrics.INGESTION_ARRIVALS_IGNORED_EVENT_TYPE,
                        VendorExchangeMetrics.INGESTION_INBOUND_BY_STATE,
                        VendorExchangeMetrics.INGESTION_INBOUND_AGE_SECONDS,
                        VendorExchangeMetrics.INGESTION_ARCHIVE_DURATION,
                        VendorExchangeMetrics.INGESTION_REGISTER_RESULT,
                        VendorExchangeMetrics.INGESTION_PARSE_WAIT_SECONDS,
                        VendorExchangeMetrics.INGESTION_COMPLETION_SOURCE,
                        VendorExchangeMetrics.INGESTION_COMPLETION_STATUS,
                        VendorExchangeMetrics.INGESTION_STAGE_ROWS,
                        VendorExchangeMetrics.INGESTION_STAGE_DOCUMENTS,
                        VendorExchangeMetrics.INGESTION_STAGE_CHUNK_DURATION,
                        VendorExchangeMetrics.INGESTION_STAGE_STREAM_429,
                        VendorExchangeMetrics.INGESTION_REVIEW_PENDING,
                        VendorExchangeMetrics.INGESTION_REVIEW_PENDING_AGE_DAYS,
                        VendorExchangeMetrics.INGESTION_REVIEW_DECISIONS,
                        VendorExchangeMetrics.INGESTION_REVIEW_CONFLICTS_409,
                        VendorExchangeMetrics.INGESTION_RELEASE_JOBS,
                        VendorExchangeMetrics.INGESTION_RELEASE_FAILED_REASON,
                        VendorExchangeMetrics.INGESTION_RELEASE_OUTCOME,
                        VendorExchangeMetrics.INGESTION_RELEASE_READBACK_SECONDS,
                        VendorExchangeMetrics.INGESTION_OV_EVENTS_NOT_OURS,
                        VendorExchangeMetrics.INGESTION_JOBS_RETRIES,
                        VendorExchangeMetrics.INGESTION_JOBS_FINAL_FAILURES));
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
                Map.entry(VendorExchangeMetrics.INGESTION_ARRIVALS_RECEIVED, Set.of("tenant", "vendor")),
                Map.entry(VendorExchangeMetrics.INGESTION_ARRIVALS_DUPLICATE_LOGGED, Set.of("tenant", "vendor")),
                Map.entry(VendorExchangeMetrics.INGESTION_ARRIVALS_UNMATCHED, Set.of("tenant", "reason")),
                Map.entry(VendorExchangeMetrics.INGESTION_ARRIVALS_IGNORED_EVENT_TYPE, Set.of("eventType")),
                Map.entry(VendorExchangeMetrics.INGESTION_REGISTER_RESULT, Set.of("result")),
                Map.entry(VendorExchangeMetrics.INGESTION_COMPLETION_SOURCE, Set.of("source")),
                Map.entry(VendorExchangeMetrics.INGESTION_COMPLETION_STATUS, Set.of("status")),
                Map.entry(VendorExchangeMetrics.INGESTION_STAGE_ROWS, Set.of("tenant", "disposition")),
                Map.entry(VendorExchangeMetrics.INGESTION_STAGE_DOCUMENTS, Set.of("tenant")),
                Map.entry(VendorExchangeMetrics.INGESTION_STAGE_STREAM_429, Set.of()),
                Map.entry(VendorExchangeMetrics.INGESTION_REVIEW_DECISIONS, Set.of("tenant", "action")),
                Map.entry(VendorExchangeMetrics.INGESTION_REVIEW_CONFLICTS_409, Set.of("tenant")),
                Map.entry(VendorExchangeMetrics.INGESTION_RELEASE_JOBS, Set.of("tenant", "result")),
                Map.entry(VendorExchangeMetrics.INGESTION_RELEASE_FAILED_REASON, Set.of("reason")),
                Map.entry(VendorExchangeMetrics.INGESTION_RELEASE_OUTCOME, Set.of("tenant", "outcome")),
                Map.entry(VendorExchangeMetrics.INGESTION_OV_EVENTS_NOT_OURS, Set.of()),
                Map.entry(VendorExchangeMetrics.INGESTION_JOBS_RETRIES, Set.of("job")),
                Map.entry(VendorExchangeMetrics.INGESTION_JOBS_FINAL_FAILURES, Set.of("job")),
                Map.entry(VendorExchangeMetrics.INGESTION_ARCHIVE_DURATION, Set.of("tenant")),
                Map.entry(VendorExchangeMetrics.INGESTION_STAGE_CHUNK_DURATION, Set.of()));

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
                registry.get(VendorExchangeMetrics.INGESTION_STAGE_ROWS)
                        .counter()
                        .count());
        Assertions.assertEquals(
                12.0,
                registry.get(VendorExchangeMetrics.INGESTION_STAGE_DOCUMENTS)
                        .counter()
                        .count());
        Assertions.assertEquals(
                1,
                registry.get(VendorExchangeMetrics.INGESTION_ARCHIVE_DURATION)
                        .timer()
                        .count());
    }

    @Test
    void tagsAreTheDesignsAndGaugesKeepTheirHolder() {
        metrics.batchCreated("org-a", "candor");
        metrics.batchCreated("org-a", "candor");
        Assertions.assertEquals(
                2.0,
                registry.get(VendorExchangeMetrics.BATCHES_CREATED)
                        .tags("tenant", "org-a", "vendor", "candor")
                        .counter()
                        .count());
        metrics.selectionPractitioners("org-a", "candor", 10);
        metrics.selectionPractitioners("org-a", "candor", 12);
        Assertions.assertEquals(
                12.0,
                registry.get(VendorExchangeMetrics.SELECTION_PRACTITIONERS)
                        .tags("tenant", "org-a", "vendor", "candor")
                        .gauge()
                        .value());
        Assertions.assertEquals(
                1,
                registry.find(VendorExchangeMetrics.SELECTION_PRACTITIONERS)
                        .gauges()
                        .size(),
                "one gauge per tag set, re-set in place");
    }
}
