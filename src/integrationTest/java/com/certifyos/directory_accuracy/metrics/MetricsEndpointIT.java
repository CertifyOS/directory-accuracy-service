package com.certifyos.directory_accuracy.metrics;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** The Prometheus endpoint lists every design name once the service has recorded each at least once. */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(ApiTestProfile.class)
class MetricsEndpointIT {

    @Inject
    DirectoryAccuracyMetrics metrics;

    @Test
    void byStateGaugesExistFromBootForEveryState() {
        String body =
                RestAssured.get("/q/metrics").then().statusCode(200).extract().asString();
        for (String state : List.of(
                "SCHEDULED",
                "NPIS_SELECTED",
                "EGRESS_REQUESTED",
                "EGRESS_COMPLETED",
                "DELIVERED",
                "EMPTY",
                "FAILED",
                "SUPERSEDED")) {
            Assertions.assertTrue(
                    body.contains("vendor_export_batches_by_state{state=\"" + state + "\""), "missing state " + state);
        }
        Assertions.assertTrue(body.contains("vendor_export_schedules_by_state{state=\"ENABLED\""));
        Assertions.assertTrue(body.contains("vendor_export_schedules_by_state{state=\"DISABLED\""));
        Assertions.assertTrue(body.contains("vendor_export_tick_schedules_due"));
        Assertions.assertTrue(body.contains("vendor_export_tick_duration_seconds"));
    }

    @Test
    void everyDesignNameAppearsOnceRecorded() {
        metrics.tick(1, Duration.ofMillis(5));
        metrics.batchCreated("org-m", "candor");
        metrics.selectionPractitioners("org-m", "candor", 3);
        metrics.selectionPage("org-m");
        metrics.egressWaitSeconds("org-m", 60);
        metrics.egressFailed("org-m", "FILE_NOT_FOUND");
        metrics.completion("DEADLINE");
        metrics.eventRejected("BAD_SCHEMA");
        metrics.reconcileMismatch("org-m");
        metrics.jobRetry("FinishJob");
        metrics.jobFinalFailure("FinishJob");

        String body =
                RestAssured.get("/q/metrics").then().statusCode(200).extract().asString();
        for (String name : List.of(
                "vendor_export_tick_duration_seconds",
                "vendor_export_tick_schedules_due",
                "vendor_export_schedules_by_state",
                "vendor_export_batches_created_total{tenant=\"org-m\",vendor=\"candor\"}",
                "vendor_export_selection_practitioners{tenant=\"org-m\",vendor=\"candor\"}",
                "vendor_export_selection_pages_total{tenant=\"org-m\"}",
                "vendor_export_egress_wait_seconds{tenant=\"org-m\"}",
                "vendor_export_egress_failed_total{reason=\"FILE_NOT_FOUND\",tenant=\"org-m\"}",
                "vendor_export_completion_source_total{source=\"DEADLINE\"}",
                "vendor_export_event_rejected_total{reason=\"BAD_SCHEMA\"}",
                "vendor_export_reconcile_mismatch_total{tenant=\"org-m\"}",
                "vendor_export_batches_by_state",
                "vendor_export_jobs_retries_total{job=\"FinishJob\"}",
                "vendor_export_jobs_final_failures_total{job=\"FinishJob\"}")) {
            Assertions.assertTrue(body.contains(name), "missing " + name);
        }
        // No label carries a practitioner identifier: neither a label named npi nor a ten-digit value.
        // (The HTTP request metrics legitimately carry the /npis route in their uri label.)
        Assertions.assertFalse(body.contains("npi=\""), "no label named npi");
        Assertions.assertFalse(
                java.util.regex.Pattern.compile("=\"\\d{10}\"").matcher(body).find(), "no ten-digit label value");
    }
}
