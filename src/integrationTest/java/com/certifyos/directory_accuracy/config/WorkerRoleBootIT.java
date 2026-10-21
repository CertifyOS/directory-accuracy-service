package com.certifyos.directory_accuracy.config;

import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.WorkerTestProfile;
import com.certifyos.directory_accuracy.export.jobs.ReconcilerJob;
import com.certifyos.directory_accuracy.ingestion.jobs.InboundReconcilerJob;
import com.certifyos.directory_accuracy.ingestion.jobs.ReleaseReconcilerJob;
import com.mongodb.client.MongoClient;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jobrunr.jobs.RecurringJob;
import org.jobrunr.storage.StorageProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** The service boots as the worker role: the JobRunr background server runs and registers itself. */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(WorkerTestProfile.class)
public class WorkerRoleBootIT {

    @Inject
    StorageProvider storage;

    @Inject
    MongoClient mongo;

    @Test
    void workerProfileFlagsAreSet() {
        Assertions.assertTrue(
                ConfigProvider.getConfig().getValue("quarkus.jobrunr.background-job-server.enabled", Boolean.class));
        Assertions.assertFalse(
                ConfigProvider.getConfig().getValue("quarkus.jobrunr.dashboard.enabled", Boolean.class),
                "the test profile must keep the dashboard port closed");
    }

    @Test
    void backgroundJobServerRegistersItsHeartbeat() {
        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> !storage.getBackgroundJobServers()
                .isEmpty());
    }

    @Test
    void jobRunrCollectionsCarryThePrefixTheDesignDocNames() {
        // Without table-prefix JobRunr 8.8.2 creates jobs, recurring_jobs, background_job_servers,
        // metadata and migrations (file-ingestion finding 72). The prefix in application.properties
        // gives them the jobrunr_ names the design doc and the runbook use.
        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> !storage.getBackgroundJobServers()
                .isEmpty());
        List<String> names =
                mongo.getDatabase("vendor_exchange").listCollectionNames().into(new java.util.ArrayList<>());
        Assertions.assertTrue(names.contains("jobrunr_background_job_servers"), "collections: " + names);
        Assertions.assertTrue(names.contains("jobrunr_metadata"), "collections: " + names);
        Assertions.assertTrue(
                names.stream().filter(n -> !n.startsWith("jobrunr_")).noneMatch(n -> n.contains("job")),
                "no unprefixed JobRunr collection expected: " + names);
    }

    @Test
    void bothReconcilersAreRegisteredWithTheirCrons() {
        List<RecurringJob> recurring = storage.getRecurringJobs();
        RecurringJob inboundReconciler = recurring.stream()
                .filter(job -> job.getId().equals(InboundReconcilerJob.RECURRING_ID))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no " + InboundReconcilerJob.RECURRING_ID + " in " + recurring));
        RecurringJob exportReconciler = recurring.stream()
                .filter(job -> job.getId().equals(ReconcilerJob.RECURRING_ID))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no " + ReconcilerJob.RECURRING_ID + " in " + recurring));
        RecurringJob releaseReconciler = recurring.stream()
                .filter(job -> job.getId().equals(ReleaseReconcilerJob.RECURRING_ID))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no " + ReleaseReconcilerJob.RECURRING_ID + " in " + recurring));
        Assertions.assertEquals(ReleaseReconcilerJob.CRON, releaseReconciler.getScheduleExpression());
        Assertions.assertEquals(InboundReconcilerJob.CRON, inboundReconciler.getScheduleExpression());
        Assertions.assertEquals(ReconcilerJob.CRON, exportReconciler.getScheduleExpression());
        Assertions.assertEquals(
                1,
                recurring.stream()
                        .filter(job -> job.getId().equals(InboundReconcilerJob.RECURRING_ID))
                        .count(),
                "registered once");
    }
}
