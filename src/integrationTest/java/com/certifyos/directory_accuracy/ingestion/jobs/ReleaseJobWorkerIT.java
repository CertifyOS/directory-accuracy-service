package com.certifyos.directory_accuracy.ingestion.jobs;

import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.WorkerTestProfile;
import com.certifyos.directory_accuracy.audit.AuditEvent;
import com.certifyos.directory_accuracy.audit.AuditEventType;
import com.certifyos.directory_accuracy.audit.AuditRepository;
import com.certifyos.directory_accuracy.ingestion.release.ReleaseFixtures;
import com.certifyos.directory_accuracy.ingestion.staging.FieldRecommendation;
import com.certifyos.directory_accuracy.ingestion.staging.FieldRecommendationRepository;
import com.certifyos.directory_accuracy.ingestion.staging.ReviewStatus;
import com.certifyos.directory_accuracy.persistence.Collections;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.awaitility.Awaitility;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.storage.StorageProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The release job on the worker role, through JobRunr: the ingestion final-failure filter is
 * registered on the job server, so a release whose job exhausts its retries ends {@code
 * RELEASE_FAILED EXHAUSTED}.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(WorkerTestProfile.class)
class ReleaseJobWorkerIT {

    @Inject
    IngestionJobEnqueuer enqueuer;

    @Inject
    StorageProvider storage;

    @Inject
    FieldRecommendationRepository repository;

    @Inject
    AuditRepository audit;

    @Inject
    Collections collections;

    @Test
    void exhaustedReleaseJobEndsReleaseFailed() {
        String tenant = "relworker-exhausted";
        FieldRecommendation doc = ReleaseFixtures.insert(
                        collections,
                        ReleaseFixtures.field(
                                tenant,
                                "cert-1",
                                "1003002890",
                                null,
                                "first_name",
                                "Dan",
                                ReviewStatus.RELEASE_REQUESTED,
                                3L,
                                ReleaseFixtures.requested("rel-worker-exhausted", "CorePractitioner")))
                .get(0);

        // The worker profile points the DAL at localhost:0 and CI has no Google credentials, so the
        // ranking read fails on every run; with job-retries=1 the job runs twice and stays FAILED.
        UUID jobId = enqueuer.release(tenant, "cert-1", "rel-worker-exhausted");
        Awaitility.await()
                .atMost(Duration.ofSeconds(90))
                .until(() -> storage.getJobById(jobId).getState() == StateName.FAILED);
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .until(() ->
                        repository.find(tenant, doc.id()).orElseThrow().reviewStatus() == ReviewStatus.RELEASE_FAILED);

        FieldRecommendation after = repository.find(tenant, doc.id()).orElseThrow();
        Assertions.assertTrue(after.goldenRecordUpdate().error().startsWith("EXHAUSTED"));
        List<AuditEvent> trail = audit.trailForField(tenant, doc.id(), 10);
        Assertions.assertEquals(
                List.of(AuditEventType.FIELD_RELEASE_FAILED),
                trail.stream().map(AuditEvent::type).toList());
        Assertions.assertEquals("EXHAUSTED", trail.get(0).detail().get("reason"));
        Assertions.assertEquals(2, ((Number) trail.get(0).detail().get("jobAttempts")).intValue());
        Assertions.assertEquals(jobId.toString(), trail.get(0).jobId());
    }
}
