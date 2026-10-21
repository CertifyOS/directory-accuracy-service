package com.certifyos.directory_accuracy.ingestion.jobs;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.ingestion.release.ReleaseFixtures;
import com.certifyos.directory_accuracy.ingestion.staging.FieldRecommendation;
import com.certifyos.directory_accuracy.ingestion.staging.FieldRecommendationRepository;
import com.certifyos.directory_accuracy.ingestion.staging.ReviewStatus;
import com.certifyos.directory_accuracy.persistence.Collections;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.storage.StorageProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The release reconciler: a release left {@code RELEASE_REQUESTED} with no job (the request's
 * post-commit enqueue failed) gets its job enqueued again; a fresh request does not.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(ApiTestProfile.class)
class ReleaseReconcilerJobIT {

    @Inject
    ReleaseReconcilerJob reconciler;

    @Inject
    Collections collections;

    @Inject
    StorageProvider storage;

    @Inject
    FieldRecommendationRepository repository;

    private static FieldRecommendation requested(String tenant, String practitionerId, String field, String releaseId) {
        return ReleaseFixtures.field(
                tenant,
                practitionerId,
                "1003002890",
                null,
                field,
                "v-" + field,
                ReviewStatus.RELEASE_REQUESTED,
                3L,
                ReleaseFixtures.requested(releaseId, "CorePractitioner"));
    }

    @Test
    void aStrandedRequestIsEnqueuedAgainAndAFreshOneIsNot() {
        String tenant = "relrec-stranded";
        List<FieldRecommendation> docs = ReleaseFixtures.insert(
                collections,
                requested(tenant, "cert-rr-1", "first_name", "rel-rr-stranded"),
                requested(tenant, "cert-rr-2", "last_name", "rel-rr-fresh"));
        // the second release was requested a moment ago; the first has sat since the fixture's time
        collections
                .fieldRecommendations()
                .updateOne(Filters.eq("_id", docs.get(1).id()), Updates.set("updatedAt", Instant.now()));
        Instant before = Instant.now();

        List<UUID> enqueued = reconciler.run();

        Instant after = Instant.now();
        UUID stranded = find("rel-rr-stranded", before, after);
        Assertions.assertTrue(enqueued.contains(stranded), "the stranded release has a job again");
        Job job = storage.getJobById(stranded);
        Assertions.assertEquals(StateName.ENQUEUED, job.getState());
        Assertions.assertEquals(ReleaseJob.class.getName(), job.getJobDetails().getClassName());
        Assertions.assertNull(find("rel-rr-fresh", before, after), "a fresh request is left to its own job");

        Instant touched =
                repository.find(tenant, docs.get(0).id()).orElseThrow().updatedAt();
        Assertions.assertEquals(
                stranded,
                IngestionJobEnqueuer.releaseReconciledId("rel-rr-stranded", touched),
                "the pass touched the fields with the second its job id is keyed on");
        List<UUID> again = reconciler.run();
        Assertions.assertTrue(
                again.stream().noneMatch(id -> id.equals(stranded) || isRelease(id, "rel-rr-stranded")),
                "a second pass does not stack another job for the release");
        enqueued.forEach(storage::deletePermanently);
        again.forEach(storage::deletePermanently);
    }

    private boolean isRelease(UUID id, String releaseId) {
        Job job = storage.getJobById(id);
        return job.getJobDetails().getJobParameters().stream()
                .anyMatch(parameter -> String.valueOf(parameter.getObject()).contains(releaseId));
    }

    private UUID find(String releaseId, Instant before, Instant after) {
        for (long second = before.getEpochSecond(); second <= after.getEpochSecond(); second++) {
            UUID id = IngestionJobEnqueuer.releaseReconciledId(releaseId, Instant.ofEpochSecond(second));
            try {
                storage.getJobById(id);
                return id;
            } catch (org.jobrunr.storage.JobNotFoundException notThisSecond) {
                // enqueued in a later second of the pass
            }
        }
        return null;
    }
}
