package com.certifyos.vendor_exchange.ingestion.jobs;

import com.certifyos.vendor_exchange.ApiTestProfile;
import com.certifyos.vendor_exchange.MongoResource;
import com.certifyos.vendor_exchange.audit.AuditEvent;
import com.certifyos.vendor_exchange.audit.AuditEventType;
import com.certifyos.vendor_exchange.audit.AuditRepository;
import com.certifyos.vendor_exchange.ingestion.release.ReleaseFixtures;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendationRepository;
import com.certifyos.vendor_exchange.ingestion.staging.ReviewStatus;
import com.certifyos.vendor_exchange.persistence.Collections;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.JobDetails;
import org.jobrunr.jobs.lambdas.JobRequest;
import org.jobrunr.jobs.states.EnqueuedState;
import org.jobrunr.jobs.states.FailedState;
import org.jobrunr.jobs.states.JobState;
import org.jobrunr.jobs.states.ScheduledState;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The release branch of the ingestion final-failure filter: a release job whose retries ran out moves
 * the release's fields to {@code RELEASE_FAILED} with reason {@code EXHAUSTED}. The job is built the
 * way JobRunr hands it to an apply-state filter once {@code RetryFilter} has given up (the last state
 * is {@code FAILED}). Every tenant here starts with {@code relfail-}.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(ApiTestProfile.class)
class IngestionFinalFailureFilterIT {

    @Inject
    IngestionFinalFailureFilter filter;

    @Inject
    FieldRecommendationRepository repository;

    @Inject
    AuditRepository audit;

    @Inject
    Collections collections;

    private static Job jobIn(JobRequest request, JobState... states) {
        return new Job(
                UUID.randomUUID(),
                0,
                new JobDetails(request),
                new ArrayList<>(List.of(states)),
                new ConcurrentHashMap<>());
    }

    private static FailedState failed() {
        return new FailedState("attempt failed", new IllegalStateException("DAL timed out"));
    }

    private FieldRecommendation read(FieldRecommendation doc) {
        return repository.find(doc.tenantId(), doc.id()).orElseThrow();
    }

    @Test
    void exhaustedReleaseJobFailsTheRelease() {
        String tenant = "relfail-exhausted";
        List<FieldRecommendation> docs = ReleaseFixtures.insert(
                collections,
                ReleaseFixtures.field(
                        tenant,
                        "cert-1",
                        "1003002890",
                        null,
                        "first_name",
                        "Dan",
                        ReviewStatus.RELEASING,
                        4L,
                        ReleaseFixtures.requested("rel-exhausted", "CorePractitioner")),
                ReleaseFixtures.field(
                        tenant,
                        "cert-1",
                        "1003002890",
                        null,
                        "last_name",
                        "Lee",
                        ReviewStatus.RELEASE_REQUESTED,
                        4L,
                        ReleaseFixtures.requested("rel-exhausted", "CorePractitioner")));
        FailedState last = failed();
        Job job = jobIn(
                new ReleaseJobRequest(tenant, "cert-1", "rel-exhausted"),
                new EnqueuedState(),
                failed(),
                new ScheduledState(java.time.Instant.now()),
                last);

        filter.onStateApplied(job, new EnqueuedState(), last);

        for (FieldRecommendation doc : docs) {
            FieldRecommendation after = read(doc);
            Assertions.assertEquals(ReviewStatus.RELEASE_FAILED, after.reviewStatus(), doc.field());
            Assertions.assertTrue(after.goldenRecordUpdate().error().startsWith("EXHAUSTED"));
            List<AuditEvent> trail = audit.trailForField(tenant, doc.id(), 10);
            Assertions.assertEquals(
                    List.of(AuditEventType.FIELD_RELEASE_FAILED),
                    trail.stream().map(AuditEvent::type).toList());
            Assertions.assertEquals("EXHAUSTED", trail.get(0).detail().get("reason"));
            Assertions.assertEquals(2, ((Number) trail.get(0).detail().get("jobAttempts")).intValue());
            Assertions.assertEquals(job.getId().toString(), trail.get(0).jobId());
        }
    }

    @Test
    void retryLeftOrAnotherJobChangesNothing() {
        String tenant = "relfail-retry";
        FieldRecommendation doc = ReleaseFixtures.insert(
                        collections,
                        ReleaseFixtures.field(
                                tenant,
                                "cert-1",
                                "1003002890",
                                null,
                                "first_name",
                                "Dan",
                                ReviewStatus.RELEASING,
                                4L,
                                ReleaseFixtures.requested("rel-retry-left", "CorePractitioner")))
                .get(0);
        FailedState attempt = failed();
        Job retrying = jobIn(
                new ReleaseJobRequest(tenant, "cert-1", "rel-retry-left"),
                attempt,
                new ScheduledState(java.time.Instant.now()));
        FailedState other = failed();
        Job deadline = jobIn(new ReleaseReadBackDeadlineJobRequest(tenant, "cert-1", "rel-retry-left"), other);

        filter.onStateApplied(retrying, new EnqueuedState(), attempt);
        filter.onStateApplied(deadline, new EnqueuedState(), other);

        Assertions.assertEquals(doc, read(doc));
    }
}
