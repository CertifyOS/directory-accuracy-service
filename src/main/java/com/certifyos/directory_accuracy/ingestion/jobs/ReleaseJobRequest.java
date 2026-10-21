package com.certifyos.directory_accuracy.ingestion.jobs;

import org.jobrunr.jobs.lambdas.JobRequest;

/**
 * The release job's payload: one release of one practitioner. Not an inbound file request; the job
 * id is {@code JobIds.of("release", releaseId, 1)}, unique per release (plan P12).
 *
 * @param tenantId the tenant
 * @param certifyPractitionerId the practitioner
 * @param releaseId the release
 */
public record ReleaseJobRequest(String tenantId, String certifyPractitionerId, String releaseId) implements JobRequest {

    @Override
    public Class<ReleaseJob> getJobRequestHandler() {
        return ReleaseJob.class;
    }

    @Override
    public String toString() {
        return "release " + releaseId + " tenant " + tenantId + " practitioner " + certifyPractitionerId;
    }
}
