package com.certifyos.vendor_exchange.ingestion.jobs;

import org.jobrunr.jobs.lambdas.JobRequest;

/**
 * The read-back deadline's payload: one release whose {@code mdm.slice.ov_generated} event may not
 * come. The job id is {@code JobIds.of("release-readback", releaseId, 1)}.
 *
 * @param tenantId the tenant
 * @param certifyPractitionerId the practitioner
 * @param releaseId the release
 */
public record ReleaseReadBackDeadlineJobRequest(String tenantId, String certifyPractitionerId, String releaseId)
        implements JobRequest {

    @Override
    public Class<ReleaseReadBackDeadlineJob> getJobRequestHandler() {
        return ReleaseReadBackDeadlineJob.class;
    }

    @Override
    public String toString() {
        return "release " + releaseId + " tenant " + tenantId + " practitioner " + certifyPractitionerId;
    }
}
