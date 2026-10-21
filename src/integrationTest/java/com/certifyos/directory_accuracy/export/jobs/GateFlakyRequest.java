package com.certifyos.directory_accuracy.export.jobs;

import org.jobrunr.jobs.lambdas.JobRequest;

/** Gate job payload for the retry case. */
public record GateFlakyRequest(String label) implements JobRequest {
    @Override
    public Class<GateHandlers.Flaky> getJobRequestHandler() {
        return GateHandlers.Flaky.class;
    }
}
