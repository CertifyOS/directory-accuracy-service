package com.certifyos.directory_accuracy.export.jobs;

import org.jobrunr.jobs.lambdas.JobRequest;

/** Gate job payload: a label, so two requests with different ids are distinct jobs. */
public record GateRunRequest(String label) implements JobRequest {
    @Override
    public Class<GateHandlers.Run> getJobRequestHandler() {
        return GateHandlers.Run.class;
    }
}
