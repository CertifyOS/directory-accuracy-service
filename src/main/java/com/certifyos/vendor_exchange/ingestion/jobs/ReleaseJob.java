package com.certifyos.vendor_exchange.ingestion.jobs;

import jakarta.enterprise.context.ApplicationScoped;
import org.jobrunr.jobs.annotations.Job;
import org.jobrunr.jobs.lambdas.JobRequestHandler;

/**
 * Writes one release's approved slice fields to the Golden record (design step 8). The body lands
 * with the next task; until then a run throws, so JobRunr keeps the release for a retry rather than
 * dropping it.
 */
@ApplicationScoped
public class ReleaseJob implements JobRequestHandler<ReleaseJobRequest> {

    /** Job name used in ids and logs. */
    public static final String NAME = "release";

    @Override
    @Job(name = "release %0")
    public void run(ReleaseJobRequest request) {
        throw new IllegalStateException("the release job body is not built yet: " + request);
    }
}
