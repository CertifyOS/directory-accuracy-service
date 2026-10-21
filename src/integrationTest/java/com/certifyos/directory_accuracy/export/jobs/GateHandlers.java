package com.certifyos.directory_accuracy.export.jobs;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.atomic.AtomicInteger;
import org.jobrunr.jobs.annotations.Job;
import org.jobrunr.jobs.lambdas.JobRequestHandler;

/** Handlers for the JobRunr gate, resolved from CDI by the extension's job activator. */
public final class GateHandlers {

    public static final AtomicInteger RUNS = new AtomicInteger();
    public static final AtomicInteger FLAKY = new AtomicInteger();

    private GateHandlers() {}

    /** Counts runs. */
    @ApplicationScoped
    public static class Run implements JobRequestHandler<GateRunRequest> {
        @Override
        public void run(GateRunRequest request) {
            RUNS.incrementAndGet();
        }
    }

    /** Fails once, then succeeds. */
    @ApplicationScoped
    public static class Flaky implements JobRequestHandler<GateFlakyRequest> {
        @Override
        @Job(retries = 2)
        public void run(GateFlakyRequest request) {
            if (FLAKY.incrementAndGet() < 2) {
                throw new IllegalStateException("first attempt fails on purpose");
            }
        }
    }
}
