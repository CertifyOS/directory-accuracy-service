package com.certifyos.directory_accuracy;

import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

/**
 * Activates the worker role for integration tests, so the {@code %worker.} keys apply and the
 * JobRunr background server runs against the Testcontainers replica set.
 */
public class WorkerTestProfile implements QuarkusTestProfile {

    @Override
    public String getConfigProfile() {
        return "test,worker";
    }

    @Override
    public Map<String, String> getConfigOverrides() {
        Map<String, String> values = TestConfig.common();
        // 5 s is JobRunr's enforced floor (AbstractStorageProvider.validatePollInterval); lower throws
        // at boot. Production stays at 15 s in application.properties.
        values.put("quarkus.jobrunr.background-job-server.poll-interval-in-seconds", "5");
        // The dashboard binds port 8000 on a real worker; a test JVM must not open it.
        values.put("quarkus.jobrunr.dashboard.enabled", "false");
        // Production retries 8 times with 3^n-second backoff (hours). One retry here, so a job that
        // always throws reaches FAILED in about 3 s and the final-failure filter can be observed.
        values.put("vendor-exchange.job-retries", "1");
        return values;
    }
}
