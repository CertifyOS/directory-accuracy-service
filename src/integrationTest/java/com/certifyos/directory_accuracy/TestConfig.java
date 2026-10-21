package com.certifyos.directory_accuracy;

import java.util.HashMap;
import java.util.Map;

/**
 * Config values shared by both test profiles. {@code DirectoryAccuracyConfig} validates every
 * registered mapping at startup regardless of whether a test injects it, so every key without a
 * default needs a value here, not only in the tests that use it.
 */
public final class TestConfig {

    private TestConfig() {}

    /**
     * Returns the common overrides for a test profile.
     *
     * @return a mutable map a profile can add to
     */
    public static Map<String, String> common() {
        Map<String, String> values = new HashMap<>();
        values.put("quarkus.oidc.tenant-enabled", "false");
        values.put("directory-accuracy.vendor-bucket", "test-vendor-bucket");
        values.put("directory-accuracy.egress.url", "http://localhost:0");
        values.put("directory-accuracy.egress.iap-client-id", "test-egress-iap-client-id");
        values.put("directory-accuracy.dal.url", "http://localhost:0");
        values.put("directory-accuracy.dal.iap-client-id", "test-dal-iap-client-id");
        values.put("directory-accuracy.api-layer.url", "http://localhost:0");
        values.put("directory-accuracy.ingestion.archive-bucket", "test-archive-bucket");
        values.put("directory-accuracy.file-ingestion.url", "http://localhost:0");
        values.put("directory-accuracy.file-ingestion.mapping-id", "map-candor-recs-v1");
        // Two rows per staging chunk, so a five-row test file is three chunks, each checkpointed.
        values.put("directory-accuracy.ingestion.stage-chunk-rows", "2");
        // api-layer internal is behind IAP with a Google-managed client; the token service is mocked
        // where a test crosses it, so this only decides whether the Proxy-Authorization header is sent.
        values.put("directory-accuracy.api-layer.iap-audience", "/projects/1/global/backendServices/2");
        // Push delivery configured, so the push filter's verifier path is exercised; the verifier
        // itself is mocked in the test that needs it (no Google certificates in CI).
        values.put("directory-accuracy.pubsub.push-service-account", "pubsub-push@test.iam.gserviceaccount.com");
        values.put(
                "directory-accuracy.pubsub.push-audience",
                "https://directory-accuracy.test/internal/vendor-exports/egress-events");
        return values;
    }
}
