package com.certifyos.directory_accuracy;

import java.util.HashMap;
import java.util.Map;

/**
 * Config values shared by both test profiles. {@code VendorExchangeConfig} validates every
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
        values.put("vendor-exchange.vendor-bucket", "test-vendor-bucket");
        values.put("vendor-exchange.egress.url", "http://localhost:0");
        values.put("vendor-exchange.egress.iap-client-id", "test-egress-iap-client-id");
        values.put("vendor-exchange.dal.url", "http://localhost:0");
        values.put("vendor-exchange.dal.iap-client-id", "test-dal-iap-client-id");
        values.put("vendor-exchange.api-layer.url", "http://localhost:0");
        values.put("vendor-exchange.ingestion.archive-bucket", "test-archive-bucket");
        values.put("vendor-exchange.file-ingestion.url", "http://localhost:0");
        values.put("vendor-exchange.file-ingestion.mapping-id", "map-candor-recs-v1");
        // Two rows per staging chunk, so a five-row test file is three chunks, each checkpointed.
        values.put("vendor-exchange.ingestion.stage-chunk-rows", "2");
        // api-layer internal is behind IAP with a Google-managed client; the token service is mocked
        // where a test crosses it, so this only decides whether the Proxy-Authorization header is sent.
        values.put("vendor-exchange.api-layer.iap-audience", "/projects/1/global/backendServices/2");
        // Push delivery configured, so the push filter's verifier path is exercised; the verifier
        // itself is mocked in the test that needs it (no Google certificates in CI).
        values.put("vendor-exchange.pubsub.push-service-account", "pubsub-push@test.iam.gserviceaccount.com");
        values.put(
                "vendor-exchange.pubsub.push-audience",
                "https://vendor-exchange.test/internal/vendor-exports/egress-events");
        return values;
    }
}
