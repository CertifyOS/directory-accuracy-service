package com.certifyos.vendor_exchange.config;

import io.smallrye.config.ConfigValidationException;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Validates the mapping with SmallRye Config directly, without booting Quarkus: required keys, the
 * kebab-case property names each accessor maps to, defaults, and the empty-means-absent rule the
 * swap-in keys rely on.
 */
class VendorExchangeConfigTest {

    private static Map<String, String> required() {
        Map<String, String> values = new HashMap<>();
        values.put("vendor-exchange.vendor-bucket", "vendor-sftp");
        values.put("vendor-exchange.egress.url", "https://egress.example");
        values.put("vendor-exchange.egress.iap-client-id", "egress-client");
        values.put("vendor-exchange.dal.url", "https://dal.example");
        values.put("vendor-exchange.dal.iap-client-id", "dal-client");
        values.put("vendor-exchange.api-layer.url", "https://api.example");
        values.put("vendor-exchange.ingestion.archive-bucket", "archive-bucket");
        values.put("vendor-exchange.file-ingestion.url", "https://file-ingestion.example");
        return values;
    }

    private static VendorExchangeConfig load(Map<String, String> values) {
        SmallRyeConfig config = new SmallRyeConfigBuilder()
                .withMapping(VendorExchangeConfig.class)
                .withSources(new PropertiesConfigSource(values, "test", 100))
                .build();
        return config.getConfigMapping(VendorExchangeConfig.class);
    }

    @Test
    void requiredKeysLoadAndDefaultsApply() {
        VendorExchangeConfig cfg = load(required());

        Assertions.assertTrue(cfg.enabled());
        Assertions.assertEquals(8, cfg.jobRetries());
        Assertions.assertEquals(30, cfg.reconcilerStaleMinutes());
        Assertions.assertEquals("vendor-sftp", cfg.vendorBucket());
        Assertions.assertEquals("https://egress.example", cfg.egress().url());
        Assertions.assertEquals("egress-client", cfg.egress().iapClientId());
        Assertions.assertEquals(6, cfg.egress().deadlineHours());
        Assertions.assertEquals(48, cfg.egress().abandonHours());
        Assertions.assertEquals("https://dal.example", cfg.dal().url());
        Assertions.assertEquals("dal-client", cfg.dal().iapClientId());
        Assertions.assertEquals("https://api.example", cfg.apiLayer().url());
        Assertions.assertFalse(cfg.permissions().enforce());
        Assertions.assertEquals(Duration.ofMinutes(5), cfg.auth().cacheTtl());
        Assertions.assertEquals(1000, cfg.auth().cacheMaxSize());
    }

    @Test
    void optionalKeysAreAbsentWhenUnsetOrEmpty() {
        Map<String, String> values = required();
        values.put("vendor-exchange.api-layer.client-id", "");
        values.put("vendor-exchange.pubsub.push-audience", "");
        VendorExchangeConfig cfg = load(values);

        Assertions.assertTrue(cfg.apiLayer().clientId().isEmpty(), "empty string must read as absent");
        Assertions.assertTrue(cfg.apiLayer().clientSecret().isEmpty(), "unset must read as absent");
        Assertions.assertTrue(cfg.pubsub().pushServiceAccount().isEmpty());
        Assertions.assertTrue(cfg.pubsub().pushAudience().isEmpty());
    }

    @Test
    void optionalKeysArePresentWhenSet() {
        Map<String, String> values = required();
        values.put("vendor-exchange.api-layer.client-id", "svc-vendor-exchange");
        values.put("vendor-exchange.api-layer.client-secret", "s3cret");
        values.put("vendor-exchange.pubsub.push-service-account", "pubsub-push@p.iam.gserviceaccount.com");
        values.put("vendor-exchange.pubsub.push-audience", "https://vendor-exchange.internal.certifyos.com/internal/x");
        VendorExchangeConfig cfg = load(values);

        Assertions.assertEquals("svc-vendor-exchange", cfg.apiLayer().clientId().orElseThrow());
        Assertions.assertEquals("s3cret", cfg.apiLayer().clientSecret().orElseThrow());
        Assertions.assertEquals(
                "pubsub-push@p.iam.gserviceaccount.com",
                cfg.pubsub().pushServiceAccount().orElseThrow());
        Assertions.assertTrue(cfg.pubsub().pushAudience().orElseThrow().endsWith("/internal/x"));
    }

    @Test
    void overridesReplaceDefaults() {
        Map<String, String> values = required();
        values.put("vendor-exchange.enabled", "false");
        values.put("vendor-exchange.job-retries", "3");
        values.put("vendor-exchange.egress.deadline-hours", "2");
        values.put("vendor-exchange.permissions.enforce", "true");
        VendorExchangeConfig cfg = load(values);

        Assertions.assertFalse(cfg.enabled());
        Assertions.assertEquals(3, cfg.jobRetries());
        Assertions.assertEquals(2, cfg.egress().deadlineHours());
        Assertions.assertTrue(cfg.permissions().enforce());
    }

    @Test
    void everyMissingRequiredKeyIsNamed() {
        ConfigValidationException refused =
                Assertions.assertThrows(ConfigValidationException.class, () -> load(Map.of()));

        String message = refused.getMessage();
        for (String key : required().keySet()) {
            Assertions.assertTrue(message.contains(key), "expected " + key + " in: " + message);
        }
    }

    @Test
    void unknownKeyUnderThePrefixIsRefused() {
        Map<String, String> values = required();
        values.put("vendor-exchange.role", "api");

        Assertions.assertThrows(ConfigValidationException.class, () -> load(values));
    }

    @Test
    void ingestionDefaultsMatchTheDesign() {
        VendorExchangeConfig cfg = load(required());

        Assertions.assertEquals("archive-bucket", cfg.ingestion().archiveBucket());
        Assertions.assertEquals(6, cfg.ingestion().ingestionDeadlineHours());
        Assertions.assertEquals(500, cfg.ingestion().stageChunkRows());
        Assertions.assertEquals(200, cfg.ingestion().reviewPageLimitMax());
        Assertions.assertEquals(12, cfg.ingestion().rejectedValueTtlMonths());
        Assertions.assertEquals(30, cfg.ingestion().releaseReadBackDeadlineMinutes());
        Assertions.assertEquals(5000, cfg.ingestion().releaseBulkMax());
        Assertions.assertEquals(30, cfg.ingestion().inboundStaleMinutes());
        Assertions.assertEquals("DATA_FILE", cfg.ingestion().completenessSignal());
        Assertions.assertEquals(
                "https://file-ingestion.example", cfg.fileIngestion().url());
        Assertions.assertEquals("vendor-ingestion", cfg.fileIngestion().consumerIdentifier());
        Assertions.assertTrue(cfg.fileIngestion().mappingId().isEmpty());
        Assertions.assertTrue(cfg.fileIngestion().iapAudience().isEmpty());
    }

    @Test
    void archiveBucketIsRequired() {
        Map<String, String> values = required();
        values.remove("vendor-exchange.ingestion.archive-bucket");

        ConfigValidationException refused =
                Assertions.assertThrows(ConfigValidationException.class, () -> load(values));

        Assertions.assertTrue(refused.getMessage().contains("vendor-exchange.ingestion.archive-bucket"));
    }

    @Test
    void fileIngestionUrlIsRequired() {
        Map<String, String> values = required();
        values.remove("vendor-exchange.file-ingestion.url");

        ConfigValidationException refused =
                Assertions.assertThrows(ConfigValidationException.class, () -> load(values));

        Assertions.assertTrue(refused.getMessage().contains("vendor-exchange.file-ingestion.url"));
    }

    @Test
    void fileIngestionSwapInKeysAreReadWhenSet() {
        Map<String, String> values = required();
        values.put("vendor-exchange.file-ingestion.mapping-id", "map-1");
        values.put("vendor-exchange.file-ingestion.iap-audience", "/projects/1/global/backendServices/3");
        VendorExchangeConfig cfg = load(values);

        Assertions.assertEquals("map-1", cfg.fileIngestion().mappingId().orElseThrow());
        Assertions.assertEquals(
                "/projects/1/global/backendServices/3",
                cfg.fileIngestion().iapAudience().orElseThrow());
    }
}
