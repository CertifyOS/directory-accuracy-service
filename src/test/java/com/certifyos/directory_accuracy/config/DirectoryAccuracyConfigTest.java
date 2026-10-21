package com.certifyos.directory_accuracy.config;

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
class DirectoryAccuracyConfigTest {

    private static Map<String, String> required() {
        Map<String, String> values = new HashMap<>();
        values.put("directory-accuracy.vendor-bucket", "vendor-sftp");
        values.put("directory-accuracy.egress.url", "https://egress.example");
        values.put("directory-accuracy.egress.iap-client-id", "egress-client");
        values.put("directory-accuracy.dal.url", "https://dal.example");
        values.put("directory-accuracy.dal.iap-client-id", "dal-client");
        values.put("directory-accuracy.api-layer.url", "https://api.example");
        values.put("directory-accuracy.ingestion.archive-bucket", "archive-bucket");
        values.put("directory-accuracy.file-ingestion.url", "https://file-ingestion.example");
        return values;
    }

    private static DirectoryAccuracyConfig load(Map<String, String> values) {
        SmallRyeConfig config = new SmallRyeConfigBuilder()
                .withMapping(DirectoryAccuracyConfig.class)
                .withSources(new PropertiesConfigSource(values, "test", 100))
                .build();
        return config.getConfigMapping(DirectoryAccuracyConfig.class);
    }

    @Test
    void requiredKeysLoadAndDefaultsApply() {
        DirectoryAccuracyConfig cfg = load(required());

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
        values.put("directory-accuracy.api-layer.client-id", "");
        values.put("directory-accuracy.pubsub.push-audience", "");
        DirectoryAccuracyConfig cfg = load(values);

        Assertions.assertTrue(cfg.apiLayer().clientId().isEmpty(), "empty string must read as absent");
        Assertions.assertTrue(cfg.apiLayer().clientSecret().isEmpty(), "unset must read as absent");
        Assertions.assertTrue(cfg.pubsub().pushServiceAccount().isEmpty());
        Assertions.assertTrue(cfg.pubsub().pushAudience().isEmpty());
    }

    @Test
    void optionalKeysArePresentWhenSet() {
        Map<String, String> values = required();
        values.put("directory-accuracy.api-layer.client-id", "svc-directory-accuracy");
        values.put("directory-accuracy.api-layer.client-secret", "s3cret");
        values.put("directory-accuracy.pubsub.push-service-account", "pubsub-push@p.iam.gserviceaccount.com");
        values.put(
                "directory-accuracy.pubsub.push-audience",
                "https://directory-accuracy.internal.certifyos.com/internal/x");
        DirectoryAccuracyConfig cfg = load(values);

        Assertions.assertEquals(
                "svc-directory-accuracy", cfg.apiLayer().clientId().orElseThrow());
        Assertions.assertEquals("s3cret", cfg.apiLayer().clientSecret().orElseThrow());
        Assertions.assertEquals(
                "pubsub-push@p.iam.gserviceaccount.com",
                cfg.pubsub().pushServiceAccount().orElseThrow());
        Assertions.assertTrue(cfg.pubsub().pushAudience().orElseThrow().endsWith("/internal/x"));
    }

    @Test
    void overridesReplaceDefaults() {
        Map<String, String> values = required();
        values.put("directory-accuracy.enabled", "false");
        values.put("directory-accuracy.job-retries", "3");
        values.put("directory-accuracy.egress.deadline-hours", "2");
        values.put("directory-accuracy.permissions.enforce", "true");
        DirectoryAccuracyConfig cfg = load(values);

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
        values.put("directory-accuracy.role", "api");

        Assertions.assertThrows(ConfigValidationException.class, () -> load(values));
    }

    @Test
    void ingestionDefaultsMatchTheDesign() {
        DirectoryAccuracyConfig cfg = load(required());

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
        values.remove("directory-accuracy.ingestion.archive-bucket");

        ConfigValidationException refused =
                Assertions.assertThrows(ConfigValidationException.class, () -> load(values));

        Assertions.assertTrue(refused.getMessage().contains("directory-accuracy.ingestion.archive-bucket"));
    }

    @Test
    void fileIngestionUrlIsRequired() {
        Map<String, String> values = required();
        values.remove("directory-accuracy.file-ingestion.url");

        ConfigValidationException refused =
                Assertions.assertThrows(ConfigValidationException.class, () -> load(values));

        Assertions.assertTrue(refused.getMessage().contains("directory-accuracy.file-ingestion.url"));
    }

    @Test
    void fileIngestionSwapInKeysAreAbsentWhenEmpty() {
        Map<String, String> values = required();
        values.put("directory-accuracy.file-ingestion.mapping-id", "");
        values.put("directory-accuracy.file-ingestion.iap-audience", "");
        DirectoryAccuracyConfig cfg = load(values);

        Assertions.assertTrue(cfg.fileIngestion().mappingId().isEmpty(), "empty string must read as absent");
        Assertions.assertTrue(cfg.fileIngestion().iapAudience().isEmpty(), "empty string must read as absent");
    }

    @Test
    void fileIngestionSwapInKeysAreReadWhenSet() {
        Map<String, String> values = required();
        values.put("directory-accuracy.file-ingestion.mapping-id", "map-1");
        values.put("directory-accuracy.file-ingestion.iap-audience", "/projects/1/global/backendServices/3");
        DirectoryAccuracyConfig cfg = load(values);

        Assertions.assertEquals("map-1", cfg.fileIngestion().mappingId().orElseThrow());
        Assertions.assertEquals(
                "/projects/1/global/backendServices/3",
                cfg.fileIngestion().iapAudience().orElseThrow());
    }
}
