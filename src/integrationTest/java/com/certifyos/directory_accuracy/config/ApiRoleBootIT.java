package com.certifyos.directory_accuracy.config;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.time.Duration;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jobrunr.scheduling.JobScheduler;
import org.jobrunr.server.BackgroundJobServer;
import org.jobrunr.storage.StorageProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** The service boots as the api role: mapping resolved, JobRunr server off, health open. */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(ApiTestProfile.class)
public class ApiRoleBootIT {

    @Inject
    VendorExchangeConfig cfg;

    @Inject
    JobScheduler scheduler;

    @Inject
    StorageProvider storage;

    @Inject
    Instance<BackgroundJobServer> backgroundJobServer;

    @Test
    void mappingIsResolvedFromTheProfile() {
        Assertions.assertEquals("test-vendor-bucket", cfg.vendorBucket());
        Assertions.assertEquals("test-dal-iap-client-id", cfg.dal().iapClientId());
        Assertions.assertTrue(cfg.enabled());
        Assertions.assertEquals(8, cfg.jobRetries());
    }

    @Test
    void swapInKeysResolveToAbsentThroughTheEmptyDefault() {
        // application.properties maps these to ${ENV:} so an unset environment variable is an empty
        // string, which the mapping must read as Optional.empty().
        Assertions.assertTrue(cfg.apiLayer().clientId().isEmpty());
        Assertions.assertTrue(cfg.apiLayer().clientSecret().isEmpty());
        // The push keys are the same kind of swap-in, but the test profile sets them so the push
        // filter's verifier path runs in EgressEventResourceIT; a set value reads as present.
        Assertions.assertEquals(
                "pubsub-push@test.iam.gserviceaccount.com",
                cfg.pubsub().pushServiceAccount().orElseThrow());
        Assertions.assertTrue(cfg.pubsub().pushAudience().isPresent());
    }

    @Test
    void retriesFollowTheMapping() {
        Assertions.assertEquals(
                8,
                ConfigProvider.getConfig().getValue("quarkus.jobrunr.jobs.default-number-of-retries", Integer.class));
        Assertions.assertNotNull(scheduler, "the scheduler bean exists on the api role so handlers can enqueue");
    }

    @Test
    void jobRunrServerDoesNotRunOnTheApiRole() {
        // Behavioural, not a config echo: the BackgroundJobServer bean exists on both roles (the
        // extension decides start or no-start from the enabled flag at runtime), so the proof is that
        // it is not running and never writes a heartbeat row, observed across more than one poll
        // interval (5 s in the test profile). This JVM has its own Mongo container, so an empty
        // server list cannot be another role's leftover.
        if (backgroundJobServer.isResolvable()) {
            Assertions.assertFalse(backgroundJobServer.get().isRunning(), "server must not run on api");
        }
        Awaitility.await()
                .during(Duration.ofSeconds(7))
                .atMost(Duration.ofSeconds(12))
                .until(() -> storage.getBackgroundJobServers().isEmpty());
    }

    @Test
    void healthIsOpenWithoutAToken() {
        RestAssured.get("/q/health/live").then().statusCode(200);
    }

    @Test
    void operatorPathsRequireAuthentication() {
        RestAssured.get("/v1/vendor-exports/schedules").then().statusCode(401);
    }

    @Test
    void unknownPathsAreDeniedByDefault() {
        // deny-all on /* : a path nobody declared is refused before any resource could answer.
        RestAssured.get("/anything-nobody-declared").then().statusCode(401);
    }

    @Test
    void readinessReportsMongo() {
        // 503 overall: the api-layer check is DOWN by design while the machine client is absent
        // (ApiLayerReadinessIT). The Mongo check itself is present and UP.
        RestAssured.get("/q/health/ready")
                .then()
                .statusCode(503)
                .body(
                        "checks.find { it.name.toLowerCase().contains('mongo') }.status",
                        org.hamcrest.Matchers.equalTo("UP"));
    }
}
