package com.certifyos.directory_accuracy.clients;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

/** With no machine client configured (the test profile, and production until the swap-in), readiness names api-layer as the pending hook-up. */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(ApiTestProfile.class)
class ApiLayerReadinessIT {

    @Test
    void readinessNamesTheUnconfiguredApiLayer() {
        RestAssured.given()
                .get("/q/health/ready")
                .then()
                .statusCode(503)
                .body("status", Matchers.equalTo("DOWN"))
                .body("checks.find { it.name == 'api-layer' }.status", Matchers.equalTo("DOWN"))
                .body("checks.find { it.name == 'api-layer' }.data.reason", Matchers.containsString("not configured"))
                .body("checks.find { it.name == 'MongoDB connection health check' }.status", Matchers.equalTo("UP"));
    }

    @Test
    void livenessIsUnaffected() {
        RestAssured.given().get("/q/health/live").then().statusCode(200).body("status", Matchers.equalTo("UP"));
    }
}
