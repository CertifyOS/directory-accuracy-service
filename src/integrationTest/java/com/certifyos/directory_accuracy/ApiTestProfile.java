package com.certifyos.directory_accuracy;

import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

/**
 * Activates the api role for integration tests. {@code test,api} rather than a bare {@code api}:
 * a bare profile name may replace {@code test} instead of adding to it, which would make every
 * {@code %test.} key inert. The OIDC tenant stays off, matching api-layer's own test config: filter
 * logic is exercised through {@code @TestSecurity}, never HTTP-layer authentication (a live tenant
 * with a placeholder issuer still attempts real discovery under {@code @QuarkusTest}).
 */
public class ApiTestProfile implements QuarkusTestProfile {

    @Override
    public String getConfigProfile() {
        return "test,api";
    }

    @Override
    public Map<String, String> getConfigOverrides() {
        return TestConfig.common();
    }
}
