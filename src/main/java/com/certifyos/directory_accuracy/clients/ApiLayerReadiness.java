package com.certifyos.directory_accuracy.clients;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.HealthCheckResponseBuilder;
import org.eclipse.microprofile.health.Readiness;

/**
 * Reports api-layer as DOWN with reason "not configured" while the Auth0 machine client is absent,
 * so {@code /q/health/ready} on the deployed service names the pending hook-up instead of hiding
 * it. The load balancer and the instance groups use {@code /q/health/live}, which this does not
 * affect. No call is made: the check is about configuration, not api-layer's availability.
 */
@Readiness
@ApplicationScoped
public class ApiLayerReadiness implements HealthCheck {

    /** The check's name in the health document. */
    public static final String NAME = "api-layer";

    private final ApiLayerTokenService tokens;

    public ApiLayerReadiness(ApiLayerTokenService tokens) {
        this.tokens = tokens;
    }

    @Override
    public HealthCheckResponse call() {
        boolean configured = tokens.configured();
        HealthCheckResponseBuilder response = HealthCheckResponse.named(NAME).status(configured);
        if (!configured) {
            response.withData("reason", "not configured: API_LAYER_CLIENT_ID and API_LAYER_CLIENT_SECRET are absent");
        }
        return response.build();
    }
}
