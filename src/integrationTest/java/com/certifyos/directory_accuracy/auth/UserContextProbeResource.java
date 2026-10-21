package com.certifyos.directory_accuracy.auth;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.List;
import java.util.Map;

/**
 * Test-only endpoint that echoes the filled {@link UserContext}, so the filter chain is observed on
 * a real {@code /v1} route. Lives in the integrationTest source set and never reaches the jar.
 */
@Path("/v1/_probe")
public class UserContextProbeResource {

    private final UserContext ctx;

    public UserContextProbeResource(UserContext ctx) {
        this.ctx = ctx;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Probe get() {
        return new Probe(ctx.email(), ctx.tenantId(), ctx.permissions());
    }

    record Probe(String email, String tenantId, Map<String, Map<String, List<String>>> permissions) {}
}
