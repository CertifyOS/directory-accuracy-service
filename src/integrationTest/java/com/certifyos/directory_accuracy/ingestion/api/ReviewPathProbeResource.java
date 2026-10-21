package com.certifyos.directory_accuracy.ingestion.api;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;

/**
 * Test-only root resource on {@code /v1/vendor-review}, the path the release resource shares with the
 * review resource of another step: shows that two root resources on one path both resolve. Lives in
 * the integrationTest source set and never reaches the jar.
 */
@Path("/v1/vendor-review")
public class ReviewPathProbeResource {

    @GET
    @Path("/path-probe")
    public Response probe() {
        return Response.noContent().build();
    }
}
