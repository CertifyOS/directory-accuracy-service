package com.certifyos.directory_accuracy.http;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Response;

/** Builds the problem+json responses filters abort with and mappers return. */
public final class Problems {

    private Problems() {}

    /**
     * A response carrying a {@link Problem}.
     *
     * @param status the HTTP status
     * @param code the stable code
     * @param detail the explanation
     * @param instance the request path
     * @return the response
     */
    public static Response response(int status, String code, String detail, String instance) {
        return Response.status(status)
                .type(Problem.MEDIA_TYPE)
                .entity(Problem.of(status, code, detail, instance))
                .build();
    }

    /**
     * Aborts a request from a filter with a problem body.
     *
     * @param rc the request
     * @param status the HTTP status
     * @param code the stable code
     * @param detail the explanation
     */
    public static void abort(ContainerRequestContext rc, int status, String code, String detail) {
        rc.abortWith(response(status, code, detail, path(rc)));
    }

    /**
     * The request path, as the {@code instance} of a problem.
     *
     * @param rc the request
     * @return the absolute path
     */
    public static String path(ContainerRequestContext rc) {
        return rc.getUriInfo().getRequestUri().getPath();
    }
}
