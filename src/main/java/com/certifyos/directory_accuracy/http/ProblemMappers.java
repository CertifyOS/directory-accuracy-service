package com.certifyos.directory_accuracy.http;

import com.certifyos.directory_accuracy.persistence.AlreadyExistsException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

/**
 * The only place an exception becomes an HTTP error body. Resources throw; nothing builds an error
 * response by hand. Messages of unexpected failures stay in the log, never in the answer.
 */
public class ProblemMappers {

    private static final Logger LOG = Logger.getLogger(ProblemMappers.class);

    /**
     * A unique-key refusal from a repository.
     *
     * @param failure the refusal
     * @param uri the request
     * @return 409 with code {@code ALREADY_EXISTS}
     */
    @ServerExceptionMapper
    public Response alreadyExists(AlreadyExistsException failure, UriInfo uri) {
        return Problems.response(
                409, "ALREADY_EXISTS", failure.getMessage(), uri.getRequestUri().getPath());
    }

    /**
     * A refusal a service threw with its own status and code.
     *
     * @param refusal the refusal
     * @param uri the request
     * @return the refusal's status with a problem body
     */
    @ServerExceptionMapper
    public Response problem(ProblemException refusal, UriInfo uri) {
        return Problems.response(
                refusal.status(),
                refusal.code(),
                refusal.getMessage(),
                uri.getRequestUri().getPath());
    }

    /**
     * A value a domain constructor refused (an id with an underscore, a day of month out of range).
     *
     * @param refusal the exception, whose message is the validation text
     * @param uri the request
     * @return 400 with code {@code INVALID_REQUEST}
     */
    @ServerExceptionMapper
    public Response invalidArgument(IllegalArgumentException refusal, UriInfo uri) {
        return Problems.response(
                400,
                "INVALID_REQUEST",
                refusal.getMessage(),
                uri.getRequestUri().getPath());
    }

    /**
     * A JAX-RS failure: unmatched path, wrong method, unreadable body, or one a resource threw.
     *
     * @param failure the exception, carrying its status
     * @param uri the request
     * @return the same status with a problem body
     */
    @ServerExceptionMapper
    public Response webApplication(WebApplicationException failure, UriInfo uri) {
        int status = failure.getResponse() == null ? 500 : failure.getResponse().getStatus();
        String path = uri.getRequestUri().getPath();
        if (status >= 500) {
            LOG.errorf(failure, "request to %s failed with %d", path, status);
            return Problems.response(status, codeFor(status), "the request could not be completed", path);
        }
        return Problems.response(status, codeFor(status), failure.getMessage(), path);
    }

    /**
     * Anything else: a bug or an unavailable dependency.
     *
     * @param failure the exception
     * @param uri the request
     * @return 500 with code {@code INTERNAL}
     */
    @ServerExceptionMapper
    public Response unexpected(RuntimeException failure, UriInfo uri) {
        String path = uri.getRequestUri().getPath();
        LOG.errorf(failure, "unhandled %s on %s", failure.getClass().getSimpleName(), path);
        return Problems.response(500, "INTERNAL", "the request could not be completed", path);
    }

    static String codeFor(int status) {
        return switch (status) {
            case 400 -> "BAD_REQUEST";
            case 401 -> "UNAUTHENTICATED";
            case 403 -> "FORBIDDEN";
            case 404 -> "NOT_FOUND";
            case 405 -> "METHOD_NOT_ALLOWED";
            case 406 -> "NOT_ACCEPTABLE";
            case 409 -> "CONFLICT";
            case 415 -> "UNSUPPORTED_MEDIA_TYPE";
            case 500 -> "INTERNAL";
            case 503 -> "UNAVAILABLE";
            default -> "HTTP_" + status;
        };
    }
}
