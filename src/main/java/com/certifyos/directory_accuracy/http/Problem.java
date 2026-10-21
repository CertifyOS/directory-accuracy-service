package com.certifyos.directory_accuracy.http;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.ws.rs.core.Response;
import java.util.Locale;
import java.util.Objects;

/**
 * An RFC 9457 problem details body. Every error answer this service gives has this shape and the
 * {@code application/problem+json} media type; {@code code} is the stable machine-readable key a
 * client switches on, {@code type} is a URN derived from it, {@code title} is the HTTP reason
 * phrase and {@code instance} is the request path.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Problem(String type, String title, int status, String detail, String instance, String code) {

    /** The media type of every error body. */
    public static final String MEDIA_TYPE = "application/problem+json";

    static final String TYPE_PREFIX = "urn:certifyos:vendor-exchange:problem:";

    public Problem {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(title, "title");
        Objects.requireNonNull(code, "code");
        if (status < 400 || status > 599) {
            throw new IllegalArgumentException("a problem needs an error status, not " + status);
        }
    }

    /**
     * Builds a problem from the parts a filter or mapper knows.
     *
     * @param status the HTTP status
     * @param code the stable code, upper snake case
     * @param detail a human-readable explanation, may be null
     * @param instance the request path, may be null
     * @return the problem
     */
    public static Problem of(int status, String code, String detail, String instance) {
        String slug = code.toLowerCase(Locale.ROOT).replace('_', '-');
        return new Problem(TYPE_PREFIX + slug, titleFor(status), status, detail, instance, code);
    }

    static String titleFor(int status) {
        Response.Status known = Response.Status.fromStatusCode(status);
        return known == null ? "Error" : known.getReasonPhrase();
    }
}
