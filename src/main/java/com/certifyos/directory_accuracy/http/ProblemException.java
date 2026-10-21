package com.certifyos.directory_accuracy.http;

/**
 * A refusal a service throws for the mappers to render as problem+json: the status, the stable
 * code and a detail safe to show the caller. Resources never build error responses themselves.
 */
public class ProblemException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final int status;
    private final String code;

    public ProblemException(int status, String code, String detail) {
        super(detail);
        this.status = status;
        this.code = code;
    }

    /** The HTTP status. */
    public int status() {
        return status;
    }

    /** The stable code. */
    public String code() {
        return code;
    }

    /** 400 with a code. */
    public static ProblemException badRequest(String code, String detail) {
        return new ProblemException(400, code, detail);
    }

    /** 404 with a code. */
    public static ProblemException notFound(String code, String detail) {
        return new ProblemException(404, code, detail);
    }

    /** 409 with a code. */
    public static ProblemException conflict(String code, String detail) {
        return new ProblemException(409, code, detail);
    }

    /** 503 with a code. */
    public static ProblemException unavailable(String code, String detail) {
        return new ProblemException(503, code, detail);
    }
}
