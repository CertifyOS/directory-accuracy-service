package com.certifyos.directory_accuracy.auth;

/** A push identity token that failed verification; the message says which check, never the token. */
public class PushTokenRejectedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public PushTokenRejectedException(String message) {
        super(message);
    }

    public PushTokenRejectedException(String message, Throwable cause) {
        super(message, cause);
    }
}
