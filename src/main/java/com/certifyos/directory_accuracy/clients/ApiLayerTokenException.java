package com.certifyos.directory_accuracy.clients;

/** The client-credentials call to api-layer failed; the message names the status or the transport failure, never the secret. */
public class ApiLayerTokenException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ApiLayerTokenException(String message) {
        super(message);
    }

    public ApiLayerTokenException(String message, Throwable cause) {
        super(message, cause);
    }
}
