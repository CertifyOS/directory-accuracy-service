package com.certifyos.directory_accuracy.clients;

/** The generation an archive copy names does not exist (a 404 from the source). */
public class ArchiveSourceMissingException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ArchiveSourceMissingException(String message, Throwable cause) {
        super(message, cause);
    }
}
