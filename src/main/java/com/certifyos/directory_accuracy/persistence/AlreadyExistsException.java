package com.certifyos.directory_accuracy.persistence;

/**
 * A write hit a unique index: the document, or its natural key, already exists. Thrown by the
 * repositories instead of leaking the driver's {@code MongoWriteException}; callers decide whether
 * that is a 409, a no-op (an idempotent retry) or a bug.
 */
public class AlreadyExistsException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String collection;
    private final String key;

    public AlreadyExistsException(String collection, String key) {
        super(collection + " already has " + key);
        this.collection = collection;
        this.key = key;
    }

    /** The collection whose unique index refused the write. */
    public String collection() {
        return collection;
    }

    /** The key that already exists, as the caller named it. */
    public String key() {
        return key;
    }
}
