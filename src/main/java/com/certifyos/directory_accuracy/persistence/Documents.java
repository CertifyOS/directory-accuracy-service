package com.certifyos.directory_accuracy.persistence;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoWriteException;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import org.bson.Document;

/** Small readers for the BSON documents the repositories map to records. */
public final class Documents {

    private Documents() {}

    /**
     * Reads a date field as an {@link Instant}.
     *
     * @param doc the document
     * @param key the field
     * @return the instant, or null when the field is absent
     */
    public static Instant instant(Document doc, String key) {
        // A document read from MongoDB holds a Date (BSON date); one built in memory and not yet
        // written still holds the Instant it was appended with. Both read back the same.
        Object value = doc.get(key);
        if (value instanceof Date date) {
            return date.toInstant();
        }
        if (value instanceof Instant instant) {
            return instant;
        }
        return null;
    }

    /**
     * Reads a nested document.
     *
     * @param doc the document
     * @param key the field
     * @return the nested document, or empty when absent
     */
    public static Optional<Document> nested(Document doc, String key) {
        return Optional.ofNullable(doc.get(key, Document.class));
    }

    /**
     * Reads a list of strings.
     *
     * @param doc the document
     * @param key the field
     * @return an immutable list, empty when absent
     */
    public static List<String> strings(Document doc, String key) {
        List<String> values = doc.getList(key, String.class);
        return values == null ? List.of() : List.copyOf(values);
    }

    /**
     * Reads a long that may have been stored as an int.
     *
     * @param doc the document
     * @param key the field
     * @return the value, or null when absent
     */
    public static Long longValue(Document doc, String key) {
        Number number = doc.get(key, Number.class);
        return number == null ? null : number.longValue();
    }

    /**
     * Reads an int that may have been stored as a long.
     *
     * @param doc the document
     * @param key the field
     * @return the value, or null when absent
     */
    public static Integer intValue(Document doc, String key) {
        Number number = doc.get(key, Number.class);
        return number == null ? null : number.intValue();
    }

    /**
     * Appends a field only when its value is present.
     *
     * @param doc the document being built
     * @param key the field
     * @param value the value, possibly null
     * @return the same document, for chaining
     */
    public static Document put(Document doc, String key, Object value) {
        if (value != null) {
            doc.append(key, value);
        }
        return doc;
    }

    /**
     * Whether a write failure is a unique-index violation. Inside a transaction the driver reports
     * it as a plain {@link MongoWriteException} with no transient label, so the transaction is not
     * retried and the caller sees it (file-ingestion finding 24).
     *
     * @param failure the driver exception
     * @return true for a duplicate key
     */
    public static boolean isDuplicateKey(RuntimeException failure) {
        if (failure instanceof MongoWriteException write) {
            return write.getError().getCategory() == ErrorCategory.DUPLICATE_KEY;
        }
        if (failure instanceof MongoBulkWriteException bulk) {
            return bulk.getWriteErrors().stream().allMatch(error -> error.getCategory() == ErrorCategory.DUPLICATE_KEY);
        }
        return false;
    }
}
