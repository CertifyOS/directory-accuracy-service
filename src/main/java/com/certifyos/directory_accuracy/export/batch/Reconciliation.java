package com.certifyos.directory_accuracy.export.batch;

import com.certifyos.directory_accuracy.persistence.Documents;
import java.time.Instant;
import org.bson.Document;

/**
 * The count check the finish job writes once: registered NPIs against practitioners egress reports in
 * the object metadata. A mismatch is counted, audited and alerted, never guessed at.
 *
 * @param registered rows in the NPI registry for the batch
 * @param inFile practitioners egress reported ({@code totalRecords})
 * @param match whether the two agree
 * @param reconciledAt when the finish job compared them
 */
public record Reconciliation(long registered, long inFile, boolean match, Instant reconciledAt) {

    /**
     * Compares the two counts.
     *
     * @param registered rows in the registry
     * @param inFile practitioners in the file
     * @param now the comparison time
     * @return the result
     */
    public static Reconciliation of(long registered, long inFile, Instant now) {
        return new Reconciliation(registered, inFile, registered == inFile, now);
    }

    /** The BSON shape. */
    public Document toDocument() {
        return new Document("registered", registered)
                .append("inFile", inFile)
                .append("match", match)
                .append("reconciledAt", reconciledAt);
    }

    /**
     * Reads the BSON shape.
     *
     * @param doc the nested reconciliation document
     * @return the result
     */
    public static Reconciliation fromDocument(Document doc) {
        return new Reconciliation(
                Documents.longValue(doc, "registered"),
                Documents.longValue(doc, "inFile"),
                doc.getBoolean("match", false),
                Documents.instant(doc, "reconciledAt"));
    }
}
