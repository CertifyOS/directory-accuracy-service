package com.certifyos.directory_accuracy.export.batch;

import com.certifyos.directory_accuracy.persistence.Documents;
import java.time.Instant;
import org.bson.Document;

/**
 * The count check the finish job writes once: the distinct practitioners egress selected against
 * the practitioners egress reports in the object metadata. A mismatch is counted, audited and
 * alerted, never guessed at; egress's list-used file ({@code selection.listUsedUri}) is what an
 * operator diffs against the file.
 *
 * @param selected distinct practitioners egress selected ({@code selection.distinct})
 * @param inFile practitioners egress reported in the file ({@code totalRecords})
 * @param match whether the two agree
 * @param reconciledAt when the finish job compared them
 */
public record Reconciliation(long selected, long inFile, boolean match, Instant reconciledAt) {

    /**
     * Compares the two counts.
     *
     * @param selected distinct practitioners egress selected
     * @param inFile practitioners in the file
     * @param now the comparison time
     * @return the result
     */
    public static Reconciliation of(long selected, long inFile, Instant now) {
        return new Reconciliation(selected, inFile, selected == inFile, now);
    }

    /** The BSON shape. */
    public Document toDocument() {
        return new Document("selected", selected)
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
                Documents.longValue(doc, "selected"),
                Documents.longValue(doc, "inFile"),
                doc.getBoolean("match", false),
                Documents.instant(doc, "reconciledAt"));
    }
}
