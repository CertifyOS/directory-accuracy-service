package com.certifyos.directory_accuracy.export.batch;

import com.certifyos.directory_accuracy.persistence.Documents;
import org.bson.Document;

/**
 * The delivered file, as read from the object egress placed. The service never downloads it; these
 * are object metadata facts.
 *
 * @param name the object's file name
 * @param path the full {@code gs://} path
 * @param rowCount file rows (practitioner × location)
 * @param bytes object size
 * @param schemaVersion the contract version, {@code certify-export-v1}
 * @param md5 the object's MD5 as the bucket reports it, recorded not compared
 */
public record FileDetails(String name, String path, Long rowCount, Long bytes, String schemaVersion, String md5) {

    /** The BSON shape. */
    public Document toDocument() {
        Document doc = new Document();
        Documents.put(doc, "name", name);
        Documents.put(doc, "path", path);
        Documents.put(doc, "rowCount", rowCount);
        Documents.put(doc, "bytes", bytes);
        Documents.put(doc, "schemaVersion", schemaVersion);
        Documents.put(doc, "md5", md5);
        return doc;
    }

    /**
     * Reads the BSON shape.
     *
     * @param doc the nested file document
     * @return the details
     */
    public static FileDetails fromDocument(Document doc) {
        return new FileDetails(
                doc.getString("name"),
                doc.getString("path"),
                Documents.longValue(doc, "rowCount"),
                Documents.longValue(doc, "bytes"),
                doc.getString("schemaVersion"),
                doc.getString("md5"));
    }
}
