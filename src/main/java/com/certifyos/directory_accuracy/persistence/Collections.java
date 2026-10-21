package com.certifyos.directory_accuracy.persistence;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import org.bson.Document;

/**
 * The eight collections this service owns in database {@code directory_accuracy}. JobRunr owns five more
 * ({@code jobrunr_*}) in the same database and our code never names them.
 */
@ApplicationScoped
public class Collections {

    /** Database name; also the JobRunr database and the Testcontainers replica set database. */
    public static final String DATABASE = "directory_accuracy";

    private final MongoClient client;

    public Collections(MongoClient client) {
        this.client = client;
    }

    MongoDatabase database() {
        return client.getDatabase(DATABASE);
    }

    /** One document per tenant and vendor. */
    public MongoCollection<Document> schedules() {
        return database().getCollection("vendor_export_schedules");
    }

    /** One document per tenant, vendor, period and sequence. */
    public MongoCollection<Document> batches() {
        return database().getCollection("vendor_export_batches");
    }

    /** One document per NPI within a batch; written once. */
    public MongoCollection<Document> npis() {
        return database().getCollection("vendor_export_npis");
    }

    /** Append-only audit trail; one document per lifecycle event. */
    public MongoCollection<Document> events() {
        return database().getCollection("vendor_export_events");
    }

    /** One document per recommended field value from the vendor, staged for review. */
    public MongoCollection<Document> fieldRecommendations() {
        return database().getCollection("field_recommendations");
    }

    /** Vendor files that matched no batch, kept until an operator resolves them. */
    public MongoCollection<Document> unmatchedUploads() {
        return database().getCollection("vendor_unmatched_uploads");
    }

    /** Values a reviewer rejected, suppressed until they expire. */
    public MongoCollection<Document> rejectedValues() {
        return database().getCollection("vendor_rejected_values");
    }

    /** Append-only review trail; one document per review action. */
    public MongoCollection<Document> reviewEvents() {
        return database().getCollection("vendor_review_events");
    }

    /**
     * Every collection this service owns, in declaration order.
     *
     * @return an unmodifiable list of the service collections
     */
    public List<MongoCollection<Document>> all() {
        return List.of(
                schedules(),
                batches(),
                npis(),
                events(),
                fieldRecommendations(),
                unmatchedUploads(),
                rejectedValues(),
                reviewEvents());
    }
}
