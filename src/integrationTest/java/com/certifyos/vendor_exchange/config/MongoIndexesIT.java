package com.certifyos.vendor_exchange.config;

import com.certifyos.vendor_exchange.ApiTestProfile;
import com.certifyos.vendor_exchange.MongoResource;
import com.certifyos.vendor_exchange.persistence.Collections;
import com.mongodb.client.MongoCollection;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Every index the design names exists by name, with the right keys, uniqueness and partial filter. */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(ApiTestProfile.class)
public class MongoIndexesIT {

    @Inject
    Collections collections;

    @Inject
    MongoIndexes indexes;

    private static Map<String, Document> byName(MongoCollection<Document> collection) {
        Map<String, Document> found = new HashMap<>();
        for (Document index : collection.listIndexes()) {
            found.put(index.getString("name"), index);
        }
        return found;
    }

    @Test
    void scheduleIndexes() {
        Map<String, Document> found = byName(collections.schedules());
        Assertions.assertEquals(
                new Document("enabled", 1).append("nextDueAt", 1),
                found.get("schedules_due").get("key"));
        Assertions.assertEquals(
                new Document("tenantId", 1), found.get("schedules_tenant").get("key"));
    }

    @Test
    void batchIndexes() {
        Map<String, Document> found = byName(collections.batches());
        Document periodSeq = found.get("batches_period_seq_unique");
        Assertions.assertEquals(
                new Document("tenantId", 1)
                        .append("vendor", 1)
                        .append("period", 1)
                        .append("seq", 1),
                periodSeq.get("key"));
        Assertions.assertTrue(periodSeq.getBoolean("unique", false));
        Assertions.assertEquals(
                new Document("state", 1).append("updatedAt", 1),
                found.get("batches_state_updated_at").get("key"));
        Assertions.assertEquals(
                new Document("egress.correlationId", 1),
                found.get("batches_correlation_id").get("key"));
        Assertions.assertNotNull(found.get("batches_correlation_id").get("partialFilterExpression"));
        Assertions.assertEquals(
                new Document("tenantId", 1).append("deliveredAt", 1),
                found.get("batches_tenant_delivered_at").get("key"));
    }

    @Test
    void npiIndexes() {
        Map<String, Document> found = byName(collections.npis());
        Assertions.assertEquals(
                new Document("exportBatchId", 1), found.get("npis_batch").get("key"));
        Assertions.assertEquals(
                new Document("tenantId", 1).append("npi", 1),
                found.get("npis_tenant_npi").get("key"));
    }

    @Test
    void eventIndexes() {
        Map<String, Document> found = byName(collections.events());
        Assertions.assertEquals(
                new Document("exportBatchId", 1).append("occurredAt", 1),
                found.get("events_by_batch").get("key"));
        Document received = found.get("events_received_message_unique");
        Assertions.assertTrue(received.getBoolean("unique", false));
        Assertions.assertEquals(
                new Document("type", "EXPORT_EVENT_RECEIVED"), received.get("partialFilterExpression", Document.class));
    }

    @Test
    void creatingAgainIsANoOp() {
        int before = byName(collections.batches()).size();
        indexes.createAll();
        Assertions.assertEquals(before, byName(collections.batches()).size());
    }

    @Test
    void ingestionIndexesExist() {
        Map<String, Document> batches = byName(collections.batches());
        assertKey(
                batches,
                "batches_inbound_state_updated",
                new Document("inboundFiles.state", 1).append("inboundFiles.updatedAt", 1));
        assertKey(batches, "batches_inbound_external_ref", new Document("inboundFiles.externalRef", 1));
        Assertions.assertEquals(
                new Document("inboundFiles.externalRef", new Document("$exists", true)),
                batches.get("batches_inbound_external_ref").get("partialFilterExpression", Document.class));
        assertKey(batches, "batches_inbound_upload_id_unique", new Document("inboundFiles.uploadId", 1));
        Assertions.assertEquals(
                new Document("inboundFiles.uploadId", new Document("$exists", true)),
                batches.get("batches_inbound_upload_id_unique").get("partialFilterExpression", Document.class));
        Assertions.assertEquals(
                Boolean.TRUE,
                batches.get("batches_inbound_upload_id_unique").getBoolean("unique"),
                "one upload lives on one batch");
        Assertions.assertFalse(batches.containsKey("batches_inbound_upload_id"), "the non-unique index is gone");

        Map<String, Document> recommendations = byName(collections.fieldRecommendations());
        Document openStates = new Document("reviewStatus", new Document("$in", List.of("PENDING", "SKIPPED")));
        assertKey(
                recommendations,
                "field_recs_open_list",
                new Document("tenantId", 1)
                        .append("reviewStatus", 1)
                        .append("field", 1)
                        .append("stagedAt", 1)
                        .append("_id", 1));
        Assertions.assertEquals(
                openStates, recommendations.get("field_recs_open_list").get("partialFilterExpression", Document.class));
        assertKey(
                recommendations,
                "field_recs_batch_counts",
                new Document("tenantId", 1)
                        .append("exportBatchId", 1)
                        .append("reviewStatus", 1)
                        .append("field", 1));
        assertKey(
                recommendations,
                "field_recs_practitioner",
                new Document("tenantId", 1)
                        .append("practitioner.certifyPractitionerId", 1)
                        .append("exportBatchId", 1));
        assertKey(
                recommendations,
                "field_recs_npi",
                new Document("tenantId", 1).append("practitioner.npi", 1).append("exportBatchId", 1));
        assertKey(
                recommendations,
                "field_recs_release",
                new Document("tenantId", 1).append("goldenRecordUpdate.releaseId", 1));
        Assertions.assertEquals(
                new Document("goldenRecordUpdate.releaseId", new Document("$exists", true)),
                recommendations.get("field_recs_release").get("partialFilterExpression", Document.class));
        assertKey(
                recommendations,
                "field_recs_open_list_any_field",
                new Document("tenantId", 1)
                        .append("reviewStatus", 1)
                        .append("stagedAt", 1)
                        .append("_id", 1));
        Assertions.assertEquals(
                openStates,
                recommendations.get("field_recs_open_list_any_field").get("partialFilterExpression", Document.class));

        Map<String, Document> rejected = byName(collections.rejectedValues());
        assertKey(rejected, "rejected_values_ttl", new Document("expiresAt", 1));
        Assertions.assertEquals(0, rejected.get("rejected_values_ttl").getInteger("expireAfterSeconds"));
        assertKey(
                rejected,
                "rejected_values_practitioner",
                new Document("tenantId", 1).append("certifyPractitionerId", 1));

        Map<String, Document> unmatched = byName(collections.unmatchedUploads());
        assertKey(unmatched, "unmatched_resolution", new Document("resolution.kind", 1).append("receivedAt", 1));
        assertKey(
                unmatched,
                "unmatched_open_by_tenant",
                new Document("folderTenant", 1)
                        .append("resolution.kind", 1)
                        .append("receivedAt", 1)
                        .append("_id", 1));

        Map<String, Document> reviewEvents = byName(collections.reviewEvents());
        assertKey(reviewEvents, "review_events_by_field", new Document("fieldRecommendationId", 1).append("_id", 1));
        assertKey(
                reviewEvents,
                "review_events_by_release",
                new Document("tenantId", 1).append("releaseId", 1).append("_id", 1));
        Assertions.assertEquals(
                new Document("releaseId", new Document("$exists", true)),
                reviewEvents.get("review_events_by_release").get("partialFilterExpression", Document.class));
        assertKey(reviewEvents, "review_events_by_tenant", new Document("tenantId", 1).append("occurredAt", 1));
    }

    private static void assertKey(Map<String, Document> found, String name, Document key) {
        Assertions.assertNotNull(found.get(name), "missing index " + name);
        Assertions.assertEquals(key, found.get(name).get("key"), name);
    }

    @Test
    void noIndexExpiresDocuments() {
        // Seven-year retention (design decision D16, plan decision P4): the one way Mongo deletes on
        // its own is a TTL index, so no service collection may carry expireAfterSeconds except the
        // rejected-value suppression window, which is short-lived by design.
        for (var collection : collections.all()) {
            String collectionName = collection.getNamespace().getCollectionName();
            for (Document index : collection.listIndexes()) {
                boolean allowed = collectionName.equals("vendor_rejected_values")
                        && "rejected_values_ttl".equals(index.getString("name"));
                if (!allowed) {
                    Assertions.assertFalse(
                            index.containsKey("expireAfterSeconds"),
                            collectionName + " index " + index.getString("name") + " is a TTL index");
                }
            }
        }
    }
}
