package com.certifyos.vendor_exchange.config;

import com.certifyos.vendor_exchange.persistence.Collections;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import java.util.concurrent.TimeUnit;
import org.bson.conversions.Bson;

/**
 * Every index the design names, created on every boot of either role. Safe: MongoDB joins an
 * in-progress identical build and is a no-op against an existing one, so several instances calling
 * {@code createIndex} with the same spec at once is fine (JobRunr does the same in this database).
 * These are correctness guards (uniqueness of batches and NPIs, idempotency of received events), so
 * they stay synchronous at boot. Any index added after go-live is built out of band first: a boot
 * time build over a large collection would hold the instance's health check hostage. Index options
 * never change in place (MongoDB refuses with IndexOptionsConflict); a changed index is a new name.
 */
@ApplicationScoped
public class MongoIndexes {

    private final Collections collections;

    public MongoIndexes(Collections collections) {
        this.collections = collections;
    }

    void onStart(@Observes StartupEvent event) {
        createAll();
    }

    /** Creates every index; idempotent. Public so a test can call it without a boot. */
    public void createAll() {
        // schedules: _id = tenant|vendor is unique by construction
        collections
                .schedules()
                .createIndex(Indexes.ascending("enabled", "nextDueAt"), new IndexOptions().name("schedules_due"));
        collections.schedules().createIndex(Indexes.ascending("tenantId"), new IndexOptions().name("schedules_tenant"));

        // batches
        collections
                .batches()
                .createIndex(
                        Indexes.ascending("tenantId", "vendor", "period", "seq"),
                        new IndexOptions().name("batches_period_seq_unique").unique(true));
        collections
                .batches()
                .createIndex(
                        Indexes.ascending("state", "updatedAt"), new IndexOptions().name("batches_state_updated_at"));
        collections
                .batches()
                .createIndex(
                        Indexes.ascending("egress.correlationId"),
                        new IndexOptions()
                                .name("batches_correlation_id")
                                .partialFilterExpression(Filters.exists("egress.correlationId")));
        collections
                .batches()
                .createIndex(
                        Indexes.ascending("tenantId", "deliveredAt"),
                        new IndexOptions().name("batches_tenant_delivered_at"));

        // npis: _id = batch|npi is unique by construction
        collections.npis().createIndex(Indexes.ascending("exportBatchId"), new IndexOptions().name("npis_batch"));
        collections
                .npis()
                .createIndex(Indexes.ascending("tenantId", "npi"), new IndexOptions().name("npis_tenant_npi"));

        // events
        collections
                .events()
                .createIndex(
                        Indexes.ascending("exportBatchId", "occurredAt"), new IndexOptions().name("events_by_batch"));
        collections
                .events()
                .createIndex(
                        Indexes.ascending("tenantId", "vendor", "occurredAt"),
                        new IndexOptions().name("events_by_tenant_vendor"));
        collections
                .events()
                .createIndex(
                        Indexes.ascending("detail.messageId"),
                        new IndexOptions()
                                .name("events_received_message_unique")
                                .unique(true)
                                .partialFilterExpression(Filters.eq("type", "EXPORT_EVENT_RECEIVED")));

        createIngestionIndexes();
    }

    private void createIngestionIndexes() {
        // batches: the reconciler reads by updatedAt, "without progress" being a question about the last write
        collections
                .batches()
                .createIndex(
                        Indexes.ascending("inboundFiles.state", "inboundFiles.updatedAt"),
                        new IndexOptions().name("batches_inbound_state_updated"));
        collections
                .batches()
                .createIndex(
                        Indexes.ascending("inboundFiles.externalRef"),
                        new IndexOptions()
                                .name("batches_inbound_external_ref")
                                .partialFilterExpression(Filters.exists("inboundFiles.externalRef")));
        // batches: a redelivered storage event finds its entry by upload id, and one upload lives on
        // one batch. A unique multikey index enforces each upload id across documents (not within one
        // array, which the claim's filter guards). Replaces batches_inbound_upload_id, which was never
        // released: MongoDB refuses a second index on the same key under another name.
        collections
                .batches()
                .createIndex(
                        Indexes.ascending("inboundFiles.uploadId"),
                        new IndexOptions()
                                .name("batches_inbound_upload_id_unique")
                                .unique(true)
                                .partialFilterExpression(Filters.exists("inboundFiles.uploadId")));

        // field_recommendations
        Bson openStates = Filters.in("reviewStatus", "PENDING", "SKIPPED");
        collections
                .fieldRecommendations()
                .createIndex(
                        Indexes.ascending("tenantId", "reviewStatus", "field", "stagedAt", "_id"),
                        new IndexOptions().name("field_recs_open_list").partialFilterExpression(openStates));
        collections
                .fieldRecommendations()
                .createIndex(
                        Indexes.ascending("tenantId", "exportBatchId", "reviewStatus", "field"),
                        new IndexOptions().name("field_recs_batch_counts"));
        collections
                .fieldRecommendations()
                .createIndex(
                        Indexes.ascending("tenantId", "practitioner.certifyPractitionerId", "exportBatchId"),
                        new IndexOptions().name("field_recs_practitioner"));
        collections
                .fieldRecommendations()
                .createIndex(
                        Indexes.ascending("tenantId", "practitioner.npi", "exportBatchId"),
                        new IndexOptions().name("field_recs_npi"));
        collections
                .fieldRecommendations()
                .createIndex(
                        Indexes.ascending("tenantId", "goldenRecordUpdate.releaseId"),
                        new IndexOptions()
                                .name("field_recs_release")
                                .partialFilterExpression(Filters.exists("goldenRecordUpdate.releaseId")));
        // the open list without a field filter
        collections
                .fieldRecommendations()
                .createIndex(
                        Indexes.ascending("tenantId", "reviewStatus", "stagedAt", "_id"),
                        new IndexOptions()
                                .name("field_recs_open_list_any_field")
                                .partialFilterExpression(openStates));

        // vendor_rejected_values: the only TTL index in the service (design D16, plan P4)
        collections
                .rejectedValues()
                .createIndex(
                        Indexes.ascending("expiresAt"),
                        new IndexOptions().name("rejected_values_ttl").expireAfter(0L, TimeUnit.SECONDS));
        collections
                .rejectedValues()
                .createIndex(
                        Indexes.ascending("tenantId", "certifyPractitionerId"),
                        new IndexOptions().name("rejected_values_practitioner"));

        // vendor_unmatched_uploads
        collections
                .unmatchedUploads()
                .createIndex(
                        Indexes.ascending("resolution.kind", "receivedAt"),
                        new IndexOptions().name("unmatched_resolution"));

        // vendor_review_events
        collections
                .reviewEvents()
                .createIndex(
                        Indexes.ascending("fieldRecommendationId", "_id"),
                        new IndexOptions().name("review_events_by_field"));
        collections
                .reviewEvents()
                .createIndex(
                        Indexes.ascending("tenantId", "releaseId", "_id"),
                        new IndexOptions()
                                .name("review_events_by_release")
                                .partialFilterExpression(Filters.exists("releaseId")));
        collections
                .reviewEvents()
                .createIndex(
                        Indexes.ascending("tenantId", "occurredAt"),
                        new IndexOptions().name("review_events_by_tenant"));
    }
}
