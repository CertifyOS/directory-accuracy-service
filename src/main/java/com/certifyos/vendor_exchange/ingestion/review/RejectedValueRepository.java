package com.certifyos.vendor_exchange.ingestion.review;

import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation;
import com.certifyos.vendor_exchange.persistence.Collections;
import com.certifyos.vendor_exchange.persistence.Documents;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.PushOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.bson.Document;
import org.bson.conversions.Bson;

/**
 * The {@code vendor_rejected_values} collection (design step 7, D16). Written only by the decision
 * transaction on {@code REJECT}, one upsert by {@code _id}; read by nothing in the MVP but the
 * report-shaped {@link #forPractitioner}, which has no endpoint yet. The TTL index on
 * {@code expiresAt} (the service's only one, plan P4) removes a row twelve months after its latest
 * rejection.
 */
@ApplicationScoped
public class RejectedValueRepository {

    private final Collections collections;

    public RejectedValueRepository(Collections collections) {
        this.collections = collections;
    }

    /**
     * What a rejection did to the memory.
     *
     * @param id the row
     * @param rejectionCount the row's count after this rejection, 1 when it was new
     */
    public record Upserted(String id, long rejectionCount) {}

    /**
     * Remembers one rejection: one {@code findOneAndUpdate} by {@code _id} with upsert. A new row gets
     * its identity, the normalised and raw value, the normalisation version and
     * {@code firstRejectedAt} ({@code $setOnInsert}); every rejection adds one to
     * {@code rejectionCount}, moves {@code lastRejectedAt} and {@code expiresAt}, and appends to
     * {@code rejections[]}, which keeps the latest {@link RejectedValue#HISTORY_LIMIT}.
     *
     * @param session the decision's transaction
     * @param recommendation the field recommendation being rejected
     * @param actor the reviewer
     * @param reason the reviewer's comment
     * @param now the decision time
     * @param ttlMonths months after this rejection the row expires
     * @return the row id and its count
     */
    public Upserted upsert(
            ClientSession session,
            FieldRecommendation recommendation,
            String actor,
            String reason,
            Instant now,
            int ttlMonths) {
        String id = RejectedValue.idFor(recommendation);
        Document identity = new Document("tenantId", recommendation.tenantId())
                .append("certifyPractitionerId", recommendation.practitioner().certifyPractitionerId());
        Documents.put(
                identity,
                "certifyLocationId",
                recommendation.location() == null
                        ? null
                        : recommendation.location().certifyLocationId());
        identity.append("field", recommendation.field())
                .append("normalizedValue", RejectedValue.keyValue(recommendation))
                .append("normalizationVersion", ValueNormalizer.VERSION);
        Documents.put(identity, "rawValue", recommendation.vendorValue());
        identity.append("firstRejectedAt", now);
        RejectedValue.Rejection rejection =
                new RejectedValue.Rejection(recommendation.exportBatchId(), actor, now, reason);
        Bson update = Updates.combine(
                Updates.setOnInsert(identity),
                Updates.inc("rejectionCount", 1L),
                Updates.set("lastRejectedAt", now),
                Updates.set("expiresAt", expiry(now, ttlMonths)),
                Updates.pushEach(
                        "rejections",
                        List.of(rejection.toDocument()),
                        new PushOptions().slice(-RejectedValue.HISTORY_LIMIT)));
        Document after = rejectedValues()
                .findOneAndUpdate(
                        session,
                        Filters.eq("_id", id),
                        update,
                        new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));
        Long count = after == null ? null : Documents.longValue(after, "rejectionCount");
        return new Upserted(id, count == null ? 0 : count);
    }

    /**
     * {@code now} plus whole calendar months, in UTC; {@code Instant.plus} has no month unit. A
     * setting below one month is refused: it would put {@code expiresAt} at or before now, and the TTL
     * monitor would delete the row on its next pass without any error.
     */
    static Instant expiry(Instant now, int ttlMonths) {
        if (ttlMonths < 1) {
            throw new IllegalArgumentException(
                    "vendor-exchange.ingestion.rejected-value-ttl-months must be at least 1, got " + ttlMonths);
        }
        return now.atZone(ZoneOffset.UTC).plusMonths(ttlMonths).toInstant();
    }

    /**
     * One row of a tenant.
     *
     * @param tenantId the tenant
     * @param id the row id
     * @return the row, or empty when absent or another tenant's
     */
    public Optional<RejectedValue> find(String tenantId, String id) {
        return Optional.ofNullable(rejectedValues()
                        .find(Filters.and(Filters.eq("_id", id), Filters.eq("tenantId", tenantId)))
                        .first())
                .map(RejectedValue::fromDocument);
    }

    /**
     * Everything rejected for one practitioner, on {@code rejected_values_practitioner}: the report
     * the design names ("show me everything rejected for this practitioner").
     *
     * @param tenantId the tenant
     * @param certifyPractitionerId the practitioner
     * @param limit the most rows to return, at least 1
     * @return the rows, latest rejection first, then by id
     */
    public List<RejectedValue> forPractitioner(String tenantId, String certifyPractitionerId, int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be at least 1, got " + limit);
        }
        List<RejectedValue> rows = new ArrayList<>();
        for (Document doc : rejectedValues()
                .find(Filters.and(
                        Filters.eq("tenantId", tenantId), Filters.eq("certifyPractitionerId", certifyPractitionerId)))
                .sort(Sorts.descending("lastRejectedAt", "_id"))
                .limit(limit)) {
            rows.add(RejectedValue.fromDocument(doc));
        }
        return rows;
    }

    private MongoCollection<Document> rejectedValues() {
        return collections.rejectedValues();
    }
}
