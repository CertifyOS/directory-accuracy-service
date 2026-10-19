package com.certifyos.vendor_exchange.ingestion.release;

import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendationRepository;
import com.certifyos.vendor_exchange.ingestion.staging.ReviewStatus;
import com.certifyos.vendor_exchange.persistence.Collections;
import com.certifyos.vendor_exchange.persistence.Documents;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.bson.Document;
import org.bson.conversions.Bson;

/**
 * The release lane's own reads of {@code field_recommendations}, beside the staging repository's
 * moves ({@code decide}, {@code moveAll}, {@code countReleasing}), which this lane also uses. Kept
 * here so the staging repository stays within its size and the release queries sit with their only
 * caller. Every read takes the tenant and carries a limit.
 */
@ApplicationScoped
public class ReleaseFieldRepository {

    private final Collections collections;
    private final FieldRecommendationRepository staged;

    public ReleaseFieldRepository(Collections collections, FieldRecommendationRepository staged) {
        this.collections = collections;
        this.staged = staged;
    }

    /** The staging repository, whose guarded moves and reads the release lane uses as they are. */
    public FieldRecommendationRepository staged() {
        return staged;
    }

    /**
     * The open release of a practitioner: one whose fields still wait in {@code RELEASE_REQUESTED},
     * which a new request joins. Read inside the request's transaction, on {@code
     * field_recs_practitioner}.
     *
     * @param session the request's transaction
     * @param tenantId the tenant
     * @param certifyPractitionerId the practitioner
     * @return the release id, or empty when the practitioner has no open release
     */
    public Optional<String> openRelease(ClientSession session, String tenantId, String certifyPractitionerId) {
        Document open = recommendations()
                .find(
                        session,
                        Filters.and(
                                Filters.eq("tenantId", tenantId),
                                Filters.eq("practitioner.certifyPractitionerId", certifyPractitionerId),
                                Filters.eq("reviewStatus", ReviewStatus.RELEASE_REQUESTED.name()),
                                Filters.exists("goldenRecordUpdate.releaseId")))
                .projection(Projections.include("goldenRecordUpdate.releaseId"))
                .sort(Sorts.ascending("_id"))
                .limit(1)
                .first();
        return Optional.ofNullable(open)
                .flatMap(doc -> Documents.nested(doc, "goldenRecordUpdate"))
                .map(update -> update.getString("releaseId"));
    }

    /**
     * A batch's {@code APPROVED} documents of the given fields, for a bulk release of the batch, on
     * {@code field_recs_batch_counts}.
     *
     * @param tenantId the tenant
     * @param exportBatchId the batch
     * @param fields the fields to take
     * @param limit the most documents to return, at least 1
     * @return the documents by id
     */
    public List<FieldRecommendation> approvedInBatch(
            String tenantId, String exportBatchId, Collection<String> fields, int limit) {
        return read(
                Filters.and(
                        Filters.eq("tenantId", tenantId),
                        Filters.eq("exportBatchId", exportBatchId),
                        Filters.eq("reviewStatus", ReviewStatus.APPROVED.name()),
                        Filters.in("field", fields)),
                limit);
    }

    List<FieldRecommendation> read(Bson filter, int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be at least 1, got " + limit);
        }
        List<FieldRecommendation> docs = new ArrayList<>();
        for (Document doc :
                recommendations().find(filter).sort(Sorts.ascending("_id")).limit(limit)) {
            docs.add(FieldRecommendation.fromDocument(doc));
        }
        return docs;
    }

    MongoCollection<Document> recommendations() {
        return collections.fieldRecommendations();
    }
}
