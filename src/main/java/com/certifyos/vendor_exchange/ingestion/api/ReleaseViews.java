package com.certifyos.vendor_exchange.ingestion.api;

import com.certifyos.vendor_exchange.ingestion.api.ReviewViews.FieldView;
import com.certifyos.vendor_exchange.ingestion.release.ReleaseService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** The release endpoints' bodies and answers. */
public final class ReleaseViews {

    private ReleaseViews() {}

    /** {@code POST /fields/{id}/release} body: the version the reviewer read. */
    public record ReleaseRequest(Long version) {}

    /** {@code POST /fields/{id}/release} answer. */
    public record ReleaseAccepted(String releaseId) {}

    /**
     * {@code POST /releases} body: either {@code ids} with their {@code versions}, or {@code
     * exportBatchId} with {@code all = true} for every approved slice field of the batch.
     */
    public record BulkReleaseRequest(List<String> ids, Map<String, Long> versions, String exportBatchId, Boolean all) {
        public BulkReleaseRequest {
            // Copies that keep a null element or value, which the resource refuses or skips by name.
            ids = ids == null ? null : Collections.unmodifiableList(new ArrayList<>(ids));
            versions = versions == null ? null : Collections.unmodifiableMap(new LinkedHashMap<>(versions));
        }
    }

    /** One release a bulk call created or joined. */
    public record ReleaseSummary(String releaseId, String certifyPractitionerId, List<String> fieldRecommendationIds) {
        public ReleaseSummary {
            fieldRecommendationIds = List.copyOf(fieldRecommendationIds);
        }

        static ReleaseSummary of(ReleaseService.Requested requested) {
            return new ReleaseSummary(
                    requested.releaseId(), requested.certifyPractitionerId(), requested.fieldRecommendationIds());
        }
    }

    /** A bulk item that was not released, with the code a single request would have answered. */
    public record SkippedItem(String id, String code) {}

    /** {@code POST /releases} answer. */
    public record BulkReleaseAccepted(List<ReleaseSummary> releases, List<SkippedItem> skipped) {
        public BulkReleaseAccepted {
            releases = List.copyOf(releases);
            skipped = List.copyOf(skipped);
        }

        static BulkReleaseAccepted of(ReleaseService.Bulk bulk) {
            return new BulkReleaseAccepted(
                    bulk.releases().stream().map(ReleaseSummary::of).toList(),
                    bulk.skipped().stream()
                            .map(item -> new SkippedItem(item.id(), item.code()))
                            .toList());
        }
    }

    /** {@code GET /releases/{releaseId}} answer: the release's fields with their state and outcome. */
    public record ReleaseView(String releaseId, List<FieldView> fields) {
        public ReleaseView {
            fields = List.copyOf(fields);
        }
    }

    /** {@code POST /fields/{id}/rollback} body. */
    public record RollbackRequest(Long version, String reason) {}
}
