package com.certifyos.directory_accuracy.export.api;

import com.certifyos.directory_accuracy.export.batch.EgressDetails;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.batch.ExportNpi;
import com.certifyos.directory_accuracy.export.batch.FileDetails;
import com.certifyos.directory_accuracy.export.batch.Reconciliation;
import com.certifyos.directory_accuracy.export.schedule.ScheduleRequests;
import com.certifyos.directory_accuracy.export.schedule.ScheduleRequests.ClauseRequest;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/** The batch as operators and the reviewer UI read it: the document minus internal job ids. */
public final class BatchViews {

    private BatchViews() {}

    /** The selection copied at creation and its progress. */
    public record SelectionView(
            Map<String, ClauseRequest> criteria, Integer page, Integer practitionersSelected, Instant completedAt) {}

    /** The egress request as recorded, without the deadline job id. */
    public record EgressView(
            String correlationId,
            String jobReference,
            String templateId,
            Integer templateVersion,
            String mappingsCsvUrl,
            String separator,
            String outputFormat,
            List<String> rowExpansionKeys,
            EgressDetails.Destination destination,
            Instant requestedAt,
            Instant completedAt,
            String completionSource,
            String fileProducedBy) {
        static EgressView of(EgressDetails egress) {
            return egress == null
                    ? null
                    : new EgressView(
                            egress.correlationId(),
                            egress.jobReference(),
                            egress.templateId(),
                            egress.templateVersion(),
                            egress.mappingsCsvUrl(),
                            egress.separator(),
                            egress.outputFormat(),
                            egress.rowExpansionKeys(),
                            egress.destination(),
                            egress.requestedAt(),
                            egress.completedAt(),
                            egress.completionSource() == null
                                    ? null
                                    : egress.completionSource().name(),
                            egress.fileProducedBy());
        }
    }

    /** One batch. */
    public record BatchView(
            String id,
            String tenantId,
            String vendor,
            String period,
            int seq,
            String state,
            int attempt,
            SelectionView selection,
            EgressView egress,
            FileDetails file,
            Reconciliation reconciliation,
            String failedStep,
            String lastError,
            String supersededBy,
            Instant acknowledgedAt,
            String inboundBatchId,
            Instant deliveredAt,
            long version,
            Instant createdAt,
            Instant updatedAt) {

        /** Builds the view. */
        public static BatchView of(ExportBatch batch) {
            return new BatchView(
                    batch.id(),
                    batch.tenantId(),
                    batch.vendor(),
                    batch.period(),
                    batch.seq(),
                    batch.state().name(),
                    batch.attempt(),
                    new SelectionView(
                            ScheduleRequests.toSelection(batch.selection().criteria()),
                            batch.selection().page(),
                            batch.selection().practitionersSelected(),
                            batch.selection().completedAt()),
                    EgressView.of(batch.egress()),
                    batch.file(),
                    batch.reconciliation(),
                    batch.failedStep() == null ? null : batch.failedStep().name(),
                    batch.lastError(),
                    batch.supersededBy(),
                    batch.acknowledgedAt(),
                    batch.inboundBatchId(),
                    batch.deliveredAt(),
                    batch.version(),
                    batch.createdAt(),
                    batch.updatedAt());
        }
    }

    /** One registered practitioner. */
    public record NpiView(String npi, String certifyPractitionerId, Instant registeredAt) {
        static NpiView of(ExportNpi row) {
            return new NpiView(row.npi(), row.certifyPractitionerId(), row.registeredAt());
        }
    }

    /**
     * A keyset page of NPIs, ordered by NPI.
     *
     * @param items the page
     * @param nextAfter the {@code after} value for the next page, or null on the last page
     */
    public record NpiPage(List<NpiView> items, String nextAfter) {}

    /** {@code retry} and {@code supersede} body. */
    public record ReasonRequest(String reason) {}

    /** {@code retry} answer. */
    public record RetryResponse(String exportBatchId, int attempt, String jobId) {}

    /** {@code supersede} answer. */
    public record SupersedeResponse(String exportBatchId, String jobId) {}
}
