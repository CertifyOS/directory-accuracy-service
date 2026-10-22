package com.certifyos.directory_accuracy.export.api;

import com.certifyos.directory_accuracy.export.batch.EgressDetails;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.batch.FileDetails;
import com.certifyos.directory_accuracy.export.batch.Reconciliation;
import java.time.Instant;
import java.util.List;

/** The batch as operators and the reviewer UI read it: the document minus internal job ids. */
public final class BatchViews {

    private BatchViews() {}

    /** The saved filter id copied at creation and egress's selection counts once it completed. */
    public record SelectionView(
            String filterId,
            Long requested,
            Long distinct,
            Long unmatched,
            Long parts,
            String reusedFrom,
            String listUsedUri,
            Boolean reEvaluated,
            Instant completedAt) {

        static SelectionView of(ExportBatch.Selection selection) {
            return new SelectionView(
                    selection.filterId(),
                    selection.requested(),
                    selection.distinct(),
                    selection.unmatched(),
                    selection.parts(),
                    selection.reusedFrom(),
                    selection.listUsedUri(),
                    selection.reEvaluated(),
                    selection.completedAt());
        }
    }

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
                    SelectionView.of(batch.selection()),
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

    /** {@code retry} and {@code supersede} body. */
    public record ReasonRequest(String reason) {}

    /** {@code retry} answer. */
    public record RetryResponse(String exportBatchId, int attempt, String jobId) {}

    /** {@code supersede} answer. */
    public record SupersedeResponse(String exportBatchId, String jobId) {}
}
