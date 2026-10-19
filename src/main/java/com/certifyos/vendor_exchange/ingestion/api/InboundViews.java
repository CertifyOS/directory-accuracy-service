package com.certifyos.vendor_exchange.ingestion.api;

import com.certifyos.vendor_exchange.ingestion.inbound.InboundFile;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundNames;
import com.certifyos.vendor_exchange.ingestion.inbound.UnmatchedOperations;
import com.certifyos.vendor_exchange.ingestion.inbound.UnmatchedUpload;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Inbound files and unmatched uploads as operators read them: the documents minus internal job ids. */
public final class InboundViews {

    private InboundViews() {}

    /** One {@code inboundFiles[]} entry, every field but the job id. */
    public record InboundFileView(
            String uploadId,
            String generation,
            String vendorBatchId,
            String fileDate,
            String state,
            int attempt,
            String archivePath,
            Long archiveBytes,
            String externalRef,
            String ingestionId,
            InboundFile.Parse parse,
            InboundFile.Checkpoint checkpoint,
            Map<String, Long> dispositions,
            InboundFile.Written written,
            String failedStep,
            String failureReason,
            String lastError,
            String overwroteGeneration,
            Instant eventTime,
            Instant receivedAt,
            Instant requestedAt,
            Instant parsedAt,
            Instant stagedAt,
            Instant updatedAt) {

        public InboundFileView {
            // Insertion order is kept, as InboundFile keeps it, so the view lists the design's order.
            dispositions =
                    dispositions == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(dispositions));
        }

        /** Builds the view. */
        public static InboundFileView of(InboundFile entry) {
            return new InboundFileView(
                    entry.uploadId(),
                    entry.generation(),
                    entry.vendorBatchId(),
                    entry.fileDate(),
                    entry.state() == null ? null : entry.state().name(),
                    entry.attempt(),
                    entry.archivePath(),
                    entry.archiveBytes(),
                    entry.externalRef(),
                    entry.ingestionId(),
                    entry.parse(),
                    entry.checkpoint(),
                    entry.dispositions(),
                    entry.written(),
                    entry.failedStep(),
                    entry.failureReason(),
                    entry.lastError(),
                    entry.overwroteGeneration(),
                    entry.eventTime(),
                    entry.receivedAt(),
                    entry.requestedAt(),
                    entry.parsedAt(),
                    entry.stagedAt(),
                    entry.updatedAt());
        }
    }

    /** What an unmatched upload's object name said. */
    public record ParsedNameView(
            String folderTenant,
            String fileTenant,
            String exportBatchRef,
            String vendorBatchId,
            String fileDate,
            String fileName) {
        static ParsedNameView of(InboundNames.Parsed parsed) {
            return parsed == null
                    ? null
                    : new ParsedNameView(
                            parsed.folderTenant(),
                            parsed.fileTenant(),
                            parsed.exportBatchRef(),
                            parsed.vendorBatchId(),
                            parsed.fileDate(),
                            parsed.fileName());
        }
    }

    /** How an operator closed an unmatched upload. */
    public record ResolutionView(String kind, String exportBatchId, String reason, String actor, Instant at) {
        static ResolutionView of(UnmatchedUpload.Resolution resolution) {
            return resolution == null
                    ? null
                    : new ResolutionView(
                            resolution.kind(),
                            resolution.exportBatchId(),
                            resolution.reason(),
                            resolution.actor(),
                            resolution.at());
        }
    }

    /** One unmatched upload, with its resolution once closed. */
    public record UnmatchedUploadView(
            String id,
            String folderTenant,
            ParsedNameView parsed,
            String reason,
            Instant receivedAt,
            ResolutionView resolution) {

        /** Builds the view. */
        public static UnmatchedUploadView of(UnmatchedUpload upload) {
            return new UnmatchedUploadView(
                    upload.id(),
                    upload.folderTenant(),
                    ParsedNameView.of(upload.parsed()),
                    upload.reason(),
                    upload.receivedAt(),
                    ResolutionView.of(upload.resolution()));
        }
    }

    /** {@code attach} body. */
    public record AttachRequest(String exportBatchId, String reason) {}

    /** {@code discard} body. */
    public record DiscardRequest(String reason) {}

    /** {@code attach} answer: the entry's state after the claim and the job this call enqueued, if any. */
    public record AttachResponse(String uploadId, String state, String jobId) {
        static AttachResponse of(UnmatchedOperations.Attached attached) {
            return new AttachResponse(
                    attached.uploadId(),
                    attached.state().name(),
                    attached.jobId() == null ? null : attached.jobId().toString());
        }
    }
}
