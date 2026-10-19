package com.certifyos.vendor_exchange.ingestion.jobs;

import com.certifyos.vendor_exchange.clients.ArchiveStore;
import com.certifyos.vendor_exchange.clients.FileIngestionClient;
import com.certifyos.vendor_exchange.config.VendorExchangeConfig;
import com.certifyos.vendor_exchange.export.batch.ExportBatch;
import com.certifyos.vendor_exchange.export.schedule.SelectionCriteria;
import com.certifyos.vendor_exchange.export.schedule.SelectionCriteria.Clause;
import com.certifyos.vendor_exchange.export.schedule.SelectionCriteria.Operator;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFile;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileRepository.Located;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileState;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundLifecycle;
import com.certifyos.vendor_exchange.metrics.VendorExchangeMetrics;
import java.time.Clock;
import java.time.Instant;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

/**
 * The two branches the shared integration profile cannot reach: the kill switch and an unset
 * mapping id (the test profile sets both on).
 */
class RequestIngestionJobTest {

    static final Instant NOW = Instant.parse("2026-10-15T09:13:00Z");
    static final ExportBatch BATCH = ExportBatch.scheduled(
            "org-xyz",
            "candor",
            YearMonth.of(2026, 10),
            1,
            new SelectionCriteria(List.of(new Clause("data.delegationStatus", Operator.IN, List.of("Direct")))),
            NOW);
    static final String UPLOAD_ID =
            "vendor-sftp|to/org-xyz/org-xyz_" + BATCH.id() + "_CB-2026-10-001_20261015.csv|1760520000123456";

    @Test
    void disabledServiceThrowsForARetryInsteadOfSucceedingAsANoOp() {
        InboundJobSupport support = Mockito.mock(InboundJobSupport.class);
        Mockito.when(support.enabled()).thenReturn(false);
        ArchiveStore archive = Mockito.mock(ArchiveStore.class);
        FileIngestionClient fileIngestion = Mockito.mock(FileIngestionClient.class);
        RequestIngestionJob job = new RequestIngestionJob(
                support,
                Mockito.mock(InboundLifecycle.class),
                archive,
                fileIngestion,
                Mockito.mock(IngestionJobEnqueuer.class),
                Clock.fixed(NOW, ZoneOffset.UTC));

        IllegalStateException deferred = Assertions.assertThrows(
                IllegalStateException.class, () -> job.run(new RequestIngestionJobRequest(BATCH.id(), UPLOAD_ID, 1)));

        Assertions.assertTrue(deferred.getMessage().contains("disabled"), deferred.getMessage());
        Mockito.verify(support, Mockito.never())
                .loadExpecting(
                        ArgumentMatchers.any(),
                        ArgumentMatchers.any(),
                        ArgumentMatchers.anyInt(),
                        ArgumentMatchers.any());
        Mockito.verifyNoInteractions(archive, fileIngestion);
    }

    @Test
    void unsetMappingIdFailsTheEntryWithoutCallingFileIngestion() {
        VendorExchangeConfig cfg = Mockito.mock(VendorExchangeConfig.class, Answers.RETURNS_DEEP_STUBS);
        Mockito.when(cfg.ingestion().archiveBucket()).thenReturn("test-archive-bucket");
        Mockito.when(cfg.fileIngestion().mappingId()).thenReturn(Optional.empty());
        InboundJobSupport support = Mockito.mock(InboundJobSupport.class);
        Mockito.when(support.enabled()).thenReturn(true);
        Mockito.when(support.config()).thenReturn(cfg);
        Mockito.when(support.metrics()).thenReturn(Mockito.mock(VendorExchangeMetrics.class));
        InboundFile entry = InboundFile.fromDocument(new Document("uploadId", UPLOAD_ID)
                .append("generation", "1760520000123456")
                .append("state", InboundFileState.ARCHIVING.name())
                .append("attempt", 1)
                .append("archivePath", "gs://test-archive-bucket/already/there.csv"));
        Located located = new Located(BATCH, entry);
        Mockito.when(support.loadExpecting(
                        ArgumentMatchers.eq(RequestIngestionJob.NAME),
                        ArgumentMatchers.eq(UPLOAD_ID),
                        ArgumentMatchers.eq(1),
                        ArgumentMatchers.any()))
                .thenReturn(Optional.of(located));
        InboundLifecycle lifecycle = Mockito.mock(InboundLifecycle.class);
        ArchiveStore archive = Mockito.mock(ArchiveStore.class);
        FileIngestionClient fileIngestion = Mockito.mock(FileIngestionClient.class);
        IngestionJobEnqueuer enqueuer = Mockito.mock(IngestionJobEnqueuer.class);
        RequestIngestionJob job = new RequestIngestionJob(
                support, lifecycle, archive, fileIngestion, enqueuer, Clock.fixed(NOW, ZoneOffset.UTC));

        job.run(new RequestIngestionJobRequest(BATCH.id(), UPLOAD_ID, 1));

        Mockito.verify(lifecycle)
                .failed(located, InboundFileState.ARCHIVING, "REGISTER", "MAPPING_UNKNOWN", "not configured", 1, NOW);
        Mockito.verifyNoInteractions(archive, fileIngestion, enqueuer);
    }
}
