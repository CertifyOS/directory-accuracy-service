package com.certifyos.directory_accuracy.export.jobs;

import com.certifyos.directory_accuracy.config.DirectoryAccuracyConfig;
import com.certifyos.directory_accuracy.export.batch.BatchState;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.batch.ExportBatchRepository;
import java.time.Instant;
import java.time.YearMonth;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** The precheck every batch job runs first: kill switch, missing row, wrong state, right state. */
class BatchJobSupportTest {

    static final ExportBatch SCHEDULED = ExportBatch.scheduled(
            "org-xyz", "candor", YearMonth.of(2026, 10), 1, "flt-direct", Instant.parse("2026-10-01T06:00:12Z"));

    private static DirectoryAccuracyConfig config(boolean enabled) {
        DirectoryAccuracyConfig cfg = Mockito.mock(DirectoryAccuracyConfig.class);
        Mockito.when(cfg.enabled()).thenReturn(enabled);
        return cfg;
    }

    @Test
    void disabledServiceNeverReadsTheRow() {
        ExportBatchRepository batches = Mockito.mock(ExportBatchRepository.class);
        BatchJobSupport support = new BatchJobSupport(config(false), batches);

        Assertions.assertTrue(support.loadExpecting("select", SCHEDULED.id(), BatchState.SCHEDULED)
                .isEmpty());
        Mockito.verifyNoInteractions(batches);
    }

    @Test
    void missingRowIsANoOp() {
        ExportBatchRepository batches = Mockito.mock(ExportBatchRepository.class);
        Mockito.when(batches.find(SCHEDULED.id())).thenReturn(Optional.empty());
        BatchJobSupport support = new BatchJobSupport(config(true), batches);

        Assertions.assertTrue(support.loadExpecting("select", SCHEDULED.id(), BatchState.SCHEDULED)
                .isEmpty());
    }

    @Test
    void wrongStateIsANoOpAndRightStateReturnsTheRow() {
        ExportBatchRepository batches = Mockito.mock(ExportBatchRepository.class);
        Mockito.when(batches.find(SCHEDULED.id())).thenReturn(Optional.of(SCHEDULED));
        BatchJobSupport support = new BatchJobSupport(config(true), batches);

        Assertions.assertTrue(support.loadExpecting("finish", SCHEDULED.id(), BatchState.EGRESS_COMPLETED)
                .isEmpty());
        Assertions.assertEquals(
                SCHEDULED,
                support.loadExpecting("select", SCHEDULED.id(), BatchState.SCHEDULED)
                        .orElseThrow());
    }
}
