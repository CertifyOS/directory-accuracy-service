package com.certifyos.directory_accuracy.ingestion.jobs;

import com.certifyos.directory_accuracy.clients.FileIngestionClient;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundLifecycle;
import com.certifyos.directory_accuracy.ingestion.staging.RowStager;
import com.certifyos.directory_accuracy.ingestion.staging.StagingLifecycle;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

/** The branch the shared integration profile cannot reach: the kill switch (the test profile sets it on). */
class StageRowsJobTest {

    @Test
    void disabledServiceThrowsForARetryInsteadOfSucceedingAsANoOp() {
        InboundJobSupport support = Mockito.mock(InboundJobSupport.class);
        Mockito.when(support.enabled()).thenReturn(false);
        FileIngestionClient fileIngestion = Mockito.mock(FileIngestionClient.class);
        RowStager stager = Mockito.mock(RowStager.class);
        StageRowsJob job = new StageRowsJob(
                support,
                Mockito.mock(InboundLifecycle.class),
                Mockito.mock(StagingLifecycle.class),
                stager,
                fileIngestion,
                Mockito.mock(ObjectMapper.class),
                Clock.systemUTC());

        IllegalStateException deferred = Assertions.assertThrows(
                IllegalStateException.class,
                () -> job.run(new StageRowsJobRequest("org-1-candor-2026-10-001", "vendor-sftp|to/org-1/f.csv|17", 1)));

        Assertions.assertTrue(deferred.getMessage().contains("disabled"), deferred.getMessage());
        Mockito.verify(support, Mockito.never())
                .loadExpecting(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any());
        Mockito.verifyNoInteractions(fileIngestion, stager);
    }
}
