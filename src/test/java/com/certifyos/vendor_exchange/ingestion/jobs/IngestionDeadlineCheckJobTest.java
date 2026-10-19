package com.certifyos.vendor_exchange.ingestion.jobs;

import com.certifyos.vendor_exchange.clients.FileIngestionClient;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundLifecycle;
import java.time.Clock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

class IngestionDeadlineCheckJobTest {

    @Test
    void disabledServiceThrowsForARetryInsteadOfSucceedingAsANoOp() {
        InboundJobSupport support = Mockito.mock(InboundJobSupport.class);
        Mockito.when(support.enabled()).thenReturn(false);
        FileIngestionClient fileIngestion = Mockito.mock(FileIngestionClient.class);
        IngestionDeadlineCheckJob job = new IngestionDeadlineCheckJob(
                support, Mockito.mock(InboundLifecycle.class), fileIngestion, Clock.systemUTC());

        IllegalStateException deferred = Assertions.assertThrows(
                IllegalStateException.class,
                () -> job.run(new IngestionDeadlineCheckJobRequest(
                        "org-1-candor-2026-10-001", "vendor-sftp|to/org-1/f.csv|17", 1)));

        Assertions.assertTrue(deferred.getMessage().contains("disabled"), deferred.getMessage());
        Mockito.verify(support, Mockito.never())
                .loadExpecting(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any());
        Mockito.verifyNoInteractions(fileIngestion);
    }
}
