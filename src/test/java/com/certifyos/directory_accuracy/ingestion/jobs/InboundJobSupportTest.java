package com.certifyos.directory_accuracy.ingestion.jobs;

import com.certifyos.directory_accuracy.config.VendorExchangeConfig;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFile;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFileRepository;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFileRepository.Located;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFileState;
import com.certifyos.directory_accuracy.metrics.VendorExchangeMetrics;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import org.bson.Document;
import org.jboss.logging.MDC;
import org.jobrunr.jobs.context.JobContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** The precheck every inbound job runs: missing entry, wrong state, right state, and the attempt count. */
class InboundJobSupportTest {

    static final String UPLOAD_ID = RequestIngestionJobTest.UPLOAD_ID;
    static final Set<InboundFileState> EXPECTED = EnumSet.of(InboundFileState.RECEIVED, InboundFileState.ARCHIVING);

    private static InboundJobSupport support(InboundFileRepository inbound) {
        VendorExchangeConfig cfg = Mockito.mock(VendorExchangeConfig.class);
        Mockito.when(cfg.enabled()).thenReturn(true);
        return new InboundJobSupport(cfg, inbound, Mockito.mock(VendorExchangeMetrics.class));
    }

    private static Located in(InboundFileState state) {
        InboundFile entry = InboundFile.fromDocument(new Document("uploadId", UPLOAD_ID)
                .append("generation", "1760520000123456")
                .append("state", state.name())
                .append("attempt", 1));
        return new Located(RequestIngestionJobTest.BATCH, entry);
    }

    private static RequestIngestionJobRequest request(int attempt) {
        return new RequestIngestionJobRequest(RequestIngestionJobTest.BATCH.id(), UPLOAD_ID, attempt);
    }

    @Test
    void missingEntryIsEmpty() {
        InboundFileRepository inbound = Mockito.mock(InboundFileRepository.class);
        Mockito.when(inbound.findByUploadId(UPLOAD_ID)).thenReturn(Optional.empty());

        Assertions.assertTrue(support(inbound)
                .loadExpecting("request-ingestion", request(1), EXPECTED)
                .isEmpty());
    }

    @Test
    void entryOfAnotherAttemptIsEmpty() {
        InboundFileRepository inbound = Mockito.mock(InboundFileRepository.class);
        Mockito.when(inbound.findByUploadId(UPLOAD_ID)).thenReturn(Optional.of(in(InboundFileState.ARCHIVING)));

        Assertions.assertTrue(
                support(inbound)
                        .loadExpecting("request-ingestion", request(2), EXPECTED)
                        .isEmpty(),
                "a request for attempt 2 does not work on the attempt 1 entry");
    }

    @Test
    void entryOnAnotherBatchIsEmpty() {
        InboundFileRepository inbound = Mockito.mock(InboundFileRepository.class);
        Mockito.when(inbound.findByUploadId(UPLOAD_ID)).thenReturn(Optional.of(in(InboundFileState.ARCHIVING)));

        Assertions.assertTrue(
                support(inbound)
                        .loadExpecting(
                                "request-ingestion",
                                new RequestIngestionJobRequest("org-xyz-candor-2026-09-001", UPLOAD_ID, 1),
                                EXPECTED)
                        .isEmpty(),
                "a request for another batch does not work on this batch's entry");
    }

    @Test
    void entryInAnotherStateIsEmpty() {
        InboundFileRepository inbound = Mockito.mock(InboundFileRepository.class);
        Mockito.when(inbound.findByUploadId(UPLOAD_ID)).thenReturn(Optional.of(in(InboundFileState.PARSED)));

        Assertions.assertTrue(support(inbound)
                .loadExpecting("request-ingestion", request(1), EXPECTED)
                .isEmpty());
    }

    @Test
    void entryInAnExpectedStateIsReturnedAndPutsTheTenantOnTheContext() {
        InboundFileRepository inbound = Mockito.mock(InboundFileRepository.class);
        Located located = in(InboundFileState.ARCHIVING);
        Mockito.when(inbound.findByUploadId(UPLOAD_ID)).thenReturn(Optional.of(located));

        try (IngestionLogContext ignored =
                IngestionLogContext.open(located.batch().id(), UPLOAD_ID, null)) {
            Assertions.assertEquals(
                    Optional.of(located), support(inbound).loadExpecting("request-ingestion", request(1), EXPECTED));
            Assertions.assertEquals("org-xyz", MDC.get(IngestionLogContext.TENANT_ID));
        }
    }

    @Test
    void jobAttemptsIsOneOutsideAJobRunAndFailuresPlusOneInside() {
        Assertions.assertEquals(1, InboundJobSupport.jobAttempts(null));
        Assertions.assertEquals(1, InboundJobSupport.jobAttempts(JobContext.Null));
        JobContext thirdRun = new JobContext() {
            @Override
            public int amountOfFailures() {
                return 2;
            }
        };
        Assertions.assertEquals(3, InboundJobSupport.jobAttempts(thirdRun));
    }
}
