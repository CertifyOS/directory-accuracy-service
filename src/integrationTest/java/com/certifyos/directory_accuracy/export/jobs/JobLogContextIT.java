package com.certifyos.directory_accuracy.export.jobs;

import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.WorkerTestProfile;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.batch.ExportBatchRepository;
import com.certifyos.directory_accuracy.persistence.Transactions;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.time.YearMonth;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.awaitility.Awaitility;
import org.jboss.logmanager.ExtHandler;
import org.jboss.logmanager.ExtLogRecord;
import org.jboss.logmanager.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A job run on a JobRunr worker thread logs with {@code exportBatchId}, {@code tenantId} and
 * {@code jobId} on the MDC. The records are captured from the JBoss log manager, the same path the
 * JSON console formatter reads the MDC from.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(WorkerTestProfile.class)
class JobLogContextIT {

    static final String FILTER_ID = "flt-direct";

    @Inject
    ExportBatchRepository batches;

    @Inject
    Transactions transactions;

    @Inject
    JobEnqueuer enqueuer;

    private final List<ExtLogRecord> records = new CopyOnWriteArrayList<>();
    private final ExtHandler capture = new ExtHandler() {
        @Override
        protected void doPublish(ExtLogRecord record) {
            // The MDC is copied into the record lazily, by whichever handler or formatter asks first,
            // on the logging thread. Copy here, on the job's thread, before the context is closed.
            record.copyMdc();
            records.add(record);
        }
    };

    @BeforeEach
    void before() {
        Logger.getLogger("com.certifyos.directory_accuracy.export.jobs").addHandler(capture);
    }

    @AfterEach
    void after() {
        Logger.getLogger("com.certifyos.directory_accuracy.export.jobs").removeHandler(capture);
    }

    @Test
    void jobLogLinesCarryBatchTenantAndJobId() {
        ExportBatch batch =
                ExportBatch.scheduled("mdc-tenant", "candor", YearMonth.of(2026, 10), 1, FILTER_ID, Instant.now());
        transactions.run(session -> {
            batches.insert(session, batch);
            return null;
        });

        UUID jobId = enqueuer.requestEgress(batch.id(), 1);

        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> startLine(batch.id())
                .isPresent());
        ExtLogRecord line = startLine(batch.id()).orElseThrow();
        Assertions.assertEquals(batch.id(), line.getMdc(JobLogContext.EXPORT_BATCH_ID));
        Assertions.assertEquals("mdc-tenant", line.getMdc(JobLogContext.TENANT_ID));
        Assertions.assertEquals(jobId.toString(), line.getMdc(JobLogContext.JOB_ID));
    }

    private Optional<ExtLogRecord> startLine(String exportBatchId) {
        return records.stream()
                .filter(record -> record.getFormattedMessage() != null)
                .filter(record -> record.getFormattedMessage().contains(exportBatchId + ": starting"))
                .findFirst();
    }
}
