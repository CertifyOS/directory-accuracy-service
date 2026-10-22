package com.certifyos.directory_accuracy.audit;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;
import org.jboss.logmanager.ExtHandler;
import org.jboss.logmanager.ExtLogRecord;
import org.jboss.logmanager.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Every audit write logs one structured line on the audit logger, the signal the deployment's alerts count. */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(ApiTestProfile.class)
class AuditLogLineIT {

    @Inject
    AuditRepository audit;

    private final List<ExtLogRecord> records = new CopyOnWriteArrayList<>();
    private final ExtHandler capture = new ExtHandler() {
        @Override
        protected void doPublish(ExtLogRecord record) {
            record.copyMdc();
            records.add(record);
        }
    };

    @BeforeEach
    void before() {
        Logger.getLogger(AuditRepository.AUDIT_LOGGER).addHandler(capture);
    }

    @AfterEach
    void after() {
        Logger.getLogger(AuditRepository.AUDIT_LOGGER).removeHandler(capture);
    }

    @Test
    void eachWritePathLogsOneLineWithTheAlertFieldsAndNoPractitionerIdentifier() {
        String batchId = "log-line-candor-2026-10-001";
        audit.write(AuditEvent.forBatch(AuditEventType.EXPORT_EGRESS_COMPLETED, "log-line", "candor", batchId, 1)
                .detail("outputUri", "gs://b/from/log-line/f.csv")
                .detail("totalRecords", 3)
                .detail("waitSeconds", 42)
                .detail("completionSource", "DEADLINE")
                .build());
        audit.writeIdempotent(AuditEvent.forBatch(AuditEventType.EXPORT_RECONCILED, "log-line", "candor", batchId, 1)
                .detail("selected", 1L)
                .detail("inFile", 1L)
                .detail("match", true)
                .detail("firstNpi", "1234567893")
                .build());
        audit.write(AuditEvent.of(AuditEventType.EXPORT_EVENT_REJECTED, null, null)
                .detail("messageId", "m-9")
                .detail("reason", "UNKNOWN_SCHEMA")
                .build());

        List<String> lines = records.stream()
                .map(ExtLogRecord::getFormattedMessage)
                .filter(line -> line != null && line.contains("log-line")
                        || line != null && line.contains("EXPORT_EVENT_REJECTED"))
                .toList();
        Assertions.assertEquals(3, lines.size(), lines.toString());
        Assertions.assertTrue(
                lines.get(0)
                        .startsWith("AUDIT type=EXPORT_EGRESS_COMPLETED tenantId=log-line vendor=candor exportBatchId="
                                + batchId + " attempt=1 actor=system:vendor-export"),
                lines.get(0));
        Assertions.assertTrue(lines.get(0).endsWith(" completionSource=DEADLINE"), lines.get(0));
        Assertions.assertTrue(lines.get(1).startsWith("AUDIT type=EXPORT_RECONCILED"), lines.get(1));
        Assertions.assertTrue(
                lines.get(2).contains("tenantId=null vendor=null exportBatchId=null")
                        && lines.get(2).endsWith(" reason=UNKNOWN_SCHEMA"),
                lines.get(2));
        Pattern npi = Pattern.compile("\\b\\d{10}\\b");
        for (String line : lines) {
            Assertions.assertFalse(npi.matcher(line).find(), "no practitioner identifier on the line: " + line);
            Assertions.assertEquals(
                    AuditRepository.AUDIT_LOGGER,
                    records.get(lines.indexOf(line)).getLoggerName());
        }
    }
}
