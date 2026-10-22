package com.certifyos.directory_accuracy.export.batch;

import com.certifyos.directory_accuracy.export.batch.EgressDetails.CompletionSource;
import com.certifyos.directory_accuracy.export.batch.EgressDetails.Destination;
import java.time.Instant;
import java.time.YearMonth;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ExportBatchTest {

    static final String FILTER_ID = "flt-direct";

    static ExportBatch scheduled() {
        return ExportBatch.scheduled(
                "org-xyz", "candor", YearMonth.of(2026, 10), 1, FILTER_ID, Instant.parse("2026-10-01T06:00:12Z"));
    }

    @Test
    void scheduledBatchHasTheDesignDefaults() {
        ExportBatch batch = scheduled();
        Assertions.assertEquals("org-xyz-candor-2026-10-001", batch.id());
        Assertions.assertEquals("2026-10", batch.period());
        Assertions.assertEquals(BatchState.SCHEDULED, batch.state());
        Assertions.assertEquals(1, batch.attempt());
        Assertions.assertEquals(1L, batch.version());
        Assertions.assertEquals(FILTER_ID, batch.selection().filterId(), "filter id copied at creation");
        Assertions.assertFalse(batch.selection().hasCounts(), "no counts before egress");
        Assertions.assertNull(batch.egress());
        Assertions.assertNull(batch.reconciliation());
    }

    @Test
    void scheduledBatchRoundTripsWithoutAbsentFields() {
        Document doc = scheduled().toDocument();
        Assertions.assertFalse(doc.containsKey("egress"));
        Assertions.assertFalse(doc.containsKey("failedStep"));
        Assertions.assertFalse(doc.containsKey("deliveredAt"));
        Assertions.assertEquals(new Document("filterId", FILTER_ID), doc.get("selection", Document.class));
        Assertions.assertEquals(scheduled(), ExportBatch.fromDocument(doc));
    }

    @Test
    void deliveredBatchRoundTripsEveryNestedRecord() {
        Instant now = Instant.parse("2026-10-01T07:12:09Z");
        ExportBatch delivered = new ExportBatch(
                "org-xyz-candor-2026-10-001",
                "org-xyz",
                "candor",
                "2026-10",
                1,
                BatchState.DELIVERED,
                1,
                new ExportBatch.Selection(
                        FILTER_ID,
                        1250L,
                        1240L,
                        10L,
                        1L,
                        "org-xyz-candor-2026-10-001-r1",
                        "gs://egress-list-used/org-xyz-candor-2026-10-001-r2.csv",
                        null,
                        Instant.parse("2026-10-01T07:11:52Z")),
                new EgressDetails(
                        "org-xyz-candor-2026-10-001-r1",
                        "practitioner:org-xyz:org-xyz-candor-2026-10-001-r1",
                        "c9a418b5-348e-44e6-95f0-04c45f527367",
                        3,
                        "gs://egress-templates/org-xyz/c9a418b5/v3/mappings.csv",
                        ";",
                        "csv",
                        List.of("locations"),
                        new Destination("vendor-sftp", "from/org-xyz/org-xyz_org-xyz-candor-2026-10-001_20261001.csv"),
                        Instant.parse("2026-10-01T06:00:44Z"),
                        "8f3c",
                        Instant.parse("2026-10-01T07:11:52Z"),
                        CompletionSource.EVENT,
                        "org-xyz-candor-2026-10-001-r1"),
                new FileDetails(
                        "org-xyz_org-xyz-candor-2026-10-001_20261001.csv",
                        "gs://vendor-sftp/from/org-xyz/org-xyz_org-xyz-candor-2026-10-001_20261001.csv",
                        1873L,
                        1912044L,
                        "certify-export-v1",
                        "8f3c2e1a9b0d4c5e6f7a8b9c0d1e2f30"),
                Reconciliation.of(1240, 1240, now),
                null,
                null,
                null,
                null,
                null,
                now,
                7L,
                Instant.parse("2026-10-01T06:00:12Z"),
                now);

        Document doc = delivered.toDocument();
        Assertions.assertEquals("EVENT", doc.get("egress", Document.class).getString("completionSource"));
        Assertions.assertTrue(doc.get("reconciliation", Document.class).getBoolean("match"));
        Assertions.assertEquals(delivered, ExportBatch.fromDocument(doc));
    }

    @Test
    void reconciliationMatchIsComputed() {
        Instant now = Instant.now();
        Assertions.assertTrue(Reconciliation.of(10, 10, now).match());
        Assertions.assertFalse(Reconciliation.of(10, 9, now).match());
    }
}
