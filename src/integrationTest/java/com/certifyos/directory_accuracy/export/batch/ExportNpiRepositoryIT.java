package com.certifyos.directory_accuracy.export.batch;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.persistence.Transactions;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(ApiTestProfile.class)
public class ExportNpiRepositoryIT {

    static final Instant NOW = Instant.parse("2026-10-01T06:00:31Z");

    @Inject
    ExportNpiRepository npis;

    @Inject
    Transactions transactions;

    private static List<ExportNpi> page(String batchId, int from, int count) {
        List<ExportNpi> rows = new ArrayList<>();
        for (int index = from; index < from + count; index++) {
            String npi = String.format(java.util.Locale.ROOT, "%010d", 1000000000L + index);
            rows.add(ExportNpi.of(batchId, "npi-tenant", npi, "cert-" + index, NOW));
        }
        return rows;
    }

    @Test
    void repeatedPageIsANoOpAndCountsStayExact() {
        String batchId = "npi-tenant-candor-2026-10-001";
        long first = transactions.run(session -> npis.upsertAll(session, page(batchId, 0, 1200)));
        long again = transactions.run(session -> npis.upsertAll(session, page(batchId, 0, 1200)));
        long overlap = transactions.run(session -> npis.upsertAll(session, page(batchId, 1100, 200)));

        Assertions.assertEquals(1200, first, "more than two bulk chunks of 500");
        Assertions.assertEquals(0, again, "a retried page inserts nothing");
        Assertions.assertEquals(100, overlap, "only the new rows of an overlapping page are inserted");
        Assertions.assertEquals(1300, npis.countForBatch(batchId));
    }

    @Test
    void existingRowIsNeverRewritten() {
        String batchId = "npi-tenant-candor-2026-10-002";
        transactions.run(session ->
                npis.upsertAll(session, List.of(ExportNpi.of(batchId, "npi-tenant", "1234567893", "cert-a", NOW))));
        transactions.run(session -> npis.upsertAll(
                session,
                List.of(ExportNpi.of(batchId, "npi-tenant", "1234567893", "cert-CHANGED", NOW.plusSeconds(5)))));

        ExportNpi stored = npis.listForBatch(batchId, null, 10).get(0);
        Assertions.assertEquals("cert-a", stored.certifyPractitionerId());
        Assertions.assertEquals(NOW, stored.registeredAt());
    }

    @Test
    void listIsKeysetPagedByNpi() {
        String batchId = "npi-tenant-candor-2026-10-003";
        transactions.run(session -> npis.upsertAll(session, page(batchId, 0, 7)));

        List<ExportNpi> firstPage = npis.listForBatch(batchId, null, 3);
        List<ExportNpi> secondPage = npis.listForBatch(batchId, firstPage.get(2).npi(), 3);
        List<ExportNpi> thirdPage = npis.listForBatch(batchId, secondPage.get(2).npi(), 3);

        Assertions.assertEquals(3, firstPage.size());
        Assertions.assertEquals(3, secondPage.size());
        Assertions.assertEquals(1, thirdPage.size());
        Assertions.assertTrue(firstPage.get(2).npi().compareTo(secondPage.get(0).npi()) < 0);
        Assertions.assertEquals(7, npis.npisForBatch(batchId).size());
        Assertions.assertEquals(
                npis.npisForBatch(batchId).get(0), firstPage.get(0).npi());
    }
}
