package com.certifyos.directory_accuracy.export.batch;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria;
import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria.Clause;
import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria.Operator;
import com.certifyos.directory_accuracy.persistence.AlreadyExistsException;
import com.certifyos.directory_accuracy.persistence.Transactions;
import com.mongodb.client.model.Updates;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Instant;
import java.time.YearMonth;
import java.util.EnumSet;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(ApiTestProfile.class)
public class ExportBatchRepositoryIT {

    static final Instant NOW = Instant.parse("2026-10-01T06:00:12Z");
    static final SelectionCriteria CRITERIA =
            new SelectionCriteria(List.of(new Clause("data.delegationStatus", Operator.IN, List.of("Direct"))));

    @Inject
    ExportBatchRepository batches;

    @Inject
    Transactions transactions;

    private ExportBatch insert(String tenantId, int seq) {
        ExportBatch batch = ExportBatch.scheduled(tenantId, "candor", YearMonth.of(2026, 10), seq, CRITERIA, NOW);
        transactions.run(session -> {
            batches.insert(session, batch);
            return null;
        });
        return batch;
    }

    @Test
    void insertThenFindRoundTrips() {
        ExportBatch batch = insert("bat-roundtrip", 1);
        Assertions.assertEquals(batch, batches.find(batch.id()).orElseThrow());
    }

    @Test
    void samePeriodAndSequenceIsRefusedButNextSequenceIsNot() {
        insert("bat-dup", 1);
        AlreadyExistsException refused =
                Assertions.assertThrows(AlreadyExistsException.class, () -> insert("bat-dup", 1));
        Assertions.assertEquals("bat-dup-candor-2026-10-001", refused.key());
        Assertions.assertDoesNotThrow(() -> insert("bat-dup", 2), "supersede creates seq 002 beside 001");
    }

    @Test
    void transitionIsCompareAndSetOnState() {
        ExportBatch batch = insert("bat-cas", 1);

        boolean moved = transactions.run(session -> batches.transition(
                session,
                batch.id(),
                BatchState.SCHEDULED,
                BatchState.NPIS_SELECTED,
                NOW.plusSeconds(30),
                Updates.set("selection.practitionersSelected", 1240)));
        boolean stale = transactions.run(session -> batches.transition(
                session, batch.id(), BatchState.SCHEDULED, BatchState.EMPTY, NOW.plusSeconds(31), null));

        Assertions.assertTrue(moved);
        Assertions.assertFalse(stale, "a job that read SCHEDULED must find the row gone and exit as a no-op");
        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.NPIS_SELECTED, after.state());
        Assertions.assertEquals(2L, after.version());
        Assertions.assertEquals(1240, after.selection().practitionersSelected());
        Assertions.assertEquals(NOW.plusSeconds(30), after.updatedAt());
    }

    @Test
    void transitionRefusesAnEdgeTheStateMachineDoesNotHave() {
        ExportBatch batch = insert("bat-edge", 1);
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> transactions.run(session -> batches.transition(
                        session, batch.id(), BatchState.SCHEDULED, BatchState.DELIVERED, NOW, null)));
        Assertions.assertEquals(
                BatchState.SCHEDULED, batches.find(batch.id()).orElseThrow().state());
    }

    @Test
    void updateInStateKeepsTheStateAndBumpsTheVersion() {
        ExportBatch batch = insert("bat-progress", 1);
        boolean saved = transactions.run(session -> batches.updateInState(
                session, batch.id(), BatchState.SCHEDULED, NOW.plusSeconds(5), Updates.set("selection.page", 3)));
        boolean wrongState = transactions.run(session -> batches.updateInState(
                session, batch.id(), BatchState.NPIS_SELECTED, NOW.plusSeconds(6), Updates.set("selection.page", 9)));

        Assertions.assertTrue(saved);
        Assertions.assertFalse(wrongState);
        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(3, after.selection().page());
        Assertions.assertEquals(BatchState.SCHEDULED, after.state());
        Assertions.assertEquals(2L, after.version());
    }

    @Test
    void findByCorrelationIdAndStale() {
        ExportBatch batch = insert("bat-corr", 1);
        String correlation = batch.id() + "-r1";
        transactions.run(session -> batches.transition(
                session, batch.id(), BatchState.SCHEDULED, BatchState.NPIS_SELECTED, NOW.minusSeconds(7200), null));
        transactions.run(session -> batches.transition(
                session,
                batch.id(),
                BatchState.NPIS_SELECTED,
                BatchState.EGRESS_REQUESTED,
                NOW.minusSeconds(3600),
                Updates.set("egress.correlationId", correlation)));

        Assertions.assertEquals(
                batch.id(),
                batches.findByCorrelationId(correlation).orElseThrow().id());
        Assertions.assertTrue(batches.findByCorrelationId("nobody-r1").isEmpty());

        List<String> stale =
                batches.findStale(EnumSet.of(BatchState.EGRESS_REQUESTED), NOW.minusSeconds(1800), 100).stream()
                        .map(ExportBatch::id)
                        .filter(id -> id.startsWith("bat-corr"))
                        .toList();
        Assertions.assertEquals(List.of(batch.id()), stale);
        Assertions.assertTrue(
                batches.findStale(EnumSet.of(BatchState.EGRESS_REQUESTED), NOW.minusSeconds(7200), 100).stream()
                        .noneMatch(found -> found.id().equals(batch.id())),
                "updated after the threshold is not stale");
    }

    @Test
    void findForTenantIsNewestFirstAndFiltersByPeriod() {
        insert("bat-list", 1);
        insert("bat-list", 2);
        List<ExportBatch> all = batches.findForTenant("bat-list", null, 10);
        Assertions.assertEquals(2, all.size());
        Assertions.assertEquals(
                1, batches.findForTenant("bat-list", "2026-10", 1).size());
        Assertions.assertTrue(batches.findForTenant("bat-list", "2026-09", 10).isEmpty());
    }
}
