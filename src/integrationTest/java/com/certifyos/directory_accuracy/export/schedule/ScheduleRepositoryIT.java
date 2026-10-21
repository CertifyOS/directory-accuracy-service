package com.certifyos.directory_accuracy.export.schedule;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria.Clause;
import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria.Operator;
import com.certifyos.directory_accuracy.persistence.AlreadyExistsException;
import com.certifyos.directory_accuracy.persistence.Transactions;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(ApiTestProfile.class)
public class ScheduleRepositoryIT {

    static final Instant NOW = Instant.parse("2026-10-01T06:00:00Z");
    static final SelectionCriteria CRITERIA =
            new SelectionCriteria(List.of(new Clause("data.delegationStatus", Operator.IN, List.of("Direct"))));

    @Inject
    ScheduleRepository schedules;

    @Inject
    Transactions transactions;

    private Schedule insert(String tenantId, Instant nextDueAt, boolean enabled) {
        Schedule created = Schedule.create(
                tenantId,
                "candor",
                Cadence.monthly(1),
                ZoneId.of("UTC"),
                CRITERIA,
                "tpl-" + tenantId,
                nextDueAt,
                "user:ops",
                NOW);
        Schedule schedule = enabled
                ? created
                : new Schedule(
                        created.id(),
                        created.tenantId(),
                        created.vendor(),
                        false,
                        created.cadence(),
                        created.timezone(),
                        created.selection(),
                        created.egressTemplateId(),
                        created.nextDueAt(),
                        null,
                        null,
                        NOW,
                        "hold",
                        1L,
                        "user:ops",
                        NOW,
                        "user:ops",
                        NOW);
        transactions.run(session -> {
            schedules.insert(session, schedule);
            return null;
        });
        return schedule;
    }

    @Test
    void insertThenFindRoundTrips() {
        Schedule schedule = insert("sch-roundtrip", NOW.plusSeconds(3600), true);
        Assertions.assertEquals(
                schedule, schedules.find("sch-roundtrip", "candor").orElseThrow());
        Assertions.assertTrue(schedules.find("sch-roundtrip", "other").isEmpty());
    }

    @Test
    void secondInsertForTheSamePairIsRefused() {
        insert("sch-dup", NOW, true);
        AlreadyExistsException refused = Assertions.assertThrows(
                AlreadyExistsException.class,
                () -> transactions.run(session -> {
                    schedules.insert(session, insertCandidate("sch-dup"));
                    return null;
                }));
        Assertions.assertEquals("sch-dup|candor", refused.key());
    }

    private static Schedule insertCandidate(String tenantId) {
        return Schedule.create(
                tenantId, "candor", Cadence.monthly(1), ZoneId.of("UTC"), CRITERIA, "tpl", NOW, "user:ops", NOW);
    }

    @Test
    void findDueReturnsOnlyEnabledAndPastDue() {
        insert("sch-due-past", NOW.minusSeconds(60), true);
        insert("sch-due-future", NOW.plusSeconds(60), true);
        insert("sch-due-disabled", NOW.minusSeconds(60), false);

        List<String> due = schedules.findDue(NOW, 100).stream()
                .map(Schedule::tenantId)
                .filter(tenant -> tenant.startsWith("sch-due-"))
                .toList();
        Assertions.assertEquals(List.of("sch-due-past"), due);
    }

    @Test
    void advanceIsCompareAndSetOnVersion() {
        Schedule schedule = insert("sch-advance", NOW.minusSeconds(60), true);
        Instant next = NOW.plusSeconds(86400);

        boolean first = transactions.run(session -> schedules.advance(
                session, schedule.id(), schedule.version(), next, "sch-advance-candor-2026-10-001", NOW));
        boolean stale = transactions.run(session ->
                schedules.advance(session, schedule.id(), schedule.version(), next.plusSeconds(1), "other", NOW));

        Assertions.assertTrue(first);
        Assertions.assertFalse(stale, "a second writer with the old version must be refused");
        Schedule after = schedules.find("sch-advance", "candor").orElseThrow();
        Assertions.assertEquals(2L, after.version());
        Assertions.assertEquals(next, after.nextDueAt());
        Assertions.assertEquals("sch-advance-candor-2026-10-001", after.lastBatchId());
        Assertions.assertEquals(com.certifyos.directory_accuracy.audit.Actors.SYSTEM, after.updatedBy());
        Assertions.assertTrue(schedules.findDue(NOW, 100).stream()
                .noneMatch(found -> found.id().equals(schedule.id())));
    }

    @Test
    void findByTenantIsScopedAndLimited() {
        insert("sch-tenant", NOW, true);
        Assertions.assertEquals(1, schedules.findByTenant("sch-tenant", 10).size());
        Assertions.assertTrue(schedules.findByTenant("sch-nobody", 10).isEmpty());
    }
}
