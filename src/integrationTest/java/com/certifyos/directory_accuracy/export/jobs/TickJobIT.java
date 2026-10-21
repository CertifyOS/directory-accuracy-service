package com.certifyos.directory_accuracy.export.jobs;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.audit.AuditEvent;
import com.certifyos.directory_accuracy.audit.AuditEventType;
import com.certifyos.directory_accuracy.export.batch.BatchState;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.batch.ExportBatchRepository;
import com.certifyos.directory_accuracy.export.schedule.Cadence;
import com.certifyos.directory_accuracy.export.schedule.Schedule;
import com.certifyos.directory_accuracy.export.schedule.ScheduleRepository;
import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria;
import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria.Clause;
import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria.Operator;
import com.certifyos.directory_accuracy.persistence.Collections;
import com.certifyos.directory_accuracy.persistence.Transactions;
import com.mongodb.client.model.Filters;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Instant;
import java.time.YearMonth;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;
import org.bson.Document;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.storage.StorageProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The tick body on the api role: the background server is off, so an enqueued select job stays
 * {@code ENQUEUED} in storage where the test can see it. Other classes leave due schedules behind
 * in the shared database, so every assertion filters on this class's tenants.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(ApiTestProfile.class)
public class TickJobIT {

    static final SelectionCriteria CRITERIA =
            new SelectionCriteria(List.of(new Clause("data.delegationStatus", Operator.IN, List.of("Direct"))));
    static final ZoneId NEW_YORK = ZoneId.of("America/New_York");

    @Inject
    TickJob tick;

    @Inject
    ScheduleRepository schedules;

    @Inject
    ExportBatchRepository batches;

    @Inject
    Collections collections;

    @Inject
    Transactions transactions;

    @Inject
    StorageProvider storage;

    private Schedule dueSchedule(String tenantId, ZoneId zone, Instant nextDueAt) {
        Schedule schedule = Schedule.create(
                tenantId,
                "candor",
                Cadence.monthly(1),
                zone,
                CRITERIA,
                "tpl-" + tenantId,
                nextDueAt,
                "user:ops",
                Instant.now());
        transactions.run(session -> {
            schedules.insert(session, schedule);
            return null;
        });
        return schedule;
    }

    private static List<TickJob.TickResult.Created> mine(TickJob.TickResult result, String prefix) {
        return result.batchesCreated().stream()
                .filter(created -> created.tenantId().startsWith(prefix))
                .toList();
    }

    @Test
    void twoDueSchedulesBecomeTwoBatchesAdvancedSchedulesEventsAndJobs() {
        Instant before = Instant.now();
        // Due since 1 September 00:00 New York; the period is the due month in the schedule's zone,
        // not the month the tick happens to run in.
        dueSchedule("tick-two-ny", NEW_YORK, Instant.parse("2026-09-01T04:00:00Z"));
        Schedule utc = dueSchedule("tick-two-utc", ZoneId.of("UTC"), before.minusSeconds(60));

        TickJob.TickResult result = tick.run();

        List<TickJob.TickResult.Created> created = mine(result, "tick-two-");
        Assertions.assertEquals(2, created.size(), result.toString());
        TickJob.TickResult.Created ny = created.stream()
                .filter(entry -> entry.tenantId().equals("tick-two-ny"))
                .findFirst()
                .orElseThrow();
        Assertions.assertEquals("tick-two-ny-candor-2026-09-001", ny.exportBatchId());
        TickJob.TickResult.Created utcCreated = created.stream()
                .filter(entry -> entry.tenantId().equals("tick-two-utc"))
                .findFirst()
                .orElseThrow();
        Assertions.assertEquals(
                "tick-two-utc-candor-" + YearMonth.from(utc.nextDueAt().atZone(ZoneId.of("UTC"))) + "-001",
                utcCreated.exportBatchId());

        for (TickJob.TickResult.Created entry : created) {
            ExportBatch batch = batches.find(entry.exportBatchId()).orElseThrow();
            Assertions.assertEquals(BatchState.SCHEDULED, batch.state());
            Assertions.assertEquals(1, batch.seq());
            Assertions.assertEquals(1, batch.attempt());
            Assertions.assertEquals(CRITERIA, batch.selection().criteria(), "the criteria are copied at creation");

            Schedule after = schedules.find(entry.tenantId(), "candor").orElseThrow();
            Assertions.assertTrue(after.nextDueAt().isAfter(before), "nextDueAt advanced into the future");
            Assertions.assertEquals(entry.nextDueAt(), after.nextDueAt());
            Assertions.assertEquals(entry.exportBatchId(), after.lastBatchId());
            Assertions.assertNotNull(after.lastRunAt());
            Assertions.assertEquals(2L, after.version());

            Document doc = collections
                    .events()
                    .find(Filters.and(
                            Filters.eq("type", AuditEventType.EXPORT_BATCH_SCHEDULED.name()),
                            Filters.eq("exportBatchId", entry.exportBatchId())))
                    .first();
            Assertions.assertNotNull(doc, "EXPORT_BATCH_SCHEDULED for " + entry.exportBatchId());
            AuditEvent event = AuditEvent.fromDocument(doc);
            Assertions.assertEquals("TICK", event.detail().get("trigger"));
            Assertions.assertEquals(1, event.detail().get("seq"));
            Assertions.assertEquals(batch.period(), event.detail().get("period"));
            Assertions.assertEquals(entry.jobId(), event.jobId());

            Assertions.assertEquals(
                    StateName.ENQUEUED,
                    storage.getJobById(UUID.fromString(entry.jobId())).getState(),
                    "the select job is enqueued once the transaction committed");
        }
        Assertions.assertTrue(
                collections
                                .events()
                                .countDocuments(Filters.and(
                                        Filters.eq("type", AuditEventType.EXPORT_TICK_COMPLETED.name()),
                                        Filters.eq("detail.tickId", result.tickId())))
                        == 1L,
                "one EXPORT_TICK_COMPLETED per tick");
    }

    @Test
    void theSameTickTwiceCreatesNothingMore() {
        dueSchedule("tick-again", ZoneId.of("UTC"), Instant.now().minusSeconds(60));

        TickJob.TickResult first = tick.run();
        TickJob.TickResult second = tick.run();

        Assertions.assertEquals(1, mine(first, "tick-again").size());
        Assertions.assertTrue(mine(second, "tick-again").isEmpty(), "advanced nextDueAt means not due again");
        Assertions.assertEquals(1, batches.findForTenant("tick-again", null, 10).size());
    }

    @Test
    void aPeriodThatAlreadyHasItsBatchIsSkippedAndTheScheduleMovesPastIt() {
        Instant due = Instant.now().minusSeconds(60);
        Schedule schedule = dueSchedule("tick-dup", ZoneId.of("UTC"), due);
        YearMonth period = YearMonth.from(due.atZone(ZoneId.of("UTC")));
        transactions.run(session -> {
            batches.insert(session, ExportBatch.scheduled("tick-dup", "candor", period, 1, CRITERIA, Instant.now()));
            return null;
        });

        TickJob.TickResult result = tick.run();

        Assertions.assertTrue(mine(result, "tick-dup").isEmpty());
        Assertions.assertTrue(result.skippedAlreadyExists() >= 1, result.toString());
        Schedule after = schedules.find("tick-dup", "candor").orElseThrow();
        Assertions.assertEquals(schedule.version() + 1, after.version(), "the schedule moved past the period");
        Assertions.assertTrue(
                YearMonth.from(after.nextDueAt().atZone(ZoneId.of("UTC"))).isAfter(period),
                "next due is in a later month, so the tick never finds this period due again: " + after.nextDueAt());
        Assertions.assertNull(after.lastBatchId(), "no batch was created by this tick");
        Assertions.assertEquals(1, batches.findForTenant("tick-dup", null, 10).size());
        Assertions.assertTrue(mine(tick.run(), "tick-dup").isEmpty(), "and the next tick does not see it due");
    }

    @Test
    void aCadenceFinerThanMonthlyIsNeverDueAgainInTheSameMonth() {
        // Every Monday: after the month's batch, the next due date must be the first Monday of a
        // later month, else the following Monday would find the month's batch and stick.
        Instant due = Instant.now().minusSeconds(60);
        Schedule weekly = Schedule.create(
                "tick-weekly",
                "candor",
                Cadence.cron("0 0 * * 1"),
                ZoneId.of("UTC"),
                CRITERIA,
                "tpl",
                due,
                "user:ops",
                Instant.now());
        transactions.run(session -> {
            schedules.insert(session, weekly);
            return null;
        });

        TickJob.TickResult result = tick.run();

        Assertions.assertEquals(1, mine(result, "tick-weekly").size());
        Schedule after = schedules.find("tick-weekly", "candor").orElseThrow();
        YearMonth batchPeriod = YearMonth.from(due.atZone(ZoneId.of("UTC")));
        YearMonth nextPeriod = YearMonth.from(after.nextDueAt().atZone(ZoneId.of("UTC")));
        Assertions.assertEquals(
                batchPeriod.plusMonths(1), nextPeriod, after.nextDueAt().toString());
        Assertions.assertEquals(
                java.time.DayOfWeek.MONDAY,
                after.nextDueAt().atZone(ZoneId.of("UTC")).getDayOfWeek());
    }
}
