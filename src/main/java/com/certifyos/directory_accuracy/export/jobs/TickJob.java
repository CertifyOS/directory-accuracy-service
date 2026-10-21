package com.certifyos.directory_accuracy.export.jobs;

import com.certifyos.directory_accuracy.audit.Actors;
import com.certifyos.directory_accuracy.audit.AuditEvent;
import com.certifyos.directory_accuracy.audit.AuditEventType;
import com.certifyos.directory_accuracy.audit.AuditRepository;
import com.certifyos.directory_accuracy.config.DirectoryAccuracyConfig;
import com.certifyos.directory_accuracy.export.batch.BatchLifecycle;
import com.certifyos.directory_accuracy.export.batch.StaleScheduleException;
import com.certifyos.directory_accuracy.export.schedule.Schedule;
import com.certifyos.directory_accuracy.export.schedule.ScheduleRepository;
import com.certifyos.directory_accuracy.metrics.DirectoryAccuracyMetrics;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.jboss.logging.Logger;
import org.jobrunr.jobs.annotations.Recurring;

/**
 * Lifecycle step 1: the daily tick. One indexed query for due schedules, then one transaction per
 * due row (batch insert, {@code nextDueAt} advance, {@code EXPORT_BATCH_SCHEDULED}) and one select
 * job each, through {@link BatchLifecycle}. Calls no other service. The period is the month of the
 * due {@code nextDueAt} in the schedule's timezone, so a late tick still names the right month; the
 * sequence is always 1, because the tick never creates a second batch for a period (supersede
 * does). A period that already has its batch is counted as {@code skippedAlreadyExists}, and a
 * schedule changed under the tick is skipped and logged.
 *
 * <p>The recurring registration is the {@code @Recurring} annotation on {@link #tick()}: the Quarkus
 * extension finds it at build time and registers it by id at startup, idempotently, with no lambda
 * for JobRunr to analyse (a no-capture IoC lambda in a CDI bean fails its bytecode analysis).
 */
@ApplicationScoped
public class TickJob {

    /** Recurring job id; registration is idempotent on it. */
    public static final String RECURRING_ID = "vendor-export-tick";

    /** 06:00 UTC daily. */
    public static final String CRON = "0 6 * * *";

    /** The most due schedules one tick handles; a bigger backlog waits for the next tick. */
    static final int DUE_LIMIT = 1000;

    private static final Logger LOG = Logger.getLogger(TickJob.class);

    private final DirectoryAccuracyConfig cfg;
    private final ScheduleRepository schedules;
    private final BatchLifecycle lifecycle;
    private final AuditRepository audit;
    private final DirectoryAccuracyMetrics metrics;
    private final Clock clock;

    public TickJob(
            DirectoryAccuracyConfig cfg,
            ScheduleRepository schedules,
            BatchLifecycle lifecycle,
            AuditRepository audit,
            DirectoryAccuracyMetrics metrics,
            Clock clock) {
        this.metrics = metrics;
        this.cfg = cfg;
        this.schedules = schedules;
        this.lifecycle = lifecycle;
        this.audit = audit;
        this.clock = clock;
    }

    /** The recurring entry point: 06:00 UTC daily, registered by id at startup. */
    @Recurring(id = RECURRING_ID, cron = CRON, zoneId = "UTC")
    public void tick() {
        run();
    }

    /**
     * One tick: find due schedules, create their batches, record the outcome.
     *
     * @return what the tick did, also returned by the manual trigger endpoint
     */
    public TickResult run() {
        Instant started = clock.instant();
        String tickId = "tick-" + started;
        if (!cfg.enabled()) {
            LOG.infof("%s: service disabled, inserting nothing", tickId);
            return new TickResult(tickId, 0, List.of(), 0, 0);
        }
        List<Schedule> due = schedules.findDue(started, DUE_LIMIT);
        List<TickResult.Created> created = new ArrayList<>();
        int skipped = 0;
        for (Schedule schedule : due) {
            YearMonth period = YearMonth.from(schedule.nextDueAt().atZone(schedule.timezone()));
            try {
                Optional<BatchLifecycle.Scheduled> scheduled = lifecycle.schedule(
                        schedule, period, 1, BatchLifecycle.Trigger.TICK, Actors.SYSTEM, clock.instant(), null);
                if (scheduled.isPresent()) {
                    created.add(new TickResult.Created(
                            schedule.tenantId(),
                            schedule.vendor(),
                            scheduled.get().batch().id(),
                            scheduled.get().jobId().toString(),
                            scheduled.get().nextDueAt()));
                } else {
                    skipped++;
                    lifecycle.skipPeriod(schedule, period, clock.instant());
                }
            } catch (StaleScheduleException changed) {
                LOG.warnf("%s: %s; the schedule is left for the next tick", tickId, changed.getMessage());
            }
        }
        long durationMs = clock.millis() - started.toEpochMilli();
        TickResult result = new TickResult(tickId, due.size(), created, skipped, durationMs);
        metrics.tick(due.size(), Duration.ofMillis(durationMs));
        audit.write(AuditEvent.of(AuditEventType.EXPORT_TICK_COMPLETED, null, null)
                .occurredAt(clock.instant())
                .detail("tickId", tickId)
                .detail("schedulesDue", result.schedulesDue())
                .detail(
                        "batchesCreated",
                        created.stream().map(TickResult.Created::exportBatchId).toList())
                .detail("skippedAlreadyExists", result.skippedAlreadyExists())
                .detail("durationMs", durationMs)
                .build());
        LOG.infof(
                "%s: schedulesDue=%d batchesCreated=%d skippedAlreadyExists=%d",
                tickId, due.size(), created.size(), skipped);
        return result;
    }

    /**
     * The tick's outcome, in the design's shape.
     *
     * @param tickId {@code tick-<instant>}
     * @param schedulesDue how many schedules were due
     * @param batchesCreated the batches created, one per due schedule
     * @param skippedAlreadyExists due schedules whose period already had a batch
     * @param durationMs wall time
     */
    public record TickResult(
            String tickId, int schedulesDue, List<Created> batchesCreated, int skippedAlreadyExists, long durationMs) {
        public TickResult {
            batchesCreated = List.copyOf(batchesCreated);
        }

        /**
         * One batch the tick created.
         *
         * @param tenantId the tenant
         * @param vendor the vendor
         * @param exportBatchId the batch
         * @param jobId the select job
         * @param nextDueAt the schedule's new due instant
         */
        public record Created(String tenantId, String vendor, String exportBatchId, String jobId, Instant nextDueAt) {}
    }
}
