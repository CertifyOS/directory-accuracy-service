package com.certifyos.directory_accuracy.export;

import com.certifyos.directory_accuracy.export.batch.BatchState;
import com.certifyos.directory_accuracy.export.batch.ExportBatchRepository;
import com.certifyos.directory_accuracy.export.schedule.ScheduleRepository;
import com.certifyos.directory_accuracy.metrics.VendorExchangeMetrics;
import io.micrometer.core.instrument.Tags;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.jboss.logging.Logger;

/**
 * {@code vendor_export.batches.by_state{state}} and {@code vendor_export.schedules.by_state{state}}
 * from the database. One gauge per state is registered at startup so every series exists from the
 * first scrape; values come from one aggregation per collection, cached for sixty seconds, so a
 * frequent scraper never turns into database load. A failed refresh keeps the last snapshot.
 */
@ApplicationScoped
public class ExportStateGauges {

    static final Duration MAX_AGE = Duration.ofSeconds(60);
    private static final Logger LOG = Logger.getLogger(ExportStateGauges.class);

    private final ExportBatchRepository batches;
    private final ScheduleRepository schedules;
    private final VendorExchangeMetrics metrics;
    private final Clock clock;
    private volatile Snapshot snapshot = new Snapshot(Instant.MIN, Map.of(), Map.of());

    public ExportStateGauges(
            ExportBatchRepository batches, ScheduleRepository schedules, VendorExchangeMetrics metrics, Clock clock) {
        this.batches = batches;
        this.schedules = schedules;
        this.metrics = metrics;
        this.clock = clock;
    }

    void register(@Observes StartupEvent event) {
        for (BatchState state : BatchState.values()) {
            metrics.registerGauge(
                    VendorExchangeMetrics.BATCHES_BY_STATE, Tags.of("state", state.name()), () -> batchesIn(state));
        }
        metrics.registerGauge(
                VendorExchangeMetrics.SCHEDULES_BY_STATE, Tags.of("state", "ENABLED"), () -> schedules(true));
        metrics.registerGauge(
                VendorExchangeMetrics.SCHEDULES_BY_STATE, Tags.of("state", "DISABLED"), () -> schedules(false));
    }

    long batchesIn(BatchState state) {
        return current().batchesByState().getOrDefault(state.name(), 0L);
    }

    long schedules(boolean enabled) {
        return current().schedulesByEnabled().getOrDefault(enabled, 0L);
    }

    private Snapshot current() {
        Snapshot seen = snapshot;
        if (Duration.between(seen.takenAt(), clock.instant()).compareTo(MAX_AGE) <= 0) {
            return seen;
        }
        return refresh();
    }

    private synchronized Snapshot refresh() {
        Snapshot seen = snapshot;
        if (Duration.between(seen.takenAt(), clock.instant()).compareTo(MAX_AGE) <= 0) {
            return seen;
        }
        try {
            snapshot = new Snapshot(clock.instant(), batches.countByState(), schedules.countByEnabled());
        } catch (RuntimeException failure) {
            LOG.warnf("state gauges kept their last values, refresh failed: %s", failure.toString());
            snapshot = new Snapshot(clock.instant(), seen.batchesByState(), seen.schedulesByEnabled());
        }
        return snapshot;
    }

    record Snapshot(Instant takenAt, Map<String, Long> batchesByState, Map<Boolean, Long> schedulesByEnabled) {}
}
