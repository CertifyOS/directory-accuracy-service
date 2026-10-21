package com.certifyos.directory_accuracy.export.schedule;

import com.certifyos.directory_accuracy.audit.AuditEvent;
import com.certifyos.directory_accuracy.audit.AuditEventType;
import com.certifyos.directory_accuracy.audit.AuditRepository;
import com.certifyos.directory_accuracy.export.batch.BatchLifecycle;
import com.certifyos.directory_accuracy.export.batch.StaleScheduleException;
import com.certifyos.directory_accuracy.export.schedule.ScheduleRequests.PutRequest;
import com.certifyos.directory_accuracy.http.ProblemException;
import com.certifyos.directory_accuracy.persistence.AlreadyExistsException;
import com.certifyos.directory_accuracy.persistence.Ids;
import com.certifyos.directory_accuracy.persistence.Transactions;
import com.mongodb.client.model.Updates;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Clock;
import java.time.Instant;
import java.time.YearMonth;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.conversions.Bson;
import org.jboss.logging.Logger;

/**
 * The schedule writes from the design's Contracts step 0, each one transaction with its audit
 * event: create or replace, disable, enable with catch-up, and preview against api-layer.
 * Validation order on {@code PUT}: tenant id, schema, timezone and cadence, then the template,
 * then the write.
 */
@ApplicationScoped
public class ScheduleService {

    static final int LIST_LIMIT = 100;

    /** The most schedule events one read returns, newest first. */
    public static final int TRAIL_LIMIT = 200;

    private static final Logger LOG = Logger.getLogger(ScheduleService.class);

    private final ScheduleRepository schedules;
    private final AuditRepository audit;
    private final Transactions transactions;
    private final TemplateProvisioner templates;
    private final BatchLifecycle lifecycle;
    private final SelectionPreview preview;
    private final Clock clock;

    public ScheduleService(
            ScheduleRepository schedules,
            AuditRepository audit,
            Transactions transactions,
            TemplateProvisioner templates,
            BatchLifecycle lifecycle,
            SelectionPreview preview,
            Clock clock) {
        this.schedules = schedules;
        this.audit = audit;
        this.transactions = transactions;
        this.templates = templates;
        this.lifecycle = lifecycle;
        this.preview = preview;
        this.clock = clock;
    }

    /** The outcome of a {@code PUT}: the schedule and whether it was created. */
    public record Written(Schedule schedule, boolean created) {}

    /**
     * Creates (no {@code version}) or replaces (with {@code version}) a schedule.
     *
     * @param tenantId the tenant, from the path
     * @param vendor the vendor, from the path
     * @param request the body
     * @param actor the operator
     * @return the written schedule
     */
    public Written put(String tenantId, String vendor, PutRequest request, String actor) {
        String scheduleId = Ids.scheduleId(tenantId, vendor);
        SelectionCriteria selection = ScheduleRequests.toCriteria(request.selection());
        ZoneId zone = ScheduleRequests.toZone(request.timezone());
        if (request.cadence() == null) {
            throw ProblemException.badRequest("CADENCE_INVALID", "cadence is required");
        }
        Cadence cadence = request.cadence().toCadence();
        Optional<Schedule> existing = schedules.find(tenantId, vendor);
        if (request.version() == null) {
            if (existing.isPresent()) {
                throw ProblemException.conflict(
                        "SCHEDULE_EXISTS", "schedule " + scheduleId + " exists; send its version to replace it");
            }
            return create(tenantId, vendor, request, selection, zone, cadence, actor);
        }
        Schedule current = existing.orElseThrow(
                () -> ProblemException.notFound("SCHEDULE_NOT_FOUND", "no schedule " + scheduleId));
        return replace(current, request, selection, zone, cadence, actor);
    }

    private Written create(
            String tenantId,
            String vendor,
            PutRequest request,
            SelectionCriteria selection,
            ZoneId zone,
            Cadence cadence,
            String actor) {
        TemplateProvisioner.Provisioned template = templates.provision(tenantId, request.templateOverride());
        Instant now = clock.instant();
        Schedule schedule = Schedule.create(
                tenantId, vendor, cadence, zone, selection, template.templateId(), cadence.next(now, zone), actor, now);
        try {
            transactions.run(session -> {
                schedules.insert(session, schedule);
                audit.write(
                        session,
                        AuditEvent.of(AuditEventType.SCHEDULE_CREATED, tenantId, vendor)
                                .actor(actor)
                                .occurredAt(now)
                                .detail("cadence", cadence.toDocument())
                                .detail("timezone", zone.getId())
                                .detail("selection", selection.toDocuments())
                                .detail("egressTemplateId", template.templateId())
                                .detail("templateCreated", template.created())
                                .detail("nextDueAt", schedule.nextDueAt())
                                .build());
                return null;
            });
        } catch (AlreadyExistsException raced) {
            throw ProblemException.conflict("SCHEDULE_EXISTS", "schedule " + schedule.id() + " exists");
        }
        LOG.infof("schedule %s created by %s, next due %s", schedule.id(), actor, schedule.nextDueAt());
        return new Written(schedule, true);
    }

    private Written replace(
            Schedule current,
            PutRequest request,
            SelectionCriteria selection,
            ZoneId zone,
            Cadence cadence,
            String actor) {
        if (request.version() != current.version()) {
            throw ProblemException.conflict(
                    "VERSION_STALE",
                    "schedule is at version " + current.version() + ", request carried " + request.version());
        }
        String templateId = request.templateOverride().isPresent()
                ? templates
                        .provision(current.tenantId(), request.templateOverride())
                        .templateId()
                : current.egressTemplateId();
        Instant now = clock.instant();
        boolean whenChanged = !cadence.equals(current.cadence()) || !zone.equals(current.timezone());
        Instant nextDueAt = whenChanged ? cadence.next(now, zone) : current.nextDueAt();
        List<Map<String, Object>> changes = changes(current, cadence, zone, selection, templateId);
        Bson update = Updates.combine(
                Updates.set("cadence", cadence.toDocument()),
                Updates.set("timezone", zone.getId()),
                Updates.set("selection", selection.toDocuments()),
                Updates.set("egressTemplateId", templateId),
                Updates.set("nextDueAt", nextDueAt),
                Updates.set("updatedBy", actor),
                Updates.set("updatedAt", now),
                Updates.inc("version", 1L));
        AuditEvent event = AuditEvent.of(AuditEventType.SCHEDULE_UPDATED, current.tenantId(), current.vendor())
                .actor(actor)
                .occurredAt(now)
                .detail("version", current.version() + 1)
                .detail("changes", changes)
                .detail("nextDueAtBefore", current.nextDueAt())
                .detail("nextDueAtAfter", nextDueAt)
                .build();
        write(current, update, event);
        return new Written(schedules.find(current.tenantId(), current.vendor()).orElseThrow(), false);
    }

    private static List<Map<String, Object>> changes(
            Schedule current, Cadence cadence, ZoneId zone, SelectionCriteria selection, String templateId) {
        List<Map<String, Object>> changes = new ArrayList<>();
        if (!cadence.equals(current.cadence())) {
            changes.add(change("cadence", current.cadence().toDocument(), cadence.toDocument()));
        }
        if (!zone.equals(current.timezone())) {
            changes.add(change("timezone", current.timezone().getId(), zone.getId()));
        }
        if (!selection.equals(current.selection())) {
            changes.add(change("selection", current.selection().toDocuments(), selection.toDocuments()));
        }
        if (!templateId.equals(current.egressTemplateId())) {
            changes.add(change("egressTemplateId", current.egressTemplateId(), templateId));
        }
        return changes;
    }

    private static Map<String, Object> change(String field, Object before, Object after) {
        return Map.of("field", field, "before", before, "after", after);
    }

    /**
     * Disables a schedule; the tick skips it until enabled again.
     *
     * @param tenantId the tenant
     * @param vendor the vendor
     * @param reason mandatory
     * @param actor the operator
     * @return the schedule after the write
     */
    public Schedule disable(String tenantId, String vendor, String reason, String actor) {
        String why = ScheduleRequests.requireReason(reason);
        Schedule current = require(tenantId, vendor);
        if (!current.enabled()) {
            throw ProblemException.conflict("ALREADY_DISABLED", "schedule " + current.id() + " is already disabled");
        }
        Instant now = clock.instant();
        Bson update = Updates.combine(
                Updates.set("enabled", false),
                Updates.set("disabledAt", now),
                Updates.set("disabledReason", why),
                Updates.set("updatedBy", actor),
                Updates.set("updatedAt", now),
                Updates.inc("version", 1L));
        AuditEvent event = AuditEvent.of(AuditEventType.SCHEDULE_DISABLED, tenantId, vendor)
                .actor(actor)
                .occurredAt(now)
                .detail("reason", why)
                .detail("disabledAt", now)
                .build();
        write(current, update, event);
        return require(tenantId, vendor);
    }

    /**
     * Enables a schedule. With catch-up, the most recent occurrence missed while disabled becomes
     * due now; without, the next future occurrence.
     *
     * @param tenantId the tenant
     * @param vendor the vendor
     * @param reason mandatory
     * @param catchUp whether to run the one missed period
     * @param actor the operator
     * @return the schedule after the write
     */
    public Schedule enable(String tenantId, String vendor, String reason, boolean catchUp, String actor) {
        String why = ScheduleRequests.requireReason(reason);
        Schedule current = require(tenantId, vendor);
        if (current.enabled()) {
            throw ProblemException.conflict("ALREADY_ENABLED", "schedule " + current.id() + " is already enabled");
        }
        Instant now = clock.instant();
        Instant nextDueAt = catchUp && current.disabledAt() != null
                ? current.cadence()
                        .lastOccurrenceBetween(current.disabledAt(), now, current.timezone())
                        .orElseGet(() -> current.cadence().next(now, current.timezone()))
                : current.cadence().next(now, current.timezone());
        Bson update = Updates.combine(
                Updates.set("enabled", true),
                Updates.unset("disabledAt"),
                Updates.unset("disabledReason"),
                Updates.set("nextDueAt", nextDueAt),
                Updates.set("updatedBy", actor),
                Updates.set("updatedAt", now),
                Updates.inc("version", 1L));
        AuditEvent event = AuditEvent.of(AuditEventType.SCHEDULE_ENABLED, tenantId, vendor)
                .actor(actor)
                .occurredAt(now)
                .detail("reason", why)
                .detail("catchUp", catchUp)
                .detail("nextDueAt", nextDueAt)
                .build();
        write(current, update, event);
        return require(tenantId, vendor);
    }

    /**
     * Creates the current period's batch now, as the tick would: insert, advance {@code nextDueAt},
     * {@code EXPORT_BATCH_SCHEDULED} with {@code trigger = MANUAL} and {@code SCHEDULE_RUN_NOW} in
     * one transaction, then the select job. How a pilot's first export is started.
     *
     * @param tenantId the tenant
     * @param vendor the vendor
     * @param actor the operator
     * @return the batch, its select job and the schedule's new due instant
     */
    public BatchLifecycle.Scheduled runNow(String tenantId, String vendor, String actor) {
        Schedule schedule = require(tenantId, vendor);
        if (!schedule.enabled()) {
            throw ProblemException.conflict(
                    "SCHEDULE_DISABLED", "schedule " + schedule.id() + " is disabled; enable it first");
        }
        Instant now = clock.instant();
        YearMonth period = YearMonth.from(now.atZone(schedule.timezone()));
        String exportBatchId = Ids.batchId(tenantId, vendor, period, 1);
        AuditEvent runNow = AuditEvent.of(AuditEventType.SCHEDULE_RUN_NOW, tenantId, vendor)
                .actor(actor)
                .occurredAt(now)
                .detail("period", period.toString())
                .detail("exportBatchId", exportBatchId)
                .build();
        try {
            return lifecycle
                    .schedule(schedule, period, 1, BatchLifecycle.Trigger.MANUAL, actor, now, runNow)
                    .orElseThrow(() -> ProblemException.conflict(
                            "BATCH_EXISTS", "period " + period + " already has batch " + exportBatchId));
        } catch (StaleScheduleException changed) {
            throw ProblemException.conflict("VERSION_STALE", changed.getMessage());
        }
    }

    /**
     * How many practitioners a selection matches today, through api-layer with page size 1.
     *
     * @param tenantId the tenant
     * @param vendor the vendor, for the stored selection
     * @param selection the selection to test, or null for the stored one
     * @return the count
     */
    public long preview(String tenantId, String vendor, Map<String, ScheduleRequests.ClauseRequest> selection) {
        SelectionCriteria criteria = selection == null || selection.isEmpty()
                ? require(tenantId, vendor).selection()
                : ScheduleRequests.toCriteria(selection);
        return preview.count(tenantId, criteria);
    }

    /** One schedule, or 404. */
    public Schedule require(String tenantId, String vendor) {
        return schedules
                .find(tenantId, vendor)
                .orElseThrow(() -> ProblemException.notFound(
                        "SCHEDULE_NOT_FOUND", "no schedule " + Ids.scheduleId(tenantId, vendor)));
    }

    /**
     * A schedule's own audit events (creation, changes, enable and disable, run-now), newest first.
     *
     * @param tenantId the tenant
     * @param vendor the vendor
     * @return the events, at most {@link #TRAIL_LIMIT}
     */
    public List<AuditEvent> trail(String tenantId, String vendor) {
        require(tenantId, vendor);
        return audit.trailForSchedule(tenantId, vendor, TRAIL_LIMIT);
    }

    /** A tenant's schedules. */
    public List<Schedule> list(String tenantId) {
        return schedules.findByTenant(tenantId, LIST_LIMIT);
    }

    private void write(Schedule current, Bson update, AuditEvent event) {
        boolean applied = transactions.run(session -> {
            boolean ok = schedules.update(session, current.id(), current.version(), update);
            if (ok) {
                audit.write(session, event);
            }
            return ok;
        });
        if (!applied) {
            throw ProblemException.conflict(
                    "VERSION_STALE", "schedule " + current.id() + " changed under this request");
        }
    }
}
