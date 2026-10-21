package com.certifyos.directory_accuracy.export.schedule;

import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria.Clause;
import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria.Operator;
import com.certifyos.directory_accuracy.http.ProblemException;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** The request bodies of the schedule endpoints, in the design's shapes, and their conversions. */
public final class ScheduleRequests {

    private ScheduleRequests() {}

    /** {@code cadence}: {@code { "type": "monthly", "dayOfMonth": 1 }} or {@code { "type": "cron", "expression": "..." }}. */
    public record CadenceRequest(String type, Integer dayOfMonth, String expression) {
        Cadence toCadence() {
            if (type == null) {
                throw ProblemException.badRequest("CADENCE_INVALID", "cadence.type is required");
            }
            try {
                return switch (type.toLowerCase(java.util.Locale.ROOT)) {
                    case "monthly" -> Cadence.monthly(dayOfMonth == null ? 0 : dayOfMonth);
                    case "cron" -> Cadence.cron(expression);
                    default -> throw ProblemException.badRequest(
                            "CADENCE_INVALID", "cadence.type must be monthly or cron");
                };
            } catch (IllegalArgumentException invalid) {
                throw ProblemException.badRequest("CADENCE_INVALID", invalid.getMessage());
            }
        }

        static CadenceRequest of(Cadence cadence) {
            return new CadenceRequest(
                    cadence.type().name().toLowerCase(java.util.Locale.ROOT),
                    cadence.dayOfMonth(),
                    cadence.expression());
        }
    }

    /** One clause of {@code selection}: exactly one of {@code eq}, {@code in}, {@code gte}, {@code lte}. */
    public record ClauseRequest(String eq, List<String> in, String gte, String lte) {
        Clause toClause(String field) {
            List<Clause> candidates = new ArrayList<>();
            if (eq != null) {
                candidates.add(new Clause(field, Operator.EQ, List.of(eq)));
            }
            if (in != null) {
                candidates.add(new Clause(field, Operator.IN, in));
            }
            if (gte != null) {
                candidates.add(new Clause(field, Operator.GTE, List.of(gte)));
            }
            if (lte != null) {
                candidates.add(new Clause(field, Operator.LTE, List.of(lte)));
            }
            if (candidates.size() != 1) {
                throw ProblemException.badRequest(SelectionSchema.CODE, field + ": exactly one of eq, in, gte, lte");
            }
            return candidates.get(0);
        }

        static ClauseRequest of(Clause clause) {
            return switch (clause.operator()) {
                case EQ -> new ClauseRequest(clause.values().get(0), null, null, null);
                case IN -> new ClauseRequest(null, clause.values(), null, null);
                case GTE -> new ClauseRequest(null, null, clause.values().get(0), null);
                case LTE -> new ClauseRequest(null, null, null, clause.values().get(0));
            };
        }
    }

    /**
     * Converts the request's selection object to criteria and validates it against the schema.
     *
     * @param selection field to clause
     * @return the criteria
     */
    public static SelectionCriteria toCriteria(Map<String, ClauseRequest> selection) {
        if (selection == null || selection.isEmpty()) {
            throw ProblemException.badRequest(SelectionSchema.CODE, "selection needs at least one clause");
        }
        List<Clause> clauses = new ArrayList<>();
        for (Map.Entry<String, ClauseRequest> entry : selection.entrySet()) {
            if (entry.getValue() == null) {
                throw ProblemException.badRequest(SelectionSchema.CODE, entry.getKey() + ": clause is required");
            }
            clauses.add(entry.getValue().toClause(entry.getKey()));
        }
        SelectionCriteria criteria;
        try {
            criteria = new SelectionCriteria(clauses);
        } catch (IllegalArgumentException invalid) {
            throw ProblemException.badRequest(SelectionSchema.CODE, invalid.getMessage());
        }
        SelectionSchema.validate(criteria);
        return criteria;
    }

    /** The selection in request shape, for views. */
    public static Map<String, ClauseRequest> toSelection(SelectionCriteria criteria) {
        Map<String, ClauseRequest> selection = new java.util.LinkedHashMap<>();
        for (Clause clause : criteria.clauses()) {
            selection.put(clause.field(), ClauseRequest.of(clause));
        }
        return selection;
    }

    /** Parses an IANA zone. */
    public static ZoneId toZone(String timezone) {
        if (timezone == null || timezone.isBlank()) {
            throw ProblemException.badRequest("TIMEZONE_INVALID", "timezone is required");
        }
        try {
            return ZoneId.of(timezone);
        } catch (DateTimeException invalid) {
            throw ProblemException.badRequest("TIMEZONE_INVALID", "unknown timezone " + timezone);
        }
    }

    /** {@code PUT} body. */
    public record PutRequest(
            Long version,
            CadenceRequest cadence,
            String timezone,
            Map<String, ClauseRequest> selection,
            String egressTemplateId) {
        Optional<String> templateOverride() {
            return Optional.ofNullable(egressTemplateId).filter(id -> !id.isBlank());
        }
    }

    /** {@code disable} body. */
    public record DisableRequest(String reason) {}

    /** {@code enable} body. */
    public record EnableRequest(String reason, Boolean catchUp) {}

    /** {@code preview} body: the selection to test, or empty to test the stored one. */
    public record PreviewRequest(Map<String, ClauseRequest> selection) {}

    /** {@code run-now} answer. */
    public record RunNowResponse(String exportBatchId, String jobId, java.time.Instant nextDueAt) {}

    /** {@code preview} answer. */
    public record PreviewResponse(long totalCount) {}

    /** A schedule as the API shows it. */
    public record ScheduleView(
            String tenantId,
            String vendor,
            boolean enabled,
            CadenceRequest cadence,
            String timezone,
            Map<String, ClauseRequest> selection,
            String egressTemplateId,
            java.time.Instant nextDueAt,
            String lastBatchId,
            java.time.Instant lastRunAt,
            java.time.Instant disabledAt,
            String disabledReason,
            long version,
            java.time.Instant createdAt,
            java.time.Instant updatedAt) {

        static ScheduleView of(Schedule schedule) {
            return new ScheduleView(
                    schedule.tenantId(),
                    schedule.vendor(),
                    schedule.enabled(),
                    CadenceRequest.of(schedule.cadence()),
                    schedule.timezone().getId(),
                    toSelection(schedule.selection()),
                    schedule.egressTemplateId(),
                    schedule.nextDueAt(),
                    schedule.lastBatchId(),
                    schedule.lastRunAt(),
                    schedule.disabledAt(),
                    schedule.disabledReason(),
                    schedule.version(),
                    schedule.createdAt(),
                    schedule.updatedAt());
        }
    }

    static String requireReason(String reason) {
        if (reason == null || reason.isBlank()) {
            throw ProblemException.badRequest("REASON_REQUIRED", "reason is required");
        }
        return reason.trim();
    }
}
