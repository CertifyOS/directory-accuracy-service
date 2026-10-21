package com.certifyos.directory_accuracy.export.schedule;

import com.certifyos.directory_accuracy.http.ProblemException;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Optional;
import java.util.regex.Pattern;

/** The request bodies of the schedule endpoints, in the design's shapes, and their conversions. */
public final class ScheduleRequests {

    /** A saved filter id as egress accepts it: letters, digits, {@code _} and {@code -}, at most 255. */
    private static final Pattern FILTER_ID = Pattern.compile("^[A-Za-z0-9_-]{1,255}$");

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

    /**
     * Checks a saved filter id for form only. Whether the filter exists and is usable is egress's
     * check on the first export ({@code FILTER_NOT_USABLE}); this service never reads the filter.
     *
     * @param filterId the id from the request
     * @return the id, unchanged
     */
    public static String toFilterId(String filterId) {
        if (filterId == null || !FILTER_ID.matcher(filterId).matches()) {
            throw ProblemException.badRequest(
                    "FILTER_ID_INVALID", "filterId is required: 1-255 letters, digits, '_' or '-'");
        }
        return filterId;
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
            Long version, CadenceRequest cadence, String timezone, String filterId, String egressTemplateId) {
        Optional<String> templateOverride() {
            return Optional.ofNullable(egressTemplateId).filter(id -> !id.isBlank());
        }
    }

    /** {@code disable} body. */
    public record DisableRequest(String reason) {}

    /** {@code enable} body. */
    public record EnableRequest(String reason, Boolean catchUp) {}

    /** {@code run-now} answer. */
    public record RunNowResponse(String exportBatchId, String jobId, java.time.Instant nextDueAt) {}

    /** A schedule as the API shows it. */
    public record ScheduleView(
            String tenantId,
            String vendor,
            boolean enabled,
            CadenceRequest cadence,
            String timezone,
            String filterId,
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
                    schedule.filterId(),
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
