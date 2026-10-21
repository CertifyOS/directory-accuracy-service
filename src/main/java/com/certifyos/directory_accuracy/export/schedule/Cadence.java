package com.certifyos.directory_accuracy.export.schedule;

import com.certifyos.directory_accuracy.persistence.Documents;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;
import org.bson.Document;
import org.jobrunr.scheduling.cron.CronExpression;

/**
 * When a schedule is due. {@code MONTHLY} on a day 1 to 28 (so every month has the day) or a cron
 * expression. The next occurrence is computed by the schedule service in the schedule's timezone.
 *
 * @param type monthly or cron
 * @param dayOfMonth 1 to 28 when monthly, null otherwise
 * @param expression a cron expression when cron, null otherwise
 */
public record Cadence(Type type, Integer dayOfMonth, String expression) {

    /** Cadence kinds. */
    public enum Type {
        MONTHLY,
        CRON
    }

    public Cadence {
        if (type == null) {
            throw new IllegalArgumentException("cadence type is required");
        }
        if (type == Type.MONTHLY && (dayOfMonth == null || dayOfMonth < 1 || dayOfMonth > 28)) {
            throw new IllegalArgumentException("monthly cadence needs dayOfMonth 1..28, got " + dayOfMonth);
        }
        if (type == Type.CRON) {
            if (expression == null || expression.isBlank()) {
                throw new IllegalArgumentException("cron cadence needs an expression");
            }
            try {
                new CronExpression(expression);
            } catch (RuntimeException invalid) {
                throw new IllegalArgumentException("cron expression is invalid: " + expression, invalid);
            }
        }
    }

    /**
     * The first occurrence strictly after an instant, in the schedule's timezone. Monthly cadences
     * occur at local midnight on their day; cron cadences follow the expression in that zone.
     *
     * @param after the instant to look after, usually now
     * @param zone the schedule's timezone
     * @return the next occurrence, UTC
     */
    public Instant next(Instant after, ZoneId zone) {
        if (type == Type.MONTHLY) {
            LocalDate day = after.atZone(zone).toLocalDate();
            ZonedDateTime candidate = day.withDayOfMonth(dayOfMonth).atStartOfDay(zone);
            if (!candidate.toInstant().isAfter(after)) {
                candidate = day.plusMonths(1).withDayOfMonth(dayOfMonth).atStartOfDay(zone);
            }
            return candidate.toInstant();
        }
        return new CronExpression(expression).next(after, after, zone);
    }

    /**
     * The first occurrence that is after {@code now} and falls in a month after {@code period}, in
     * the schedule's timezone. The schedule's next due instant after a batch for {@code period}:
     * an occurrence still inside that month would find the month's batch already created and be
     * skipped forever (run-now before the cadence day, or any cron finer than monthly).
     *
     * @param now the time of the write
     * @param zone the schedule's timezone
     * @param period the month the batch just created is for
     * @return the next occurrence, UTC
     */
    public Instant nextAfterPeriod(Instant now, ZoneId zone, java.time.YearMonth period) {
        Instant monthStart = period.plusMonths(1).atDay(1).atStartOfDay(zone).toInstant();
        Instant from = now.isAfter(monthStart) ? now : monthStart.minusNanos(1);
        return next(from, zone);
    }

    /**
     * The most recent occurrence at or after {@code from} and not after {@code until}, for a
     * schedule re-enabled with catch-up: the one missed period to run now.
     *
     * @param from when the schedule was disabled
     * @param until now
     * @param zone the schedule's timezone
     * @return the occurrence, or empty when none fell in the window
     */
    public Optional<Instant> lastOccurrenceBetween(Instant from, Instant until, ZoneId zone) {
        Instant last = null;
        Instant cursor = from.minusSeconds(1);
        while (true) {
            Instant candidate = next(cursor, zone);
            if (candidate.isAfter(until)) {
                return Optional.ofNullable(last);
            }
            last = candidate;
            cursor = candidate;
        }
    }

    /**
     * A monthly cadence.
     *
     * @param dayOfMonth 1 to 28
     * @return the cadence
     */
    public static Cadence monthly(int dayOfMonth) {
        return new Cadence(Type.MONTHLY, dayOfMonth, null);
    }

    /**
     * A cron cadence.
     *
     * @param expression the cron expression
     * @return the cadence
     */
    public static Cadence cron(String expression) {
        return new Cadence(Type.CRON, null, expression);
    }

    /** The BSON shape: {@code { type, dayOfMonth? , expression? }}. */
    public Document toDocument() {
        Document doc = new Document("type", type.name().toLowerCase(java.util.Locale.ROOT));
        Documents.put(doc, "dayOfMonth", dayOfMonth);
        Documents.put(doc, "expression", expression);
        return doc;
    }

    /**
     * Reads the BSON shape.
     *
     * @param doc the nested cadence document
     * @return the cadence
     */
    public static Cadence fromDocument(Document doc) {
        Type type = Type.valueOf(doc.getString("type").toUpperCase(java.util.Locale.ROOT));
        return new Cadence(type, Documents.intValue(doc, "dayOfMonth"), doc.getString("expression"));
    }
}
