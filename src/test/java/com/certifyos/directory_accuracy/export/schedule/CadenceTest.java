package com.certifyos.directory_accuracy.export.schedule;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CadenceTest {

    static final ZoneId NEW_YORK = ZoneId.of("America/New_York");

    @Test
    void monthlyNextIsLocalMidnightStrictlyAfter() {
        Cadence first = Cadence.monthly(1);
        // 2026-10-01 00:00 New York is 04:00Z; one second before, the occurrence is still ahead.
        Assertions.assertEquals(
                Instant.parse("2026-10-01T04:00:00Z"), first.next(Instant.parse("2026-10-01T03:59:59Z"), NEW_YORK));
        // Exactly on the occurrence, the next one is a month later (strictly after).
        Assertions.assertEquals(
                Instant.parse("2026-11-01T04:00:00Z"), first.next(Instant.parse("2026-10-01T04:00:00Z"), NEW_YORK));
        // Mid-month, next month's day.
        Assertions.assertEquals(
                Instant.parse("2026-11-15T05:00:00Z"),
                Cadence.monthly(15).next(Instant.parse("2026-10-20T12:00:00Z"), NEW_YORK),
                "November is on standard time, so local midnight is 05:00Z");
    }

    @Test
    void cronNextFollowsTheExpressionInTheZone() {
        Cadence cadence = Cadence.cron("0 6 * * 1");
        Instant next = cadence.next(Instant.parse("2026-10-01T12:00:00Z"), ZoneId.of("UTC"));
        Assertions.assertEquals(Instant.parse("2026-10-05T06:00:00Z"), next, "the following Monday 06:00");
    }

    @Test
    void nextAfterPeriodSkipsTheRestOfTheBatchMonth() {
        ZoneId utc = ZoneId.of("UTC");
        // run-now on Oct 6 with a day-15 schedule: Oct 15 would find October's batch; Nov 15 is next.
        Assertions.assertEquals(
                Instant.parse("2026-11-15T00:00:00Z"),
                Cadence.monthly(15)
                        .nextAfterPeriod(Instant.parse("2026-10-06T10:00:00Z"), utc, java.time.YearMonth.of(2026, 10)));
        // weekly cron: first Monday of November, not the next Monday in October.
        Assertions.assertEquals(
                Instant.parse("2026-11-02T00:00:00Z"),
                Cadence.cron("0 0 * * 1")
                        .nextAfterPeriod(Instant.parse("2026-10-05T00:00:00Z"), utc, java.time.YearMonth.of(2026, 10)));
        // the tick on the cadence day: same answer as before the fix.
        Assertions.assertEquals(
                Instant.parse("2026-11-01T04:00:00Z"),
                Cadence.monthly(1)
                        .nextAfterPeriod(
                                Instant.parse("2026-10-01T06:00:00Z"), NEW_YORK, java.time.YearMonth.of(2026, 10)));
        // a late batch whose month is already over: never in the past.
        Instant late = Instant.parse("2026-12-03T00:00:00Z");
        Assertions.assertTrue(Cadence.monthly(1)
                .nextAfterPeriod(late, utc, java.time.YearMonth.of(2026, 10))
                .isAfter(late));
    }

    @Test
    void lastOccurrenceBetweenIsTheMostRecentMissedPeriod() {
        Cadence first = Cadence.monthly(1);
        ZoneId utc = ZoneId.of("UTC");
        Optional<Instant> missed = first.lastOccurrenceBetween(
                Instant.parse("2026-08-15T00:00:00Z"), Instant.parse("2026-10-20T00:00:00Z"), utc);
        Assertions.assertEquals(Optional.of(Instant.parse("2026-10-01T00:00:00Z")), missed);

        Optional<Instant> none = first.lastOccurrenceBetween(
                Instant.parse("2026-10-02T00:00:00Z"), Instant.parse("2026-10-20T00:00:00Z"), utc);
        Assertions.assertTrue(none.isEmpty(), "nothing fell in the window");

        Optional<Instant> onTheEdge = first.lastOccurrenceBetween(
                Instant.parse("2026-10-01T00:00:00Z"), Instant.parse("2026-10-01T00:00:00Z"), utc);
        Assertions.assertEquals(
                Optional.of(Instant.parse("2026-10-01T00:00:00Z")), onTheEdge, "from and until are inclusive");
    }
}
