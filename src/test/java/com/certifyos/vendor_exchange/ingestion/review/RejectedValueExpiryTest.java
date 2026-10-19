package com.certifyos.vendor_exchange.ingestion.review;

import java.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** The rejected-value expiry: whole calendar months in UTC, and never a setting that expires at once. */
class RejectedValueExpiryTest {

    private static final Instant NOW = Instant.parse("2026-01-31T10:00:00Z");

    @Test
    void addsWholeCalendarMonthsInUtc() {
        Assertions.assertEquals(Instant.parse("2027-01-31T10:00:00Z"), RejectedValueRepository.expiry(NOW, 12));
        Assertions.assertEquals(
                Instant.parse("2026-02-28T10:00:00Z"),
                RejectedValueRepository.expiry(NOW, 1),
                "a short month clamps to its last day");
    }

    @Test
    void aSettingBelowOneMonthIsRefused() {
        for (int months : new int[] {0, -1}) {
            IllegalArgumentException refused = Assertions.assertThrows(
                    IllegalArgumentException.class, () -> RejectedValueRepository.expiry(NOW, months));
            Assertions.assertTrue(refused.getMessage().contains("rejected-value-ttl-months"), refused.getMessage());
        }
    }
}
