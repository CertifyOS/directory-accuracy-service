package com.certifyos.vendor_exchange.ingestion.inbound;

import java.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** The lifecycle's pure arithmetic: response days, wait seconds and the error cut. */
class InboundLifecycleTest {

    private static final Instant DELIVERED = Instant.parse("2026-10-13T06:00:00Z");

    @Test
    void daysToResponseCountsWholeDaysAndNeverGoesNegative() {
        Assertions.assertEquals(2, InboundLifecycle.daysToResponse(DELIVERED, Instant.parse("2026-10-15T09:12:40Z")));
        Assertions.assertEquals(1, InboundLifecycle.daysToResponse(DELIVERED, Instant.parse("2026-10-15T05:59:59Z")));
        Assertions.assertEquals(
                0,
                InboundLifecycle.daysToResponse(DELIVERED, Instant.parse("2026-10-11T00:00:00Z")),
                "a storage clock ahead of ours is not a negative response time");
        Assertions.assertEquals(0, InboundLifecycle.daysToResponse(null, DELIVERED));
    }

    @Test
    void waitSecondsIsZeroWithoutARequestTime() {
        Assertions.assertEquals(60, InboundLifecycle.waitSeconds(DELIVERED, DELIVERED.plusSeconds(60)));
        Assertions.assertEquals(0, InboundLifecycle.waitSeconds(DELIVERED, DELIVERED.minusSeconds(5)));
        Assertions.assertEquals(0, InboundLifecycle.waitSeconds(null, DELIVERED));
    }

    @Test
    void lastErrorIsCutTo500Characters() {
        Assertions.assertNull(InboundLifecycle.truncate(null));
        Assertions.assertEquals("short", InboundLifecycle.truncate("short"));
        String exact = "e".repeat(InboundLifecycle.LAST_ERROR_MAX);
        Assertions.assertSame(exact, InboundLifecycle.truncate(exact));
        Assertions.assertEquals(exact, InboundLifecycle.truncate(exact + "tail"));
    }
}
