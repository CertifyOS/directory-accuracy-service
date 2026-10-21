package com.certifyos.directory_accuracy.export.schedule;

import java.time.Instant;
import java.time.ZoneId;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ScheduleTest {

    static Schedule sample() {
        return Schedule.create(
                "org-xyz",
                "candor",
                Cadence.monthly(1),
                ZoneId.of("America/New_York"),
                "flt-direct",
                "c9a418b5-348e-44e6-95f0-04c45f527367",
                Instant.parse("2026-11-01T05:00:00Z"),
                "user:ops-1",
                Instant.parse("2026-09-15T14:02:11Z"));
    }

    @Test
    void createIsEnabledAtVersionOneWithNoRunHistory() {
        Schedule schedule = sample();
        Assertions.assertEquals("org-xyz|candor", schedule.id());
        Assertions.assertTrue(schedule.enabled());
        Assertions.assertEquals(1L, schedule.version());
        Assertions.assertNull(schedule.lastBatchId());
        Assertions.assertNull(schedule.disabledAt());
        Assertions.assertEquals("user:ops-1", schedule.updatedBy());
    }

    @Test
    void roundTripsThroughBsonWithoutAbsentFields() {
        Schedule schedule = sample();
        Document doc = schedule.toDocument();
        Assertions.assertFalse(doc.containsKey("lastBatchId"), "absent fields are left out, not stored as null");
        Assertions.assertFalse(doc.containsKey("disabledReason"));
        Assertions.assertEquals("America/New_York", doc.getString("timezone"));
        Assertions.assertEquals("monthly", doc.get("cadence", Document.class).getString("type"));
        Assertions.assertEquals(schedule, Schedule.fromDocument(doc));
    }

    @Test
    void cadenceRulesFromTheDesign() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> Cadence.monthly(29), "days 29-31 are refused");
        Assertions.assertThrows(IllegalArgumentException.class, () -> Cadence.monthly(0));
        Assertions.assertThrows(IllegalArgumentException.class, () -> Cadence.cron(" "));
        Assertions.assertEquals(
                Cadence.cron("0 6 1 * *"),
                Cadence.fromDocument(Cadence.cron("0 6 1 * *").toDocument()));
    }
}
