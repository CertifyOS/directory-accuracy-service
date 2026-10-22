package com.certifyos.directory_accuracy.export.schedule;

import com.certifyos.directory_accuracy.export.schedule.ScheduleRequests.CadenceRequest;
import com.certifyos.directory_accuracy.http.ProblemException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ScheduleRequestsTest {

    @Test
    void filterIdIsStoredAsGiven() {
        Assertions.assertEquals("flt_candor-Direct-01", ScheduleRequests.toFilterId("flt_candor-Direct-01"));
        Assertions.assertEquals("a".repeat(255), ScheduleRequests.toFilterId("a".repeat(255)));
    }

    @Test
    void filterIdIsCheckedForFormOnly() {
        for (String invalid : new String[] {null, "", " ", "flt.direct", "flt direct", "flt/1", "a".repeat(256)}) {
            ProblemException refused =
                    Assertions.assertThrows(ProblemException.class, () -> ScheduleRequests.toFilterId(invalid));
            Assertions.assertEquals("FILTER_ID_INVALID", refused.code(), "for " + invalid);
            Assertions.assertEquals(400, refused.status());
        }
    }

    @Test
    void cadenceRequestMapsToTheDomainAndRefusesTheRest() {
        Assertions.assertEquals(Cadence.monthly(1), new CadenceRequest("monthly", 1, null).toCadence());
        Assertions.assertEquals(Cadence.cron("0 6 1 * *"), new CadenceRequest("cron", null, "0 6 1 * *").toCadence());
        Assertions.assertEquals(
                "CADENCE_INVALID",
                Assertions.assertThrows(
                                ProblemException.class, () -> new CadenceRequest("weekly", null, null).toCadence())
                        .code());
        Assertions.assertEquals(
                "CADENCE_INVALID",
                Assertions.assertThrows(
                                ProblemException.class, () -> new CadenceRequest("monthly", 31, null).toCadence())
                        .code(),
                "day 31 is refused by Cadence and surfaces under the cadence code");
    }

    @Test
    void timezoneMustBeAKnownZone() {
        Assertions.assertEquals(
                "America/New_York", ScheduleRequests.toZone("America/New_York").getId());
        Assertions.assertEquals(
                "TIMEZONE_INVALID",
                Assertions.assertThrows(ProblemException.class, () -> ScheduleRequests.toZone("Mars/Olympus"))
                        .code());
        Assertions.assertEquals(
                "TIMEZONE_INVALID",
                Assertions.assertThrows(ProblemException.class, () -> ScheduleRequests.toZone(" "))
                        .code());
    }
}
