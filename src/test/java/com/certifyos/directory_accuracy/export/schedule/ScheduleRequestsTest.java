package com.certifyos.directory_accuracy.export.schedule;

import com.certifyos.directory_accuracy.export.schedule.ScheduleRequests.CadenceRequest;
import com.certifyos.directory_accuracy.export.schedule.ScheduleRequests.ClauseRequest;
import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria.Operator;
import com.certifyos.directory_accuracy.http.ProblemException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ScheduleRequestsTest {

    @Test
    void selectionObjectBecomesValidatedCriteriaAndBack() {
        Map<String, ClauseRequest> selection = new LinkedHashMap<>();
        selection.put("data.delegationStatus", new ClauseRequest(null, List.of("Direct"), null, null));
        selection.put("credentialingStatus", new ClauseRequest("Approved", null, null, null));
        selection.put("data.credentialingDueDate", new ClauseRequest(null, null, "2026-01-01", null));

        SelectionCriteria criteria = ScheduleRequests.toCriteria(selection);

        Assertions.assertEquals(3, criteria.clauses().size());
        Assertions.assertEquals(Operator.IN, criteria.clauses().get(0).operator());
        Assertions.assertEquals(Operator.EQ, criteria.clauses().get(1).operator());
        Assertions.assertEquals(Operator.GTE, criteria.clauses().get(2).operator());
        Assertions.assertEquals(selection, ScheduleRequests.toSelection(criteria));
    }

    @Test
    void clauseCarriesExactlyOneOperator() {
        ProblemException two = Assertions.assertThrows(
                ProblemException.class,
                () -> ScheduleRequests.toCriteria(
                        Map.of("credentialingStatus", new ClauseRequest("A", List.of("B"), null, null))));
        Assertions.assertEquals(SelectionSchema.CODE, two.code());
        ProblemException none = Assertions.assertThrows(
                ProblemException.class,
                () -> ScheduleRequests.toCriteria(
                        Map.of("credentialingStatus", new ClauseRequest(null, null, null, null))));
        Assertions.assertEquals(SelectionSchema.CODE, none.code());
        Assertions.assertEquals(
                SelectionSchema.CODE,
                Assertions.assertThrows(ProblemException.class, () -> ScheduleRequests.toCriteria(Map.of()))
                        .code());
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
