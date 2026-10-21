package com.certifyos.directory_accuracy.export.schedule;

import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria.Clause;
import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria.Operator;
import com.certifyos.directory_accuracy.http.ProblemException;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SelectionSchemaTest {

    private static ProblemException refused(Clause... clauses) {
        return Assertions.assertThrows(
                ProblemException.class, () -> SelectionSchema.validate(new SelectionCriteria(List.of(clauses))));
    }

    @Test
    void everyDesignFieldWithItsOperatorsPasses() {
        SelectionSchema.validate(new SelectionCriteria(List.of(
                new Clause("data.delegationStatus", Operator.IN, List.of("Direct", "Delegated")),
                new Clause("data.practitionerRoles", Operator.IN, List.of("Rendering")),
                new Clause("data.practitionerType", Operator.IN, List.of("MD")),
                new Clause("data.licensedStates", Operator.IN, List.of("NY")),
                new Clause("data.statesToCredential", Operator.IN, List.of("NY")),
                new Clause("data.lineOfBusiness", Operator.IN, List.of("Commercial")),
                new Clause("credentialingStatus", Operator.EQ, List.of("Approved")),
                new Clause("data.credentialingDueDate", Operator.GTE, List.of("2026-01-01")),
                new Clause("data.userDefinedFields.region", Operator.EQ, List.of("east")),
                new Clause("rosterIds", Operator.IN, List.of("8b1f9f2e-8f0e-4d0a-9f0f-2f6a4a1d9c11")))));
    }

    @Test
    void unknownFieldIsRefusedWithTheStableCode() {
        ProblemException refused = refused(new Clause("data.secret", Operator.EQ, List.of("x")));
        Assertions.assertEquals(400, refused.status());
        Assertions.assertEquals(SelectionSchema.CODE, refused.code());
        Assertions.assertTrue(refused.getMessage().contains("data.secret"), refused.getMessage());
    }

    @Test
    void operatorMustBeAllowedForTheField() {
        refused(new Clause("data.practitionerRoles", Operator.EQ, List.of("Rendering")));
        refused(new Clause("credentialingStatus", Operator.GTE, List.of("A")));
        refused(new Clause("data.credentialingDueDate", Operator.IN, List.of("2026-01-01")));
    }

    @Test
    void dueDateMustBeAnIsoDate() {
        refused(new Clause("data.credentialingDueDate", Operator.LTE, List.of("01/01/2026")));
    }

    @Test
    void rosterIdsAreUuidsAndAtMostTwenty() {
        refused(new Clause("rosterIds", Operator.IN, List.of("not-a-uuid")));
        List<String> many = java.util.stream.IntStream.range(0, 21)
                .mapToObj(index -> java.util
                        .UUID
                        .nameUUIDFromBytes(("r" + index).getBytes(java.nio.charset.StandardCharsets.UTF_8))
                        .toString())
                .toList();
        refused(new Clause("rosterIds", Operator.IN, many));
    }

    @Test
    void oneClausePerField() {
        refused(
                new Clause("credentialingStatus", Operator.EQ, List.of("A")),
                new Clause("credentialingStatus", Operator.IN, List.of("A", "B")));
    }
}
