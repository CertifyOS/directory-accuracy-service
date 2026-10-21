package com.certifyos.directory_accuracy.export.schedule;

import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria.Clause;
import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria.Operator;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SelectionCriteriaTest {

    @Test
    void roundTripsThroughBsonAsAListOfClauses() {
        SelectionCriteria criteria = new SelectionCriteria(List.of(
                new Clause("data.delegationStatus", Operator.IN, List.of("Direct", "Delegated")),
                new Clause("data.credentialingDueDate", Operator.GTE, List.of("2026-01-01"))));

        List<Document> docs = criteria.toDocuments();
        Assertions.assertEquals(
                "data.delegationStatus", docs.get(0).getString("field"), "field is a value, never a key");
        Assertions.assertEquals("in", docs.get(0).getString("op"));
        Assertions.assertEquals(criteria, SelectionCriteria.fromDocuments(docs));
    }

    @Test
    void singleValueOperatorsTakeExactlyOneValue() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new Clause("credentialingStatus", Operator.EQ, List.of("A", "B")));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new Clause("x", Operator.IN, List.of()));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new Clause(" ", Operator.IN, List.of("a")));
    }

    @Test
    void criteriaNeedAtLeastOneClauseAndAreImmutable() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new SelectionCriteria(List.of()));
        List<Clause> clauses = new java.util.ArrayList<>(List.of(new Clause("rosterIds", Operator.IN, List.of("r1"))));
        SelectionCriteria criteria = new SelectionCriteria(clauses);
        clauses.clear();
        Assertions.assertEquals(1, criteria.clauses().size(), "the record keeps its own copy");
    }

    @Test
    void filterIsTheApiLayerShapeFieldToOperatorToValue() {
        SelectionCriteria criteria = new SelectionCriteria(List.of(
                new Clause("data.delegationStatus", Operator.IN, List.of("Direct", "Delegated")),
                new Clause("credentialingStatus", Operator.EQ, List.of("Approved")),
                new Clause("data.credentialingDueDate", Operator.LTE, List.of("2026-12-31"))));

        java.util.Map<String, java.util.Map<String, Object>> filter = criteria.toFilter();

        Assertions.assertEquals(
                List.of("Direct", "Delegated"),
                filter.get("data.delegationStatus").get("in"));
        Assertions.assertEquals(
                "Approved", filter.get("credentialingStatus").get("eq"), "eq carries one value, not a list");
        Assertions.assertEquals(
                "2026-12-31", filter.get("data.credentialingDueDate").get("lte"));
    }
}
