package com.certifyos.directory_accuracy.export.schedule;

import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria.Clause;
import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria.Operator;
import com.certifyos.directory_accuracy.http.ProblemException;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * The fields and operators a selection may use, from the design's Contracts step 2. Checked on
 * every schedule write and again when a batch copies the criteria. Unknown fields or operators
 * are refused, never ignored; values are checked for type, never against platform enumerations
 * (preview is where an operator sees what the criteria match).
 */
public final class SelectionSchema {

    /** The problem code for a refused selection. */
    public static final String CODE = "SELECTION_INVALID";

    static final String USER_DEFINED_PREFIX = "data.userDefinedFields.";
    static final String DUE_DATE = "data.credentialingDueDate";
    static final String ROSTER_IDS = "rosterIds";
    static final int ROSTER_IDS_MAX = 20;

    private static final Set<Operator> EQ_IN = Set.of(Operator.EQ, Operator.IN);
    private static final Set<Operator> IN_ONLY = Set.of(Operator.IN);
    private static final Set<Operator> RANGE = Set.of(Operator.GTE, Operator.LTE);
    private static final Map<String, Set<Operator>> FIELDS = Map.of(
            "data.delegationStatus",
            EQ_IN,
            "data.practitionerRoles",
            IN_ONLY,
            "data.practitionerType",
            IN_ONLY,
            "data.licensedStates",
            IN_ONLY,
            "data.statesToCredential",
            IN_ONLY,
            "data.lineOfBusiness",
            IN_ONLY,
            "credentialingStatus",
            EQ_IN,
            DUE_DATE,
            RANGE,
            ROSTER_IDS,
            IN_ONLY);

    private SelectionSchema() {}

    /**
     * Checks a selection.
     *
     * @param criteria the clauses
     * @throws ProblemException 400 {@code SELECTION_INVALID} naming every problem found
     */
    public static void validate(SelectionCriteria criteria) {
        List<String> problems = new ArrayList<>();
        Set<String> seen = new java.util.HashSet<>();
        for (Clause clause : criteria.clauses()) {
            if (!seen.add(clause.field())) {
                problems.add(clause.field() + ": one clause per field");
            }
            problems.addAll(check(clause));
        }
        if (!problems.isEmpty()) {
            throw ProblemException.badRequest(CODE, String.join("; ", problems));
        }
    }

    private static List<String> check(Clause clause) {
        Set<Operator> allowed = allowedOperators(clause.field());
        if (allowed == null) {
            return List.of(clause.field() + ": unknown field");
        }
        if (!allowed.contains(clause.operator())) {
            return List.of(clause.field() + ": operator "
                    + clause.operator().name().toLowerCase(java.util.Locale.ROOT) + " not allowed");
        }
        return checkValues(clause);
    }

    private static List<String> checkValues(Clause clause) {
        List<String> problems = new ArrayList<>();
        for (String value : clause.values()) {
            if (value == null || value.isBlank()) {
                problems.add(clause.field() + ": blank value");
            }
        }
        if (DUE_DATE.equals(clause.field())) {
            problems.addAll(checkDates(clause));
        }
        if (ROSTER_IDS.equals(clause.field())) {
            problems.addAll(checkRosterIds(clause));
        }
        return problems;
    }

    private static List<String> checkDates(Clause clause) {
        List<String> problems = new ArrayList<>();
        for (String value : clause.values()) {
            try {
                LocalDate.parse(value);
            } catch (DateTimeParseException notADate) {
                problems.add(clause.field() + ": '" + value + "' is not an ISO date");
            }
        }
        return problems;
    }

    private static List<String> checkRosterIds(Clause clause) {
        List<String> problems = new ArrayList<>();
        if (clause.values().size() > ROSTER_IDS_MAX) {
            problems.add(clause.field() + ": at most " + ROSTER_IDS_MAX + " ids");
        }
        for (String value : clause.values()) {
            try {
                UUID.fromString(value);
            } catch (IllegalArgumentException notAUuid) {
                problems.add(clause.field() + ": '" + value + "' is not a UUID");
            }
        }
        return problems;
    }

    static Set<Operator> allowedOperators(String field) {
        if (field.startsWith(USER_DEFINED_PREFIX) && field.length() > USER_DEFINED_PREFIX.length()) {
            return EQ_IN;
        }
        return FIELDS.get(field);
    }
}
