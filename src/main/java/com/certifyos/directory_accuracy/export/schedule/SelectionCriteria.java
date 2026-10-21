package com.certifyos.directory_accuracy.export.schedule;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.bson.Document;

/**
 * The practitioner selection criteria: one clause per field, all clauses combined with AND, the
 * shape api-layer's {@code filter} parameter takes. Typed, never a raw map. Stored as a list of
 * clauses rather than the API's object keyed by field name, because field names contain dots
 * ({@code data.delegationStatus}) and a dotted key inside a BSON document is a trap for every later
 * query and update path.
 *
 * @param clauses the clauses, in the order given
 */
public record SelectionCriteria(List<Clause> clauses) {

    /** Operators the schema accepts; which fields allow which is the schema's job, not this record's. */
    public enum Operator {
        EQ,
        IN,
        GTE,
        LTE
    }

    /**
     * One clause.
     *
     * @param field the api-layer field path, for example {@code data.delegationStatus}
     * @param operator the operator
     * @param values the values; exactly one for eq, gte and lte
     */
    public record Clause(String field, Operator operator, List<String> values) {
        public Clause {
            if (field == null || field.isBlank()) {
                throw new IllegalArgumentException("clause field is required");
            }
            if (operator == null) {
                throw new IllegalArgumentException("clause operator is required");
            }
            if (values == null || values.isEmpty()) {
                throw new IllegalArgumentException("clause values are required for " + field);
            }
            if (operator != Operator.IN && values.size() != 1) {
                throw new IllegalArgumentException(operator + " takes exactly one value for " + field);
            }
            values = List.copyOf(values);
        }

        Document toDocument() {
            return new Document("field", field)
                    .append("op", operator.name().toLowerCase(Locale.ROOT))
                    .append("values", values);
        }

        static Clause fromDocument(Document doc) {
            return new Clause(
                    doc.getString("field"),
                    Operator.valueOf(doc.getString("op").toUpperCase(Locale.ROOT)),
                    doc.getList("values", String.class));
        }
    }

    public SelectionCriteria {
        if (clauses == null || clauses.isEmpty()) {
            throw new IllegalArgumentException("selection needs at least one clause");
        }
        clauses = List.copyOf(clauses);
    }

    /**
     * api-layer's {@code filter} shape: {@code { "<field>": { "<op>": value } }}, one value for eq,
     * gte and lte, a list for in. The caller serialises it to JSON.
     *
     * @return the filter, fields in clause order
     */
    public Map<String, Map<String, Object>> toFilter() {
        Map<String, Map<String, Object>> filter = new LinkedHashMap<>();
        for (Clause clause : clauses) {
            Object value = clause.operator() == Operator.IN
                    ? clause.values()
                    : clause.values().get(0);
            filter.put(clause.field(), Map.of(clause.operator().name().toLowerCase(Locale.ROOT), value));
        }
        return filter;
    }

    /** The BSON shape: a list of {@code { field, op, values }}. */
    public List<Document> toDocuments() {
        List<Document> docs = new ArrayList<>(clauses.size());
        for (Clause clause : clauses) {
            docs.add(clause.toDocument());
        }
        return docs;
    }

    /**
     * Reads the BSON shape.
     *
     * @param docs the stored clause documents
     * @return the criteria
     */
    public static SelectionCriteria fromDocuments(List<Document> docs) {
        List<Clause> clauses = new ArrayList<>(docs.size());
        for (Document doc : docs) {
            clauses.add(Clause.fromDocument(doc));
        }
        return new SelectionCriteria(clauses);
    }
}
