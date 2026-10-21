package com.certifyos.directory_accuracy.export.schedule;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** The shipped mappings CSV keeps api-layer's header and the rules its parser enforces. */
class MappingsCsvTest {

    static final List<String> HEADER = List.of(
            "output_column",
            "attribute_path",
            "json_extraction_path",
            "multi_value_handling",
            "max_columns",
            "column_order",
            "entity_group",
            "default_value");

    private static List<String[]> rows() {
        String csv = new String(TemplateProvisioner.mappingsCsv(), StandardCharsets.UTF_8);
        return csv.lines()
                .filter(line -> !line.isBlank())
                .map(line -> line.split(",", -1))
                .toList();
    }

    @Test
    void headerIsApiLayersAndEveryRowHasEveryColumn() {
        List<String[]> rows = rows();
        Assertions.assertEquals(HEADER, List.of(rows.get(0)));
        for (String[] row : rows) {
            Assertions.assertEquals(HEADER.size(), row.length, String.join(",", row));
        }
        Assertions.assertEquals(31, rows.size(), "header plus the 30 columns of certify-export-v1");
    }

    @Test
    void outputColumnsAreUniqueAndOrderedAndEveryRowNamesAnAttribute() {
        List<String[]> rows = rows().subList(1, rows().size());
        Set<String> names = new HashSet<>();
        int expectedOrder = 1;
        for (String[] row : rows) {
            Assertions.assertTrue(names.add(row[0]), "duplicate output column " + row[0]);
            Assertions.assertFalse(row[1].isBlank(), "attribute_path is required by api-layer: " + row[0]);
            Assertions.assertEquals(String.valueOf(expectedOrder++), row[5], "column_order of " + row[0]);
            Assertions.assertTrue(
                    row[3].isEmpty()
                            || Set.of("rows", "columns", "comma_separated").contains(row[3]),
                    "multi_value_handling of " + row[0]);
        }
    }

    @Test
    void constantsUseTheNonResolvingPathSoEgressFallsBackToTheDefault() {
        for (String[] row : rows()) {
            if ("_vendor_exchange.constant".equals(row[2])) {
                Assertions.assertFalse(row[7].isBlank(), row[0] + " is a constant and needs a default_value");
            }
        }
        Assertions.assertTrue(
                rows().stream().anyMatch(row -> "schema_version".equals(row[0]) && "certify-export-v1".equals(row[7])));
    }
}
