package com.certifyos.directory_accuracy.ingestion.staging;

import com.certifyos.directory_accuracy.clients.FileIngestionCell;
import com.certifyos.directory_accuracy.ingestion.staging.WideRow.FieldCell;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class WideRowTest {

    @Test
    void readsOneDesignRow() {
        WideRow row = WideRow.from(DesignRows.row());

        Assertions.assertEquals(DesignRows.SHEET, row.sheet());
        Assertions.assertEquals(DesignRows.ROW, row.row());
        Assertions.assertTrue(row.schemaValid());
        Assertions.assertEquals(List.of(), row.problems());
        Assertions.assertEquals("PRACTITIONER", row.entityType());
        Assertions.assertEquals("cert-000123", row.certifyPractitionerId());
        Assertions.assertEquals("1003002890", row.npi());
        Assertions.assertEquals("loc-7781", row.certifyLocationId());
        Assertions.assertTrue(row.hasLocationId());
        Assertions.assertTrue(row.isPractitioner());

        List<String> fields = new ArrayList<>(CandorRecsMapping.PRACTITIONER_FIELDS);
        fields.addAll(CandorRecsMapping.LOCATION_FIELDS);
        Assertions.assertEquals(fields, List.copyOf(row.cells().keySet()), "one cell per field, in field order");

        Assertions.assertEquals(
                new FieldCell(
                        "practice_name",
                        FieldLevel.LOCATION,
                        "INVALID",
                        "AdventHealth",
                        "Greenwood Village",
                        List.of("direct_outreach"),
                        "VERY HIGH",
                        DesignRows.EVIDENCE),
                row.cells().get("practice_name"));
        FieldCell phone = row.cells().get("phone");
        Assertions.assertEquals("VALID", phone.vendorStatus());
        Assertions.assertEquals("3036790177", phone.sentValue());
        Assertions.assertNull(phone.vendorValue());
        Assertions.assertEquals(List.of(), phone.reasons());
        Assertions.assertEquals("[]", phone.evidenceJson());

        FieldCell location = row.cells().get("location");
        Assertions.assertEquals(FieldLevel.LOCATION, location.level());
        Assertions.assertEquals("9100 E Mineral Cir|Suite 200|Centennial|CO|80112", location.sentValue());
        Assertions.assertEquals("", location.vendorStatus(), "an absent status cell reads as blank");

        FieldCell npi = row.cells().get("npi");
        Assertions.assertEquals(FieldLevel.PRACTITIONER, npi.level());
        Assertions.assertEquals("1003002890", npi.sentValue(), "the npi field's sent value is the npi column");

        FieldCell firstName = row.cells().get("first_name");
        Assertions.assertEquals(FieldLevel.PRACTITIONER, firstName.level());
        Assertions.assertEquals(
                List.of("direct_outreach", "nppes_update"),
                firstName.reasons(),
                "split on ';', trimmed, blanks dropped");
        Assertions.assertEquals("called the office", firstName.evidenceJson(), "the raw evidence text");
        Assertions.assertEquals("UNKNOWN", row.cells().get("fax").vendorStatus());
    }

    @Test
    void blankLocationIdHasNoLocationId() {
        Map<String, FileIngestionCell> blank = Map.of("certify_location_id", DesignRows.valid("  "));
        Map<String, FileIngestionCell> absent = new HashMap<>();
        absent.put("certify_location_id", null);

        Assertions.assertFalse(WideRow.from(DesignRows.row("VALID", blank)).hasLocationId());
        WideRow withoutCell = WideRow.from(DesignRows.row("VALID", absent));
        Assertions.assertFalse(withoutCell.hasLocationId());
        Assertions.assertNull(withoutCell.certifyLocationId());
        Assertions.assertTrue(withoutCell.isPractitioner(), "the rest of the row still reads");
    }

    @Test
    void schemaInvalidRowKeepsProblems() {
        Map<String, FileIngestionCell> overrides = Map.of(
                "npi", new FileIngestionCell("10030", "INVALID", List.of("npi: digits(10)")),
                "zip", new FileIngestionCell("8011", "INVALID", List.of("zip: maxLength", "zip: digits")));

        WideRow row = WideRow.from(DesignRows.row("INVALID", overrides));

        Assertions.assertFalse(row.schemaValid());
        Assertions.assertEquals(List.of("npi: digits(10)", "zip: maxLength", "zip: digits"), row.problems());
        Assertions.assertEquals("10030", row.npi(), "the values still read, for the counts");
        Assertions.assertTrue(WideRow.from(DesignRows.row("PARTIAL", Map.of())).schemaValid(), "only INVALID fails");
    }

    @Test
    void nonPractitionerRowIsNotPractitioner() {
        WideRow facility = WideRow.from(DesignRows.row("VALID", Map.of("entity_type", DesignRows.valid("FACILITY"))));
        Map<String, FileIngestionCell> absent = new HashMap<>();
        absent.put("entity_type", null);
        WideRow missing = WideRow.from(DesignRows.row("VALID", absent));

        Assertions.assertFalse(facility.isPractitioner());
        Assertions.assertEquals("FACILITY", facility.entityType());
        Assertions.assertFalse(missing.isPractitioner());
        Assertions.assertNull(missing.entityType());
    }

    @Test
    void entityTypeIsReadInAnyCaseAndTrimmed() {
        for (String echoed : new String[] {"practitioner", "Practitioner", " PRACTITIONER "}) {
            WideRow row = WideRow.from(DesignRows.row("VALID", Map.of("entity_type", DesignRows.valid(echoed))));

            Assertions.assertTrue(row.isPractitioner(), "the export sends practitioner; a vendor may echo " + echoed);
        }
    }

    @Test
    void vendorStatusIsUpperCased() {
        String column = CandorRecsMapping.column("phone", CandorRecsMapping.Suffix.STATUS);
        WideRow row = WideRow.from(DesignRows.row("VALID", Map.of(column, DesignRows.valid(" Invalid "))));

        Assertions.assertEquals("INVALID", row.cells().get("phone").vendorStatus());
    }
}
