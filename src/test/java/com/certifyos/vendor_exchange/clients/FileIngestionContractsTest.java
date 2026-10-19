package com.certifyos.vendor_exchange.clients;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** The row shape file-ingestion streams today, read from its {@code RowView.of}. */
class FileIngestionContractsTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void rowReadsRowViewShapeAndIgnoresUnknownProperties() throws Exception {
        FileIngestionRow row = mapper.readValue(
                """
                {"sheet":"Physicians Directory Accuracy","row":913,"status":"VALID",
                 "values":{"phone":{"value":"(303) 679-0177","status":"VALID"},
                           "practice_name":{"value":"Greenwood Village","status":"INVALID",
                                            "problems":["maxLength: too long"]}},
                 "warnings":["w1"],
                 "header":["phone","practice_name"],
                 "raw":["(303) 679-0177","Greenwood Village"]}
                """,
                FileIngestionRow.class);

        Assertions.assertEquals("Physicians Directory Accuracy", row.sheet());
        Assertions.assertEquals(913, row.row());
        Assertions.assertEquals("VALID", row.status());
        Assertions.assertEquals(List.of("w1"), row.warnings());
        Assertions.assertEquals(
                new FileIngestionCell("(303) 679-0177", "VALID", List.of()),
                row.values().get("phone"));
        FileIngestionCell practiceName = row.values().get("practice_name");
        Assertions.assertEquals("Greenwood Village", practiceName.value());
        Assertions.assertEquals("INVALID", practiceName.status());
        Assertions.assertEquals(List.of("maxLength: too long"), practiceName.problems());
    }

    @Test
    void rowKeepsColumnOrderAndANullCell() throws Exception {
        FileIngestionRow row = mapper.readValue(
                """
                {"sheet":"S","row":1,"status":"VALID",
                 "values":{"zip":{"value":"80111","status":"VALID"},"fax":null,"city":{"value":"Denver","status":"VALID"}}}
                """,
                FileIngestionRow.class);

        Assertions.assertEquals(
                List.of("zip", "fax", "city"), List.copyOf(row.values().keySet()));
        Assertions.assertNull(row.values().get("fax"));
        Assertions.assertEquals(List.of(), row.warnings());
    }
}
