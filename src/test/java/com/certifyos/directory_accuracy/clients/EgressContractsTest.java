package com.certifyos.directory_accuracy.clients;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** The wire shapes egress uses today, read from its {@code EgressExportResource}. */
class EgressContractsTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void newJobComesBackFlat() throws Exception {
        EgressExportResponse response = mapper.readValue(
                """
                {"type":"practitioner","tenant_id":"org-a","correlation_id":"org-a-candor-2026-10-001-r1",
                 "table":"practitioner:org-a:org-a-candor-2026-10-001-r1","ttl_days":7,"state":"PENDING",
                 "template_id":"t-1","use_template_pipeline":true}
                """,
                EgressExportResponse.class);
        Assertions.assertNull(response.job());
        Assertions.assertEquals(
                "org-a-candor-2026-10-001-r1", response.jobReference().correlationId());
        Assertions.assertEquals(7, response.jobReference().ttlDays());
    }

    @Test
    void knownCorrelationIdComesBackWrapped() throws Exception {
        EgressExportResponse response = mapper.readValue(
                """
                {"job":{"type":"practitioner","tenant_id":"org-a","correlation_id":"c-1","table":"t","ttl_days":7}}
                """,
                EgressExportResponse.class);
        Assertions.assertEquals("c-1", response.jobReference().correlationId());
        Assertions.assertEquals("org-a", response.jobReference().tenantId());
    }

    @Test
    void statusReadsSnakeCase() throws Exception {
        EgressStatusResponse status = mapper.readValue(
                """
                {"type":"practitioner","tenant_id":"org-a","correlation_id":"c-1","table":"t","total_rows":120,
                 "rows_with_data":120,"successful_rows":118,"unsuccessful_rows":2,"failed_rows":2,
                 "state":"COMPLETED","phase":"EXPORT_DONE","gcs_uri":"gs://b/f.csv","gcs_complete":true}
                """,
                EgressStatusResponse.class);
        Assertions.assertEquals(120L, status.totalRows());
        Assertions.assertEquals("gs://b/f.csv", status.gcsUri());
        Assertions.assertTrue(status.gcsComplete());
    }

    @Test
    void requestUsesEgressFieldNamesAndOmitsNulls() throws Exception {
        EgressExportRequest request = new EgressExportRequest(
                EgressExportRequest.PRACTITIONER,
                "org-a",
                "org-a-candor-2026-10-001-r1",
                "t-1",
                "gs://egress-templates/org-a/t-1/v3/mappings.csv",
                ";",
                "csv",
                "[\"locations\"]",
                EgressExportRequest.Selection.savedFilter("flt-direct"),
                EgressClient.INITIATOR,
                new EgressExportRequest.Destination("vendor-sftp", "from/org-a/file.csv"));
        String json = mapper.writeValueAsString(request);
        Assertions.assertFalse(json.contains("npiFilter"), "egress refuses a selection beside an NPI list");
        Assertions.assertTrue(json.contains("\"selection\":{\"source\":\"SAVED_FILTER\""));
        Assertions.assertTrue(
                json.contains("\"destination\":{\"bucket\":\"vendor-sftp\",\"objectName\":\"from/org-a/file.csv\"}"));
        Assertions.assertTrue(
                json.contains("\"rowExpansionKeys\":\"[\\\"locations\\\"]\""), "a string, as egress declares it");
        String sparse = mapper.writeValueAsString(new EgressExportRequest(
                EgressExportRequest.PRACTITIONER, "org-a", "c", null, null, null, null, null, null, null, null));
        Assertions.assertFalse(sparse.contains("templateId"), "nulls are omitted");
    }

    @Test
    void firstAttemptSelectionNamesTheFilter() throws Exception {
        String json = mapper.writeValueAsString(EgressExportRequest.Selection.savedFilter("flt_candor-1"));
        Assertions.assertEquals(
                Map.of("source", "SAVED_FILTER", "filterId", "flt_candor-1", "listUsed", true),
                mapper.readValue(json, Map.class));
    }

    @Test
    void retrySelectionNamesTheEarlierAttemptOnly() throws Exception {
        String json = mapper.writeValueAsString(EgressExportRequest.Selection.reuse("org-a-candor-2026-10-001-r1"));
        Assertions.assertEquals(
                Map.of("listUsed", true, "reuseSelectionFrom", "org-a-candor-2026-10-001-r1"),
                mapper.readValue(json, Map.class),
                "no source and no filterId: egress refuses either beside a reuse");
    }

    @Test
    void statusSelectionBlockIsCamelCaseInsideTheSnakeCaseResponse() throws Exception {
        EgressStatusResponse status = mapper.readValue(
                """
                {"type":"practitioner","tenant_id":"org-a","correlation_id":"c-1","state":"FAILED",
                 "selection":{"source":"SAVED_FILTER","ref":"flt-1","state":"FAILED","cursor":null,
                  "expectedCount":null,"requested":0,"distinct":0,"duplicates":0,"matched":0,"unmatched":0,
                  "parts":0,"window":{"start":"2026-10-01T06:00:00Z","end":null},
                  "listUsedUri":"gs://egress/list-used/c-1.csv","reusedFrom":null,"attempts":1,
                  "failureReason":"NO_MATCHING_PRACTITIONERS"}}
                """,
                EgressStatusResponse.class);
        Assertions.assertEquals(0L, status.selection().distinct());
        Assertions.assertEquals(
                "gs://egress/list-used/c-1.csv", status.selection().listUsedUri());
        Assertions.assertEquals("NO_MATCHING_PRACTITIONERS", status.selection().failureReason());
    }

    @Test
    void statusWithoutSelectionReadsNull() throws Exception {
        EgressStatusResponse status =
                mapper.readValue("{\"correlation_id\":\"c-1\",\"state\":\"COMPLETED\"}", EgressStatusResponse.class);
        Assertions.assertNull(status.selection());
    }
}
