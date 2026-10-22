package com.certifyos.directory_accuracy.export.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class EgressEventTest {

    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Test
    void parsesTheDesignBodyAndIgnoresUnknownFields() throws Exception {
        String body =
                """
                {"schemaVersion":"1","type":"EXPORT_COMPLETED","tenantId":"org-a",
                 "correlationId":"org-a-candor-2026-10-001-r1","phase":"COMPLETED",
                 "outputUri":"gs://vendor-bucket/from/org-a/file.csv","totalRecords":120,"totalRows":120,
                 "failureMessage":null,"completedAt":"2026-10-01T07:12:00Z","somethingNew":true}
                """;
        EgressEvent event = mapper.readValue(body, EgressEvent.class);
        Assertions.assertEquals("org-a-candor-2026-10-001-r1", event.correlationId());
        Assertions.assertEquals(120L, event.totalRecords());
        Assertions.assertEquals(Instant.parse("2026-10-01T07:12:00Z"), event.completedAt());
        Assertions.assertNull(event.failureMessage());
    }

    @Test
    void parsesTheFailureReasonAndSelectionBlock() throws Exception {
        String body =
                """
                {"schemaVersion":"1","type":"EXPORT_FAILED","tenantId":"org-a",
                 "correlationId":"org-a-candor-2026-10-001-r2","phase":"FAILED",
                 "failureMessage":"selection failed","failureReason":"REUSE_SOURCE_UNUSABLE (source list is empty)",
                 "selection":{"requested":0,"distinct":0,"matched":0,"unmatched":0,"parts":0,
                  "listUsedUri":null,"reusedFrom":"org-a-candor-2026-10-001-r1"},
                 "completedAt":"2026-10-01T07:12:00Z"}
                """;
        EgressEvent event = mapper.readValue(body, EgressEvent.class);
        Assertions.assertEquals("REUSE_SOURCE_UNUSABLE (source list is empty)", event.failureReason());
        Assertions.assertEquals("org-a-candor-2026-10-001-r1", event.selection().reusedFrom());
    }

    @Test
    void envelopeAttributesAreNeverNull() throws Exception {
        PubSubPushMessage envelope = mapper.readValue(
                """
                {"message":{"data":"e30=","messageId":"m-1","publishTime":"2026-10-01T07:12:01Z"},
                 "subscription":"projects/p/subscriptions/vendor-export-egress-events"}
                """,
                PubSubPushMessage.class);
        Assertions.assertEquals("m-1", envelope.message().messageId());
        Assertions.assertTrue(envelope.message().attributes().isEmpty());
    }
}
