package com.certifyos.vendor_exchange.ingestion.events;

import com.certifyos.vendor_exchange.export.events.PubSubPushMessage;
import com.certifyos.vendor_exchange.ingestion.staging.FieldLevel;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Survivorship's {@code mdm.slice.ov_generated} envelope as the push delivers it. */
class OvGeneratedEventTest {

    static final ObjectMapper MAPPER = new ObjectMapper();

    static PubSubPushMessage.Message push(String json, Map<String, String> attributes) {
        return new PubSubPushMessage.Message(
                Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8)), "m-1", null, attributes);
    }

    @Test
    void readsTheEnvelope() {
        OvGeneratedEvent event = OvGeneratedEvent.fromMessage(
                        push(
                                "{\"eventId\": \"e-1\", \"serviceSource\": \"survivorship\", \"extra\": 1,"
                                        + " \"payload\": {\"certifyId\": \"cert-1\", \"tenantId\": \"org-xyz\","
                                        + " \"entityType\": \"PRACTITIONER\", \"triggeringCrosswalks\": ["
                                        + "{\"certifyId\": \"s-1\", \"crosswalkId\": \"1003002890\","
                                        + " \"sourceType\": \"candor:org-xyz\", \"updatedAt\": \"2026-10-20T14:00:05Z\"}]}}",
                                Map.of()),
                        MAPPER)
                .orElseThrow();

        Assertions.assertEquals("e-1", event.eventId());
        Assertions.assertEquals("cert-1", event.payload().certifyId());
        Assertions.assertEquals("org-xyz", event.payload().tenantId());
        Assertions.assertEquals(FieldLevel.PRACTITIONER, event.level());
        Assertions.assertTrue(event.triggeredByCandor());
        Assertions.assertEquals(
                "candor:org-xyz", event.payload().triggeringCrosswalks().get(0).sourceType());
    }

    @Test
    void attributesFillWhatThePayloadLacks() {
        OvGeneratedEvent event = OvGeneratedEvent.fromMessage(
                        push(
                                "{\"payload\": {\"certifyId\": \"loc-1\", \"tenantId\": \"org-xyz\"}}",
                                Map.of(
                                        "entityType",
                                        "location",
                                        "triggeringSourceTypes",
                                        "roster:org-xyz,candor:org-xyz")),
                        MAPPER)
                .orElseThrow();

        Assertions.assertEquals(FieldLevel.LOCATION, event.level());
        Assertions.assertTrue(event.triggeredByCandor());
        Assertions.assertEquals(List.of(), event.payload().triggeringCrosswalks());
    }

    @Test
    void notOursAndUnknownTypes() {
        OvGeneratedEvent roster = OvGeneratedEvent.fromMessage(
                        push(
                                "{\"payload\": {\"certifyId\": \"c\", \"tenantId\": \"t\", \"entityType\": \"facility\","
                                        + " \"triggeringCrosswalks\": [{\"sourceType\": \"roster:t\"}]}}",
                                Map.of("triggeringSourceTypes", "roster:t")),
                        MAPPER)
                .orElseThrow();

        Assertions.assertFalse(roster.triggeredByCandor());
        Assertions.assertNull(roster.level());
    }

    @Test
    void unreadableIsEmpty() {
        Assertions.assertTrue(
                OvGeneratedEvent.fromMessage(push("[1, 2]", Map.of()), MAPPER).isEmpty());
        Assertions.assertTrue(OvGeneratedEvent.fromMessage(push("{\"payload\": {}}", Map.of()), MAPPER)
                .isEmpty());
        Assertions.assertTrue(OvGeneratedEvent.fromMessage(
                        new PubSubPushMessage.Message("@@not base64@@", "m", null, Map.of()), MAPPER)
                .isEmpty());
        Assertions.assertTrue(
                OvGeneratedEvent.fromMessage(new PubSubPushMessage.Message(null, "m", null, Map.of()), MAPPER)
                        .isEmpty());
    }
}
