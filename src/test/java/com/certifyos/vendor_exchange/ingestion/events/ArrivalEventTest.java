package com.certifyos.vendor_exchange.ingestion.events;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ArrivalEventTest {

    private static Map<String, String> attributes() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("bucketId", "vendor-sftp");
        attributes.put("objectId", "to/org-xyz/file.csv");
        attributes.put("objectGeneration", "1760520000123456");
        attributes.put("eventType", "OBJECT_FINALIZE");
        attributes.put("eventTime", "2026-10-15T09:12:41.123Z");
        attributes.put("overwroteGeneration", "1760519000000000");
        return attributes;
    }

    @Test
    void readsTheSixAttributes() {
        ArrivalEvent event = ArrivalEvent.fromAttributes(attributes()).orElseThrow();

        Assertions.assertEquals("vendor-sftp", event.bucketId());
        Assertions.assertEquals("to/org-xyz/file.csv", event.objectId());
        Assertions.assertEquals("1760520000123456", event.objectGeneration());
        Assertions.assertEquals("OBJECT_FINALIZE", event.eventType());
        Assertions.assertEquals(Instant.parse("2026-10-15T09:12:41.123Z"), event.eventTime());
        Assertions.assertEquals("1760519000000000", event.overwroteGeneration());
        Assertions.assertTrue(event.isFinalize());
        Assertions.assertEquals("vendor-sftp|to/org-xyz/file.csv|1760520000123456", event.uploadId());
    }

    @Test
    void missingGenerationIsEmpty() {
        Map<String, String> noGeneration = attributes();
        noGeneration.remove("objectGeneration");
        Assertions.assertTrue(ArrivalEvent.fromAttributes(noGeneration).isEmpty());

        Map<String, String> noBucket = attributes();
        noBucket.remove("bucketId");
        Assertions.assertTrue(ArrivalEvent.fromAttributes(noBucket).isEmpty());

        Map<String, String> noObject = attributes();
        noObject.remove("objectId");
        Assertions.assertTrue(ArrivalEvent.fromAttributes(noObject).isEmpty());
        Assertions.assertTrue(ArrivalEvent.fromAttributes(null).isEmpty());
    }

    @Test
    void unreadableIdentityIsEmpty() {
        for (String[] bad : new String[][] {
            {"objectId", " "},
            {"bucketId", "vendor sftp"},
            {"bucketId", "vendor|sftp"},
            {"objectGeneration", "abc"},
            {"objectGeneration", "0"},
            {"objectGeneration", "-5"},
            {"objectGeneration", "12 3"}
        }) {
            Map<String, String> attributes = attributes();
            attributes.put(bad[0], bad[1]);
            Assertions.assertTrue(ArrivalEvent.fromAttributes(attributes).isEmpty(), bad[0] + "=" + bad[1]);
        }
    }

    @Test
    void anObjectNameWithWhitespaceOrAPipeIsReadAndItsUploadIdEscaped() {
        for (String name :
                new String[] {"to/org-xyz/Copy of results.csv", "to/org|xyz/file.csv", "to/org\txyz/f.csv"}) {
            Map<String, String> attributes = attributes();
            attributes.put("objectId", name);
            ArrivalEvent event = ArrivalEvent.fromAttributes(attributes).orElseThrow();

            Assertions.assertEquals(name, event.objectId());
            Assertions.assertEquals(3, event.uploadId().split("\\|", -1).length, event.uploadId());
            Assertions.assertTrue(event.uploadId().codePoints().noneMatch(Character::isWhitespace), event.uploadId());
        }
    }

    @Test
    void optionalAttributesMayBeAbsentOrUnparseable() {
        Map<String, String> attributes = attributes();
        attributes.remove("overwroteGeneration");
        attributes.put("eventTime", "not a time");
        ArrivalEvent event = ArrivalEvent.fromAttributes(attributes).orElseThrow();

        Assertions.assertNull(event.overwroteGeneration());
        Assertions.assertNull(event.eventTime());
    }

    @Test
    void deleteEventIsNotFinalize() {
        Map<String, String> attributes = attributes();
        attributes.put("eventType", "OBJECT_DELETE");
        Assertions.assertFalse(
                ArrivalEvent.fromAttributes(attributes).orElseThrow().isFinalize());

        attributes.remove("eventType");
        Assertions.assertFalse(
                ArrivalEvent.fromAttributes(attributes).orElseThrow().isFinalize());
    }
}
