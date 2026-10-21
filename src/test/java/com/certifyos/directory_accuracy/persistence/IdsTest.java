package com.certifyos.directory_accuracy.persistence;

import java.time.YearMonth;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class IdsTest {

    @Test
    void batchIdHasTheDesignShape() {
        Assertions.assertEquals(
                "org-xyz-candor-2026-10-001", Ids.batchId("org-xyz", "candor", YearMonth.of(2026, 10), 1));
        Assertions.assertEquals(
                "org-xyz-candor-2026-10-002", Ids.batchId("org-xyz", "candor", YearMonth.of(2026, 10), 2));
    }

    @Test
    void tenantIdWithUnderscoreIsRefusedBecauseTheFilenameUsesUnderscores() {
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> Ids.batchId("org_xyz", "candor", YearMonth.of(2026, 10), 1));
        Assertions.assertThrows(IllegalArgumentException.class, () -> Ids.scheduleId("org_xyz", "candor"));
    }

    @Test
    void sequenceOutsideOneToNineNinetyNineIsRefused() {
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> Ids.batchId("org-xyz", "candor", YearMonth.of(2026, 10), 0));
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> Ids.batchId("org-xyz", "candor", YearMonth.of(2026, 10), 1000));
    }

    @Test
    void scheduleAndNpiIdsUseThePipeSeparator() {
        Assertions.assertEquals("org-xyz|candor", Ids.scheduleId("org-xyz", "candor"));
        Assertions.assertEquals(
                "org-xyz-candor-2026-10-001|1234567893", Ids.npiId("org-xyz-candor-2026-10-001", "1234567893"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> Ids.npiId("b", "12345"));
    }

    @Test
    void correlationIdRoundTripsToTheBatchId() {
        String correlation = Ids.correlationId("org-xyz-candor-2026-10-001", 2);
        Assertions.assertEquals("org-xyz-candor-2026-10-001-r2", correlation);
        Assertions.assertEquals("org-xyz-candor-2026-10-001", Ids.batchIdOf(correlation));
        Assertions.assertThrows(IllegalArgumentException.class, () -> Ids.correlationId("x", 0));
        Assertions.assertThrows(IllegalArgumentException.class, () -> Ids.batchIdOf("no-suffix"));
    }

    @Test
    void eventIdsAreVersionSevenPrefixedAndSortInCreationOrder() {
        List<String> ids = new ArrayList<>();
        for (int count = 0; count < 1000; count++) {
            ids.add(Ids.eventId());
        }
        List<String> sorted = new ArrayList<>(ids);
        sorted.sort(null);
        Assertions.assertEquals(ids, sorted, "v7 ids must sort in creation order");
        Assertions.assertEquals(1000, ids.stream().distinct().count());
        for (String id : ids) {
            Assertions.assertTrue(id.startsWith("ev-"));
            Assertions.assertEquals(7, UUID.fromString(id.substring(3)).version());
        }
    }

    @Test
    void fieldRecommendationIdJoinsTheLegsWithPipes() {
        Assertions.assertEquals(
                "org-xyz|candor|org-xyz-candor-2026-10-001|cert-000123|loc-7781|phone",
                Ids.fieldRecommendationId(
                        "org-xyz", "candor", "org-xyz-candor-2026-10-001", "cert-000123", "loc-7781", "phone"));
    }

    @Test
    void fieldRecommendationIdUsesADashForAPractitionerLevelField() {
        Assertions.assertEquals(
                "org-xyz|candor|org-xyz-candor-2026-10-001|cert-000123|-|npi",
                Ids.fieldRecommendationId(
                        "org-xyz", "candor", "org-xyz-candor-2026-10-001", "cert-000123", null, "npi"));
    }

    @Test
    void uploadIdAndExternalRefUseThePipeSeparator() {
        Assertions.assertEquals(
                "bkt|dir/file.csv|1760520000123456", Ids.uploadId("bkt", "dir/file.csv", "1760520000123456"));
        Assertions.assertEquals("b|1760520000123456", Ids.externalRef("b", "1760520000123456"));
    }

    @Test
    void uploadIdEscapesWhitespacePipeAndPercentInTheObjectName() {
        Assertions.assertEquals(
                "bkt|to/org-xyz/Copy%20of%20results.csv|1", Ids.uploadId("bkt", "to/org-xyz/Copy of results.csv", "1"));
        Assertions.assertEquals("bkt|a%7Cb%25c%09d%C2%A0e|1", Ids.uploadId("bkt", "a|b%c\td\u00a0e", "1"));
        Assertions.assertNotEquals(Ids.uploadId("bkt", "a b", "1"), Ids.uploadId("bkt", "a%20b", "1"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> Ids.uploadId("bkt", " ", "1"));
        for (String name : new String[] {"plain/name.csv", "Copy of results.csv", "a|b%c\td\u00a0e", "tést 😀.csv"}) {
            String leg = Ids.uploadId("bkt", name, "1").split("\\|", -1)[1];
            Assertions.assertEquals(name, Ids.objectNameOf(leg), "the escape is undone exactly");
        }
    }

    @Test
    void rejectedValueIdEndsWithTheSha256OfTheNormalisedValue() throws Exception {
        String expected = java.util.HexFormat.of()
                .formatHex(java.security.MessageDigest.getInstance("SHA-256")
                        .digest("3036790177".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        String id = Ids.rejectedValueId("org-xyz", "cert-000123", "loc-7781", "phone", "3036790177");
        Assertions.assertEquals("org-xyz|cert-000123|loc-7781|phone|" + expected, id);
        Assertions.assertTrue(id.matches(".*\\|[0-9a-f]{64}$"));
        Assertions.assertEquals(
                "org-xyz|cert-000123|-|phone|" + expected,
                Ids.rejectedValueId("org-xyz", "cert-000123", null, "phone", "3036790177"));
    }

    @Test
    void rejectedValueMayContainAPipeBecauseItIsHashed() throws Exception {
        String value = "5351 S ROSLYN ST STE 200|GREENWOOD VILLAGE|CO|80111";
        String expected = java.util.HexFormat.of()
                .formatHex(java.security.MessageDigest.getInstance("SHA-256")
                        .digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        Assertions.assertEquals(
                "org-xyz|cert-000123|loc-7781|location|" + expected,
                Ids.rejectedValueId("org-xyz", "cert-000123", "loc-7781", "location", value));
    }

    @Test
    void releaseIdIsVersionSevenPrefixed() {
        String id = Ids.releaseId();
        Assertions.assertTrue(id.startsWith("rel-"));
        Assertions.assertEquals(7, UUID.fromString(id.substring(4)).version());
        Assertions.assertNotEquals(id, Ids.releaseId());
    }

    @Test
    void aLegWithAPipeOrNothingInItIsRefused() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> Ids.uploadId("b|k", "o", "1"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> Ids.uploadId("b", " ", "1"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> Ids.externalRef(null, "1"));
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> Ids.fieldRecommendationId("t", "s", "b", "p|q", null, "phone"));
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> Ids.fieldRecommendationId("t", "s", "b", "p", "", "phone"));
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> Ids.rejectedValueId("t|x", "p", null, "phone", "value"));
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> Ids.rejectedValueId("t", "p", null, "phone", " "));
    }

    @Test
    void aLegWithWhitespaceIsRefusedSoTheAuditLineKeepsItsKeyValueGrammar() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> Ids.uploadId("b c", "o", "1"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> Ids.uploadId("b", "o", "1\n"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> Ids.externalRef("b\tx", "1"));
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> Ids.fieldRecommendationId("t", "s", "b", "p", "l 1", "phone"));
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> Ids.rejectedValueId("t", "p\u00a0q", null, "phone", "value"));
        Assertions.assertTrue(
                Ids.rejectedValueId("t", "p", null, "phone", "a value with spaces")
                        .startsWith("t|p|-|phone|"),
                "the hashed value may contain whitespace");
    }

    @Test
    void aLocationLegOfALiteralDashIsRefusedBecauseDashMeansNoLocation() {
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> Ids.fieldRecommendationId("t", "s", "b", "p", "-", "phone"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> Ids.rejectedValueId("t", "p", "-", "phone", "v"));
    }
}
