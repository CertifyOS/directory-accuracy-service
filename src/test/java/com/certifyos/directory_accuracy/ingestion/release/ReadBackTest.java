package com.certifyos.directory_accuracy.ingestion.release;

import com.certifyos.directory_accuracy.clients.OperationalValue;
import com.certifyos.directory_accuracy.ingestion.release.ReadBack.Outcome;
import com.certifyos.directory_accuracy.ingestion.release.ReadBack.Verdict;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Read-back verdicts from the operational value's contribution map (plan P9). */
class ReadBackTest {

    static final String SOURCE = "candor:org-xyz";
    static final String OWN = "slice-candor";

    static Map<String, String> fields(String... pairs) {
        Map<String, String> fields = new LinkedHashMap<>();
        for (int index = 0; index < pairs.length; index += 2) {
            fields.put(pairs[index], pairs[index + 1]);
        }
        return fields;
    }

    @Test
    void wonOutrankedNotAppliedUnknown() {
        OperationalValue ov = new OperationalValue(
                "cert-1",
                "org-xyz",
                Map.of(),
                List.of("slice-roster", OWN),
                Map.of("firstName", SOURCE, "data.lastName", "roster:org-xyz"));

        Map<String, Verdict> verdicts =
                ReadBack.evaluate(ov, OWN, SOURCE, fields("fr-1", "first_name", "fr-2", "last_name", "fr-3", "phone"));

        Assertions.assertEquals(new Verdict(Outcome.WON, SOURCE), verdicts.get("fr-1"));
        Assertions.assertEquals(new Verdict(Outcome.OUTRANKED, "roster:org-xyz"), verdicts.get("fr-2"));
        Assertions.assertEquals(new Verdict(Outcome.UNKNOWN, null), verdicts.get("fr-3"), "no key for the path");

        OperationalValue without = new OperationalValue(
                "cert-1", "org-xyz", Map.of(), List.of("slice-roster"), Map.of("firstName", "roster:org-xyz"));
        Assertions.assertEquals(
                Map.of("fr-1", new Verdict(Outcome.NOT_APPLIED, null), "fr-2", new Verdict(Outcome.NOT_APPLIED, null)),
                ReadBack.evaluate(without, OWN, SOURCE, fields("fr-1", "first_name", "fr-2", "last_name")),
                "our slice absent from contributingSlices: every field NOT_APPLIED");
        Assertions.assertEquals(
                Map.of("fr-1", new Verdict(Outcome.NOT_APPLIED, null)),
                ReadBack.evaluate(ov, null, SOURCE, fields("fr-1", "first_name")),
                "no slice id of ours: NOT_APPLIED");
    }

    @Test
    void keyShapesAndValueShapes() {
        OperationalValue ov = new OperationalValue(
                "loc-1",
                "org-xyz",
                Map.of(),
                List.of(OWN),
                Map.of(
                        "first_name",
                        SOURCE,
                        "data.contacts.phone",
                        Map.of("sourceType", "roster:org-xyz", "priority", 3),
                        "contacts",
                        SOURCE,
                        "lastName",
                        42));

        Map<String, Verdict> verdicts = ReadBack.evaluate(
                ov, OWN, SOURCE, fields("a", "first_name", "b", "phone", "c", "fax", "d", "last_name", "e", "npi"));

        Assertions.assertEquals(new Verdict(Outcome.WON, SOURCE), verdicts.get("a"), "snake-case key");
        Assertions.assertEquals(
                new Verdict(Outcome.OUTRANKED, "roster:org-xyz"), verdicts.get("b"), "object value with sourceType");
        Assertions.assertEquals(new Verdict(Outcome.WON, SOURCE), verdicts.get("c"), "the whole contacts array");
        Assertions.assertEquals(new Verdict(Outcome.UNKNOWN, null), verdicts.get("d"), "a value we cannot read");
        Assertions.assertEquals(new Verdict(Outcome.UNKNOWN, null), verdicts.get("e"), "not a slice field");
    }

    @Test
    void cleansedSliceOfOursCountsAsWon() {
        OperationalValue ov = new OperationalValue(
                "cert-1", "org-xyz", Map.of(), List.of(OWN), Map.of("firstName", "certify-cleanser:" + SOURCE));

        Assertions.assertEquals(
                new Verdict(Outcome.WON, "certify-cleanser:" + SOURCE),
                ReadBack.evaluate(ov, OWN, SOURCE, fields("fr-1", "first_name")).get("fr-1"));
    }
}
