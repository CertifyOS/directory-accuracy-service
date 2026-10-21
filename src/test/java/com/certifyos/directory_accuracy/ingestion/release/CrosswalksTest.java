package com.certifyos.directory_accuracy.ingestion.release;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The crosswalk recipes against the api-layer's own. The expected hash was computed once from verbatim
 * copies of the api-layer's {@code LocationHashService.createAddressHash} and {@code normalizeString}
 * and {@code EntityCrosswalkGenerator.formatLocationCrosswalkId} (api-layer {@code main}, 2026-10-19),
 * and agrees with {@code shasum -a 256} over {@code 5351_S_ROSLYN_ST|SUITE_200|GREENWOOD_VILLAGE|CO|80111|SERVICE|}.
 */
class CrosswalksTest {

    static final String HASH = "afc25220a8c9a4431425f9de2eeb87d9be1f14f8453a66847f7a9ff5973670b9";

    static Map<String, Object> address() {
        Map<String, Object> address = new LinkedHashMap<>();
        address.put("addressLine1", "5351 S. Roslyn St");
        address.put("addressLine2", "Suite 200");
        address.put("city", "Greenwood Village");
        address.put("state", "CO");
        address.put("zip", "80111");
        address.put("addressType", "service");
        return address;
    }

    @Test
    void recipesMatchTheApiLayer() {
        Assertions.assertEquals("candor:org-xyz", Crosswalks.sourceType("org-xyz"));
        Assertions.assertEquals("1003002890", Crosswalks.practitioner("1003002890"));
        Assertions.assertEquals("5351_S_ROSLYN_ST_SUITE_200", Crosswalks.normalize("5351 S. Roslyn St, Suite 200"));
        Assertions.assertEquals("HELLO_WORLD", Crosswalks.normalize("___hello   world___"));
        Assertions.assertNull(Crosswalks.normalize(null));
        Assertions.assertEquals(HASH, Crosswalks.addressHash(address()));
        Assertions.assertEquals(
                "GREENWOOD_VILLAGE_CLINIC_" + HASH, Crosswalks.location("Greenwood Village Clinic", address()));
        Assertions.assertEquals(
                "GREENWOOD_VILLAGE_CLINIC_no_address", Crosswalks.location("Greenwood Village Clinic", null));
        Assertions.assertEquals("UNKNOWNLOCATION_" + HASH, Crosswalks.location(" ", address()));
    }

    @Test
    void blankLegsAreLeftOutOfTheHash() {
        Map<String, Object> withBlanks = new HashMap<>(address());
        withBlanks.put("addressLine2", "  ");
        Map<String, Object> without = new HashMap<>(address());
        without.remove("addressLine2");
        withBlanks.put("county", "Arapahoe");

        Assertions.assertEquals(Crosswalks.addressHash(without), Crosswalks.addressHash(withBlanks));
        Assertions.assertNotEquals(HASH, Crosswalks.addressHash(without));
        Map<String, Object> empty = new HashMap<>();
        empty.put("city", "");
        Assertions.assertNull(Crosswalks.addressHash(empty), "no non-blank leg is no hash");
        Assertions.assertNull(Crosswalks.addressHash(null));
    }

    @Test
    void practitionerNeedsAnNpi() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> Crosswalks.practitioner(" "));
        Assertions.assertThrows(IllegalArgumentException.class, () -> Crosswalks.practitioner(null));
    }
}
