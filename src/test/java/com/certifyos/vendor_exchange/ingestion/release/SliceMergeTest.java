package com.certifyos.vendor_exchange.ingestion.release;

import com.certifyos.vendor_exchange.clients.OperationalValue;
import com.certifyos.vendor_exchange.clients.Slice;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Read our slice, merge, write it back whole (design D18): the seed, the overlay and this release. */
class SliceMergeTest {

    static final String SOURCE = "candor:org-xyz";

    static OperationalValue goldenPractitioner() {
        return new OperationalValue(
                "cert-000123",
                "org-xyz",
                Map.of("npi", "1003002890", "firstName", "Dana", "lastName", "Whitfield", "middleName", "Q"),
                List.of("slice-roster"),
                Map.of());
    }

    static OperationalValue goldenLocation() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("name", "Greenwood Village Clinic");
        data.put(
                "contacts",
                List.of(Map.of("type", "phone", "value", "3036790177"), Map.of("type", "fax", "value", "3036790178")));
        data.put("addresses", List.of(mailing(), CrosswalksTest.address()));
        return new OperationalValue("loc-7781", "org-xyz", data, List.of("slice-loc-roster"), Map.of());
    }

    static Map<String, Object> mailing() {
        Map<String, Object> mailing = new LinkedHashMap<>(CrosswalksTest.address());
        mailing.put("addressType", "mailing");
        mailing.put("addressLine1", "PO Box 9");
        return mailing;
    }

    @Test
    void practitionerSeedOverlayRelease() {
        Slice own = new Slice(
                "slice-candor",
                SOURCE,
                "1003002890",
                Map.of("npi", "1003002890", "firstName", "Dana", "lastName", "Whitfield-Lee"),
                Instant.parse("2026-09-20T10:00:00Z"));

        SliceMerge.PractitionerSlice merged =
                SliceMerge.practitioner(goldenPractitioner(), own, Map.of("first_name", "Danielle"));

        Assertions.assertEquals(
                Map.of("npi", "1003002890", "firstName", "Danielle", "lastName", "Whitfield-Lee"),
                merged.data(),
                "last month's lastName kept, this release's firstName lands, nothing else from the Golden record");
    }

    @Test
    void practitionerWithoutOwnSliceIsSeededFromTheGoldenRecord() {
        SliceMerge.PractitionerSlice merged =
                SliceMerge.practitioner(goldenPractitioner(), null, Map.of("last_name", "Smith"));

        Assertions.assertEquals(Map.of("npi", "1003002890", "firstName", "Dana", "lastName", "Smith"), merged.data());
    }

    @Test
    void practitionerSeedReadsSnakeCaseKeysAndKeepsTheNpi() {
        OperationalValue snake = new OperationalValue(
                "cert-1", "org-xyz", Map.of("npi", "1003002890", "first_name", "Dana", "last_name", "Lee"), null, null);
        Slice ownWithoutNpi = new Slice("s", SOURCE, "1003002890", Map.of("lastName", "Lee-Ray"), null);

        SliceMerge.PractitionerSlice merged = SliceMerge.practitioner(snake, ownWithoutNpi, Map.of());

        Assertions.assertEquals(Map.of("npi", "1003002890", "firstName", "Dana", "lastName", "Lee-Ray"), merged.data());
    }

    @Test
    void locationReplacesOneContactKeepsTheRest() {
        Map<String, Object> service =
                SliceMerge.serviceAddress(goldenLocation()).orElseThrow();

        SliceMerge.LocationSlices merged =
                SliceMerge.location(goldenLocation(), service, null, Map.of("phone", "3035550100"));

        Assertions.assertEquals("Greenwood Village Clinic", merged.location().get("name"));
        Assertions.assertEquals(
                List.of(Map.of("type", "phone", "value", "3035550100"), Map.of("type", "fax", "value", "3036790178")),
                merged.location().get("contacts"),
                "the Golden fax is kept, the phone is replaced");
        Assertions.assertEquals(CrosswalksTest.address(), merged.address(), "the Golden service address unchanged");
        Assertions.assertEquals("GREENWOOD_VILLAGE_CLINIC_" + CrosswalksTest.HASH, merged.locationCrosswalk());
        Assertions.assertEquals(CrosswalksTest.HASH, merged.addressCrosswalk());
    }

    @Test
    void locationOverlaysOwnSliceAndAppendsAMissingContact() {
        Slice own = new Slice(
                "slice-candor-loc",
                SOURCE,
                "GREENWOOD_VILLAGE_CLINIC_" + CrosswalksTest.HASH,
                Map.of(
                        "name",
                        "Greenwood Village Clinic",
                        "contacts",
                        List.of(Map.of("type", "phone", "value", "3031112222"))),
                null);

        SliceMerge.LocationSlices merged =
                SliceMerge.location(goldenLocation(), CrosswalksTest.address(), own, Map.of("fax", "3039998888"));

        Assertions.assertEquals(
                List.of(Map.of("type", "phone", "value", "3031112222"), Map.of("type", "fax", "value", "3039998888")),
                merged.location().get("contacts"),
                "own slice's earlier phone release kept; the fax entry it lacked is added");
    }

    @Test
    void contactKeyedByTypeIsReplacedInPlace() {
        OperationalValue keyed = new OperationalValue(
                "loc-1",
                "org-xyz",
                Map.of("name", "Clinic", "contacts", List.of(Map.of("phone", "3036790177", "label", "main"))),
                null,
                null);

        SliceMerge.LocationSlices merged =
                SliceMerge.location(keyed, CrosswalksTest.address(), null, Map.of("phone", "3035550100"));

        Assertions.assertEquals(
                List.of(Map.of("phone", "3035550100", "label", "main")),
                merged.location().get("contacts"));
    }

    @Test
    void serviceAddressShapes() {
        Assertions.assertEquals(
                CrosswalksTest.address(),
                SliceMerge.serviceAddress(goldenLocation()).orElseThrow());
        Map<String, Object> single = new LinkedHashMap<>(CrosswalksTest.address());
        single.remove("addressType");
        OperationalValue withServiceAddress = new OperationalValue(
                "loc-2", "org-xyz", Map.of("name", "Clinic", "serviceAddress", single), null, null);
        Assertions.assertEquals(
                CrosswalksTest.address(),
                SliceMerge.serviceAddress(withServiceAddress).orElseThrow(),
                "a lone service address gets its addressType, which the hash includes");
        OperationalValue none = new OperationalValue(
                "loc-3", "org-xyz", Map.of("name", "Clinic", "addresses", List.of(mailing())), null, null);
        Assertions.assertTrue(SliceMerge.serviceAddress(none).isEmpty());
    }

    @Test
    void fieldToDataPath() {
        Assertions.assertEquals(
                Map.of(
                        "first_name", "firstName",
                        "last_name", "lastName",
                        "phone", "contacts.phone",
                        "fax", "contacts.fax"),
                SliceMerge.fieldToDataPath());
    }
}
