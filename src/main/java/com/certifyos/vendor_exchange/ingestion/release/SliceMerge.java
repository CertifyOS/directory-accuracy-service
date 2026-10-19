package com.certifyos.vendor_exchange.ingestion.release;

import com.certifyos.vendor_exchange.clients.OperationalValue;
import com.certifyos.vendor_exchange.clients.Slice;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Read our own slice, merge, write it back whole (design D18): DAL {@code UPSERT} replaces a slice's
 * {@code data}, so writing only this month's field would erase last month's. Each merge seeds from
 * the Golden record, overlays our slice as read, then overlays this release.
 *
 * <p>Provisional until Gate 7's live reads (plan P9, ask 17): the Golden data is read under
 * camel-case keys with a snake-case fallback; a contact entry is {@code {type, value}} or keyed by
 * the type ({@code {phone: ...}}); the Golden service address is the {@code addresses[]} entry with
 * {@code addressType = service}, or a {@code serviceAddress} object.
 */
public final class SliceMerge {

    static final String NPI = "npi";
    static final String FIRST_NAME = "firstName";
    static final String LAST_NAME = "lastName";
    static final String NAME = "name";
    static final String CONTACTS = "contacts";
    static final String TYPE = "type";
    static final String VALUE = "value";
    static final String ADDRESS_TYPE = "addressType";
    static final String SERVICE = "service";

    private static final Map<String, String> FIELD_TO_DATA_PATH = Map.of(
            "first_name", FIRST_NAME, "last_name", LAST_NAME, "phone", CONTACTS + ".phone", "fax", CONTACTS + ".fax");

    private static final Map<String, String> PRACTITIONER_KEYS =
            Map.of("first_name", FIRST_NAME, "last_name", LAST_NAME);

    private SliceMerge() {}

    /**
     * The practitioner slice to write.
     *
     * @param data the slice's {@code data}: {@code npi}, {@code firstName}, {@code lastName} always
     */
    public record PractitionerSlice(Map<String, Object> data) {
        public PractitionerSlice {
            data = Collections.unmodifiableMap(new LinkedHashMap<>(data));
        }
    }

    /**
     * The location and address slices to write.
     *
     * @param location the location slice's {@code data}
     * @param address the address slice's {@code data}: the Golden service address unchanged
     * @param locationCrosswalk {@link Crosswalks#location} of the Golden name and service address
     * @param addressCrosswalk {@link Crosswalks#addressHash} of the service address
     */
    public record LocationSlices(
            Map<String, Object> location,
            Map<String, Object> address,
            String locationCrosswalk,
            String addressCrosswalk) {
        public LocationSlices {
            location = Collections.unmodifiableMap(new LinkedHashMap<>(location));
            address = Collections.unmodifiableMap(new LinkedHashMap<>(address));
        }
    }

    /**
     * Merges a practitioner slice: seed {@code {npi, firstName, lastName}} from the Golden record,
     * overlay our slice's data, overlay the released values. The NPI is kept when our slice lacks it,
     * because matching joins by {@code data.npi}.
     *
     * @param golden the practitioner's operational value
     * @param own our slice as read, or null when there is none yet
     * @param fieldValues released field to value ({@code first_name}, {@code last_name})
     * @return the slice data
     */
    public static PractitionerSlice practitioner(OperationalValue golden, Slice own, Map<String, String> fieldValues) {
        Map<String, Object> data = new LinkedHashMap<>();
        String npi = text(golden.data(), NPI, NPI);
        data.put(NPI, npi);
        data.put(FIRST_NAME, text(golden.data(), FIRST_NAME, "first_name"));
        data.put(LAST_NAME, text(golden.data(), LAST_NAME, "last_name"));
        if (own != null) {
            data.putAll(own.data());
        }
        if (data.get(NPI) == null) {
            data.put(NPI, npi);
        }
        fieldValues.forEach((field, value) -> {
            String key = PRACTITIONER_KEYS.get(field);
            if (key != null) {
                data.put(key, value);
            }
        });
        return new PractitionerSlice(data);
    }

    /**
     * Merges the location slices: seed {@code {name, contacts}} from the Golden location, overlay our
     * slice's data, replace (or add) the contact entry of each released field; the address slice is the
     * Golden service address unchanged, so the slices join the Golden location by name and address.
     *
     * @param goldenLocation the location's operational value
     * @param goldenServiceAddress the Golden service address, from {@link #serviceAddress}
     * @param ownLocation our location slice as read, or null when there is none yet
     * @param fieldValues released field to value ({@code phone}, {@code fax})
     * @return the slices and their crosswalk ids
     */
    public static LocationSlices location(
            OperationalValue goldenLocation,
            Map<String, Object> goldenServiceAddress,
            Slice ownLocation,
            Map<String, String> fieldValues) {
        String name = text(goldenLocation.data(), NAME, NAME);
        Map<String, Object> data = new LinkedHashMap<>();
        data.put(NAME, name);
        data.put(CONTACTS, goldenLocation.data().get(CONTACTS));
        if (ownLocation != null) {
            data.putAll(ownLocation.data());
        }
        List<Object> contacts = contactsOf(data.get(CONTACTS));
        fieldValues.forEach((field, value) -> {
            if (FIELD_TO_DATA_PATH.getOrDefault(field, "").startsWith(CONTACTS + ".")) {
                replaceContact(contacts, field, value);
            }
        });
        data.put(CONTACTS, contacts);
        return new LocationSlices(
                data,
                goldenServiceAddress,
                Crosswalks.location(name, goldenServiceAddress),
                Crosswalks.addressHash(goldenServiceAddress));
    }

    /**
     * The Golden location's service address (provisional shape, see the class comment).
     *
     * @param goldenLocation the location's operational value
     * @return the address legs, with {@code addressType} set, or empty when the location has none
     */
    public static Optional<Map<String, Object>> serviceAddress(OperationalValue goldenLocation) {
        Object addresses = goldenLocation.data().get("addresses");
        if (addresses instanceof List<?> list) {
            for (Object item : list) {
                if (item instanceof Map<?, ?> address
                        && SERVICE.equalsIgnoreCase(String.valueOf(address.get(ADDRESS_TYPE)))) {
                    return Optional.of(copy(address));
                }
            }
        }
        if (goldenLocation.data().get("serviceAddress") instanceof Map<?, ?> address) {
            Map<String, Object> legs = copy(address);
            legs.putIfAbsent(ADDRESS_TYPE, SERVICE);
            return Optional.of(legs);
        }
        return Optional.empty();
    }

    /**
     * Where each slice field lives in the slice's {@code data}, the path the read-back looks up.
     *
     * @return field to data path
     */
    public static Map<String, String> fieldToDataPath() {
        return FIELD_TO_DATA_PATH;
    }

    private static List<Object> contactsOf(Object value) {
        List<Object> contacts = new ArrayList<>();
        if (value instanceof List<?> list) {
            for (Object item : list) {
                contacts.add(item instanceof Map<?, ?> entry ? copy(entry) : item);
            }
        }
        return contacts;
    }

    private static void replaceContact(List<Object> contacts, String type, String value) {
        for (int index = 0; index < contacts.size(); index++) {
            if (contacts.get(index) instanceof Map<?, ?> entry) {
                Map<String, Object> replaced = copy(entry);
                if (entry.containsKey(type)) {
                    replaced.put(type, value);
                    contacts.set(index, replaced);
                    return;
                }
                if (type.equalsIgnoreCase(String.valueOf(entry.get(TYPE)))) {
                    replaced.put(VALUE, value);
                    contacts.set(index, replaced);
                    return;
                }
            }
        }
        Map<String, Object> added = new LinkedHashMap<>();
        added.put(TYPE, type);
        added.put(VALUE, value);
        contacts.add(added);
    }

    private static Map<String, Object> copy(Map<?, ?> map) {
        Map<String, Object> copy = new LinkedHashMap<>();
        map.forEach((key, value) -> copy.put(String.valueOf(key), value));
        return copy;
    }

    /** A Golden value read under its camel-case key, else its snake-case one. */
    static String text(Map<String, Object> data, String key, String fallback) {
        Object value = data.get(key);
        if (value == null) {
            value = data.get(fallback);
        }
        return value == null ? null : value.toString();
    }
}
