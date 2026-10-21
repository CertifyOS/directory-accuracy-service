package com.certifyos.directory_accuracy.ingestion.staging;

import java.util.List;

/**
 * The column names of the {@code candor-recs-v1} mapping, the vendor's wide row as file-ingestion
 * parses it. The mapping itself, the body of {@code POST /v1/mappings}, is versioned beside the code
 * in {@code src/main/resources/mappings/candor-recs-v1.json}. Each of the ten fields has the value we
 * sent and five suffixed vendor columns ({@link Suffix}); the sent value of {@code location} is the
 * five address legs, and that of {@code npi} is the {@code npi} id column. The design quotes 59
 * columns for the September 2026 sample, while the mapping lists 67; the exact column set is
 * reconciled against the vendor's file when the mapping is registered with file-ingestion (rollout
 * ask 7).
 */
public final class CandorRecsMapping {

    public static final String MAPPING_NAME = "candor-recs-v1";
    public static final String PRACTITIONER_ID = "certify_practitioner_id";
    public static final String NPI = "npi";
    public static final String LOCATION_ID = "certify_location_id";
    public static final String ENTITY_TYPE = "entity_type";
    public static final String ENTITY_PRACTITIONER = "PRACTITIONER";

    /** Fields written once per practitioner, with no location. */
    public static final List<String> PRACTITIONER_FIELDS = List.of("npi", "first_name", "last_name", "specialties");

    /** Fields written once per location we sent. */
    public static final List<String> LOCATION_FIELDS =
            List.of("location", "practice_name", "phone", "fax", "accepting_new_patients", "pcp_at_location");

    /** The sent columns that make up the sent value of {@code location}, in order. */
    public static final List<String> LOCATION_LEGS = List.of("address_line1", "address_line2", "city", "state", "zip");

    /** The vendor's five columns per field, {@code <field><suffix>}. */
    public enum Suffix {
        STATUS("_verification_status"),
        VALUE("_candor_value"),
        REASON("_verification_reason"),
        CONFIDENCE("_verification_confidence"),
        EVIDENCE("_verification_evidence");

        private final String text;

        Suffix(String text) {
            this.text = text;
        }

        /**
         * The suffix as it appears in the column name.
         *
         * @return for example {@code _verification_status}
         */
        public String text() {
            return text;
        }
    }

    private CandorRecsMapping() {}

    /**
     * The level of a field.
     *
     * @param field one of the ten field names
     * @return its level
     * @throws IllegalArgumentException for a name that is not one of the ten
     */
    public static FieldLevel levelOf(String field) {
        if (field == null) {
            throw new IllegalArgumentException("field must not be null");
        }
        if (PRACTITIONER_FIELDS.contains(field)) {
            return FieldLevel.PRACTITIONER;
        }
        if (LOCATION_FIELDS.contains(field)) {
            return FieldLevel.LOCATION;
        }
        throw new IllegalArgumentException("not a " + MAPPING_NAME + " field: " + field);
    }

    /**
     * A vendor column of a field.
     *
     * @param field one of the ten field names
     * @param suffix which of the five vendor columns
     * @return {@code <field><suffix>}, for example {@code phone_verification_status}
     * @throws IllegalArgumentException for a name that is not one of the ten
     */
    public static String column(String field, Suffix suffix) {
        levelOf(field);
        return field + suffix.text();
    }
}
