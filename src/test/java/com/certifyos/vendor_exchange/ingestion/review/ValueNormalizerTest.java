package com.certifyos.vendor_exchange.ingestion.review;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** The design's version-1 normalisation table, one case per rule. */
class ValueNormalizerTest {

    @Test
    void versionIsOne() {
        Assertions.assertEquals(1, ValueNormalizer.VERSION);
    }

    @Test
    void phonesAndFaxesAreDigitsWithoutTheLeadingOne() {
        Assertions.assertEquals("3036790177", ValueNormalizer.normalize("phone", "(303) 679-0177"));
        Assertions.assertEquals("3036790177", ValueNormalizer.normalize("phone", "1-303-679-0177"));
        Assertions.assertEquals("3036790177", ValueNormalizer.normalize("fax", "+1 303.679.0177"));
        Assertions.assertEquals("3036790177", ValueNormalizer.normalize("fax", "13036790177"));
        Assertions.assertEquals("113036790177", ValueNormalizer.normalize("fax", "113036790177"), "only at 11 digits");
        Assertions.assertEquals(
                "23036790177", ValueNormalizer.normalize("phone", "2-303-679-0177"), "only a leading 1 is dropped");
        Assertions.assertEquals("6790177", ValueNormalizer.normalize("phone", "679-0177"));
    }

    @Test
    void namesAreTrimmedUpperCasedWithoutPunctuationAndSingleSpaced() {
        Assertions.assertEquals(
                "ST JOHNS CLINIC", ValueNormalizer.normalize("practice_name", "  St. John's   Clinic "));
        Assertions.assertEquals("OBRIEN", ValueNormalizer.normalize("last_name", "O'Brien"));
        Assertions.assertEquals("MARY ANN", ValueNormalizer.normalize("first_name", "mary  ann,"));
    }

    @Test
    void specialtiesAreSortedUpperCasedAndJoinedWithSemicolons() {
        Assertions.assertEquals(
                "CARDIOLOGY;INTERNAL MEDICINE",
                ValueNormalizer.normalize("specialties", "Cardiology; internal medicine"));
        Assertions.assertEquals(
                "CARDIOLOGY;INTERNAL MEDICINE",
                ValueNormalizer.normalize("specialties", "internal  medicine, cardiology;; Cardiology"));
    }

    @Test
    void flagsAreTrueOrFalse() {
        Assertions.assertEquals("true", ValueNormalizer.normalize("accepting_new_patients", "Y"));
        Assertions.assertEquals("true", ValueNormalizer.normalize("accepting_new_patients", " yes "));
        Assertions.assertEquals("true", ValueNormalizer.normalize("pcp_at_location", "TRUE"));
        Assertions.assertEquals("true", ValueNormalizer.normalize("pcp_at_location", "1"));
        Assertions.assertEquals("false", ValueNormalizer.normalize("pcp_at_location", "N"));
        Assertions.assertEquals("false", ValueNormalizer.normalize("accepting_new_patients", "no"));
    }

    @Test
    void locationLegsDropTheSecondLineAndFoldStreetWords() {
        Assertions.assertEquals(
                "5351 S ROSLYN ST STE 200|GREENWOOD VILLAGE|CO|80111",
                ValueNormalizer.normalize("location", "5351 S. Roslyn St, Suite 200||Greenwood Village|co|80111-1234"));
        Assertions.assertEquals(
                "12 MAIN ST|DENVER|CO|80202",
                ValueNormalizer.normalize("location", "12 Main Street|Floor 3|Denver| CO |80202"));
        Assertions.assertEquals(
                "9 PARK AVE STE 4|NEW YORK|NY|10001",
                ValueNormalizer.normalize("location", "9 Park Avenue Suite 4|New York|ny|10001"),
                "four legs read as line1, city, state, zip");
    }

    /** The vendor's location value is one text cell, not the five legs we sent; it reads as one line. */
    @Test
    void locationFreeTextIsOneNormalisedLine() {
        Assertions.assertEquals(
                "5351 S ROSLYN ST STE 200 GREENWOOD VILLAGE CO 80111",
                ValueNormalizer.normalize("location", "5351 S. Roslyn Street, Suite 200, Greenwood Village, CO 80111"));
    }

    @Test
    void npiIsDigits() {
        Assertions.assertEquals("1003002890", ValueNormalizer.normalize("npi", " 1003-002-890 "));
    }

    @Test
    void nullOrBlankIsEmpty() {
        Assertions.assertEquals("", ValueNormalizer.normalize("phone", null));
        Assertions.assertEquals("", ValueNormalizer.normalize("location", "  "));
        Assertions.assertEquals("", ValueNormalizer.normalize("specialties", " ; , "));
        Assertions.assertEquals("", ValueNormalizer.normalize("phone", "n/a"), "no digits is no value");
    }

    @Test
    void unknownFieldIsRefused() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> ValueNormalizer.normalize("nickname", "x"));
    }
}
