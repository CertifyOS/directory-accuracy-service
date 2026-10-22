package com.certifyos.directory_accuracy.clients;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class EgressFailureReasonTest {

    @Test
    void readsABareCode() {
        Assertions.assertEquals("NO_MATCHING_PRACTITIONERS", EgressFailureReason.code("NO_MATCHING_PRACTITIONERS"));
    }

    @Test
    void dropsTheDetail() {
        Assertions.assertEquals(
                "REUSE_SOURCE_UNUSABLE", EgressFailureReason.code("REUSE_SOURCE_UNUSABLE (source list is empty)"));
    }

    @Test
    void missingTextHasNoCode() {
        Assertions.assertNull(EgressFailureReason.code(null));
        Assertions.assertNull(EgressFailureReason.code("  "));
    }

    @Test
    void matchesOnTheCodeOnly() {
        Assertions.assertTrue(
                EgressFailureReason.is("NO_MATCHING_PRACTITIONERS", EgressFailureReason.NO_MATCHING_PRACTITIONERS));
        Assertions.assertTrue(EgressFailureReason.is(
                "REUSE_SOURCE_UNUSABLE (source selection no longer exists)",
                EgressFailureReason.REUSE_SOURCE_UNUSABLE));
        Assertions.assertFalse(EgressFailureReason.is(
                "NO_MATCHING_PRACTITIONERS_LATER", EgressFailureReason.NO_MATCHING_PRACTITIONERS));
        Assertions.assertFalse(EgressFailureReason.is(null, EgressFailureReason.NO_MATCHING_PRACTITIONERS));
    }
}
