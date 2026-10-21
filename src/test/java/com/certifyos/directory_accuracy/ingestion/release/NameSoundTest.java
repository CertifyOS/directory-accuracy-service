package com.certifyos.directory_accuracy.ingestion.release;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** The name-sound guard: the Soundex practitioner matching joins on (design, ask 13). */
class NameSoundTest {

    @Test
    void bobAndRobertDiffer() {
        Assertions.assertFalse(NameSound.same("Bob", "Robert"));
    }

    @Test
    void jonAndJohnAreTheSame() {
        Assertions.assertTrue(NameSound.same("Jon", "John"));
        Assertions.assertTrue(NameSound.same("Smith", "Smyth"));
        Assertions.assertTrue(NameSound.same("danielle", "DANIEL"), "case does not matter");
        Assertions.assertTrue(NameSound.same("O'Brien", "OBrien"), "letters only");
    }

    @Test
    void blankCases() {
        Assertions.assertTrue(NameSound.same(null, " "), "both blank are the same");
        Assertions.assertTrue(NameSound.same("", "-"), "no letters reads as blank");
        Assertions.assertFalse(NameSound.same("Dana", ""));
        Assertions.assertFalse(NameSound.same(null, "Dana"));
    }
}
