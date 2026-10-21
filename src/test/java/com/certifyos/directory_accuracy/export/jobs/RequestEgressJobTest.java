package com.certifyos.directory_accuracy.export.jobs;

import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RequestEgressJobTest {

    @Test
    void rowExpansionKeysTravelAsJsonArrayText() {
        // Egress declares rowExpansionKeys as a String on the wire (finding 57a).
        Assertions.assertEquals("[\"locations\"]", RequestEgressJob.jsonArray(List.of("locations")));
        Assertions.assertEquals("[]", RequestEgressJob.jsonArray(List.of()));
        Assertions.assertEquals("[\"a\",\"b\"]", RequestEgressJob.jsonArray(List.of("a", "b")));
    }
}
