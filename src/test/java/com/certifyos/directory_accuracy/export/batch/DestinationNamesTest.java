package com.certifyos.directory_accuracy.export.batch;

import java.time.LocalDate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DestinationNamesTest {

    @Test
    void namesFollowTheOutboundFileContract() {
        String file = DestinationNames.fileName("org-xyz", "org-xyz-candor-2026-10-001", LocalDate.of(2026, 10, 1));
        Assertions.assertEquals("org-xyz_org-xyz-candor-2026-10-001_20261001.csv", file);
        String object = DestinationNames.objectName("org-xyz", file);
        Assertions.assertEquals("from/org-xyz/org-xyz_org-xyz-candor-2026-10-001_20261001.csv", object);
        Assertions.assertEquals(
                "gs://vendor-sftp/from/org-xyz/org-xyz_org-xyz-candor-2026-10-001_20261001.csv",
                DestinationNames.gsPath("vendor-sftp", object));
    }
}
