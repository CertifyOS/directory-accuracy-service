package com.certifyos.vendor_exchange.ingestion.inbound;

import com.certifyos.vendor_exchange.ingestion.inbound.InboundNames.Parsed;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class InboundNamesTest {

    private static final String EXAMPLE = "to/org-xyz/org-xyz_org-xyz-candor-2026-10-001_CB-2026-10-001_20261015.csv";

    @Test
    void parsesTheDesignExample() {
        Parsed parsed = InboundNames.parse(EXAMPLE).orElseThrow();

        Assertions.assertEquals("org-xyz", parsed.folderTenant());
        Assertions.assertEquals("org-xyz", parsed.fileTenant());
        Assertions.assertEquals("org-xyz-candor-2026-10-001", parsed.exportBatchRef());
        Assertions.assertEquals("CB-2026-10-001", parsed.vendorBatchId());
        Assertions.assertEquals("20261015", parsed.fileDate());
        Assertions.assertEquals("org-xyz_org-xyz-candor-2026-10-001_CB-2026-10-001_20261015.csv", parsed.fileName());
    }

    @Test
    void extensionIsCaseInsensitiveAndTenantMismatchStillParses() {
        Parsed parsed = InboundNames.parse("to/a/b_ref_vb_20261015.CSV").orElseThrow();

        Assertions.assertEquals("a", parsed.folderTenant());
        Assertions.assertEquals("b", parsed.fileTenant());
        Assertions.assertEquals("b_ref_vb_20261015.CSV", parsed.fileName());
    }

    @Test
    void rejectsFromPrefixAndBadDate() {
        Assertions.assertTrue(
                InboundNames.parse(EXAMPLE.replace("to/", "from/")).isEmpty());
        Assertions.assertTrue(
                InboundNames.parse(EXAMPLE.replace("20261015", "20261315")).isEmpty());
        Assertions.assertTrue(
                InboundNames.parse(EXAMPLE.replace("20261015", "2026101")).isEmpty());
        Assertions.assertTrue(
                InboundNames.parse(EXAMPLE.replace(".csv", ".txt")).isEmpty());
        Assertions.assertTrue(InboundNames.parse(EXAMPLE.replace("to/org-xyz/", "to/org-xyz/sub/"))
                .isEmpty());
        Assertions.assertTrue(
                InboundNames.parse("to/org-xyz/org-xyz_ref_20261015.csv").isEmpty());
        Assertions.assertTrue(
                InboundNames.parse("to/org_xyz/org_xyz_ref_vb_20261015.csv").isEmpty());
        Assertions.assertTrue(InboundNames.parse("to//x_ref_vb_20261015.csv").isEmpty());
        Assertions.assertTrue(InboundNames.parse("").isEmpty());
        Assertions.assertTrue(InboundNames.parse(null).isEmpty());
    }

    @Test
    void archiveObjectShape() {
        Assertions.assertEquals(
                "org-xyz/org-xyz-candor-2026-10-001/1760520000123456/f.csv",
                InboundNames.archiveObject("org-xyz", "org-xyz-candor-2026-10-001", "1760520000123456", "f.csv"));
    }

    @Test
    void reasonCodesAreStable() {
        Assertions.assertEquals("to/", InboundNames.TO_PREFIX);
        Assertions.assertEquals("UNPARSEABLE_NAME", InboundNames.UNPARSEABLE_NAME);
        Assertions.assertEquals("UNKNOWN_BATCH", InboundNames.UNKNOWN_BATCH);
        Assertions.assertEquals("TENANT_MISMATCH", InboundNames.TENANT_MISMATCH);
        Assertions.assertEquals("BATCH_NOT_DELIVERED", InboundNames.BATCH_NOT_DELIVERED);
    }
}
