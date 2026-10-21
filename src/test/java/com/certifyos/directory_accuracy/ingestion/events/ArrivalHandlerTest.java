package com.certifyos.directory_accuracy.ingestion.events;

import com.certifyos.directory_accuracy.config.VendorExchangeConfig;
import java.time.Clock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.Mockito;

class ArrivalHandlerTest {

    @Test
    void refusesACompletenessSignalOtherThanDataFile() {
        VendorExchangeConfig cfg = Mockito.mock(VendorExchangeConfig.class, Answers.RETURNS_DEEP_STUBS);
        Mockito.when(cfg.ingestion().completenessSignal()).thenReturn("MANIFEST");

        IllegalStateException refused = Assertions.assertThrows(
                IllegalStateException.class, () -> new ArrivalHandler(null, null, null, null, cfg, Clock.systemUTC()));

        Assertions.assertTrue(refused.getMessage().contains("MANIFEST"));
    }

    @Test
    void folderTenantIsTheSecondSegmentWhenItObeysTheTenantRules() {
        Assertions.assertEquals("org-a", ArrivalHandler.folderTenant("to/org-a/notes.txt"));
        Assertions.assertEquals("org-a", ArrivalHandler.folderTenant("to/org-a/deeper/notes.txt"));
        Assertions.assertNull(ArrivalHandler.folderTenant("to/notes.txt"), "no folder");
        Assertions.assertNull(ArrivalHandler.folderTenant("to//notes.txt"), "empty folder");
        Assertions.assertNull(ArrivalHandler.folderTenant("to/../notes.txt"), "dots");
        Assertions.assertNull(ArrivalHandler.folderTenant("to/org_a/notes.txt"), "not a tenant leg");
    }
}
