package com.certifyos.vendor_exchange.ingestion.api;

import com.certifyos.vendor_exchange.ingestion.inbound.InboundFile;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** The operator view of an inbound file keeps what the entry stores, in the stored order. */
class InboundViewsTest {

    @Test
    void dispositionsKeepTheEntrysOrder() {
        List<String> order = List.of("STAGED", "DUPLICATE", "SCHEMA_INVALID", "MISSING_IDS", "UNSUPPORTED_ENTITY");
        Document dispositions = new Document();
        long count = 1;
        for (String disposition : order) {
            dispositions.append(disposition, count++);
        }
        InboundFile entry = InboundFile.fromDocument(new Document("uploadId", "vendor-sftp|to/org-xyz/f.csv|7")
                .append("state", "STAGED")
                .append("dispositions", dispositions));

        InboundViews.InboundFileView view = InboundViews.InboundFileView.of(entry);

        Assertions.assertEquals(order, List.copyOf(view.dispositions().keySet()));
    }
}
