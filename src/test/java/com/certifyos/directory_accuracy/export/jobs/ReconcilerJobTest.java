package com.certifyos.directory_accuracy.export.jobs;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ReconcilerJobTest {

    @Test
    void theReenqueueLinePrefixIsPinnedForTheDeploymentsAlert() {
        // Alert E2 counts this line in Cloud Logging; a change here is a change to that alert.
        Assertions.assertEquals("EXPORT_RECONCILER_REENQUEUED", ReconcilerJob.REENQUEUED_PREFIX);
    }
}
