package com.certifyos.directory_accuracy.export.batch;

import java.util.EnumSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BatchStateTest {

    @Test
    void mainPathEdgesExist() {
        Assertions.assertTrue(BatchState.SCHEDULED.canTransitionTo(BatchState.NPIS_SELECTED));
        Assertions.assertTrue(BatchState.NPIS_SELECTED.canTransitionTo(BatchState.EGRESS_REQUESTED));
        Assertions.assertTrue(BatchState.EGRESS_REQUESTED.canTransitionTo(BatchState.EGRESS_COMPLETED));
        Assertions.assertTrue(BatchState.EGRESS_COMPLETED.canTransitionTo(BatchState.DELIVERED));
        Assertions.assertTrue(BatchState.DELIVERED.canTransitionTo(BatchState.SUPERSEDED));
    }

    @Test
    void sideEdgesExist() {
        Assertions.assertTrue(BatchState.SCHEDULED.canTransitionTo(BatchState.EMPTY));
        for (BatchState state : EnumSet.of(
                BatchState.SCHEDULED,
                BatchState.NPIS_SELECTED,
                BatchState.EGRESS_REQUESTED,
                BatchState.EGRESS_COMPLETED)) {
            Assertions.assertTrue(state.canTransitionTo(BatchState.FAILED), state + " must be able to fail");
        }
        Assertions.assertTrue(BatchState.FAILED.canTransitionTo(BatchState.SCHEDULED), "retry from SELECT");
        Assertions.assertTrue(BatchState.FAILED.canTransitionTo(BatchState.NPIS_SELECTED), "retry from EGRESS");
    }

    @Test
    void refusedEdges() {
        Assertions.assertFalse(
                BatchState.SCHEDULED.canTransitionTo(BatchState.EGRESS_REQUESTED), "no skipping selection");
        Assertions.assertFalse(
                BatchState.DELIVERED.canTransitionTo(BatchState.FAILED), "a delivered file is never failed");
        Assertions.assertFalse(BatchState.DELIVERED.canTransitionTo(BatchState.DELIVERED));
        Assertions.assertFalse(BatchState.FAILED.canTransitionTo(BatchState.DELIVERED));
        Assertions.assertTrue(BatchState.EMPTY.allowedNext().isEmpty());
        Assertions.assertTrue(BatchState.SUPERSEDED.allowedNext().isEmpty());
    }

    @Test
    void terminalAndReconcilableSets() {
        Assertions.assertEquals(
                EnumSet.of(BatchState.DELIVERED, BatchState.EMPTY, BatchState.FAILED, BatchState.SUPERSEDED),
                EnumSet.allOf(BatchState.class).stream()
                        .filter(BatchState::isTerminal)
                        .collect(java.util.stream.Collectors.toCollection(() -> EnumSet.noneOf(BatchState.class))));
        Assertions.assertEquals(
                EnumSet.of(BatchState.SCHEDULED, BatchState.NPIS_SELECTED, BatchState.EGRESS_COMPLETED),
                BatchState.reconcilable());
        Assertions.assertFalse(
                BatchState.reconcilable().contains(BatchState.EGRESS_REQUESTED),
                "a batch waiting on egress is not stuck; the deadline check owns it");
    }
}
