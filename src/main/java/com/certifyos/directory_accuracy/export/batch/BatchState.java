package com.certifyos.directory_accuracy.export.batch;

import java.util.EnumSet;
import java.util.Set;

/**
 * The export batch lifecycle. Main path {@code SCHEDULED → NPIS_SELECTED → EGRESS_REQUESTED →
 * EGRESS_COMPLETED → DELIVERED}; side states {@code EMPTY}, {@code FAILED}, {@code SUPERSEDED}.
 * Every transition is a compare-and-set on the stored state; this enum says which edges exist.
 */
public enum BatchState {
    SCHEDULED,
    NPIS_SELECTED,
    EGRESS_REQUESTED,
    EGRESS_COMPLETED,
    DELIVERED,
    EMPTY,
    FAILED,
    SUPERSEDED;

    /**
     * Whether a transition from this state to {@code next} is allowed.
     *
     * <ul>
     *   <li>{@code SCHEDULED} → {@code NPIS_SELECTED}, {@code EMPTY}, {@code FAILED}
     *   <li>{@code NPIS_SELECTED} → {@code EGRESS_REQUESTED}, {@code FAILED}
     *   <li>{@code EGRESS_REQUESTED} → {@code EGRESS_COMPLETED}, {@code FAILED}
     *   <li>{@code EGRESS_COMPLETED} → {@code DELIVERED}, {@code FAILED}
     *   <li>{@code DELIVERED} → {@code SUPERSEDED}
     *   <li>{@code FAILED} → {@code SCHEDULED} or {@code NPIS_SELECTED} (operator retry puts the batch back where it failed)
     *   <li>{@code EMPTY}, {@code SUPERSEDED}: terminal
     * </ul>
     *
     * @param next the state to move to
     * @return true when the edge exists
     */
    public boolean canTransitionTo(BatchState next) {
        return allowedNext().contains(next);
    }

    /** The states this state may move to. */
    public Set<BatchState> allowedNext() {
        return switch (this) {
            case SCHEDULED -> EnumSet.of(NPIS_SELECTED, EMPTY, FAILED);
            case NPIS_SELECTED -> EnumSet.of(EGRESS_REQUESTED, FAILED);
            case EGRESS_REQUESTED -> EnumSet.of(EGRESS_COMPLETED, FAILED);
            case EGRESS_COMPLETED -> EnumSet.of(DELIVERED, FAILED);
            case DELIVERED -> EnumSet.of(SUPERSEDED);
            case FAILED -> EnumSet.of(SCHEDULED, NPIS_SELECTED);
            case EMPTY, SUPERSEDED -> EnumSet.noneOf(BatchState.class);
        };
    }

    /** Whether no job will ever run for a batch in this state. */
    public boolean isTerminal() {
        return this == DELIVERED || this == EMPTY || this == FAILED || this == SUPERSEDED;
    }

    /** The states the hourly reconciler watches: a row sitting here without a job is stuck. */
    public static Set<BatchState> reconcilable() {
        return EnumSet.of(SCHEDULED, NPIS_SELECTED, EGRESS_COMPLETED);
    }
}
