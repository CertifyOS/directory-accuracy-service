package com.certifyos.directory_accuracy.export.batch;

import java.util.EnumSet;
import java.util.Set;

/**
 * The export batch lifecycle. Main path {@code SCHEDULED → EGRESS_REQUESTED → EGRESS_COMPLETED →
 * DELIVERED}; side states {@code EMPTY}, {@code FAILED}, {@code SUPERSEDED}. Every transition is a
 * compare-and-set on the stored state; this enum says which edges exist.
 */
public enum BatchState {
    SCHEDULED,
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
     *   <li>{@code SCHEDULED} → {@code EGRESS_REQUESTED}, {@code FAILED}
     *   <li>{@code EGRESS_REQUESTED} → {@code EGRESS_COMPLETED}, {@code EMPTY} (the saved filter
     *       matched nobody), {@code FAILED}, {@code SCHEDULED} (egress could not reuse a retry's
     *       list after accepting it; the next attempt asks again)
     *   <li>{@code EGRESS_COMPLETED} → {@code DELIVERED}, {@code FAILED}
     *   <li>{@code DELIVERED} → {@code SUPERSEDED}
     *   <li>{@code FAILED} → {@code SCHEDULED} (operator retry asks egress again)
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
            case SCHEDULED -> EnumSet.of(EGRESS_REQUESTED, FAILED);
            case EGRESS_REQUESTED -> EnumSet.of(EGRESS_COMPLETED, EMPTY, FAILED, SCHEDULED);
            case EGRESS_COMPLETED -> EnumSet.of(DELIVERED, FAILED);
            case DELIVERED -> EnumSet.of(SUPERSEDED);
            case FAILED -> EnumSet.of(SCHEDULED);
            case EMPTY, SUPERSEDED -> EnumSet.noneOf(BatchState.class);
        };
    }

    /** Whether no job will ever run for a batch in this state. */
    public boolean isTerminal() {
        return this == DELIVERED || this == EMPTY || this == FAILED || this == SUPERSEDED;
    }

    /** The states the hourly reconciler watches: a row sitting here without a job is stuck. */
    public static Set<BatchState> reconcilable() {
        return EnumSet.of(SCHEDULED, EGRESS_COMPLETED);
    }
}
