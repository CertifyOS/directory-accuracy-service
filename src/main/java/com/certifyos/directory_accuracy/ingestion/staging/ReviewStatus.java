package com.certifyos.directory_accuracy.ingestion.staging;

import java.util.EnumSet;
import java.util.Set;

/**
 * Where a field recommendation is in review and release. {@code PENDING} and {@code SKIPPED} are
 * open (a reviewer can still decide); {@code REJECTED} and {@code RELEASED} are final for the batch.
 * Every move is a guarded update on {@code reviewStatus} and {@code version}; this enum says which
 * reviewer actions each state allows.
 */
public enum ReviewStatus {
    PENDING,
    SKIPPED,
    APPROVED,
    REJECTED,
    RELEASE_REQUESTED,
    RELEASING,
    RELEASED,
    RELEASE_FAILED;

    /** What a reviewer or operator can do to a recommendation. */
    public enum Action {
        APPROVE,
        REJECT,
        SKIP,
        ROLLBACK,
        RELEASE
    }

    /**
     * Whether {@code action} may start from this state.
     *
     * <ul>
     *   <li>{@code APPROVE}, {@code REJECT}: from {@code PENDING} or {@code SKIPPED}
     *   <li>{@code SKIP}: from {@code PENDING}
     *   <li>{@code ROLLBACK}: from {@code APPROVED}
     *   <li>{@code RELEASE}: from {@code APPROVED}, or {@code RELEASE_FAILED} (an operator re-release
     *       after a fix)
     * </ul>
     *
     * @param action the action
     * @return true when the action is allowed here
     */
    public boolean allows(Action action) {
        return switch (action) {
            case APPROVE, REJECT -> this == PENDING || this == SKIPPED;
            case SKIP -> this == PENDING;
            case ROLLBACK -> this == APPROVED;
            case RELEASE -> this == APPROVED || this == RELEASE_FAILED;
        };
    }

    /**
     * The states a reviewer can still decide, the ones the open-list index covers.
     *
     * @return {@code PENDING} and {@code SKIPPED}
     */
    public static Set<ReviewStatus> open() {
        return EnumSet.of(PENDING, SKIPPED);
    }
}
