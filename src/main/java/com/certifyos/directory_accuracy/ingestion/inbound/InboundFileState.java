package com.certifyos.directory_accuracy.ingestion.inbound;

import java.util.EnumSet;
import java.util.Set;

/**
 * The lifecycle of one {@code inboundFiles[]} entry. Main path {@code RECEIVED → ARCHIVING →
 * INGESTION_REQUESTED → PARSED → STAGING → STAGED}; side states {@code DUPLICATE_LOGGED},
 * {@code INGESTION_FAILED}, {@code FAILED}. Every transition is a guarded update on the entry's
 * state; this enum says which edges exist.
 */
public enum InboundFileState {
    RECEIVED,
    ARCHIVING,
    INGESTION_REQUESTED,
    PARSED,
    STAGING,
    STAGED,
    DUPLICATE_LOGGED,
    INGESTION_FAILED,
    FAILED;

    /**
     * Whether a transition from this state to {@code next} is allowed.
     *
     * <ul>
     *   <li>{@code RECEIVED} → {@code ARCHIVING}, {@code FAILED}
     *   <li>{@code ARCHIVING} → {@code INGESTION_REQUESTED}, {@code FAILED}
     *   <li>{@code INGESTION_REQUESTED} → {@code PARSED}, {@code INGESTION_FAILED}, {@code FAILED}
     *   <li>{@code PARSED} → {@code STAGING}, {@code FAILED}
     *   <li>{@code STAGING} → {@code STAGED}, {@code FAILED}
     *   <li>{@code FAILED} → {@code RECEIVED} or {@code PARSED} (operator retry, a later amendment)
     *   <li>{@code STAGED}, {@code DUPLICATE_LOGGED}, {@code INGESTION_FAILED}: terminal
     * </ul>
     *
     * @param next the state to move to
     * @return true when the edge exists
     */
    public boolean canTransitionTo(InboundFileState next) {
        return allowedNext().contains(next);
    }

    /** The states this state may move to. */
    public Set<InboundFileState> allowedNext() {
        return switch (this) {
            case RECEIVED -> EnumSet.of(ARCHIVING, FAILED);
            case ARCHIVING -> EnumSet.of(INGESTION_REQUESTED, FAILED);
            case INGESTION_REQUESTED -> EnumSet.of(PARSED, INGESTION_FAILED, FAILED);
            case PARSED -> EnumSet.of(STAGING, FAILED);
            case STAGING -> EnumSet.of(STAGED, FAILED);
            case FAILED -> EnumSet.of(RECEIVED, PARSED);
            case STAGED, DUPLICATE_LOGGED, INGESTION_FAILED -> EnumSet.noneOf(InboundFileState.class);
        };
    }

    /** Whether the entry can never move again. {@code FAILED} is not terminal: an operator retry leaves it. */
    public boolean isTerminal() {
        return this == STAGED || this == DUPLICATE_LOGGED || this == INGESTION_FAILED;
    }

    /** The states the reconciler watches: an entry sitting here without progress is stuck. */
    public static Set<InboundFileState> reconcilable() {
        return EnumSet.of(RECEIVED, ARCHIVING, PARSED, STAGING);
    }
}
