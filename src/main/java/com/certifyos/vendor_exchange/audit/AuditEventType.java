package com.certifyos.vendor_exchange.audit;

/**
 * The audit event catalogue from the designs' per-event contracts tables: 24 export types and 19
 * ingestion types, each committed either inside the transition it describes or as a standalone fact.
 * {@link Scope} says whether the event belongs to a schedule (found by tenant and vendor), to a batch
 * (carries the batch id) or to one field recommendation (carries its id and is stored with the
 * review events).
 */
public enum AuditEventType {
    SCHEDULE_CREATED(Scope.SCHEDULE),
    SCHEDULE_UPDATED(Scope.SCHEDULE),
    SCHEDULE_ENABLED(Scope.SCHEDULE),
    SCHEDULE_RUN_NOW(Scope.SCHEDULE),
    SCHEDULE_DISABLED(Scope.SCHEDULE),
    EXPORT_TICK_COMPLETED(Scope.SERVICE),
    EXPORT_BATCH_SCHEDULED(Scope.BATCH),
    NPIS_REGISTERED(Scope.BATCH),
    EXPORT_SELECTION_COMPLETED(Scope.BATCH),
    EXPORT_BATCH_EMPTY(Scope.BATCH),
    EXPORT_EGRESS_REQUESTED(Scope.BATCH),
    EXPORT_EVENT_RECEIVED(Scope.BATCH),
    EXPORT_EVENT_REJECTED(Scope.SERVICE),
    EXPORT_EVENT_MISSED(Scope.BATCH),
    EXPORT_EGRESS_STALE(Scope.BATCH),
    EXPORT_EGRESS_COMPLETED(Scope.BATCH),
    NPIS_RECONCILED(Scope.BATCH),
    EXPORT_BATCH_DELIVERED(Scope.BATCH),
    EXPORT_BATCH_FAILED(Scope.BATCH),
    EXPORT_RETRY_REQUESTED(Scope.BATCH),
    EXPORT_PRIOR_ATTEMPT_CANCELLED(Scope.BATCH),
    EXPORT_PRIOR_ATTEMPT_CANCEL_REJECTED(Scope.BATCH),
    EXPORT_BATCH_SUPERSEDED(Scope.BATCH),
    EXPORT_BATCH_ACKNOWLEDGED(Scope.BATCH),
    INBOUND_FILE_RECEIVED(Scope.BATCH),
    INBOUND_FILE_DUPLICATE_LOGGED(Scope.BATCH),
    INBOUND_FILE_UNMATCHED(Scope.SERVICE),
    INBOUND_UPLOAD_ATTACHED(Scope.BATCH),
    INBOUND_UPLOAD_DISCARDED(Scope.SERVICE),
    INBOUND_FILE_ARCHIVED(Scope.BATCH),
    INBOUND_FILE_INGESTION_REQUESTED(Scope.BATCH),
    INBOUND_FILE_PARSED(Scope.BATCH),
    INBOUND_FILE_INGESTION_FAILED(Scope.BATCH),
    INBOUND_FILE_STAGED(Scope.BATCH),
    INBOUND_FILE_FAILED(Scope.BATCH),
    INBOUND_FILE_RECONCILED(Scope.BATCH),
    FIELD_APPROVED(Scope.FIELD),
    FIELD_REJECTED(Scope.FIELD),
    FIELD_SKIPPED(Scope.FIELD),
    FIELD_ROLLED_BACK(Scope.FIELD),
    FIELD_RELEASE_REQUESTED(Scope.FIELD),
    FIELD_RELEASED(Scope.FIELD),
    FIELD_RELEASE_FAILED(Scope.FIELD);

    /** What an event is about, which decides which ids it must carry and where it is stored. */
    public enum Scope {
        /** A schedule: tenant and vendor set, no batch id. */
        SCHEDULE,
        /** A batch: tenant, vendor and batch id set. */
        BATCH,
        /** The service itself (a tick) or a message that matched no batch: tenant and batch optional. */
        SERVICE,
        /**
         * One field recommendation: tenant, vendor and field recommendation id set. Stored in
         * {@code vendor_review_events}; every other scope is stored in {@code vendor_export_events}.
         */
        FIELD
    }

    private final Scope scope;

    AuditEventType(Scope scope) {
        this.scope = scope;
    }

    /** The scope this type belongs to. */
    public Scope scope() {
        return scope;
    }

    /** Whether events of this type must carry an export batch id. */
    public boolean requiresBatch() {
        return scope == Scope.BATCH;
    }

    /** Whether events of this type must carry a field recommendation id. */
    public boolean requiresField() {
        return scope == Scope.FIELD;
    }

    /** Whether events of this type must carry a tenant and a vendor. */
    public boolean requiresTenant() {
        return scope != Scope.SERVICE;
    }
}
