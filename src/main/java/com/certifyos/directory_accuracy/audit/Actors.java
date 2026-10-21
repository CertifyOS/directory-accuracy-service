package com.certifyos.directory_accuracy.audit;

/**
 * The actor strings the design fixes for audit events and system writes. Operator actions carry the
 * operator's user id instead of one of these.
 */
public final class Actors {

    /** The export lane's tick, jobs and reconciler. */
    public static final String SYSTEM = "system:vendor-export";

    /** The egress completion event handler. */
    public static final String EGRESS_EVENT = "system:egress-event";

    /** The ingestion lane's event handlers, jobs and reconciler. */
    public static final String INGESTION = "system:vendor-ingestion";

    private Actors() {}
}
