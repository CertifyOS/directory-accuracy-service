package com.certifyos.vendor_exchange.auth;

/**
 * The operator permissions from the designs, as DAL resource and action: two for exports, three
 * for reviewing the vendor's recommendations. Checked per endpoint only when {@code
 * vendor-exchange.permissions.enforce} is true.
 */
public enum Permission {
    /** Create, change, run and retry exports. */
    MANAGE("vendor-export", "manage"),
    /** Read schedules, batches and audit. */
    READ("vendor-export", "read"),
    /** Read field recommendations and their review trail. */
    REVIEW_READ("vendor-review", "read"),
    /** Approve, reject and skip field recommendations. */
    REVIEW_DECIDE("vendor-review", "decide"),
    /**
     * Release approved field recommendations to the directory, and roll an approval back before its
     * release (design: {@code vendor-review:release} covers release and rollback).
     */
    REVIEW_RELEASE("vendor-review", "release");

    private final String resource;
    private final String action;

    Permission(String resource, String action) {
        this.resource = resource;
        this.action = action;
    }

    /** The DAL resource name. */
    public String resource() {
        return resource;
    }

    /** The DAL action name. */
    public String action() {
        return action;
    }
}
