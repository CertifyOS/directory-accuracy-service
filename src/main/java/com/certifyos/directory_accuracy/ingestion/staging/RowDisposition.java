package com.certifyos.directory_accuracy.ingestion.staging;

/** What staging did with one wide row; counted per upload on {@code inboundFiles[].dispositions}. */
public enum RowDisposition {
    /** File-ingestion found the row valid and its ids are present; its {@code INVALID} fields were written. */
    STAGED,
    /** Every document the row would write already existed. */
    DUPLICATE,
    /** File-ingestion marked the row {@code INVALID}. */
    SCHEMA_INVALID,
    /** The practitioner id or NPI is blank, or the location id is blank for location-level fields. */
    MISSING_IDS,
    /** Not a practitioner row. */
    UNSUPPORTED_ENTITY
}
