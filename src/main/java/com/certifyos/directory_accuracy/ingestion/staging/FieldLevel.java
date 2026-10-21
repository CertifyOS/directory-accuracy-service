package com.certifyos.directory_accuracy.ingestion.staging;

/**
 * Where a vendor field lives: on the practitioner (written once per practitioner, no location) or on
 * one location we sent (written once per location).
 */
public enum FieldLevel {
    PRACTITIONER,
    LOCATION
}
