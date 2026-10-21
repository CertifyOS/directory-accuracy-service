package com.certifyos.directory_accuracy.clients;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * The two fields of a practitioner this service keeps: the platform id and the NPI. Every other
 * field of the page is dropped at the client boundary.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record PractitionerRef(String id, String npi) {}
