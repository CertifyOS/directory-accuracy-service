package com.certifyos.directory_accuracy.auth;

/** The verified identity behind a Pub/Sub push: the service account's email. */
public record PushIdentity(String email) {}
