package com.certifyos.directory_accuracy.auth;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The permission an operator endpoint needs. Every method under {@code export.api} carries one
 * (ArchitectureTest); {@link PermissionFilter} enforces it when the flag is on and documents the
 * intent when it is off.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface RequiresPermission {
    /** The permission. */
    Permission value();
}
