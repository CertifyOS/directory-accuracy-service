package com.certifyos.directory_accuracy.auth;

import jakarta.enterprise.context.RequestScoped;
import java.util.List;
import java.util.Map;

/**
 * Who is calling and for which tenant, filled by {@link UserContextFilter} for every operator
 * request under {@code /v1}. Permissions are the DAL's full map for the user, keyed by tenant, then
 * by resource, to the list of actions.
 */
@RequestScoped
public class UserContext {

    private String email;
    private String tenantId;
    private Map<String, Map<String, List<String>>> permissions = Map.of();

    /**
     * Fills the context once per request.
     *
     * @param userEmail the caller's email claim
     * @param tenant the tenant from the {@code tenant-id} header
     * @param grants the caller's permissions for every tenant
     */
    public void fill(String userEmail, String tenant, Map<String, Map<String, List<String>>> grants) {
        this.email = userEmail;
        this.tenantId = tenant;
        this.permissions = grants == null ? Map.of() : Map.copyOf(grants);
    }

    /** The caller's email. */
    public String email() {
        return email;
    }

    /** The tenant the call is scoped to. */
    public String tenantId() {
        return tenantId;
    }

    /** Permissions by tenant, resource and action. */
    public Map<String, Map<String, List<String>>> permissions() {
        return permissions;
    }
}
