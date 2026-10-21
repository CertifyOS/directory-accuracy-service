package com.certifyos.directory_accuracy.auth;

import com.certifyos.directory_accuracy.config.VendorExchangeConfig;
import com.certifyos.directory_accuracy.http.Problems;
import jakarta.enterprise.inject.Instance;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.Response;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.jboss.resteasy.reactive.server.ServerRequestFilter;

/**
 * The per-action permission check, behind {@code vendor-exchange.permissions.enforce}. Off, every
 * tenant member passes (file-ingestion's rule, today's behaviour). On, the endpoint's {@link
 * RequiresPermission} must appear in the caller's grants for the tenant, else 403. Flipping the
 * flag needs no code change once the DAL has the rows.
 */
public class PermissionFilter {

    // Instance for the same reason as in PubSubPushAuthFilter: filters are built before runtime config.
    private final Instance<VendorExchangeConfig> cfg;
    private final UserContext ctx;

    public PermissionFilter(Instance<VendorExchangeConfig> cfg, UserContext ctx) {
        this.cfg = cfg;
        this.ctx = ctx;
    }

    /**
     * Runs after the tenant filter, once the resource method is known.
     *
     * @param rc the request
     * @param info the matched resource method
     * @return a 403 problem, or empty to continue
     */
    @ServerRequestFilter(priority = Priorities.AUTHORIZATION)
    public Optional<Response> filter(ContainerRequestContext rc, ResourceInfo info) {
        Method method = info.getResourceMethod();
        RequiresPermission required = method == null ? null : method.getAnnotation(RequiresPermission.class);
        if (required == null || !cfg.get().permissions().enforce()) {
            return Optional.empty();
        }
        if (allows(ctx.permissions(), ctx.tenantId(), required.value())) {
            return Optional.empty();
        }
        return Optional.of(Problems.response(
                403,
                "PERMISSION_DENIED",
                "requires " + required.value().resource() + ":"
                        + required.value().action(),
                Problems.path(rc)));
    }

    /**
     * Whether the grants hold the permission for the tenant.
     *
     * @param grants permissions by tenant, resource and action
     * @param tenantId the tenant
     * @param permission the permission
     * @return true when granted
     */
    static boolean allows(Map<String, Map<String, List<String>>> grants, String tenantId, Permission permission) {
        if (grants == null || tenantId == null) {
            return false;
        }
        Map<String, List<String>> forTenant = grants.get(tenantId);
        if (forTenant == null) {
            return false;
        }
        List<String> actions = forTenant.get(permission.resource());
        return actions != null && actions.contains(permission.action());
    }
}
