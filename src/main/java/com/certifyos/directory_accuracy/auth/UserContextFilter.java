package com.certifyos.directory_accuracy.auth;

import com.certifyos.directory_accuracy.auth.UserAuthCacheService.UserDetail;
import com.certifyos.directory_accuracy.http.Problems;
import io.quarkus.security.identity.SecurityIdentity;
import jakarta.annotation.Priority;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.ext.Provider;
import java.util.List;
import java.util.Map;
import org.eclipse.microprofile.jwt.JsonWebToken;
import org.jboss.logging.Logger;

/**
 * Tenant membership for every operator call. The HTTP layer has already required a valid platform
 * token for {@code /v1/*}; this filter reads the email claim, requires the {@code tenant-id}
 * header, looks the user up in the DAL and refuses a caller who holds no role in that tenant. This
 * is file-ingestion's rule exactly; the per-action check is {@link PermissionFilter}'s and stays
 * off until the DAL has the permission rows.
 */
@Provider
@Priority(Priorities.AUTHENTICATION - 1)
public class UserContextFilter implements ContainerRequestFilter {

    /** The Auth0 custom claim carrying the user's email. */
    public static final String EMAIL_CLAIM = "https://certifyos.com/email";

    /** The header that selects the tenant. */
    public static final String TENANT_HEADER = "tenant-id";

    private static final Logger LOG = Logger.getLogger(UserContextFilter.class);

    private final SecurityIdentity identity;
    private final UserContext ctx;
    private final UserAuthCacheService users;

    public UserContextFilter(SecurityIdentity identity, UserContext ctx, UserAuthCacheService users) {
        this.identity = identity;
        this.ctx = ctx;
        this.users = users;
    }

    @Override
    public void filter(ContainerRequestContext rc) {
        if (isExempt(rc)) {
            return;
        }
        String tenantId = header(rc, TENANT_HEADER);
        if (tenantId == null) {
            Problems.abort(rc, 400, "TENANT_REQUIRED", "tenant-id header is required");
            return;
        }
        String email = emailClaim();
        if (email == null) {
            Problems.abort(rc, 403, "TENANT_UNRESOLVED", "token carries no email claim");
            return;
        }
        UserDetail user = lookup(rc, email, tenantId);
        if (user == null) {
            return;
        }
        Map<String, List<String>> tenantGrants = user.permissions().get(tenantId);
        if (tenantGrants == null || tenantGrants.isEmpty()) {
            LOG.warnf("user %s holds no role in tenant %s", user.id(), tenantId);
            Problems.abort(rc, 403, "TENANT_FORBIDDEN", "user holds no role in tenant");
            return;
        }
        ctx.fill(email, tenantId, user.permissions());
    }

    /** Health, metrics and OpenAPI under {@code /q}, and the push endpoint under {@code /internal}, are not operator calls. */
    private static boolean isExempt(ContainerRequestContext rc) {
        // Quarkus REST returns the path with its leading slash; compare on the slash-less form.
        String path = rc.getUriInfo().getPath();
        String relative = path.startsWith("/") ? path.substring(1) : path;
        return relative.startsWith("q/") || relative.startsWith("internal/");
    }

    // Not an injected JsonWebToken: under quarkus-test-security-jwt that injects a placeholder
    // regardless of the declared claims. The identity's principal is the real token in production
    // and in @TestSecurity mode alike.
    private String emailClaim() {
        if (identity.getPrincipal() instanceof JsonWebToken jwt) {
            String email = jwt.getClaim(EMAIL_CLAIM);
            return email == null || email.isBlank() ? null : email.trim();
        }
        return null;
    }

    private UserDetail lookup(ContainerRequestContext rc, String email, String tenantId) {
        try {
            return users.byEmail(email, tenantId);
        } catch (NotFoundException missing) {
            Problems.abort(rc, 403, "TENANT_FORBIDDEN", "user not found");
            return null;
        } catch (ProcessingException | WebApplicationException failure) {
            // The caller gets a generic 503; the cause must still reach the log.
            LOG.warnf(failure, "DAL user lookup failed for tenant %s: %s", tenantId, failure.toString());
            Problems.abort(rc, 503, "DAL_UNAVAILABLE", "user lookup failed");
            return null;
        }
    }

    private static String header(ContainerRequestContext rc, String name) {
        String value = rc.getHeaderString(name);
        return value == null || value.isBlank() ? null : value.trim();
    }
}
