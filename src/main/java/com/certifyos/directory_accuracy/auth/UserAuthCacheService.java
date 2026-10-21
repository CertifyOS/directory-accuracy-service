package com.certifyos.directory_accuracy.auth;

import io.quarkus.cache.CacheKey;
import io.quarkus.cache.CacheResult;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.WebApplicationException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.eclipse.microprofile.rest.client.inject.RestClient;

/**
 * The DAL user lookup with a Caffeine cache in front of it. Only successful lookups are cached: a
 * 404 becomes a {@link NotFoundException}, which {@code @CacheResult} never stores, so a removed
 * or unknown user is re-checked on every request and never stuck on a stale negative answer.
 */
@ApplicationScoped
public class UserAuthCacheService {

    /** The cache name, configured under {@code quarkus.cache.caffeine."user-by-email"}. */
    public static final String CACHE = "user-by-email";

    private final DalUserClient dal;

    public UserAuthCacheService(@RestClient DalUserClient dal) {
        this.dal = dal;
    }

    /** A user as the filters see them: permissions by tenant, then resource, then actions. */
    public record UserDetail(String id, String email, Map<String, Map<String, List<String>>> permissions) {
        public UserDetail {
            permissions = permissions == null ? Map.of() : Map.copyOf(permissions);
        }
    }

    /**
     * Finds a user. The email alone is the cache key: the DAL returns permissions for every tenant
     * at once, so the same user asking under another tenant must hit the same entry. The tenant
     * only rides along as the header the DAL insists on.
     *
     * @param email the email claim
     * @param tenantId the caller's tenant
     * @return the user
     * @throws NotFoundException when the DAL has no such user
     * @throws WebApplicationException for any other DAL error
     */
    @CacheResult(cacheName = CACHE)
    public UserDetail byEmail(@CacheKey String email, String tenantId) {
        DalUserResponse response;
        try {
            response = dal.byEmail(email, true, tenantId);
        } catch (WebApplicationException failure) {
            // The REST client throws its own ClientWebApplicationException for every non-2xx
            // answer, never the status-specific JAX-RS subtypes, so the 404 is mapped by status.
            if (failure.getResponse() != null && failure.getResponse().getStatus() == 404) {
                throw new NotFoundException("no such user", failure);
            }
            throw failure;
        }
        if (response == null) {
            throw new NotFoundException("no such user");
        }
        return new UserDetail(response.id(), response.email(), transformPermissions(response.permissions()));
    }

    /**
     * DAL shape {@code {tenantId: [{resource, actions}]}} to {@code {tenantId: {resource: [actions]}}}.
     *
     * @param dalPermissions the DAL's list form
     * @return the nested map form, immutable
     */
    static Map<String, Map<String, List<String>>> transformPermissions(
            Map<String, List<DalResourcePermission>> dalPermissions) {
        if (dalPermissions == null || dalPermissions.isEmpty()) {
            return Map.of();
        }
        return dalPermissions.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, entry -> byResource(entry.getValue())));
    }

    private static Map<String, List<String>> byResource(List<DalResourcePermission> grants) {
        if (grants == null) {
            return Map.of();
        }
        return grants.stream()
                .filter(grant -> grant.resource() != null)
                .collect(Collectors.toUnmodifiableMap(
                        DalResourcePermission::resource, DalResourcePermission::actions, (first, second) -> first));
    }
}
