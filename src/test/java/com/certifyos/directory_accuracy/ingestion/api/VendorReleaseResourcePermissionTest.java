package com.certifyos.directory_accuracy.ingestion.api;

import com.certifyos.directory_accuracy.auth.Permission;
import com.certifyos.directory_accuracy.auth.PermissionFilter;
import com.certifyos.directory_accuracy.auth.RequiresPermission;
import com.certifyos.directory_accuracy.auth.UserContext;
import com.certifyos.directory_accuracy.config.DirectoryAccuracyConfig;
import com.certifyos.directory_accuracy.http.Problem;
import jakarta.enterprise.inject.Instance;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * The release endpoints' permissions with enforcement on, through the real filter and the real
 * resource methods. A unit test rather than a third Quarkus test profile: the integration tests run
 * on exactly two profiles (AGENTS.md).
 */
@SuppressWarnings("unchecked")
class VendorReleaseResourcePermissionTest {

    private static Map<String, Permission> declared() {
        return Arrays.stream(VendorReleaseResource.class.getDeclaredMethods())
                .filter(method -> method.isAnnotationPresent(Operation.class))
                .collect(Collectors.toMap(Method::getName, method -> method.getAnnotation(RequiresPermission.class)
                        .value()));
    }

    private static Optional<Response> filter(String methodName, List<String> actions) {
        DirectoryAccuracyConfig mapping = Mockito.mock(DirectoryAccuracyConfig.class);
        DirectoryAccuracyConfig.Permissions permissions = Mockito.mock(DirectoryAccuracyConfig.Permissions.class);
        Mockito.when(permissions.enforce()).thenReturn(true);
        Mockito.when(mapping.permissions()).thenReturn(permissions);
        Instance<DirectoryAccuracyConfig> cfg = Mockito.mock(Instance.class);
        Mockito.when(cfg.get()).thenReturn(mapping);
        UserContext ctx = new UserContext();
        ctx.fill("rev@certifyos.com", "org-a", Map.of("org-a", Map.of("vendor-review", actions)));
        ContainerRequestContext rc = Mockito.mock(ContainerRequestContext.class);
        UriInfo uri = Mockito.mock(UriInfo.class);
        Mockito.when(uri.getRequestUri()).thenReturn(URI.create("http://localhost/v1/vendor-review/releases"));
        Mockito.when(rc.getUriInfo()).thenReturn(uri);
        Method method = Arrays.stream(VendorReleaseResource.class.getDeclaredMethods())
                .filter(candidate -> candidate.getName().equals(methodName))
                .findFirst()
                .orElseThrow();
        ResourceInfo info = Mockito.mock(ResourceInfo.class);
        Mockito.when(info.getResourceMethod()).thenReturn((Method) method);
        return new PermissionFilter(cfg, ctx).filter(rc, info);
    }

    @Test
    void eachEndpointDeclaresItsPermission() {
        Assertions.assertEquals(
                Map.of(
                        "releaseField", Permission.REVIEW_RELEASE,
                        "releaseMany", Permission.REVIEW_RELEASE,
                        "rollback", Permission.REVIEW_RELEASE,
                        "release", Permission.REVIEW_READ),
                declared());
    }

    @Test
    void permissionEnforcedWhenFlagOn() {
        Optional<Response> refused = filter("releaseField", List.of("read"));

        Assertions.assertTrue(refused.isPresent());
        Assertions.assertEquals(403, refused.get().getStatus());
        Assertions.assertEquals("PERMISSION_DENIED", ((Problem) refused.get().getEntity()).code());
        Assertions.assertTrue(filter("releaseMany", List.of("read", "decide")).isPresent());
        Assertions.assertTrue(filter("release", List.of("read")).isEmpty(), "a reader reads a release");
        Assertions.assertTrue(filter("releaseField", List.of("release")).isEmpty());
    }
}
