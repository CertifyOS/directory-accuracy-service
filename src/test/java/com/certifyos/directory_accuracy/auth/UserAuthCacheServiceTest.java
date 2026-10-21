package com.certifyos.directory_accuracy.auth;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class UserAuthCacheServiceTest {

    @Test
    void transformsTheDalListShapeIntoNestedMaps() {
        Map<String, List<DalResourcePermission>> dal = Map.of(
                "org-a",
                List.of(
                        new DalResourcePermission("vendor-export", List.of("read", "manage")),
                        new DalResourcePermission("practitioner", List.of("read")),
                        new DalResourcePermission(null, List.of("ignored"))),
                "org-b",
                List.of());

        Map<String, Map<String, List<String>>> nested = UserAuthCacheService.transformPermissions(dal);

        Assertions.assertEquals(List.of("read", "manage"), nested.get("org-a").get("vendor-export"));
        Assertions.assertEquals(List.of("read"), nested.get("org-a").get("practitioner"));
        Assertions.assertEquals(2, nested.get("org-a").size(), "a null resource is dropped");
        Assertions.assertTrue(nested.get("org-b").isEmpty());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> nested.put("x", Map.of()));
    }

    @Test
    void emptyOrNullIsAnEmptyMap() {
        Assertions.assertTrue(UserAuthCacheService.transformPermissions(null).isEmpty());
        Assertions.assertTrue(
                UserAuthCacheService.transformPermissions(Map.of()).isEmpty());
    }
}
