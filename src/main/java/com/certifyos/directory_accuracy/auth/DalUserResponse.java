package com.certifyos.directory_accuracy.auth;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;

/** The DAL's answer to {@code GET /users/by-email?includePermissions=true}: permissions by tenant. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DalUserResponse(String id, String email, Map<String, List<DalResourcePermission>> permissions) {
    public DalUserResponse {
        permissions = permissions == null ? Map.of() : Map.copyOf(permissions);
    }
}
