package com.certifyos.directory_accuracy.config;

import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RoleProfileCheckTest {

    @Test
    void apiAloneIsAccepted() {
        Assertions.assertDoesNotThrow(() -> RoleProfileCheck.verify(List.of("prod", "api")));
    }

    @Test
    void workerAloneIsAccepted() {
        Assertions.assertDoesNotThrow(() -> RoleProfileCheck.verify(List.of("prod", "worker")));
    }

    @Test
    void neitherRoleIsRefused() {
        IllegalStateException refused =
                Assertions.assertThrows(IllegalStateException.class, () -> RoleProfileCheck.verify(List.of("prod")));
        Assertions.assertTrue(refused.getMessage().contains("Exactly one of the profiles api or worker"));
    }

    @Test
    void bothRolesAreRefused() {
        Assertions.assertThrows(
                IllegalStateException.class, () -> RoleProfileCheck.verify(List.of("prod", "api", "worker")));
    }
}
