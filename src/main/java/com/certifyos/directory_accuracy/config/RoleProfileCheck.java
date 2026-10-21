package com.certifyos.directory_accuracy.config;

import io.quarkus.runtime.StartupEvent;
import io.quarkus.runtime.configuration.ConfigUtils;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import java.util.List;

/**
 * One image, two roles. The role is a Quarkus profile: {@code QUARKUS_PROFILE=prod,api} serves
 * HTTP and {@code prod,worker} runs the JobRunr server. A VM that boots with neither or both is
 * misconfigured and must not stay up, so this check throws at startup and the instance group's
 * health check replaces the VM.
 */
@ApplicationScoped
public class RoleProfileCheck {

    void onStart(@Observes StartupEvent ev) {
        verify(ConfigUtils.getProfiles());
    }

    static void verify(List<String> profiles) {
        boolean api = profiles.contains("api");
        boolean worker = profiles.contains("worker");
        if (api == worker) {
            throw new IllegalStateException("Exactly one of the profiles api or worker must be active. "
                    + "Set QUARKUS_PROFILE=prod,api or prod,worker. Active: " + profiles);
        }
    }
}
