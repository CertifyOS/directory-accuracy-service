package com.certifyos.vendor_exchange.ingestion.jobs;

import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.interceptor.Interceptor;
import java.util.List;
import org.jobrunr.server.BackgroundJobServer;

/**
 * Registers {@link IngestionFinalFailureFilter} on the worker's job server, beside the export lane's
 * filter: {@code BackgroundJobServer.setJobFilters} appends (plan P6, checked with {@code javap} at
 * Gate 0), so a second observer adds the ingestion filter without touching the export lane's. Same
 * priority as the export registration, so both run before {@code JobRunrStarter} starts the server.
 */
@ApplicationScoped
public class IngestionFinalFailureFilterRegistration {

    private final Instance<BackgroundJobServer> backgroundJobServer;
    private final IngestionFinalFailureFilter filter;

    public IngestionFinalFailureFilterRegistration(
            Instance<BackgroundJobServer> backgroundJobServer, IngestionFinalFailureFilter filter) {
        this.backgroundJobServer = backgroundJobServer;
        this.filter = filter;
    }

    @Priority(Interceptor.Priority.APPLICATION)
    void onStart(@Observes StartupEvent event) {
        if (backgroundJobServer.isResolvable() && backgroundJobServer.get() != null) {
            backgroundJobServer.get().setJobFilters(List.of(filter));
        }
    }
}
