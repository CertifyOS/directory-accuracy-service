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
 * Registers {@link IngestionFinalFailureFilter} on the worker's JobRunr server, as the export lane's
 * {@code BatchFinalFailureFilterRegistration} registers its own: quarkus-jobrunr (open source) does
 * not register CDI job filters, and {@code setJobFilters} appends (plan P6, checked with {@code
 * javap} at Gate 0), so the filter joins JobRunr's retry filter and the export lane's. The priority
 * runs this observer before {@code JobRunrStarter} starts the server on the same {@code StartupEvent},
 * so no job can fail before the filter exists. The api role has no server; nothing is registered.
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
