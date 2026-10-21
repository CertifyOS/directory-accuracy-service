package com.certifyos.directory_accuracy.export.jobs;

import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.interceptor.Interceptor;
import java.util.List;
import org.jobrunr.server.BackgroundJobServer;

/**
 * quarkus-jobrunr (open source) does not auto-register CDI job filters; that is a JobRunr Pro
 * feature. The extension wires the server with its own {@code RetryFilter} and
 * {@code setJobFilters} appends, so registering here adds {@link BatchFinalFailureFilter} beside it.
 *
 * <p>{@code JobRunrStarter} observes the same {@code StartupEvent} with no priority and starts the
 * server there; CDI gives unprioritised observers one default priority, so without the
 * {@code @Priority} below a due job could be polled and fail before the filter exists. APPLICATION
 * is lower than the default (2500), so this observer runs first, deterministically
 * (file-ingestion findings 5 and 10).
 */
@ApplicationScoped
public class BatchFinalFailureFilterRegistration {

    private final Instance<BackgroundJobServer> backgroundJobServer;
    private final BatchFinalFailureFilter filter;

    public BatchFinalFailureFilterRegistration(
            Instance<BackgroundJobServer> backgroundJobServer, BatchFinalFailureFilter filter) {
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
