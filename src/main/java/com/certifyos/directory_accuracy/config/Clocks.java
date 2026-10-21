package com.certifyos.directory_accuracy.config;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.time.Clock;

/** One UTC clock for the whole service, so time is injected and tests can pin it. */
@ApplicationScoped
public class Clocks {

    @Produces
    @ApplicationScoped
    Clock clock() {
        return Clock.systemUTC();
    }
}
