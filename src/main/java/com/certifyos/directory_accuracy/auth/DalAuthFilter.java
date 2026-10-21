package com.certifyos.directory_accuracy.auth;

import com.certifyos.directory_accuracy.config.DirectoryAccuracyConfig;
import jakarta.inject.Singleton;

/** The IAP token for the DAL backend. */
@Singleton
public class DalAuthFilter extends GoogleAuthFilter {

    private final DirectoryAccuracyConfig cfg;

    public DalAuthFilter(GoogleIdTokenService tokens, DirectoryAccuracyConfig cfg) {
        super(tokens);
        this.cfg = cfg;
    }

    @Override
    protected String audience() {
        return cfg.dal().iapClientId();
    }
}
