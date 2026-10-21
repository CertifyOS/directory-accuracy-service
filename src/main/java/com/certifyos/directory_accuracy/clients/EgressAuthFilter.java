package com.certifyos.directory_accuracy.clients;

import com.certifyos.directory_accuracy.auth.GoogleAuthFilter;
import com.certifyos.directory_accuracy.auth.GoogleIdTokenService;
import com.certifyos.directory_accuracy.config.DirectoryAccuracyConfig;
import jakarta.inject.Singleton;

/** The IAP token for the egress backend. */
@Singleton
public class EgressAuthFilter extends GoogleAuthFilter {

    private final DirectoryAccuracyConfig cfg;

    public EgressAuthFilter(GoogleIdTokenService tokens, DirectoryAccuracyConfig cfg) {
        super(tokens);
        this.cfg = cfg;
    }

    @Override
    protected String audience() {
        return cfg.egress().iapClientId();
    }
}
