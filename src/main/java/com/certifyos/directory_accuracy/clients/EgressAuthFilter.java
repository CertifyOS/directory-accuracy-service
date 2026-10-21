package com.certifyos.directory_accuracy.clients;

import com.certifyos.directory_accuracy.auth.GoogleAuthFilter;
import com.certifyos.directory_accuracy.auth.GoogleIdTokenService;
import com.certifyos.directory_accuracy.config.VendorExchangeConfig;
import jakarta.inject.Singleton;

/** The IAP token for the egress backend. */
@Singleton
public class EgressAuthFilter extends GoogleAuthFilter {

    private final VendorExchangeConfig cfg;

    public EgressAuthFilter(GoogleIdTokenService tokens, VendorExchangeConfig cfg) {
        super(tokens);
        this.cfg = cfg;
    }

    @Override
    protected String audience() {
        return cfg.egress().iapClientId();
    }
}
