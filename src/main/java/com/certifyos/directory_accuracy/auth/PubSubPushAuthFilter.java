package com.certifyos.directory_accuracy.auth;

import com.certifyos.directory_accuracy.config.VendorExchangeConfig;
import com.certifyos.directory_accuracy.http.Problems;
import jakarta.annotation.Priority;
import jakarta.enterprise.inject.Instance;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.ext.Provider;
import org.jboss.logging.Logger;

/**
 * Guards the push endpoint. The HTTP layer permits {@code /internal/*} without a platform token;
 * this filter is the whole check for it. Until both push settings are configured (the subscription
 * does not exist yet), every call is refused with {@code PUSH_NOT_CONFIGURED} before the body is
 * read, so the endpoint is closed rather than open while the hook-up is pending.
 */
@Provider
@PubSubPush
@Priority(Priorities.AUTHENTICATION)
public class PubSubPushAuthFilter implements ContainerRequestFilter {

    private static final Logger LOG = Logger.getLogger(PubSubPushAuthFilter.class);
    private static final String BEARER = "Bearer ";

    // Instance, not the mapping itself: Quarkus REST instantiates @Provider filters during static
    // init, before runtime config exists, and a direct VendorExchangeConfig parameter fails the boot
    // with SRCFG00027 "Could not find a mapping". The lookup happens per request instead.
    private final Instance<VendorExchangeConfig> cfg;
    private final PushTokenVerifier verifier;

    public PubSubPushAuthFilter(Instance<VendorExchangeConfig> cfg, PushTokenVerifier verifier) {
        this.cfg = cfg;
        this.verifier = verifier;
    }

    @Override
    public void filter(ContainerRequestContext rc) {
        VendorExchangeConfig.PubSub pubsub = cfg.get().pubsub();
        if (pubsub.pushServiceAccount().isEmpty() || pubsub.pushAudience().isEmpty()) {
            Problems.abort(rc, 401, "PUSH_NOT_CONFIGURED", "push delivery is not configured on this service");
            return;
        }
        String authorization = rc.getHeaderString(HttpHeaders.AUTHORIZATION);
        if (authorization == null || !authorization.startsWith(BEARER)) {
            Problems.abort(rc, 401, "PUSH_TOKEN_REQUIRED", "a bearer identity token is required");
            return;
        }
        try {
            PushIdentity identity =
                    verifier.verify(authorization.substring(BEARER.length()).trim());
            LOG.debugf("push accepted from %s", identity.email());
        } catch (PushTokenRejectedException rejected) {
            LOG.warnf("push token rejected: %s", rejected.getMessage());
            Problems.abort(rc, 401, "PUSH_TOKEN_REJECTED", rejected.getMessage());
        }
    }
}
