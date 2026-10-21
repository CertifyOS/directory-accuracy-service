package com.certifyos.directory_accuracy.clients;

import com.certifyos.directory_accuracy.clients.ApiLayerAuthClient.TokenRequest;
import com.certifyos.directory_accuracy.clients.ApiLayerAuthClient.TokenResponse;
import com.certifyos.directory_accuracy.config.VendorExchangeConfig;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

/** The caching and error rules; the wire (headers, body) is {@code ApiLayerClientIT}'s. */
class ApiLayerTokenServiceTest {

    /** A clock the test moves forward. */
    static final class MutableClock extends Clock {
        private Instant now = Instant.parse("2026-10-05T10:00:00Z");

        void advance(Duration by) {
            now = now.plus(by);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }

    private VendorExchangeConfig cfg;
    private VendorExchangeConfig.ApiLayer apiLayer;
    private ApiLayerAuthClient auth;
    private MutableClock clock;

    @BeforeEach
    void before() {
        cfg = Mockito.mock(VendorExchangeConfig.class);
        apiLayer = Mockito.mock(VendorExchangeConfig.ApiLayer.class);
        Mockito.when(cfg.apiLayer()).thenReturn(apiLayer);
        Mockito.when(apiLayer.clientId()).thenReturn(Optional.of("client-1"));
        Mockito.when(apiLayer.clientSecret()).thenReturn(Optional.of("secret-1"));
        auth = Mockito.mock(ApiLayerAuthClient.class);
        clock = new MutableClock();
    }

    private ApiLayerTokenService service() {
        return new ApiLayerTokenService(cfg, auth, clock);
    }

    @Test
    void sendsTheMachineClientAndCachesTheToken() {
        Mockito.when(auth.clientCredentials(new TokenRequest("client-1", "secret-1")))
                .thenReturn(new TokenResponse("t-1", 3600L, "Bearer"));
        ApiLayerTokenService service = service();
        Assertions.assertEquals("t-1", service.accessToken());
        Assertions.assertEquals("t-1", service.accessToken());
        Mockito.verify(auth, Mockito.times(1)).clientCredentials(ArgumentMatchers.any());
    }

    @Test
    void refreshesOneMinuteBeforeExpiry() {
        Mockito.when(auth.clientCredentials(ArgumentMatchers.any()))
                .thenReturn(new TokenResponse("t-1", 600L, "Bearer"), new TokenResponse("t-2", 600L, "Bearer"));
        ApiLayerTokenService service = service();
        Assertions.assertEquals("t-1", service.accessToken());
        clock.advance(Duration.ofSeconds(500));
        Assertions.assertEquals("t-1", service.accessToken(), "still inside the window");
        clock.advance(Duration.ofSeconds(45));
        Assertions.assertEquals("t-2", service.accessToken(), "past expiry minus the margin");
        Mockito.verify(auth, Mockito.times(2)).clientCredentials(ArgumentMatchers.any());
    }

    @Test
    void notConfiguredThrowsWithoutCalling() {
        Mockito.when(apiLayer.clientSecret()).thenReturn(Optional.empty());
        ApiLayerTokenService service = service();
        Assertions.assertFalse(service.configured());
        Assertions.assertThrows(ApiLayerNotConfiguredException.class, service::accessToken);
        Mockito.verifyNoInteractions(auth);
    }

    @Test
    void refusalBecomesATokenExceptionNamingTheStatus() {
        Mockito.when(auth.clientCredentials(ArgumentMatchers.any()))
                .thenThrow(new WebApplicationException(Response.status(401).build()));
        ApiLayerTokenException refused = Assertions.assertThrows(ApiLayerTokenException.class, service()::accessToken);
        Assertions.assertTrue(refused.getMessage().contains("401"));
        Assertions.assertFalse(refused.getMessage().contains("secret-1"));
    }

    @Test
    void transportFailureBecomesATokenException() {
        Mockito.when(auth.clientCredentials(ArgumentMatchers.any()))
                .thenThrow(new ProcessingException("connection refused"));
        Assertions.assertThrows(ApiLayerTokenException.class, service()::accessToken);
    }

    @Test
    void emptyTokenIsRefused() {
        Mockito.when(auth.clientCredentials(ArgumentMatchers.any())).thenReturn(new TokenResponse("", 3600L, "Bearer"));
        Assertions.assertThrows(ApiLayerTokenException.class, service()::accessToken);
    }
}
