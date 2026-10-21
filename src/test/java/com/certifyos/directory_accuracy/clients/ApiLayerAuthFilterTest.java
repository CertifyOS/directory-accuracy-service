package com.certifyos.directory_accuracy.clients;

import com.certifyos.directory_accuracy.auth.GoogleIdTokenService;
import com.certifyos.directory_accuracy.config.VendorExchangeConfig;
import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import java.io.IOException;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** The two api-layer client filters: Auth0 bearer, and the IAP token in Proxy-Authorization. */
class ApiLayerAuthFilterTest {

    private ApiLayerTokenService tokens;
    private GoogleIdTokenService google;
    private VendorExchangeConfig.ApiLayer apiLayer;
    private VendorExchangeConfig cfg;
    private MultivaluedMap<String, Object> headers;
    private ClientRequestContext request;

    @BeforeEach
    void before() {
        tokens = Mockito.mock(ApiLayerTokenService.class);
        google = Mockito.mock(GoogleIdTokenService.class);
        cfg = Mockito.mock(VendorExchangeConfig.class);
        apiLayer = Mockito.mock(VendorExchangeConfig.ApiLayer.class);
        Mockito.when(cfg.apiLayer()).thenReturn(apiLayer);
        Mockito.when(tokens.accessToken()).thenReturn("auth0-token");
        Mockito.when(google.idToken("/projects/1/global/backendServices/2")).thenReturn("iap-token");
        headers = new MultivaluedHashMap<>();
        request = Mockito.mock(ClientRequestContext.class);
        Mockito.when(request.getHeaders()).thenReturn(headers);
    }

    @Test
    void authFilterSendsTheAuth0Bearer() throws IOException {
        new ApiLayerAuthFilter(tokens).filter(request);
        Assertions.assertEquals("Bearer auth0-token", headers.getFirst(HttpHeaders.AUTHORIZATION));
        Assertions.assertFalse(headers.containsKey(ApiLayerIapFilter.PROXY_AUTHORIZATION));
    }

    @Test
    void iapFilterSendsTheIdTokenWhenApiLayerIsBehindIap() throws IOException {
        Mockito.when(apiLayer.iapAudience()).thenReturn(Optional.of("/projects/1/global/backendServices/2"));
        new ApiLayerIapFilter(google, cfg).filter(request);
        Assertions.assertEquals("Bearer iap-token", headers.getFirst(ApiLayerIapFilter.PROXY_AUTHORIZATION));
        Assertions.assertFalse(headers.containsKey(HttpHeaders.AUTHORIZATION));
    }

    @Test
    void iapFilterSendsNothingWithoutAnAudience() throws IOException {
        Mockito.when(apiLayer.iapAudience()).thenReturn(Optional.empty());
        new ApiLayerIapFilter(google, cfg).filter(request);
        Assertions.assertTrue(headers.isEmpty());
        Mockito.verifyNoInteractions(google);
    }

    @Test
    void aTokenFailureIsAnIoExceptionForTheCaller() {
        Mockito.when(tokens.accessToken()).thenThrow(new ApiLayerNotConfiguredException());
        Assertions.assertThrows(IOException.class, () -> new ApiLayerAuthFilter(tokens).filter(request));
    }
}
