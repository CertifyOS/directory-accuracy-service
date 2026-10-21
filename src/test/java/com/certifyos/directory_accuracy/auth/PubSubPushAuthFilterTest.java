package com.certifyos.directory_accuracy.auth;

import com.certifyos.directory_accuracy.config.DirectoryAccuracyConfig;
import com.certifyos.directory_accuracy.http.Problem;
import jakarta.enterprise.inject.Instance;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

@SuppressWarnings("unchecked")
class PubSubPushAuthFilterTest {

    private Instance<DirectoryAccuracyConfig> cfg;
    private DirectoryAccuracyConfig.PubSub pubsub;
    private PushTokenVerifier verifier;
    private ContainerRequestContext rc;

    @BeforeEach
    void before() {
        DirectoryAccuracyConfig mapping = Mockito.mock(DirectoryAccuracyConfig.class);
        cfg = Mockito.mock(Instance.class);
        Mockito.when(cfg.get()).thenReturn(mapping);
        pubsub = Mockito.mock(DirectoryAccuracyConfig.PubSub.class);
        Mockito.when(mapping.pubsub()).thenReturn(pubsub);
        Mockito.when(pubsub.pushServiceAccount()).thenReturn(Optional.of("pubsub-push@test.iam.gserviceaccount.com"));
        Mockito.when(pubsub.pushAudience()).thenReturn(Optional.of("https://directory-accuracy.test/internal"));
        verifier = Mockito.mock(PushTokenVerifier.class);
        rc = Mockito.mock(ContainerRequestContext.class);
        UriInfo uri = Mockito.mock(UriInfo.class);
        Mockito.when(uri.getRequestUri())
                .thenReturn(URI.create("http://localhost/internal/vendor-exports/egress-events"));
        Mockito.when(rc.getUriInfo()).thenReturn(uri);
    }

    private Problem abortedWith() {
        ArgumentCaptor<Response> captor = ArgumentCaptor.forClass(Response.class);
        Mockito.verify(rc).abortWith(captor.capture());
        Assertions.assertEquals(401, captor.getValue().getStatus());
        Assertions.assertEquals(
                Problem.MEDIA_TYPE, captor.getValue().getMediaType().toString());
        return (Problem) captor.getValue().getEntity();
    }

    @Test
    void notConfiguredRefusesBeforeReadingTheToken() {
        Mockito.when(pubsub.pushAudience()).thenReturn(Optional.empty());
        new PubSubPushAuthFilter(cfg, verifier).filter(rc);
        Assertions.assertEquals("PUSH_NOT_CONFIGURED", abortedWith().code());
        Mockito.verifyNoInteractions(verifier);
    }

    @Test
    void missingBearerIsRefused() {
        Mockito.when(rc.getHeaderString(HttpHeaders.AUTHORIZATION)).thenReturn(null);
        new PubSubPushAuthFilter(cfg, verifier).filter(rc);
        Assertions.assertEquals("PUSH_TOKEN_REQUIRED", abortedWith().code());
        Mockito.verifyNoInteractions(verifier);
    }

    @Test
    void rejectedTokenIsRefused() {
        Mockito.when(rc.getHeaderString(HttpHeaders.AUTHORIZATION)).thenReturn("Bearer bad");
        Mockito.when(verifier.verify("bad")).thenThrow(new PushTokenRejectedException("token email is not verified"));
        new PubSubPushAuthFilter(cfg, verifier).filter(rc);
        Problem problem = abortedWith();
        Assertions.assertEquals("PUSH_TOKEN_REJECTED", problem.code());
        Assertions.assertEquals("token email is not verified", problem.detail());
    }

    @Test
    void acceptedTokenLetsTheRequestThrough() {
        Mockito.when(rc.getHeaderString(HttpHeaders.AUTHORIZATION)).thenReturn("Bearer good");
        Mockito.when(verifier.verify("good")).thenReturn(new PushIdentity("pubsub-push@test.iam.gserviceaccount.com"));
        new PubSubPushAuthFilter(cfg, verifier).filter(rc);
        Mockito.verify(rc, Mockito.never()).abortWith(Mockito.any());
    }
}
