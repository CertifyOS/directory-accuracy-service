package com.certifyos.directory_accuracy.clients;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.auth.GoogleIdTokenService;
import com.github.tomakehurst.wiremock.client.WireMock;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** The api-layer client against WireMock: Auth0 bearer, tenant header, query names, template read and multipart create. */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@QuarkusTestResource(WireMockUpstreams.class)
@TestProfile(ApiTestProfile.class)
class ApiLayerClientIT {

    @Inject
    @RestClient
    ApiLayerClient apiLayer;

    @Inject
    @RestClient
    ApiLayerAuthClient authClient;

    @InjectMock
    ApiLayerTokenService tokens;

    @InjectMock
    GoogleIdTokenService google;

    @BeforeEach
    void before() {
        Mockito.when(tokens.accessToken()).thenReturn("api-layer-token");
        Mockito.when(google.idToken("/projects/1/global/backendServices/2")).thenReturn("iap-token");
    }

    @Test
    void clientCredentialsCallCarriesTheIapTokenAndNoAuth0Bearer() {
        WireMockUpstreams.stubFor(WireMock.post(WireMock.urlEqualTo("/auth/client-credentials"))
                .willReturn(WireMock.okJson("{\"accessToken\":\"t-1\",\"expiresIn\":3600,\"tokenType\":\"Bearer\"}")));

        ApiLayerAuthClient.TokenResponse token =
                authClient.clientCredentials(new ApiLayerAuthClient.TokenRequest("client-1", "secret-1"));

        Assertions.assertEquals("t-1", token.accessToken());
        Assertions.assertEquals(3600L, token.expiresIn());
        WireMockUpstreams.verify(
                1,
                WireMock.postRequestedFor(WireMock.urlEqualTo("/auth/client-credentials"))
                        .withHeader(ApiLayerIapFilter.PROXY_AUTHORIZATION, WireMock.equalTo("Bearer iap-token"))
                        .withoutHeader("Authorization")
                        .withHeader("Content-Type", WireMock.containing("application/json"))
                        .withRequestBody(
                                WireMock.equalToJson("{\"clientId\":\"client-1\",\"clientSecret\":\"secret-1\"}")));
    }

    @Test
    void practitionerPageCarriesTheFilterPagingAndHeaders() {
        WireMockUpstreams.stubFor(WireMock.get(WireMock.urlPathEqualTo("/practitioners"))
                .willReturn(WireMock.okJson("{\"data\":[{\"id\":\"p-1\",\"npi\":\"1234567893\",\"firstName\":\"A\"}],"
                        + "\"totalCount\":1}")));
        String filter = "{\"data.delegationStatus\":{\"in\":[\"Direct\"]}}";

        PagedPractitioners page = apiLayer.practitionerFindMany("org-a", filter, 0, ApiLayerClient.MAX_PAGE_SIZE);

        Assertions.assertEquals(1L, page.totalCount());
        Assertions.assertEquals(
                new PractitionerRef("p-1", "1234567893"), page.data().get(0));
        WireMockUpstreams.verify(
                1,
                WireMock.getRequestedFor(WireMock.urlPathEqualTo("/practitioners"))
                        .withHeader("Authorization", WireMock.equalTo("Bearer api-layer-token"))
                        .withHeader(ApiLayerIapFilter.PROXY_AUTHORIZATION, WireMock.equalTo("Bearer iap-token"))
                        .withHeader(ApiLayerClient.TENANT_HEADER, WireMock.equalTo("org-a"))
                        .withQueryParam("filter", WireMock.equalTo(filter))
                        .withQueryParam("page", WireMock.equalTo("0"))
                        .withQueryParam("size", WireMock.equalTo("100")));
    }

    @Test
    void templateReadKeepsThePinnedFields() {
        WireMockUpstreams.stubFor(WireMock.get(WireMock.urlEqualTo("/api/v1/egress-templates/t-1"))
                .willReturn(WireMock.okJson("{\"id\":\"t-1\",\"tenantId\":\"org-a\",\"templateName\":\"Candor\","
                        + "\"entityType\":\"practitioner\",\"status\":\"active\",\"version\":3,"
                        + "\"mappingsCsvUrl\":\"gs://egress-templates/org-a/t-1/v3/mappings.csv\",\"separator\":\";\","
                        + "\"outputFormat\":\"csv\",\"rowExpansionKeys\":[\"locations\"],\"scheduleEnabled\":false}")));

        EgressTemplate template = apiLayer.getEgressTemplate("org-a", "t-1");

        Assertions.assertEquals(3, template.version());
        Assertions.assertEquals("gs://egress-templates/org-a/t-1/v3/mappings.csv", template.mappingsCsvUrl());
        Assertions.assertEquals(java.util.List.of("locations"), template.rowExpansionKeys());
        WireMockUpstreams.verify(
                1,
                WireMock.getRequestedFor(WireMock.urlEqualTo("/api/v1/egress-templates/t-1"))
                        .withHeader("Authorization", WireMock.equalTo("Bearer api-layer-token"))
                        .withHeader(ApiLayerClient.TENANT_HEADER, WireMock.equalTo("org-a")));
    }

    @Test
    void templateCreateIsMultipartWithTheUploadFlowsParts() {
        WireMockUpstreams.stubFor(WireMock.post(WireMock.urlEqualTo("/api/v1/egress-templates"))
                .willReturn(WireMock.aResponse()
                        .withStatus(201)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"id\":\"t-new\",\"version\":1,\"status\":\"active\"}")));
        byte[] csv = "source,target\nnpi,NPI\n".getBytes(StandardCharsets.UTF_8);

        EgressTemplate created = apiLayer.createEgressTemplate(
                "org-a",
                csv,
                "Candor export",
                "Vendor template",
                "practitioner",
                "csv",
                ",",
                "[\"locations\"]",
                "active");

        Assertions.assertEquals("t-new", created.id());
        WireMockUpstreams.verify(
                1,
                WireMock.postRequestedFor(WireMock.urlEqualTo("/api/v1/egress-templates"))
                        .withHeader("Authorization", WireMock.equalTo("Bearer api-layer-token"))
                        .withHeader(ApiLayerClient.TENANT_HEADER, WireMock.equalTo("org-a"))
                        .withHeader("Content-Type", WireMock.containing("multipart/form-data"))
                        .withRequestBodyPart(WireMock.aMultipart()
                                .withName("file")
                                .withBody(WireMock.containing("npi,NPI"))
                                .build())
                        .withRequestBodyPart(WireMock.aMultipart()
                                .withName("templateName")
                                .withBody(WireMock.equalTo("Candor export"))
                                .build())
                        .withRequestBodyPart(WireMock.aMultipart()
                                .withName("rowExpansionKeys")
                                .withBody(WireMock.equalTo("[\"locations\"]"))
                                .build()));
    }
}
