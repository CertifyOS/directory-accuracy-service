package com.certifyos.vendor_exchange.clients;

import com.certifyos.vendor_exchange.ApiTestProfile;
import com.certifyos.vendor_exchange.MongoResource;
import com.certifyos.vendor_exchange.auth.DalUserClient;
import com.certifyos.vendor_exchange.auth.GoogleIdTokenService;
import com.certifyos.vendor_exchange.auth.WireMockDal;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.Response;
import java.lang.reflect.RecordComponent;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * The DAL batch, slice, operational-value and tenant-configuration clients against a WireMock DAL:
 * the IAP bearer, the two headers the DAL interceptor requires, paths and the shapes read back.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@QuarkusTestResource(WireMockDal.class)
@TestProfile(ApiTestProfile.class)
class DalClientsIT {

    static final String TENANT = "org-a";
    static final String SOURCE_TYPE = "candor:org-a";

    @Inject
    @RestClient
    DalBatchClient batch;

    @Inject
    @RestClient
    DalSliceClient slices;

    @Inject
    @RestClient
    DalOperationalValueClient operationalValues;

    @Inject
    @RestClient
    DalTenantConfigClient tenantConfigs;

    // The real token service would reach for Application Default Credentials, which CI does not have.
    @InjectMock
    GoogleIdTokenService tokens;

    @BeforeEach
    void before() {
        Mockito.when(tokens.idToken("test-dal-iap-client-id")).thenReturn("dal-id-token");
        WireMockDal.resetRequests();
    }

    private static RequestPatternBuilder withDalHeaders(RequestPatternBuilder pattern) {
        return pattern.withHeader("Authorization", WireMock.equalTo("Bearer dal-id-token"))
                .withHeader("requesting-user-id", WireMock.equalTo(DalUserClient.REQUESTING_USER_ID))
                .withHeader(DalUserClient.ORGANIZATION_HEADER, WireMock.equalTo(TENANT));
    }

    @Test
    void batchSendsTransactionAndHeaders() {
        // DAL transactional_batch.model.OperationResult(alias, result), one per operation.
        WireMockDal.stubFor(
                WireMock.post(WireMock.urlEqualTo("/batch"))
                        .willReturn(
                                WireMock.okJson(
                                        """
                        [{"alias": "practitioner", "result": {"certifyId": "cert-1"}}]
                        """)));
        DalOperation upsert = new DalOperation(
                "CorePractitioner",
                "practitioner",
                "UPSERT",
                Map.of("sourceType", SOURCE_TYPE, "crosswalkId", "cert-1", "data", Map.of("first_name", "Ann")),
                Map.of(),
                List.of());

        List<DalOperationResult> results;
        try (Response response = batch.execute(TENANT, new DalBatchRequest(true, List.of(upsert)))) {
            Assertions.assertEquals(200, response.getStatus());
            results = response.readEntity(new GenericType<List<DalOperationResult>>() {});
        }

        Assertions.assertEquals(1, results.size());
        Assertions.assertEquals("practitioner", results.get(0).alias());
        Map<?, ?> result = Assertions.assertInstanceOf(Map.class, results.get(0).result());
        Assertions.assertEquals("cert-1", result.get("certifyId"));
        // The DAL's OperationResult has no per-operation success flag or error; a failed batch is the HTTP status.
        Assertions.assertEquals(
                List.of("alias", "result"),
                Arrays.stream(DalOperationResult.class.getRecordComponents())
                        .map(RecordComponent::getName)
                        .toList());
        WireMockDal.verify(
                1,
                withDalHeaders(WireMock.postRequestedFor(WireMock.urlEqualTo("/batch")))
                        .withHeader("Content-Type", WireMock.containing("application/json"))
                        .withRequestBody(
                                WireMock.equalToJson(
                                        """
                                {"transaction": true,
                                 "operations": [{"resource": "CorePractitioner", "alias": "practitioner",
                                   "operation": "UPSERT",
                                   "body": {"sourceType": "candor:org-a", "crosswalkId": "cert-1",
                                            "data": {"first_name": "Ann"}},
                                   "filters": {}, "returning": []}]}
                                """)));
    }

    @Test
    void sliceReadMapsData() {
        // DAL core_practitioner.resource.dto.CorePractitionerResponse.
        WireMockDal.stubFor(
                WireMock.get(WireMock.urlEqualTo("/core/practitioner/candor:org-a/cert-1"))
                        .willReturn(
                                WireMock.okJson(
                                        """
                        {"certifyId": "cert-1", "crosswalkId": "cert-1", "sourceId": "src-1", "sourceUrl": null,
                         "sourceType": "candor:org-a", "sourceName": "Candor", "sourceDescription": null,
                         "sourceTenantId": "org-a", "sourceTags": [],
                         "data": {"first_name": "Ann", "middle_name": null, "contacts": [{"phone": "3036790177"}]},
                         "rawSourceData": null, "survivorId": null,
                         "createdAt": "2026-10-14T12:00:00Z", "updatedAt": "2026-10-15T12:00:00Z"}
                        """)));
        // DAL core_location.resource.dto.CoreLocationResponse: the id field is certifyLocationId.
        WireMockDal.stubFor(
                WireMock.get(WireMock.urlEqualTo("/core/locations/candor:org-a/loc-1"))
                        .willReturn(
                                WireMock.okJson(
                                        """
                        {"certifyLocationId": "cert-loc-1", "crosswalkId": "loc-1", "sourceId": "src-1",
                         "sourceUrl": null, "sourceType": "candor:org-a", "sourceName": "Candor",
                         "sourceDescription": null, "handicapAccessibilityId": null,
                         "data": {"address_line_1": "1 Main St"}, "rawSourceData": null, "survivorId": null,
                         "createdAt": "2026-10-14T12:00:00Z", "updatedAt": "2026-10-15T12:00:00Z"}
                        """)));
        // DAL core_entity_address.resource.dto.CoreEntityAddressResponse.
        WireMockDal.stubFor(
                WireMock.get(WireMock.urlEqualTo("/core/entity-addresses/candor:org-a/addr-1"))
                        .willReturn(
                                WireMock.okJson(
                                        """
                        {"certifyId": "cert-addr-1", "crosswalkId": "addr-1", "sourceId": "src-1",
                         "sourceUrl": null, "sourceType": "candor:org-a", "sourceName": "Candor",
                         "sourceDescription": null, "survivorId": null, "data": {"phone": "3036790177"},
                         "rawSourceData": null, "createdAt": "2026-10-14T12:00:00Z",
                         "updatedAt": "2026-10-15T12:00:00Z"}
                        """)));

        Slice practitioner = slices.practitioner(TENANT, SOURCE_TYPE, "cert-1");
        Slice location = slices.location(TENANT, SOURCE_TYPE, "loc-1");
        Slice address = slices.entityAddress(TENANT, SOURCE_TYPE, "addr-1");

        Assertions.assertEquals("cert-1", practitioner.certifyId());
        Assertions.assertEquals(SOURCE_TYPE, practitioner.sourceType());
        Assertions.assertEquals("Ann", practitioner.data().get("first_name"));
        Assertions.assertTrue(practitioner.data().containsKey("middle_name"), "a null value survives the copy");
        Assertions.assertEquals(
                List.of(Map.of("phone", "3036790177")), practitioner.data().get("contacts"));
        Assertions.assertEquals(Instant.parse("2026-10-15T12:00:00Z"), practitioner.updatedAt());
        Assertions.assertEquals("cert-loc-1", location.certifyId(), "read from certifyLocationId");
        Assertions.assertEquals("loc-1", location.crosswalkId());
        Assertions.assertEquals("cert-addr-1", address.certifyId());
        Assertions.assertEquals("addr-1", address.crosswalkId());
        WireMockDal.verify(
                1,
                withDalHeaders(
                        WireMock.getRequestedFor(WireMock.urlEqualTo("/core/practitioner/candor:org-a/cert-1"))));
        WireMockDal.verify(
                1, withDalHeaders(WireMock.getRequestedFor(WireMock.urlEqualTo("/core/locations/candor:org-a/loc-1"))));
        WireMockDal.verify(
                1,
                withDalHeaders(
                        WireMock.getRequestedFor(WireMock.urlEqualTo("/core/entity-addresses/candor:org-a/addr-1"))));
    }

    @Test
    void sliceMissingIs404() {
        // DAL common.exception.ApiError, as GlobalExceptionMapper renders EntityNotFoundException.
        WireMockDal.stubFor(
                WireMock.get(WireMock.urlEqualTo("/core/practitioner/candor:org-a/cert-none"))
                        .willReturn(
                                WireMock.aResponse()
                                        .withStatus(404)
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(
                                                """
                                {"errors": [{"reason": "Not Found", "detail": "CorePractitioner not found",
                                  "httpStatus": 404, "title": "Entity Not Found"}]}
                                """)));

        WebApplicationException missing = Assertions.assertThrows(
                WebApplicationException.class, () -> slices.practitioner(TENANT, SOURCE_TYPE, "cert-none"));

        Assertions.assertEquals(404, missing.getResponse().getStatus());
        // The DAL also answers 404 when a required header is missing; the headers were all there.
        WireMockDal.verify(
                1,
                withDalHeaders(
                        WireMock.getRequestedFor(WireMock.urlEqualTo("/core/practitioner/candor:org-a/cert-none"))));
    }

    @Test
    void practitionerAndLocationOvRead() {
        // DAL practitioner_operational_value.resource.dto.PractitionerOVResponse; crosswalks is left
        // out when null. contributionMap is present only because includeLineage=true was asked for.
        // Open: the DAL sends contributingSlices as slice id strings; these stubs keep the object
        // entries OperationalValue reads today until the element type is ruled on.
        WireMockDal.stubFor(
                WireMock.get(WireMock.urlPathEqualTo("/practitioners/ov/cert-1"))
                        .withQueryParam("includeLineage", WireMock.equalTo("true"))
                        .willReturn(
                                WireMock.okJson(
                                        """
                        {"certifyId": "cert-1", "tenantId": "org-a",
                         "data": {"first_name": "Ann", "last_name": "Lee"},
                         "contributingSlices": [{"sourceType": "candor:org-a", "crosswalkId": "cert-1"}],
                         "contributionMap": {"first_name": "candor:org-a"},
                         "credentialingStatus": null, "nextCredentialingDate": null, "credentialingDueDate": null}
                        """)));
        // DAL location_operational_value.resource.dto.LocationOVResponse: the id field is certifyLocationId.
        WireMockDal.stubFor(
                WireMock.get(WireMock.urlPathEqualTo("/locations/ov/loc-1"))
                        .withQueryParam("includeLineage", WireMock.equalTo("true"))
                        .willReturn(
                                WireMock.okJson(
                                        """
                        {"certifyLocationId": "loc-1", "tenantId": "org-a",
                         "data": {"address_line_1": "1 Main St"}, "contributingSlices": [],
                         "contributionMap": {"address_line_1": "candor:org-a"}}
                        """)));

        OperationalValue practitioner = operationalValues.practitioner(TENANT, "cert-1", true);
        OperationalValue location = operationalValues.location(TENANT, "loc-1", true);

        Assertions.assertEquals("cert-1", practitioner.certifyId());
        Assertions.assertEquals("Lee", practitioner.data().get("last_name"));
        Assertions.assertEquals(
                List.of(Map.of("sourceType", SOURCE_TYPE, "crosswalkId", "cert-1")), practitioner.contributingSlices());
        Assertions.assertEquals(Map.of("first_name", SOURCE_TYPE), practitioner.contributionMap());
        Assertions.assertEquals("loc-1", location.certifyId(), "read from certifyLocationId");
        Assertions.assertEquals("1 Main St", location.data().get("address_line_1"));
        Assertions.assertEquals(List.of(), location.contributingSlices());
        Assertions.assertEquals(Map.of("address_line_1", SOURCE_TYPE), location.contributionMap());
        WireMockDal.verify(
                1,
                withDalHeaders(WireMock.getRequestedFor(WireMock.urlPathEqualTo("/practitioners/ov/cert-1")))
                        .withQueryParam("includeLineage", WireMock.equalTo("true")));
        WireMockDal.verify(
                1,
                withDalHeaders(WireMock.getRequestedFor(WireMock.urlPathEqualTo("/locations/ov/loc-1")))
                        .withQueryParam("includeLineage", WireMock.equalTo("true")));
    }

    @Test
    void tenantConfigurationsPage() {
        // DAL common.pagination.model.PagedResponse(data, links, totalCount) of
        // tenant_configuration.resource.dto.TenantConfigurationResponse rows.
        WireMockDal.stubFor(
                WireMock.get(WireMock.urlPathEqualTo("/tenants/org-a/configurations"))
                        .willReturn(
                                WireMock.okJson(
                                        """
                        {"data": [{"id": "cfg-1", "tenantId": "org-a", "namespace": "mdm",
                                   "configurationTypeId": "source_ranking",
                                   "configuration": {"ranking": ["candor:org-a"]},
                                   "updatedAt": "2026-10-15T12:00:00Z", "createdAt": "2026-10-14T12:00:00Z",
                                   "isEnabled": true}],
                         "links": {"self": "/tenants/org-a/configurations?page=0&size=50", "next": null,
                                   "prev": null},
                         "totalCount": 1}
                        """)));

        TenantConfigurationPage page = tenantConfigs.list(TENANT, TENANT, 0, 50);

        Assertions.assertEquals(1L, page.totalCount());
        Assertions.assertEquals("source_ranking", page.data().get(0).get("configurationTypeId"));
        Assertions.assertEquals(
                Map.of("ranking", List.of(SOURCE_TYPE)), page.data().get(0).get("configuration"));
        WireMockDal.verify(
                1,
                withDalHeaders(WireMock.getRequestedFor(WireMock.urlPathEqualTo("/tenants/org-a/configurations")))
                        .withQueryParam("page", WireMock.equalTo("0"))
                        .withQueryParam("size", WireMock.equalTo("50")));
    }
}
