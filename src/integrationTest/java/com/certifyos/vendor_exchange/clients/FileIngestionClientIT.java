package com.certifyos.vendor_exchange.clients;

import com.certifyos.vendor_exchange.ApiTestProfile;
import com.certifyos.vendor_exchange.MongoResource;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * The file-ingestion client against WireMock: Auth0 bearer, tenant and identifier headers, the
 * design's register body, the NDJSON row stream and the statuses the jobs branch on.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@QuarkusTestResource(WireMockUpstreams.class)
@TestProfile(ApiTestProfile.class)
class FileIngestionClientIT {

    static final String TENANT = "org-xyz";
    static final String IDENTIFIER = "vendor-ingestion";
    static final String REGISTER = "/v1/ingestion";
    static final String FILE_NAME = "org-xyz_org-xyz-candor-2026-10-001_CB-2026-10-001_20261015.csv";
    static final String UPLOAD_ID =
            "vendor-sftp|to/org-xyz/org-xyz_org-xyz-candor-2026-10-001_CB-2026-10-001_20261015.csv|1760520000123456";
    static final String SOURCE_URI =
            "gs://vendor-ingestion-archive/org-xyz/org-xyz-candor-2026-10-001/1760520000123456/" + FILE_NAME;
    static final String ROW_13 = "{\"sheet\":\"S\",\"row\":13,\"status\":\"VALID\","
            + "\"values\":{\"phone\":{\"value\":\"(303) 679-0177\",\"status\":\"VALID\"}}}";
    static final String ROW_14 = "{\"sheet\":\"S\",\"row\":14,\"status\":\"INVALID\","
            + "\"values\":{\"fax\":{\"value\":\"x\",\"status\":\"INVALID\",\"problems\":[\"pattern\"]}},"
            + "\"warnings\":[\"w1\"],\"raw\":[\"x\"]}";

    @Inject
    @RestClient
    FileIngestionClient fileIngestion;

    @Inject
    ObjectMapper mapper;

    @InjectMock
    ApiLayerTokenService tokens;

    @BeforeEach
    void before() {
        Mockito.when(tokens.accessToken()).thenReturn("tok");
        WireMockUpstreams.resetRequests();
    }

    private static FileIngestionRegisterRequest request(String externalRef) {
        return new FileIngestionRegisterRequest(
                "map-candor-recs-v1",
                FILE_NAME,
                SOURCE_URI,
                externalRef,
                Map.of("exportBatchId", "org-xyz-candor-2026-10-001", "uploadId", UPLOAD_ID));
    }

    private static void stubRegister(String externalRef, int status, String body) {
        WireMockUpstreams.stubFor(WireMock.post(WireMock.urlEqualTo(REGISTER))
                .withRequestBody(WireMock.matchingJsonPath("$.externalRef", WireMock.equalTo(externalRef)))
                .willReturn(WireMock.aResponse()
                        .withStatus(status)
                        .withHeader("Content-Type", "application/json")
                        .withBody(body)));
    }

    @Test
    void registerNewIs201() {
        String externalRef = "org-xyz-candor-2026-10-001|1760520000123456";
        stubRegister(externalRef, 201, "{\"ingestionId\":\"ing-new\",\"status\":\"RECEIVED\"}");

        try (Response response = fileIngestion.register(TENANT, IDENTIFIER, request(externalRef))) {
            Assertions.assertEquals(201, response.getStatus());
            FileIngestionRegisterResponse registered = response.readEntity(FileIngestionRegisterResponse.class);
            Assertions.assertEquals(new FileIngestionRegisterResponse("ing-new", "RECEIVED"), registered);
        }
        WireMockUpstreams.verify(
                1,
                WireMock.postRequestedFor(WireMock.urlEqualTo(REGISTER))
                        .withHeader(FileIngestionClient.TENANT_HEADER, WireMock.equalTo(TENANT))
                        .withHeader(FileIngestionClient.IDENTIFIER_HEADER, WireMock.equalTo(IDENTIFIER))
                        .withHeader("Authorization", WireMock.equalTo("Bearer tok"))
                        .withoutHeader(ApiLayerIapFilter.PROXY_AUTHORIZATION)
                        .withHeader("Content-Type", WireMock.containing("application/json"))
                        .withRequestBody(WireMock.equalToJson("{\"mappingId\":\"map-candor-recs-v1\","
                                + "\"filename\":\"" + FILE_NAME + "\","
                                + "\"sourceUri\":\"" + SOURCE_URI + "\","
                                + "\"externalRef\":\"org-xyz-candor-2026-10-001|1760520000123456\","
                                + "\"meta\":{\"exportBatchId\":\"org-xyz-candor-2026-10-001\","
                                + "\"uploadId\":\"" + UPLOAD_ID + "\"}}")));
    }

    @Test
    void registerRepeatIs200() {
        String externalRef = "org-xyz-candor-2026-10-001|1760520000999999";
        stubRegister(
                externalRef, 200, "{\"ingestionId\":\"ing-seen\",\"status\":\"PARSING\",\"uploadUrl\":\"https://x\"}");

        try (Response response = fileIngestion.register(TENANT, IDENTIFIER, request(externalRef))) {
            Assertions.assertEquals(200, response.getStatus());
            Assertions.assertEquals(
                    new FileIngestionRegisterResponse("ing-seen", "PARSING"),
                    response.readEntity(FileIngestionRegisterResponse.class));
        }
    }

    @Test
    void mappingUnknownIs422() {
        String externalRef = "org-xyz-candor-2026-10-001|1760520000422422";
        stubRegister(externalRef, 422, "{\"code\":\"MAPPING_UNKNOWN\"}");

        WebApplicationException refused = Assertions.assertThrows(
                WebApplicationException.class, () -> fileIngestion.register(TENANT, IDENTIFIER, request(externalRef)));

        Assertions.assertEquals(422, refused.getResponse().getStatus());
    }

    @Test
    void rowsStreamYieldsOneRowPerLine() throws Exception {
        WireMockUpstreams.stubFor(WireMock.get(WireMock.urlPathEqualTo("/v1/ingestion-rows/ing-rows"))
                .willReturn(WireMock.aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/x-ndjson")
                        .withBody(ROW_13 + "\n" + ROW_14 + "\n")));

        List<FileIngestionRow> rows;
        try (InputStream stream = fileIngestion.rows(TENANT, IDENTIFIER, "ing-rows", "12");
                MappingIterator<FileIngestionRow> lines =
                        mapper.readerFor(FileIngestionRow.class).readValues(stream)) {
            rows = lines.readAll();
        }

        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals(13, rows.get(0).row());
        Assertions.assertEquals(
                "(303) 679-0177", rows.get(0).values().get("phone").value());
        Assertions.assertEquals(14, rows.get(1).row());
        Assertions.assertEquals(
                List.of("pattern"), rows.get(1).values().get("fax").problems());
        WireMockUpstreams.verify(
                1,
                WireMock.getRequestedFor(WireMock.urlPathEqualTo("/v1/ingestion-rows/ing-rows"))
                        .withQueryParam("after", WireMock.equalTo("12"))
                        .withHeader("Accept", WireMock.containing("application/x-ndjson"))
                        .withHeader(FileIngestionClient.TENANT_HEADER, WireMock.equalTo(TENANT))
                        .withHeader(FileIngestionClient.IDENTIFIER_HEADER, WireMock.equalTo(IDENTIFIER))
                        .withHeader("Authorization", WireMock.equalTo("Bearer tok")));
    }

    @Test
    void rowsRefused429Propagates() {
        WireMockUpstreams.stubFor(WireMock.get(WireMock.urlPathEqualTo("/v1/ingestion-rows/ing-busy"))
                .willReturn(WireMock.aResponse()
                        .withStatus(429)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"code\":\"TOO_MANY_STREAMS\"}")));

        WebApplicationException refused = Assertions.assertThrows(
                WebApplicationException.class, () -> fileIngestion.rows(TENANT, IDENTIFIER, "ing-busy", null));

        Assertions.assertEquals(429, refused.getResponse().getStatus());
        WireMockUpstreams.verify(
                1,
                WireMock.getRequestedFor(WireMock.urlPathEqualTo("/v1/ingestion-rows/ing-busy"))
                        .withQueryParam("after", WireMock.absent()));
    }

    @Test
    void statusParsesCountsAndInstants() {
        WireMockUpstreams.stubFor(WireMock.get(WireMock.urlEqualTo("/v1/ingestion/ing-status"))
                .willReturn(WireMock.okJson("{\"ingestionId\":\"ing-status\",\"status\":\"PARTIAL\","
                        + "\"counts\":{\"populatedRows\":120,\"validRows\":118,\"invalidRows\":2},"
                        + "\"registeredAt\":\"2026-10-15T08:00:00Z\",\"terminalAt\":\"2026-10-15T08:20:30Z\","
                        + "\"invalidReason\":null,\"mappingId\":\"map-1\"}")));

        FileIngestionStatus status = fileIngestion.status(TENANT, IDENTIFIER, "ing-status");

        Assertions.assertEquals(
                new FileIngestionStatus(
                        "ing-status",
                        "PARTIAL",
                        new FileIngestionCounts(120L, 118L, 2L),
                        Instant.parse("2026-10-15T08:00:00Z"),
                        Instant.parse("2026-10-15T08:20:30Z"),
                        null),
                status);
        WireMockUpstreams.verify(
                1,
                WireMock.getRequestedFor(WireMock.urlEqualTo("/v1/ingestion/ing-status"))
                        .withHeader(FileIngestionClient.TENANT_HEADER, WireMock.equalTo(TENANT))
                        .withHeader(FileIngestionClient.IDENTIFIER_HEADER, WireMock.equalTo(IDENTIFIER))
                        .withHeader("Authorization", WireMock.equalTo("Bearer tok")));
    }

    @Test
    void markConsumedIsOk() {
        WireMockUpstreams.stubFor(WireMock.post(WireMock.urlEqualTo("/v1/ingestion-complete/ing-done"))
                .willReturn(WireMock.aResponse().withStatus(200)));

        try (Response response = fileIngestion.markConsumed(TENANT, IDENTIFIER, "ing-done")) {
            Assertions.assertEquals(200, response.getStatus());
        }
        WireMockUpstreams.verify(
                1,
                WireMock.postRequestedFor(WireMock.urlEqualTo("/v1/ingestion-complete/ing-done"))
                        .withHeader(FileIngestionClient.TENANT_HEADER, WireMock.equalTo(TENANT))
                        .withHeader(FileIngestionClient.IDENTIFIER_HEADER, WireMock.equalTo(IDENTIFIER))
                        .withHeader("Authorization", WireMock.equalTo("Bearer tok")));
    }
}
