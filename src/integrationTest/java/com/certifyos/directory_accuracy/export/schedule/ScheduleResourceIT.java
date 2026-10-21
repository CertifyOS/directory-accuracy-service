package com.certifyos.directory_accuracy.export.schedule;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.audit.AuditEvent;
import com.certifyos.directory_accuracy.audit.AuditEventType;
import com.certifyos.directory_accuracy.audit.AuditRepository;
import com.certifyos.directory_accuracy.auth.GoogleIdTokenService;
import com.certifyos.directory_accuracy.auth.UserContextFilter;
import com.certifyos.directory_accuracy.auth.WireMockDal;
import com.certifyos.directory_accuracy.clients.ApiLayerTokenService;
import com.certifyos.directory_accuracy.clients.WireMockUpstreams;
import com.certifyos.directory_accuracy.export.batch.BatchState;
import com.certifyos.directory_accuracy.export.batch.ExportBatchRepository;
import com.certifyos.directory_accuracy.http.Problem;
import com.github.tomakehurst.wiremock.client.WireMock;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.security.TestSecurity;
import io.quarkus.test.security.jwt.Claim;
import io.quarkus.test.security.jwt.JwtSecurity;
import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import jakarta.inject.Inject;
import java.util.List;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * The schedule endpoints end to end: platform token, DAL membership, permission, template
 * provisioning through api-layer (WireMock), the Mongo write, the audit event, and problem+json
 * refusals with their codes.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@QuarkusTestResource(WireMockDal.class)
@QuarkusTestResource(WireMockUpstreams.class)
@TestProfile(ApiTestProfile.class)
class ScheduleResourceIT {

    static final String EMAIL = UserContextFilter.EMAIL_CLAIM;
    static final String TENANT = "org-sched";
    static final String BASE = "/v1/vendor-exports/schedules/" + TENANT + "/";
    static final String TEMPLATES = "/api/v1/egress-templates";
    static final String VALID_BODY =
            "{\"cadence\":{\"type\":\"monthly\",\"dayOfMonth\":1},\"timezone\":\"America/New_York\","
                    + "\"selection\":{\"data.delegationStatus\":{\"in\":[\"Direct\"]},\"credentialingStatus\":{\"eq\":\"Approved\"}}}";

    @InjectMock
    GoogleIdTokenService google;

    @InjectMock
    ApiLayerTokenService apiLayerTokens;

    @Inject
    AuditRepository audit;

    @Inject
    ExportBatchRepository batches;

    @Inject
    org.jobrunr.storage.StorageProvider storage;

    // Emails are unique to this class: the DAL lookup is cached per email for the life of the
    // test application, which other classes with the same profile share, so a stub for an email
    // another class already resolved is never consulted.
    @BeforeEach
    void before() {
        Mockito.when(google.idToken("test-dal-iap-client-id")).thenReturn("dal-id-token");
        Mockito.when(google.idToken("/projects/1/global/backendServices/2")).thenReturn("iap-token");
        Mockito.when(apiLayerTokens.accessToken()).thenReturn("api-layer-token");
        WireMockUpstreams.resetRequests();
        WireMockDal.stubMember("sched@certifyos.com", TENANT);
        WireMockDal.stubMember("sched-other@certifyos.com", "org-sched-other");
    }

    private static RequestSpecification member() {
        return RestAssured.given().header("tenant-id", TENANT).contentType("application/json");
    }

    private static String templateJson(String id, String name, int version) {
        return "{\"id\":\"" + id + "\",\"tenantId\":\"" + TENANT + "\",\"templateName\":\"" + name
                + "\",\"entityType\":\"practitioner\",\"status\":\"active\",\"version\":" + version + "}";
    }

    private static void stubNoTemplateThenCreate(String createdId) {
        WireMockUpstreams.stubFor(WireMock.get(WireMock.urlPathEqualTo(TEMPLATES))
                .withHeader("tenant-id", WireMock.equalTo(TENANT))
                .willReturn(WireMock.okJson("{\"templates\":[],\"total\":0,\"page\":0,\"size\":10}")));
        WireMockUpstreams.stubFor(WireMock.post(WireMock.urlPathEqualTo(TEMPLATES))
                .withHeader("tenant-id", WireMock.equalTo(TENANT))
                .willReturn(WireMock.jsonResponse(templateJson(createdId, TemplateProvisioner.TEMPLATE_NAME, 1), 201)));
    }

    private static Response put(String vendor, String body) {
        return member().body(body).put(BASE + vendor);
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "sched@certifyos.com")})
    void createProvisionsTheTemplateStoresTheScheduleAndAudits() {
        stubNoTemplateThenCreate("tpl-created");

        put("create", VALID_BODY)
                .then()
                .statusCode(201)
                .body("tenantId", Matchers.equalTo(TENANT))
                .body("vendor", Matchers.equalTo("create"))
                .body("enabled", Matchers.equalTo(true))
                .body("version", Matchers.equalTo(1))
                .body("egressTemplateId", Matchers.equalTo("tpl-created"))
                .body("cadence.type", Matchers.equalTo("monthly"))
                .body("cadence.dayOfMonth", Matchers.equalTo(1))
                .body("timezone", Matchers.equalTo("America/New_York"))
                .body("selection.'data.delegationStatus'.in", Matchers.contains("Direct"))
                .body("selection.credentialingStatus.eq", Matchers.equalTo("Approved"))
                .body("nextDueAt", Matchers.endsWith("T04:00:00Z"))
                .body("lastBatchId", Matchers.nullValue());

        WireMockUpstreams.verify(
                1,
                WireMock.postRequestedFor(WireMock.urlPathEqualTo(TEMPLATES))
                        .withHeader("Authorization", WireMock.equalTo("Bearer api-layer-token"))
                        .withHeader("Proxy-Authorization", WireMock.equalTo("Bearer iap-token"))
                        .withRequestBodyPart(WireMock.aMultipart()
                                .withName("templateName")
                                .withBody(WireMock.equalTo(TemplateProvisioner.TEMPLATE_NAME))
                                .build())
                        .withRequestBodyPart(WireMock.aMultipart()
                                .withName("file")
                                .withBody(WireMock.containing("output_column,attribute_path"))
                                .build()));
        List<AuditEvent> events = audit.findForSchedule(TENANT, "create", 10);
        Assertions.assertEquals(1, events.size());
        Assertions.assertEquals(AuditEventType.SCHEDULE_CREATED, events.get(0).type());
        Assertions.assertEquals("sched@certifyos.com", events.get(0).actor());
        Assertions.assertEquals("tpl-created", events.get(0).detail().get("egressTemplateId"));
        Assertions.assertTrue((Boolean) events.get(0).detail().get("templateCreated"));

        member().get(BASE + "create").then().statusCode(200).body("egressTemplateId", Matchers.equalTo("tpl-created"));
        member().get("/v1/vendor-exports/schedules")
                .then()
                .statusCode(200)
                .body("items.vendor", Matchers.hasItem("create"));
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "sched@certifyos.com")})
    void secondCreateIsExistsAndReplaceNeedsTheCurrentVersion() {
        stubNoTemplateThenCreate("tpl-replace");
        put("replace", VALID_BODY).then().statusCode(201);

        put("replace", VALID_BODY)
                .then()
                .statusCode(409)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("SCHEDULE_EXISTS"));
        WireMockUpstreams.verify(1, WireMock.postRequestedFor(WireMock.urlPathEqualTo(TEMPLATES)));

        String stale = VALID_BODY.replaceFirst("\\{", "{\"version\":7,");
        put("replace", stale)
                .then()
                .statusCode(409)
                .body("code", Matchers.equalTo("VERSION_STALE"))
                .body("detail", Matchers.containsString("version 1"));

        String replaced =
                "{\"version\":1,\"cadence\":{\"type\":\"cron\",\"expression\":\"0 6 1 * *\"},\"timezone\":\"UTC\","
                        + "\"selection\":{\"rosterIds\":{\"in\":[\"8b1f9f2e-8f0e-4d0a-9f0f-2f6a4a1d9c11\"]}}}";
        put("replace", replaced)
                .then()
                .statusCode(200)
                .body("version", Matchers.equalTo(2))
                .body("cadence.type", Matchers.equalTo("cron"))
                .body("cadence.expression", Matchers.equalTo("0 6 1 * *"))
                .body("timezone", Matchers.equalTo("UTC"))
                .body("nextDueAt", Matchers.endsWith("-01T06:00:00Z"))
                .body("egressTemplateId", Matchers.equalTo("tpl-replace"))
                .body("enabled", Matchers.equalTo(true));

        List<AuditEvent> events = audit.findForSchedule(TENANT, "replace", 10);
        Assertions.assertTrue(
                events.stream().anyMatch(event -> event.type() == AuditEventType.SCHEDULE_UPDATED),
                "an UPDATED event is written");
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "sched@certifyos.com")})
    void existingTemplateIsReusedAndTwoAreAConflict() {
        WireMockUpstreams.stubFor(WireMock.get(WireMock.urlPathEqualTo(TEMPLATES))
                .withQueryParam("search", WireMock.equalTo(TemplateProvisioner.TEMPLATE_NAME))
                .withQueryParam("status", WireMock.equalTo("active"))
                .withQueryParam("entityType", WireMock.equalTo("practitioner"))
                .willReturn(WireMock.okJson(
                        "{\"templates\":[" + templateJson("tpl-found", TemplateProvisioner.TEMPLATE_NAME, 4)
                                + "],\"total\":1,\"page\":0,\"size\":10}")));
        put("reuse", VALID_BODY).then().statusCode(201).body("egressTemplateId", Matchers.equalTo("tpl-found"));
        WireMockUpstreams.verify(0, WireMock.postRequestedFor(WireMock.urlPathEqualTo(TEMPLATES)));

        WireMockUpstreams.stubFor(WireMock.get(WireMock.urlPathEqualTo(TEMPLATES))
                .willReturn(
                        WireMock.okJson("{\"templates\":[" + templateJson("tpl-a", TemplateProvisioner.TEMPLATE_NAME, 1)
                                + "," + templateJson("tpl-b", TemplateProvisioner.TEMPLATE_NAME, 1)
                                + "],\"total\":2,\"page\":0,\"size\":10}")));
        put("ambiguous", VALID_BODY).then().statusCode(409).body("code", Matchers.equalTo("TEMPLATE_AMBIGUOUS"));
        member().get(BASE + "ambiguous").then().statusCode(404).body("code", Matchers.equalTo("SCHEDULE_NOT_FOUND"));
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "sched@certifyos.com")})
    void operatorNamedTemplateIsVerifiedNotCreated() {
        WireMockUpstreams.stubFor(WireMock.get(WireMock.urlPathEqualTo(TEMPLATES + "/tpl-mine"))
                .willReturn(WireMock.okJson(templateJson("tpl-mine", "ops made this", 2))));
        WireMockUpstreams.stubFor(
                WireMock.get(WireMock.urlPathEqualTo(TEMPLATES + "/tpl-nope")).willReturn(WireMock.notFound()));

        String mine = VALID_BODY.replaceFirst("\\{", "{\"egressTemplateId\":\"tpl-mine\",");
        put("override", mine).then().statusCode(201).body("egressTemplateId", Matchers.equalTo("tpl-mine"));

        String nope = VALID_BODY.replaceFirst("\\{", "{\"egressTemplateId\":\"tpl-nope\",");
        put("override-missing", nope).then().statusCode(400).body("code", Matchers.equalTo("TEMPLATE_NOT_FOUND"));
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "sched@certifyos.com")})
    void invalidBodiesAreRefusedWithTheirCodes() {
        put(
                        "invalid",
                        "{\"cadence\":{\"type\":\"monthly\",\"dayOfMonth\":1},\"timezone\":\"UTC\","
                                + "\"selection\":{\"data.secret\":{\"eq\":\"x\"}}}")
                .then()
                .statusCode(400)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("SELECTION_INVALID"))
                .body("detail", Matchers.containsString("data.secret"));
        put("invalid", VALID_BODY.replace("America/New_York", "Mars/Olympus"))
                .then()
                .statusCode(400)
                .body("code", Matchers.equalTo("TIMEZONE_INVALID"));
        put("invalid", VALID_BODY.replace("\"dayOfMonth\":1", "\"dayOfMonth\":31"))
                .then()
                .statusCode(400)
                .body("code", Matchers.equalTo("CADENCE_INVALID"));
        put("invalid", "{\"timezone\":\"UTC\",\"selection\":{\"credentialingStatus\":{\"eq\":\"A\"}}}")
                .then()
                .statusCode(400)
                .body("code", Matchers.equalTo("CADENCE_INVALID"));
        put("with_underscore", VALID_BODY).then().statusCode(400).body("code", Matchers.equalTo("INVALID_REQUEST"));
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "sched@certifyos.com")})
    void disableThenEnableWithCatchUpRunsTheMissedPeriod() {
        stubNoTemplateThenCreate("tpl-toggle");
        put("toggle", VALID_BODY).then().statusCode(201);

        member().body("{}")
                .post(BASE + "toggle/disable")
                .then()
                .statusCode(400)
                .body("code", Matchers.equalTo("REASON_REQUIRED"));
        member().body("{\"reason\":\"vendor outage\"}")
                .post(BASE + "toggle/disable")
                .then()
                .statusCode(200)
                .body("enabled", Matchers.equalTo(false))
                .body("disabledReason", Matchers.equalTo("vendor outage"))
                .body("disabledAt", Matchers.notNullValue())
                .body("version", Matchers.equalTo(2));
        member().body("{\"reason\":\"again\"}")
                .post(BASE + "toggle/disable")
                .then()
                .statusCode(409)
                .body("code", Matchers.equalTo("ALREADY_DISABLED"));

        // Disabled a moment ago, so no occurrence was missed: catch-up falls back to the next one.
        member().body("{\"reason\":\"vendor back\",\"catchUp\":true}")
                .post(BASE + "toggle/enable")
                .then()
                .statusCode(200)
                .body("enabled", Matchers.equalTo(true))
                .body("disabledReason", Matchers.nullValue())
                .body("disabledAt", Matchers.nullValue())
                .body("nextDueAt", Matchers.endsWith("T04:00:00Z"))
                .body("version", Matchers.equalTo(3));
        member().body("{\"reason\":\"again\"}")
                .post(BASE + "toggle/enable")
                .then()
                .statusCode(409)
                .body("code", Matchers.equalTo("ALREADY_ENABLED"));

        List<AuditEventType> types = audit.findForSchedule(TENANT, "toggle", 10).stream()
                .map(AuditEvent::type)
                .toList();
        Assertions.assertTrue(types.contains(AuditEventType.SCHEDULE_DISABLED), types.toString());
        Assertions.assertTrue(types.contains(AuditEventType.SCHEDULE_ENABLED), types.toString());
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "sched@certifyos.com")})
    void previewCountsThroughApiLayerWithPageSizeOne() {
        WireMockUpstreams.stubFor(WireMock.get(WireMock.urlPathEqualTo("/practitioners"))
                .withHeader("tenant-id", WireMock.equalTo(TENANT))
                .withQueryParam("size", WireMock.equalTo("1"))
                .withQueryParam("filter", WireMock.equalToJson("{\"credentialingStatus\":{\"eq\":\"Approved\"}}"))
                .willReturn(WireMock.okJson("{\"data\":[{\"id\":\"p1\",\"npi\":\"1\"}],\"totalCount\":1234}")));

        member().body("{\"selection\":{\"credentialingStatus\":{\"eq\":\"Approved\"}}}")
                .post(BASE + "preview-any/preview")
                .then()
                .statusCode(200)
                .body("totalCount", Matchers.equalTo(1234));

        member().body("{}")
                .post(BASE + "preview-none/preview")
                .then()
                .statusCode(404)
                .body("code", Matchers.equalTo("SCHEDULE_NOT_FOUND"));
    }

    @Test
    @TestSecurity(user = "other")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "sched-other@certifyos.com")})
    void pathTenantMustMatchTheHeaderTenant() {
        RestAssured.given()
                .header("tenant-id", "org-sched-other")
                .contentType("application/json")
                .body(VALID_BODY)
                .put(BASE + "cross")
                .then()
                .statusCode(403)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("TENANT_MISMATCH"));
        RestAssured.given()
                .header("tenant-id", "org-sched-other")
                .get(BASE + "cross")
                .then()
                .statusCode(403)
                .body("code", Matchers.equalTo("TENANT_MISMATCH"));
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "sched@certifyos.com")})
    void runNowCreatesTheCurrentPeriodsBatchOnceAndAdvancesTheSchedule() {
        stubNoTemplateThenCreate("tpl-runnow");
        put("runnow", VALID_BODY).then().statusCode(201);
        String expectedId =
                TENANT + "-runnow-" + java.time.YearMonth.now(java.time.ZoneId.of("America/New_York")) + "-001";

        String jobId = member().post(BASE + "runnow/run-now")
                .then()
                .statusCode(201)
                .body("exportBatchId", Matchers.equalTo(expectedId))
                .body("nextDueAt", Matchers.endsWith("T04:00:00Z"))
                .extract()
                .path("jobId");

        Assertions.assertEquals(
                BatchState.SCHEDULED, batches.find(expectedId).orElseThrow().state());
        Assertions.assertEquals(
                org.jobrunr.jobs.states.StateName.ENQUEUED,
                storage.getJobById(java.util.UUID.fromString(jobId)).getState());
        member().get(BASE + "runnow")
                .then()
                .statusCode(200)
                .body("lastBatchId", Matchers.equalTo(expectedId))
                .body("lastRunAt", Matchers.notNullValue())
                .body("version", Matchers.equalTo(2));
        List<AuditEventType> types = audit.findForSchedule(TENANT, "runnow", 10).stream()
                .map(AuditEvent::type)
                .toList();
        Assertions.assertTrue(types.contains(AuditEventType.SCHEDULE_RUN_NOW), types.toString());
        Assertions.assertTrue(types.contains(AuditEventType.EXPORT_BATCH_SCHEDULED), types.toString());
        AuditEvent scheduled = audit.findForSchedule(TENANT, "runnow", 10).stream()
                .filter(event -> event.type() == AuditEventType.EXPORT_BATCH_SCHEDULED)
                .findFirst()
                .orElseThrow();
        Assertions.assertEquals("MANUAL", scheduled.detail().get("trigger"));
        Assertions.assertEquals("sched@certifyos.com", scheduled.actor());

        member().post(BASE + "runnow/run-now")
                .then()
                .statusCode(409)
                .body("code", Matchers.equalTo("BATCH_EXISTS"))
                .body("detail", Matchers.containsString(expectedId));
        member().get(BASE + "runnow").then().statusCode(200).body("version", Matchers.equalTo(2));
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "sched@certifyos.com")})
    void runNowRefusesADisabledSchedule() {
        stubNoTemplateThenCreate("tpl-runnow-off");
        put("runnow-off", VALID_BODY).then().statusCode(201);
        member().body("{\"reason\":\"hold\"}")
                .post(BASE + "runnow-off/disable")
                .then()
                .statusCode(200);

        member().post(BASE + "runnow-off/run-now")
                .then()
                .statusCode(409)
                .body("code", Matchers.equalTo("SCHEDULE_DISABLED"));
        member().post(BASE + "runnow-missing/run-now")
                .then()
                .statusCode(404)
                .body("code", Matchers.equalTo("SCHEDULE_NOT_FOUND"));
    }
}
