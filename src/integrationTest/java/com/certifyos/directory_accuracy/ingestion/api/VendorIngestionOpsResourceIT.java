package com.certifyos.directory_accuracy.ingestion.api;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.audit.AuditEvent;
import com.certifyos.directory_accuracy.audit.AuditEventType;
import com.certifyos.directory_accuracy.audit.AuditRepository;
import com.certifyos.directory_accuracy.auth.GoogleIdTokenService;
import com.certifyos.directory_accuracy.auth.UserContextFilter;
import com.certifyos.directory_accuracy.auth.WireMockDal;
import com.certifyos.directory_accuracy.export.batch.BatchState;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.batch.ExportBatchRepository;
import com.certifyos.directory_accuracy.export.events.PubSubPushMessage;
import com.certifyos.directory_accuracy.export.jobs.EgressFixtures;
import com.certifyos.directory_accuracy.export.jobs.JobIds;
import com.certifyos.directory_accuracy.http.Problem;
import com.certifyos.directory_accuracy.ingestion.InboundFixtures;
import com.certifyos.directory_accuracy.ingestion.events.ArrivalEvent;
import com.certifyos.directory_accuracy.ingestion.events.ArrivalHandler;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFile;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFileRepository;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFileState;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundNames;
import com.certifyos.directory_accuracy.ingestion.inbound.UnmatchedUpload;
import com.certifyos.directory_accuracy.ingestion.inbound.UnmatchedUploadRepository;
import com.certifyos.directory_accuracy.ingestion.jobs.IngestionJobEnqueuer;
import com.certifyos.directory_accuracy.ingestion.jobs.RequestIngestionJob;
import com.certifyos.directory_accuracy.persistence.Collections;
import com.certifyos.directory_accuracy.persistence.Transactions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.security.TestSecurity;
import io.quarkus.test.security.jwt.Claim;
import io.quarkus.test.security.jwt.JwtSecurity;
import io.restassured.RestAssured;
import io.restassured.specification.RequestSpecification;
import jakarta.inject.Inject;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.bson.Document;
import org.hamcrest.Matchers;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.storage.StorageProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Operator reads of inbound files and unmatched uploads, and the attach and discard writes,
 * through the endpoints. Every read is scoped to the caller's tenant: another tenant's batch or
 * folder reads as not found. Unique email and tenant for this class (finding 72); every batch gets
 * its own vendor, so the deterministic batch ids never collide.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@QuarkusTestResource(WireMockDal.class)
@TestProfile(ApiTestProfile.class)
class VendorIngestionOpsResourceIT {

    static final String EMAIL = UserContextFilter.EMAIL_CLAIM;
    static final String OPERATOR = "ingops@certifyos.com";
    static final String TENANT = "org-ingops";
    static final String OTHER = "org-ingops-other";
    static final String BASE = "/v1/vendor-ingestion";
    static final String ATTACH = BASE + "/unmatched-uploads/{uploadId}/attach";
    static final String DISCARD = BASE + "/unmatched-uploads/{uploadId}/discard";

    @Inject
    ExportBatchRepository batches;

    @Inject
    Collections collections;

    @Inject
    Transactions transactions;

    @Inject
    InboundFileRepository inbound;

    @Inject
    UnmatchedUploadRepository unmatched;

    @Inject
    AuditRepository audit;

    @Inject
    StorageProvider storage;

    @Inject
    ArrivalHandler handler;

    @InjectMock
    GoogleIdTokenService google;

    @BeforeEach
    void before() {
        Mockito.when(google.idToken("test-dal-iap-client-id")).thenReturn("dal-id-token");
        WireMockDal.stubMember(OPERATOR, TENANT);
    }

    private static RequestSpecification member() {
        return RestAssured.given().header("tenant-id", TENANT).contentType("application/json");
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = OPERATOR)})
    void inboundFilesAreTenantScoped() {
        ExportBatch ours = deliveredBatch(TENANT, "files");
        InboundFile entry = InboundFixtures.entryIn(InboundFileState.INGESTION_REQUESTED, TENANT, ours.id());
        collections.batches().updateOne(Filters.eq("_id", ours.id()), Updates.push("inboundFiles", entry.toDocument()));
        ExportBatch theirs = deliveredBatch(OTHER, "files");

        member().get(BASE + "/batches/" + ours.id() + "/inbound-files")
                .then()
                .statusCode(200)
                .body("items", Matchers.hasSize(1))
                .body("items[0].uploadId", Matchers.equalTo(entry.uploadId()))
                .body("items[0].state", Matchers.equalTo("INGESTION_REQUESTED"))
                .body("items[0].attempt", Matchers.equalTo(1))
                .body("items[0].externalRef", Matchers.equalTo(entry.externalRef()))
                .body("items[0].archivePath", Matchers.equalTo(entry.archivePath()))
                .body("items[0]", Matchers.not(Matchers.hasKey("jobId")));
        member().get(BASE + "/batches/" + theirs.id() + "/inbound-files")
                .then()
                .statusCode(404)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("BATCH_NOT_FOUND"));
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = OPERATOR)})
    void unmatchedListShowsOnlyOwnTenantFolder() {
        UnmatchedUpload mine = insert(parsedRow(TENANT, TENANT + "-gone-2026-10-001"));
        UnmatchedUpload foreign = insert(parsedRow(OTHER, OTHER + "-gone-2026-10-001"));

        member().queryParam("limit", 200)
                .get(BASE + "/unmatched-uploads")
                .then()
                .statusCode(200)
                .body("items.id", Matchers.hasItem(mine.id()))
                .body("items.id", Matchers.not(Matchers.hasItem(foreign.id())))
                .body("items.folderTenant", Matchers.everyItem(Matchers.equalTo(TENANT)))
                .body(
                        "items.find { it.id == '" + mine.id() + "' }.reason",
                        Matchers.equalTo(InboundNames.UNKNOWN_BATCH))
                .body(
                        "items.find { it.id == '" + mine.id() + "' }.parsed.exportBatchRef",
                        Matchers.equalTo(TENANT + "-gone-2026-10-001"));
        for (int limit : new int[] {0, 201}) {
            member().queryParam("limit", limit)
                    .get(BASE + "/unmatched-uploads")
                    .then()
                    .statusCode(400)
                    .contentType(Problem.MEDIA_TYPE)
                    .body("code", Matchers.equalTo("INVALID_REQUEST"));
        }
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = OPERATOR)})
    void attachClaimsAndEnqueues() {
        ExportBatch batch = deliveredBatch(TENANT, "attach");
        UnmatchedUpload row = insert(parsedRow(TENANT, TENANT + "-typo-2026-10-001"));
        UUID jobId = JobIds.of(RequestIngestionJob.NAME, row.id(), 1);

        member().pathParam("uploadId", row.id())
                .body(Map.of("exportBatchId", batch.id(), "reason", "vendor typed the batch ref wrong"))
                .post(ATTACH)
                .then()
                .statusCode(202)
                .body("uploadId", Matchers.equalTo(row.id()))
                .body("state", Matchers.equalTo("RECEIVED"))
                .body("jobId", Matchers.equalTo(jobId.toString()));

        UnmatchedUpload.Resolution resolution =
                unmatched.find(row.id()).orElseThrow().resolution();
        Assertions.assertEquals(UnmatchedUpload.ATTACHED, resolution.kind());
        Assertions.assertEquals(batch.id(), resolution.exportBatchId());
        Assertions.assertEquals(OPERATOR, resolution.actor());
        InboundFile entry = inbound.findByUploadId(row.id()).orElseThrow().entry();
        Assertions.assertEquals(InboundFileState.RECEIVED, entry.state());
        Assertions.assertEquals(row.receivedAt(), entry.eventTime(), "the row's arrival time is the event time");
        Assertions.assertEquals(jobId.toString(), entry.jobId(), "the job id is recorded after the enqueue");
        Assertions.assertEquals(StateName.ENQUEUED, storage.getJobById(jobId).getState());
        ExportBatch acknowledged = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(row.id(), acknowledged.inboundBatchId());
        Assertions.assertEquals(row.receivedAt(), acknowledged.acknowledgedAt());

        List<AuditEvent> trail = audit.trailForBatch(batch.id(), 100);
        AuditEvent attached = only(trail, AuditEventType.INBOUND_UPLOAD_ATTACHED);
        Assertions.assertEquals(OPERATOR, attached.actor());
        Assertions.assertEquals(row.id(), attached.uploadId());
        Assertions.assertEquals(batch.id(), attached.detail().get("exportBatchId"));
        Assertions.assertEquals(row.id(), attached.detail().get("uploadId"));
        Assertions.assertEquals(
                OPERATOR, only(trail, AuditEventType.INBOUND_FILE_RECEIVED).actor(), "the claim runs as the operator");
        Assertions.assertEquals(
                OPERATOR, only(trail, AuditEventType.EXPORT_BATCH_ACKNOWLEDGED).actor());
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = OPERATOR)})
    void attachTakesAnUnparsedName() {
        ExportBatch batch = deliveredBatch(TENANT, "unparsed");
        UnmatchedUpload row = insert(new UnmatchedUpload(
                InboundFixtures.VENDOR_BUCKET + "|to/" + TENANT + "/candor-recs-final.csv|" + generation(),
                TENANT,
                null,
                InboundNames.UNPARSEABLE_NAME,
                now(),
                null));

        member().pathParam("uploadId", row.id())
                .body(Map.of("exportBatchId", batch.id(), "reason", "vendor renamed the file"))
                .post(ATTACH)
                .then()
                .statusCode(202)
                .body("state", Matchers.equalTo("RECEIVED"));

        InboundFile entry = inbound.findByUploadId(row.id()).orElseThrow().entry();
        Assertions.assertEquals(InboundFileState.RECEIVED, entry.state());
        Assertions.assertNull(entry.vendorBatchId(), "an unparsed name has no vendor batch id");
        Assertions.assertEquals("to/" + TENANT + "/candor-recs-final.csv", entry.objectName());
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = OPERATOR)})
    void attachToAnotherTenantsBatchIs404() {
        ExportBatch theirs = deliveredBatch(OTHER, "foreign");
        UnmatchedUpload row = insert(parsedRow(TENANT, TENANT + "-foreign-2026-10-001"));
        UnmatchedUpload theirRow = insert(parsedRow(OTHER, OTHER + "-foreign-2026-10-001"));
        ExportBatch ours = deliveredBatch(TENANT, "foreign");

        member().pathParam("uploadId", row.id())
                .body(Map.of("exportBatchId", theirs.id(), "reason", "wrong tenant"))
                .post(ATTACH)
                .then()
                .statusCode(404)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("BATCH_NOT_FOUND"));
        member().pathParam("uploadId", theirRow.id())
                .body(Map.of("exportBatchId", ours.id(), "reason", "another tenant's folder"))
                .post(ATTACH)
                .then()
                .statusCode(404)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("UNMATCHED_NOT_FOUND"));
        member().pathParam("uploadId", theirRow.id())
                .body(Map.of("reason", "another tenant's folder"))
                .post(DISCARD)
                .then()
                .statusCode(404)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("UNMATCHED_NOT_FOUND"));

        Assertions.assertNull(unmatched.find(row.id()).orElseThrow().resolution(), "the row stays open");
        Assertions.assertNull(unmatched.find(theirRow.id()).orElseThrow().resolution());
        Assertions.assertTrue(inbound.findByUploadId(row.id()).isEmpty());
        Assertions.assertTrue(inbound.findByUploadId(theirRow.id()).isEmpty());
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = OPERATOR)})
    void attachOfAFileNamingAnotherTenantIs409() {
        ExportBatch ours = deliveredBatch(TENANT, "mismatch");
        // dropped in our folder, but the file names the other tenant and its batch
        String name =
                InboundNames.TO_PREFIX + TENANT + "/" + OTHER + "_" + OTHER + "-mismatch-2026-10-001_V77_20261019.csv";
        UnmatchedUpload row = insert(new UnmatchedUpload(
                InboundFixtures.VENDOR_BUCKET + "|" + name + "|" + generation(),
                TENANT,
                InboundNames.parse(name).orElseThrow(),
                InboundNames.TENANT_MISMATCH,
                now(),
                null));

        member().pathParam("uploadId", row.id())
                .body(Map.of("exportBatchId", ours.id(), "reason", "looks like ours"))
                .post(ATTACH)
                .then()
                .statusCode(409)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo(InboundNames.TENANT_MISMATCH));

        Assertions.assertNull(unmatched.find(row.id()).orElseThrow().resolution(), "the row stays open");
        Assertions.assertTrue(inbound.findByUploadId(row.id()).isEmpty(), "nothing is claimed");
        Assertions.assertNull(batches.find(ours.id()).orElseThrow().inboundBatchId());
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = OPERATOR)})
    void attachOfAnUploadOnAnotherBatchIs409() {
        ExportBatch first = deliveredBatch(TENANT, "claimed");
        ExportBatch second = deliveredBatch(TENANT, "claimedb");
        UnmatchedUpload row = insert(parsedRow(TENANT, first.id()));
        // a later delivery of the same event claimed the upload on its named batch; the row stayed open
        String objectName =
                row.id().substring(row.id().indexOf('|') + 1, row.id().lastIndexOf('|'));
        ArrivalEvent event = new ArrivalEvent(
                InboundFixtures.VENDOR_BUCKET,
                objectName,
                row.id().substring(row.id().lastIndexOf('|') + 1),
                "OBJECT_FINALIZE",
                row.receivedAt(),
                null);
        InboundFile onFirst = InboundFile.received(event, row.parsed(), now());
        Assertions.assertEquals(row.id(), onFirst.uploadId());
        collections
                .batches()
                .updateOne(Filters.eq("_id", first.id()), Updates.push("inboundFiles", onFirst.toDocument()));

        member().pathParam("uploadId", row.id())
                .body(Map.of("exportBatchId", second.id(), "reason", "vendor named the wrong batch"))
                .post(ATTACH)
                .then()
                .statusCode(409)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("ALREADY_CLAIMED"))
                .body("detail", Matchers.containsString(first.id()));

        Assertions.assertTrue(inbound.entriesFor(second.id()).isEmpty(), "nothing was claimed on the target batch");
        Assertions.assertEquals(
                first.id(),
                inbound.findByUploadId(row.id()).orElseThrow().batch().id());
        Assertions.assertNull(unmatched.find(row.id()).orElseThrow().resolution(), "the row stays open");
        Assertions.assertTrue(
                audit.trailForBatch(second.id(), 100).stream()
                        .noneMatch(written -> written.type() == AuditEventType.INBOUND_UPLOAD_ATTACHED),
                "no attach event");
    }

    /**
     * Attach first, then the storage event for the same upload arrives with its named batch
     * delivered: the unique upload index refuses the second claim, the handler acknowledges with
     * {@code ALREADY_CLAIMED}, and the upload stays on the batch the operator chose.
     */
    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = OPERATOR)})
    void redeliveryAfterAnAttachIsRefusedByTheUniqueIndex() {
        ExportBatch named = deliveredBatch(TENANT, "named");
        ExportBatch chosen = deliveredBatch(TENANT, "chosen");
        UnmatchedUpload row = insert(parsedRow(TENANT, named.id()));
        member().pathParam("uploadId", row.id())
                .body(Map.of("exportBatchId", chosen.id(), "reason", "operator chose the newer batch"))
                .post(ATTACH)
                .then()
                .statusCode(202)
                .body("state", Matchers.equalTo("RECEIVED"));

        Map<String, String> attributes = new HashMap<>();
        attributes.put("bucketId", InboundFixtures.VENDOR_BUCKET);
        attributes.put(
                "objectId",
                row.id().substring(row.id().indexOf('|') + 1, row.id().lastIndexOf('|')));
        attributes.put("objectGeneration", row.id().substring(row.id().lastIndexOf('|') + 1));
        attributes.put("eventType", "OBJECT_FINALIZE");
        attributes.put("eventTime", row.receivedAt().toString());
        ArrivalHandler.Outcome outcome = handler.handle(new PubSubPushMessage.Message(
                null, "m-redelivery", Instant.now().toString(), attributes));

        Assertions.assertEquals(ArrivalHandler.Outcome.ALREADY_CLAIMED, outcome, "acknowledged, not thrown");
        Assertions.assertTrue(inbound.entriesFor(named.id()).isEmpty(), "no second entry on the named batch");
        List<InboundFile> onChosen = inbound.entriesFor(chosen.id());
        Assertions.assertEquals(1, onChosen.size());
        Assertions.assertEquals(row.id(), onChosen.get(0).uploadId());
        Assertions.assertNull(batches.find(named.id()).orElseThrow().acknowledgedAt(), "the named batch is untouched");
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = OPERATOR)})
    void attachWhoseEnqueueFailsAnswers202WithNoJob() {
        IngestionJobEnqueuer failing = Mockito.mock(IngestionJobEnqueuer.class);
        Mockito.when(failing.requestIngestion(Mockito.anyString(), Mockito.anyString(), Mockito.anyInt()))
                .thenThrow(new IllegalStateException("job storage unavailable"));
        QuarkusMock.installMockForType(failing, IngestionJobEnqueuer.class);
        ExportBatch batch = deliveredBatch(TENANT, "enqfail");
        UnmatchedUpload row = insert(parsedRow(TENANT, TENANT + "-enqfail-2026-10-001"));

        member().pathParam("uploadId", row.id())
                .body(Map.of("exportBatchId", batch.id(), "reason", "vendor typed the batch ref wrong"))
                .post(ATTACH)
                .then()
                .statusCode(202)
                .body("state", Matchers.equalTo("RECEIVED"))
                .body("jobId", Matchers.nullValue());

        Assertions.assertEquals(
                UnmatchedUpload.ATTACHED,
                unmatched.find(row.id()).orElseThrow().resolution().kind());
        InboundFile entry = inbound.findByUploadId(row.id()).orElseThrow().entry();
        Assertions.assertEquals(InboundFileState.RECEIVED, entry.state());
        Assertions.assertNull(entry.jobId(), "left for the reconciler");
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = OPERATOR)})
    void attachOntoABatchWithAnUploadIsDuplicateLogged() {
        ExportBatch batch = deliveredBatch(TENANT, "dup");
        InboundFile first = InboundFixtures.entryIn(InboundFileState.RECEIVED, TENANT, batch.id());
        collections
                .batches()
                .updateOne(Filters.eq("_id", batch.id()), Updates.push("inboundFiles", first.toDocument()));
        UnmatchedUpload row = insert(parsedRow(TENANT, TENANT + "-dup-2026-10-001"));

        member().pathParam("uploadId", row.id())
                .body(Map.of("exportBatchId", batch.id(), "reason", "a second file for the batch"))
                .post(ATTACH)
                .then()
                .statusCode(202)
                .body("uploadId", Matchers.equalTo(row.id()))
                .body("state", Matchers.equalTo("DUPLICATE_LOGGED"))
                .body("jobId", Matchers.nullValue());

        Assertions.assertEquals(2, inbound.entriesFor(batch.id()).size());
        Assertions.assertEquals(
                UnmatchedUpload.ATTACHED,
                unmatched.find(row.id()).orElseThrow().resolution().kind());
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = OPERATOR)})
    void attachWithoutABatchIdIs400() {
        UnmatchedUpload row = insert(parsedRow(TENANT, TENANT + "-nobatch-2026-10-001"));

        member().pathParam("uploadId", row.id())
                .body(Map.of("reason", "forgot the batch"))
                .post(ATTACH)
                .then()
                .statusCode(400)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("INVALID_REQUEST"));

        Assertions.assertNull(unmatched.find(row.id()).orElseThrow().resolution(), "the row stays open");
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = OPERATOR)})
    void attachNeedsAReason() {
        ExportBatch batch = deliveredBatch(TENANT, "reason");
        UnmatchedUpload row = insert(parsedRow(TENANT, TENANT + "-reason-2026-10-001"));

        member().pathParam("uploadId", row.id())
                .body(Map.of("exportBatchId", batch.id(), "reason", "  "))
                .post(ATTACH)
                .then()
                .statusCode(400)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("REASON_REQUIRED"));

        Assertions.assertNull(unmatched.find(row.id()).orElseThrow().resolution(), "the row stays open");
        Assertions.assertTrue(inbound.findByUploadId(row.id()).isEmpty(), "nothing was claimed");
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = OPERATOR)})
    void attachToAnUndeliveredBatchIs409() {
        ExportBatch requested = EgressFixtures.batchIn(
                transactions, batches, TENANT, "undelivered", BatchState.EGRESS_REQUESTED, 1, Instant.now());
        UnmatchedUpload row = insert(parsedRow(TENANT, requested.id()));

        member().pathParam("uploadId", row.id())
                .body(Map.of("exportBatchId", requested.id(), "reason", "too early"))
                .post(ATTACH)
                .then()
                .statusCode(409)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("ATTACH_NOT_ALLOWED"));

        Assertions.assertNull(unmatched.find(row.id()).orElseThrow().resolution(), "the row stays open");
        Assertions.assertTrue(inbound.findByUploadId(row.id()).isEmpty(), "nothing was claimed");
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = OPERATOR)})
    void discardTwiceIs409() {
        UnmatchedUpload row = insert(parsedRow(TENANT, TENANT + "-noise-2026-10-001"));
        ExportBatch batch = deliveredBatch(TENANT, "discard");

        member().pathParam("uploadId", row.id())
                .body(Map.of("reason", " "))
                .post(DISCARD)
                .then()
                .statusCode(400)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("REASON_REQUIRED"));
        member().pathParam("uploadId", row.id())
                .body(Map.of("reason", "vendor test file"))
                .post(DISCARD)
                .then()
                .statusCode(200)
                .body("id", Matchers.equalTo(row.id()))
                .body("folderTenant", Matchers.equalTo(TENANT))
                .body("resolution.kind", Matchers.equalTo("DISCARDED"))
                .body("resolution.reason", Matchers.equalTo("vendor test file"))
                .body("resolution.actor", Matchers.equalTo(OPERATOR));
        member().pathParam("uploadId", row.id())
                .body(Map.of("reason", "again"))
                .post(DISCARD)
                .then()
                .statusCode(409)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("ALREADY_RESOLVED"));
        member().pathParam("uploadId", row.id())
                .body(Map.of("exportBatchId", batch.id(), "reason", "changed my mind"))
                .post(ATTACH)
                .then()
                .statusCode(409)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("ALREADY_RESOLVED"));

        List<Document> events = collections
                .events()
                .find(Filters.and(
                        Filters.eq("type", AuditEventType.INBOUND_UPLOAD_DISCARDED.name()),
                        Filters.eq("uploadId", row.id())))
                .into(new java.util.ArrayList<>());
        Assertions.assertEquals(1, events.size(), "one discard, one event");
        AuditEvent discarded = AuditEvent.fromDocument(events.get(0));
        Assertions.assertEquals(OPERATOR, discarded.actor());
        Assertions.assertNull(discarded.tenantId(), "a service event");
        Assertions.assertEquals("vendor test file", discarded.detail().get("reason"));
        Assertions.assertEquals(row.id(), discarded.detail().get("uploadId"));
        Assertions.assertTrue(inbound.findByUploadId(row.id()).isEmpty(), "a discard claims nothing");
    }

    private ExportBatch deliveredBatch(String tenant, String vendor) {
        ExportBatch egressed = EgressFixtures.batchIn(
                transactions, batches, tenant, vendor, BatchState.EGRESS_COMPLETED, 1, InboundFixtures.DELIVERED_AT);
        transactions.run(session -> batches.transition(
                session,
                egressed.id(),
                BatchState.EGRESS_COMPLETED,
                BatchState.DELIVERED,
                InboundFixtures.DELIVERED_AT.plusSeconds(60),
                null));
        return batches.find(egressed.id()).orElseThrow();
    }

    private UnmatchedUpload insert(UnmatchedUpload row) {
        boolean inserted = transactions.run(session -> unmatched.insertIfAbsent(session, row));
        Assertions.assertTrue(inserted);
        return row;
    }

    /** A row whose name parsed but pointed at a batch the arrival handler could not use. */
    private static UnmatchedUpload parsedRow(String tenant, String exportBatchRef) {
        String name = InboundNames.TO_PREFIX + tenant + "/" + tenant + "_" + exportBatchRef + "_V77_20261019.csv";
        return new UnmatchedUpload(
                InboundFixtures.VENDOR_BUCKET + "|" + name + "|" + generation(),
                tenant,
                InboundNames.parse(name).orElseThrow(),
                InboundNames.UNKNOWN_BATCH,
                now(),
                null);
    }

    private static String generation() {
        return Long.toString(ThreadLocalRandom.current().nextLong(1_000_000_000L, Long.MAX_VALUE));
    }

    /** Millisecond precision, as MongoDB stores it, so a round trip compares equal. */
    private static Instant now() {
        return Instant.now().truncatedTo(ChronoUnit.MILLIS);
    }

    private static AuditEvent only(List<AuditEvent> trail, AuditEventType type) {
        List<AuditEvent> found =
                trail.stream().filter(event -> event.type() == type).toList();
        Assertions.assertEquals(1, found.size(), type + " written once");
        return found.get(0);
    }
}
