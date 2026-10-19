package com.certifyos.vendor_exchange.ingestion.jobs;

import com.certifyos.vendor_exchange.ApiTestProfile;
import com.certifyos.vendor_exchange.MongoResource;
import com.certifyos.vendor_exchange.audit.AuditEvent;
import com.certifyos.vendor_exchange.audit.AuditEventType;
import com.certifyos.vendor_exchange.audit.AuditRepository;
import com.certifyos.vendor_exchange.clients.ApiLayerTokenService;
import com.certifyos.vendor_exchange.clients.FileIngestionCell;
import com.certifyos.vendor_exchange.clients.FileIngestionClient;
import com.certifyos.vendor_exchange.clients.FileIngestionRow;
import com.certifyos.vendor_exchange.clients.WireMockUpstreams;
import com.certifyos.vendor_exchange.export.batch.ExportBatch;
import com.certifyos.vendor_exchange.export.batch.ExportBatchRepository;
import com.certifyos.vendor_exchange.ingestion.InboundFixtures;
import com.certifyos.vendor_exchange.ingestion.InboundFixtures.Seeded;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFile;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileRepository;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileRepository.Located;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileState;
import com.certifyos.vendor_exchange.ingestion.staging.CandorRecsMapping;
import com.certifyos.vendor_exchange.ingestion.staging.CandorRecsMapping.Suffix;
import com.certifyos.vendor_exchange.ingestion.staging.DesignRows;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendationRepository;
import com.certifyos.vendor_exchange.ingestion.staging.ReviewStatus;
import com.certifyos.vendor_exchange.ingestion.staging.RowDisposition;
import com.certifyos.vendor_exchange.ingestion.staging.RowStager;
import com.certifyos.vendor_exchange.ingestion.staging.StagingLifecycle;
import com.certifyos.vendor_exchange.metrics.VendorExchangeMetrics;
import com.certifyos.vendor_exchange.persistence.Collections;
import com.certifyos.vendor_exchange.persistence.Ids;
import com.certifyos.vendor_exchange.persistence.Transactions;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.mockito.InjectSpy;
import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import org.bson.Document;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

/**
 * The stage-rows job, run directly on the api role against WireMock file-ingestion serving NDJSON.
 * The test profile stages two rows per chunk, so the five-row design file is three chunks. Every
 * tenant here starts with {@code stage-}.
 *
 * <p>The design file: practitioner B ({@code cert-000123}, the design's sample) at three locations
 * on rows 1, 4 and 5, practitioner A at two on rows 2 and 3, so both practitioners span a chunk
 * boundary. Every row has {@code first_name} and {@code practice_name} {@code INVALID}, {@code phone}
 * {@code VALID} and seven fields unverified, except row 4, whose {@code practice_name} is {@code
 * VALID}: its one document is B's {@code first_name}, which row 1 wrote in the first chunk.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@QuarkusTestResource(WireMockUpstreams.class)
@TestProfile(ApiTestProfile.class)
class StageRowsJobIT {

    static final int TRAIL_LIMIT = 100;
    static final String SHEET = DesignRows.SHEET;
    static final String PRACTITIONER_A = "cert-000456";
    static final String PRACTITIONER_B = "cert-000123";

    /** Documents of the design file: two first names, four practice names. */
    static final long DESIGN_DOCUMENTS = 6;

    /** {@code VALID} cells of the design file: one phone per row, plus row 4's practice name. */
    static final long DESIGN_CONFIRMED = 6;

    static final long DESIGN_UNVERIFIED = 5L * DesignRows.UNVERIFIED_PER_ROW;

    @Inject
    StageRowsJob job;

    @Inject
    RowStager stager;

    @Inject
    InboundFileRepository inbound;

    @Inject
    FieldRecommendationRepository recommendations;

    @Inject
    ExportBatchRepository batches;

    @Inject
    AuditRepository audit;

    @Inject
    Transactions transactions;

    @Inject
    Collections collections;

    @Inject
    ObjectMapper mapper;

    @Inject
    MeterRegistry registry;

    @Inject
    @RestClient
    FileIngestionClient fileIngestion;

    @InjectSpy
    StagingLifecycle stagingLifecycle;

    @InjectMock
    ApiLayerTokenService tokens;

    @BeforeEach
    void before() {
        Mockito.when(tokens.accessToken()).thenReturn("api-layer-token");
        WireMockUpstreams.resetRequests();
    }

    // ---------------------------------------------------------------- fixtures

    /** An entry in {@code state} whose ingestion id is {@code ing-<tenant>} and whose parse saw {@code populated} rows. */
    private Seeded seed(String tenant, InboundFileState state, long populated) {
        Seeded seeded = InboundFixtures.deliveredBatchWithEntry(transactions, batches, collections, tenant, state);
        collections
                .batches()
                .updateOne(
                        Filters.eq("_id", seeded.batch().id()),
                        Updates.combine(
                                Updates.set("inboundFiles.0.ingestionId", ingestionId(tenant)),
                                Updates.set("inboundFiles.0.parse.populatedRows", populated)));
        return new Seeded(seeded.batch(), entry(seeded.entry().uploadId()));
    }

    private static String ingestionId(String tenant) {
        return "ing-" + tenant;
    }

    private static String rowsPath(String ingestion) {
        return "/v1/ingestion-rows/" + ingestion;
    }

    private static String completePath(String ingestion) {
        return "/v1/ingestion-complete/" + ingestion;
    }

    private static String cursor(int row) {
        return SHEET + ":" + row;
    }

    private static Map<String, FileIngestionCell> practiceNameValid() {
        Map<String, FileIngestionCell> cells = new HashMap<>();
        cells.put(CandorRecsMapping.column("practice_name", Suffix.STATUS), DesignRows.valid("VALID"));
        return cells;
    }

    /** The five rows of the design file, in stream order. */
    static List<FileIngestionRow> designFile() {
        return List.of(
                DesignRows.row(1, PRACTITIONER_B, "1003002890", "loc-7781"),
                DesignRows.row(2, PRACTITIONER_A, "1003002891", "loc-a1"),
                DesignRows.row(3, PRACTITIONER_A, "1003002891", "loc-a2"),
                DesignRows.row(4, PRACTITIONER_B, "1003002890", "loc-b2", practiceNameValid()),
                DesignRows.row(5, PRACTITIONER_B, "1003002890", "loc-b3"));
    }

    private String ndjson(List<FileIngestionRow> rows) {
        StringBuilder body = new StringBuilder();
        for (FileIngestionRow row : rows) {
            try {
                body.append(mapper.writeValueAsString(row)).append('\n');
            } catch (JsonProcessingException unwritable) {
                throw new IllegalStateException(unwritable);
            }
        }
        return body.toString();
    }

    private static MappingBuilder rowsRequest(String ingestion, String after) {
        MappingBuilder request = WireMock.get(WireMock.urlPathEqualTo(rowsPath(ingestion)));
        return after == null
                ? request.withQueryParam("after", WireMock.absent())
                : request.withQueryParam("after", WireMock.equalTo(after));
    }

    private static RequestPatternBuilder rowsRequested(String ingestion, String after) {
        RequestPatternBuilder request = WireMock.getRequestedFor(WireMock.urlPathEqualTo(rowsPath(ingestion)));
        return after == null
                ? request.withQueryParam("after", WireMock.absent())
                : request.withQueryParam("after", WireMock.equalTo(after));
    }

    private void stubRows(String ingestion, String after, String body) {
        WireMockUpstreams.stubFor(rowsRequest(ingestion, after)
                .willReturn(WireMock.aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/x-ndjson")
                        .withBody(body)));
    }

    private static void stubRowsStatus(String ingestion, int status) {
        WireMockUpstreams.stubFor(rowsRequest(ingestion, null)
                .willReturn(WireMock.aResponse()
                        .withStatus(status)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"code\":\"STATUS_" + status + "\"}")));
    }

    private static void stubComplete(String ingestion, int status) {
        WireMockUpstreams.stubFor(WireMock.post(WireMock.urlEqualTo(completePath(ingestion)))
                .willReturn(WireMock.aResponse()
                        .withStatus(status)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{}")));
    }

    private static void verifyCompleted(String ingestion, int times) {
        WireMockUpstreams.verify(
                times,
                WireMock.postRequestedFor(WireMock.urlEqualTo(completePath(ingestion)))
                        .withHeader("Authorization", WireMock.equalTo("Bearer api-layer-token")));
    }

    private void run(Seeded seeded) {
        job.run(new StageRowsJobRequest(seeded.batch().id(), seeded.entry().uploadId(), 1));
    }

    private InboundFile entry(String uploadId) {
        return inbound.findByUploadId(uploadId).orElseThrow().entry();
    }

    private InboundFile entry(Seeded seeded) {
        return entry(seeded.entry().uploadId());
    }

    private Located located(Seeded seeded) {
        return inbound.findByUploadId(seeded.entry().uploadId()).orElseThrow();
    }

    private long documents(ExportBatch batch) {
        return collections.fieldRecommendations().countDocuments(Filters.eq("exportBatchId", batch.id()));
    }

    private static Map<String, Long> dispositions(Map<RowDisposition, Long> nonZero) {
        Map<String, Long> all = new LinkedHashMap<>();
        for (RowDisposition disposition : RowDisposition.values()) {
            all.put(disposition.name(), nonZero.getOrDefault(disposition, 0L));
        }
        return all;
    }

    private static long sum(InboundFile entry) {
        return entry.dispositions().values().stream().mapToLong(Long::longValue).sum();
    }

    private static InboundFile.Written written(long documents, long confirmed, long unverified, long disagreements) {
        return new InboundFile.Written(documents, confirmed, unverified, disagreements);
    }

    private List<AuditEvent> trail(Seeded seeded) {
        return audit.trailForBatch(seeded.batch().id(), TRAIL_LIMIT);
    }

    private double counter(String name, String... tags) {
        Counter found = registry.find(name).tags(tags).counter();
        return found == null ? 0 : found.count();
    }

    private static String id(ExportBatch batch, String practitioner, String location, String field) {
        return DesignRows.idOf(batch, practitioner, location, field);
    }

    // ---------------------------------------------------------------- tests

    @Test
    void stagesTheDesignFile() {
        String tenant = "stage-design";
        Seeded seeded = seed(tenant, InboundFileState.PARSED, 5);
        stubRows(ingestionId(tenant), null, ndjson(designFile()));
        stubComplete(ingestionId(tenant), 200);
        int eventsBefore = trail(seeded).size();
        double stagedRowsBefore =
                counter(VendorExchangeMetrics.INGESTION_STAGE_ROWS, "tenant", tenant, "disposition", "STAGED");

        run(seeded);

        InboundFile after = entry(seeded);
        Assertions.assertEquals(InboundFileState.STAGED, after.state());
        Assertions.assertNotNull(after.stagedAt());
        Assertions.assertEquals(dispositions(Map.of(RowDisposition.STAGED, 5L)), after.dispositions());
        Assertions.assertEquals(written(DESIGN_DOCUMENTS, DESIGN_CONFIRMED, DESIGN_UNVERIFIED, 0), after.written());
        Assertions.assertEquals(new InboundFile.Checkpoint(cursor(5), 3L), after.checkpoint());
        Assertions.assertEquals(DESIGN_DOCUMENTS, documents(seeded.batch()));

        FieldRecommendation sample = recommendations
                .find(tenant, id(seeded.batch(), PRACTITIONER_B, "loc-7781", "practice_name"))
                .orElseThrow();
        Assertions.assertEquals(ReviewStatus.PENDING, sample.reviewStatus());
        Assertions.assertEquals("AdventHealth", sample.sentValue());
        Assertions.assertEquals("Greenwood Village", sample.vendorValue());
        Assertions.assertEquals(1L, sample.version());
        Assertions.assertEquals(
                new FieldRecommendation.SourceFile(seeded.entry().uploadId(), SHEET, 1), sample.sourceFile());
        FieldRecommendation firstName = recommendations
                .find(tenant, id(seeded.batch(), PRACTITIONER_B, null, "first_name"))
                .orElseThrow();
        Assertions.assertNull(firstName.location());
        Assertions.assertEquals(1, firstName.sourceFile().row(), "written once, by the first row");

        List<AuditEvent> events =
                trail(seeded).subList(eventsBefore, trail(seeded).size());
        Assertions.assertEquals(
                List.of(AuditEventType.INBOUND_FILE_STAGED),
                events.stream().map(AuditEvent::type).toList());
        Map<String, Object> detail = events.get(0).detail();
        Assertions.assertEquals(
                new Document(dispositions(Map.of(RowDisposition.STAGED, 5L))), detail.get("dispositions"));
        Assertions.assertEquals(
                written(DESIGN_DOCUMENTS, DESIGN_CONFIRMED, DESIGN_UNVERIFIED, 0)
                        .toDocument(),
                detail.get("written"));
        Assertions.assertEquals(3L, ((Number) detail.get("chunks")).longValue());
        Assertions.assertTrue(((Number) detail.get("durationMs")).longValue() >= 0);
        Assertions.assertEquals(0L, ((Number) detail.get("practitionerFieldDisagreements")).longValue());
        Assertions.assertEquals(seeded.entry().uploadId(), events.get(0).uploadId());

        WireMockUpstreams.verify(
                1,
                rowsRequested(ingestionId(tenant), null)
                        .withHeader("tenant-id", WireMock.equalTo(tenant))
                        .withHeader("identifier", WireMock.equalTo("vendor-ingestion"))
                        .withHeader("Authorization", WireMock.equalTo("Bearer api-layer-token")));
        verifyCompleted(ingestionId(tenant), 1);
        Assertions.assertEquals(
                5.0,
                counter(VendorExchangeMetrics.INGESTION_STAGE_ROWS, "tenant", tenant, "disposition", "STAGED")
                        - stagedRowsBefore);
    }

    @Test
    void chunksAndCheckpoints() {
        String tenant = "stage-chunks";
        Seeded seeded = seed(tenant, InboundFileState.STAGING, 5);
        List<FileIngestionRow> file = designFile();
        Instant now = Instant.parse("2026-10-15T09:15:00Z");

        RowStager.Progress first = stager.stageChunk(located(seeded), null, file.subList(0, 2), now)
                .orElseThrow();

        Assertions.assertEquals(cursor(2), first.after());
        Assertions.assertEquals(4L, first.recommendations());
        InboundFile afterFirst = entry(seeded);
        Assertions.assertEquals(new InboundFile.Checkpoint(cursor(2), 1L), afterFirst.checkpoint());
        Assertions.assertEquals(dispositions(Map.of(RowDisposition.STAGED, 2L)), afterFirst.dispositions());
        Assertions.assertEquals(written(4, 2, 2L * DesignRows.UNVERIFIED_PER_ROW, 0), afterFirst.written());

        RowStager.Progress second = stager.stageChunk(located(seeded), cursor(2), file.subList(2, 4), now)
                .orElseThrow();

        Assertions.assertEquals(cursor(4), second.after());
        Assertions.assertEquals(1L, second.recommendations(), "row 3's practice name; row 4's first name exists");
        Assertions.assertEquals(
                dispositions(Map.of(RowDisposition.STAGED, 2L)),
                dispositions(second.dispositions()),
                "row 4's only document was written by this upload's first chunk, so it is not a duplicate");
        InboundFile afterSecond = entry(seeded);
        Assertions.assertEquals(new InboundFile.Checkpoint(cursor(4), 2L), afterSecond.checkpoint());
        Assertions.assertEquals(dispositions(Map.of(RowDisposition.STAGED, 4L)), afterSecond.dispositions());
        Assertions.assertEquals(written(5, 5, 4L * DesignRows.UNVERIFIED_PER_ROW, 0), afterSecond.written());
        Assertions.assertEquals(InboundFileState.STAGING, afterSecond.state());
    }

    @Test
    void anOverlappingRunOfTheSameChunkIsNotCountedTwice() {
        String tenant = "stage-overlap";
        Seeded seeded = seed(tenant, InboundFileState.STAGING, 5);
        List<FileIngestionRow> file = designFile();
        Instant now = Instant.parse("2026-10-15T09:15:00Z");
        stager.stageChunk(located(seeded), null, file.subList(0, 2), now).orElseThrow();
        InboundFile once = entry(seeded);

        // a second run that also started from no checkpoint stages the same chunk late
        Optional<RowStager.Progress> late = stager.stageChunk(located(seeded), null, file.subList(0, 2), now);

        Assertions.assertTrue(late.isEmpty(), "the checkpoint moved past where the late run started");
        InboundFile after = entry(seeded);
        Assertions.assertEquals(once.checkpoint(), after.checkpoint());
        Assertions.assertEquals(once.dispositions(), after.dispositions());
        Assertions.assertEquals(once.written(), after.written());
    }

    @Test
    void chunkOfAnEntryThatLeftStagingIsNotCheckpointed() {
        String tenant = "stage-moved";
        Seeded seeded = seed(tenant, InboundFileState.FAILED, 5);

        Optional<RowStager.Progress> progress = stager.stageChunk(
                located(seeded), null, designFile().subList(0, 2), Instant.parse("2026-10-15T09:15:00Z"));

        Assertions.assertTrue(progress.isEmpty());
        Assertions.assertNull(entry(seeded).checkpoint());
        Assertions.assertTrue(entry(seeded).dispositions().isEmpty());
    }

    @Test
    void retryAfterCrashBeforeCheckpointDoesNotDoubleCount() {
        String tenant = "stage-dropped";
        Seeded seeded = seed(tenant, InboundFileState.PARSED, 5);
        List<FileIngestionRow> file = designFile();
        // The stream drops inside row 4: chunk 1 (rows 1, 2) is checkpointed, chunk 2 holds row 3.
        String dropped = ndjson(file.subList(0, 3)) + "{\"sheet\":\"" + SHEET + "\",\"row\":4,\"status\":\"VAL";
        stubRows(ingestionId(tenant), null, dropped);
        stubRows(ingestionId(tenant), cursor(2), ndjson(file.subList(2, 5)));
        stubComplete(ingestionId(tenant), 200);

        Assertions.assertThrows(RuntimeException.class, () -> run(seeded));

        InboundFile crashed = entry(seeded);
        Assertions.assertEquals(InboundFileState.STAGING, crashed.state());
        Assertions.assertEquals(new InboundFile.Checkpoint(cursor(2), 1L), crashed.checkpoint());
        Assertions.assertEquals(dispositions(Map.of(RowDisposition.STAGED, 2L)), crashed.dispositions());
        Assertions.assertEquals(4L, documents(seeded.batch()), "chunk 2 never reached its bulk write");
        verifyCompleted(ingestionId(tenant), 0);

        run(seeded);

        WireMockUpstreams.verify(1, rowsRequested(ingestionId(tenant), cursor(2)));
        InboundFile after = entry(seeded);
        Assertions.assertEquals(InboundFileState.STAGED, after.state());
        Assertions.assertEquals(5L, sum(after));
        Assertions.assertEquals(dispositions(Map.of(RowDisposition.STAGED, 5L)), after.dispositions());
        Assertions.assertEquals(DESIGN_DOCUMENTS, documents(seeded.batch()));
        Assertions.assertEquals(written(DESIGN_DOCUMENTS, DESIGN_CONFIRMED, DESIGN_UNVERIFIED, 0), after.written());
        Assertions.assertEquals(new InboundFile.Checkpoint(cursor(5), 3L), after.checkpoint());
        verifyCompleted(ingestionId(tenant), 1);
    }

    @Test
    void retryAfterCrashAfterTheBulkWriteKeepsTheChunkStaged() {
        String tenant = "stage-crashed";
        Seeded seeded = seed(tenant, InboundFileState.PARSED, 5);
        List<FileIngestionRow> file = designFile();
        stubRows(ingestionId(tenant), null, ndjson(file));
        stubRows(ingestionId(tenant), cursor(2), ndjson(file.subList(2, 5)));
        stubComplete(ingestionId(tenant), 200);
        // Chunk 2's bulk write lands, then the worker dies before its checkpoint commits.
        Mockito.doCallRealMethod()
                .doThrow(new IllegalStateException("worker died after the bulk write"))
                .doCallRealMethod()
                .when(stagingLifecycle)
                .chunkCheckpoint(
                        ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any());

        Assertions.assertThrows(IllegalStateException.class, () -> run(seeded));

        InboundFile crashed = entry(seeded);
        Assertions.assertEquals(new InboundFile.Checkpoint(cursor(2), 1L), crashed.checkpoint());
        Assertions.assertEquals(5L, documents(seeded.batch()), "chunk 2's documents are in place");

        run(seeded);

        InboundFile after = entry(seeded);
        Assertions.assertEquals(InboundFileState.STAGED, after.state());
        Assertions.assertEquals(
                dispositions(Map.of(RowDisposition.STAGED, 5L)),
                after.dispositions(),
                "the redone chunk found its own documents: not duplicates");
        Assertions.assertEquals(
                written(DESIGN_DOCUMENTS, DESIGN_CONFIRMED, DESIGN_UNVERIFIED, 0),
                after.written(),
                "the documents the crashed attempt wrote are counted once");
        Assertions.assertEquals(DESIGN_DOCUMENTS, documents(seeded.batch()));
        Assertions.assertEquals(new InboundFile.Checkpoint(cursor(5), 3L), after.checkpoint());
    }

    @Test
    void blankLocationIdStagesPractitionerFieldsOnly() {
        String tenant = "stage-noloc";
        Seeded seeded = seed(tenant, InboundFileState.PARSED, 2);
        stubRows(
                ingestionId(tenant),
                null,
                ndjson(List.of(
                        DesignRows.row(1, "cert-c", "1003002892", " "),
                        DesignRows.row(2, "cert-d", "1003002893", "loc-d1"))));
        stubComplete(ingestionId(tenant), 200);

        run(seeded);

        InboundFile after = entry(seeded);
        Assertions.assertEquals(InboundFileState.STAGED, after.state());
        Assertions.assertEquals(
                dispositions(Map.of(RowDisposition.STAGED, 1L, RowDisposition.MISSING_IDS, 1L)), after.dispositions());
        Assertions.assertTrue(recommendations
                .find(tenant, id(seeded.batch(), "cert-c", null, "first_name"))
                .isPresent());
        Assertions.assertEquals(
                0L,
                collections
                        .fieldRecommendations()
                        .countDocuments(Filters.and(
                                Filters.eq("exportBatchId", seeded.batch().id()),
                                Filters.eq("practitioner.certifyPractitionerId", "cert-c"),
                                Filters.ne("location", null))),
                "no location-level document for the row without a location id");
        Assertions.assertEquals(3L, documents(seeded.batch()));
        // Row 1 counts only its four practitioner-level cells: first_name written, three unverified.
        Assertions.assertEquals(written(3, 1, 3 + DesignRows.UNVERIFIED_PER_ROW, 0), after.written());
    }

    @Test
    void secondUploadOfSameRowsIsDuplicate() {
        String tenant = "stage-twice";
        Seeded first = seed(tenant, InboundFileState.PARSED, 5);
        stubRows(ingestionId(tenant), null, ndjson(designFile()));
        stubComplete(ingestionId(tenant), 200);
        run(first);
        Seeded second = secondUpload(first, ingestionId(tenant) + "-2");
        stubRows(ingestionId(tenant) + "-2", null, ndjson(designFile()));
        stubComplete(ingestionId(tenant) + "-2", 200);

        run(second);

        InboundFile after = entry(second);
        Assertions.assertEquals(InboundFileState.STAGED, after.state());
        Assertions.assertEquals(dispositions(Map.of(RowDisposition.DUPLICATE, 5L)), after.dispositions());
        Assertions.assertEquals(written(0, DESIGN_CONFIRMED, DESIGN_UNVERIFIED, 0), after.written());
        Assertions.assertEquals(DESIGN_DOCUMENTS, documents(first.batch()));
        Assertions.assertEquals(
                dispositions(Map.of(RowDisposition.STAGED, 5L)), entry(first).dispositions());
    }

    /** A second upload of the same batch, already parsed (as an operator promotion would leave it). */
    private Seeded secondUpload(Seeded first, String ingestion) {
        String generation = "1760520098765432";
        String uploadId = first.entry().bucket() + "|" + first.entry().objectName() + "|" + generation;
        Document element = first.entry().toDocument();
        element.put("uploadId", uploadId);
        element.put("generation", generation);
        element.put("externalRef", Ids.externalRef(first.batch().id(), generation));
        element.put("ingestionId", ingestion);
        element.put("state", InboundFileState.PARSED.name());
        element.put("parse", new Document("status", "COMPLETED").append("populatedRows", 5L));
        element.remove("checkpoint");
        element.remove("dispositions");
        element.remove("written");
        element.remove("stagedAt");
        collections.batches().updateOne(Filters.eq("_id", first.batch().id()), Updates.push("inboundFiles", element));
        return new Seeded(first.batch(), entry(uploadId));
    }

    @Test
    void countMismatchFailsTheEntry() {
        String tenant = "stage-mismatch";
        Seeded seeded = seed(tenant, InboundFileState.PARSED, 6);
        stubRows(ingestionId(tenant), null, ndjson(designFile()));
        stubComplete(ingestionId(tenant), 200);
        int eventsBefore = trail(seeded).size();

        run(seeded);

        InboundFile after = entry(seeded);
        Assertions.assertEquals(InboundFileState.FAILED, after.state());
        Assertions.assertEquals("STAGE", after.failedStep());
        Assertions.assertEquals("COUNT_MISMATCH", after.failureReason());
        Assertions.assertEquals("5 vs 6", after.lastError());
        Assertions.assertEquals(
                List.of(AuditEventType.INBOUND_FILE_FAILED),
                trail(seeded).subList(eventsBefore, trail(seeded).size()).stream()
                        .map(AuditEvent::type)
                        .toList());
        verifyCompleted(ingestionId(tenant), 0);
    }

    @Test
    void rowsExpiredFailsTheEntry() {
        for (int status : new int[] {404, 410}) {
            String tenant = "stage-expired-" + status;
            Seeded seeded = seed(tenant, InboundFileState.PARSED, 5);
            stubRowsStatus(ingestionId(tenant), status);

            run(seeded);

            InboundFile after = entry(seeded);
            Assertions.assertEquals(InboundFileState.FAILED, after.state(), "HTTP " + status);
            Assertions.assertEquals("STAGE", after.failedStep());
            Assertions.assertEquals("ROWS_EXPIRED", after.failureReason());
            Assertions.assertEquals("HTTP " + status, after.lastError());
            Assertions.assertEquals(0L, documents(seeded.batch()));
            verifyCompleted(ingestionId(tenant), 0);
        }
    }

    @Test
    void tooManyStreamsPropagates() {
        String tenant = "stage-busy";
        Seeded seeded = seed(tenant, InboundFileState.PARSED, 5);
        stubRowsStatus(ingestionId(tenant), 429);
        double before = counter(VendorExchangeMetrics.INGESTION_STAGE_STREAM_429);

        WebApplicationException refused = Assertions.assertThrows(WebApplicationException.class, () -> run(seeded));

        Assertions.assertEquals(429, refused.getResponse().getStatus());
        Assertions.assertEquals(1.0, counter(VendorExchangeMetrics.INGESTION_STAGE_STREAM_429) - before);
        InboundFile after = entry(seeded);
        Assertions.assertEquals(InboundFileState.STAGING, after.state(), "left for the JobRunr retry");
        Assertions.assertNull(after.failureReason());
    }

    @Test
    void markConsumedFailureLeavesTheEntryStaged() {
        String tenant = "stage-courtesy";
        Seeded seeded = seed(tenant, InboundFileState.PARSED, 5);
        stubRows(ingestionId(tenant), null, ndjson(designFile()));
        stubComplete(ingestionId(tenant), 503);

        run(seeded);

        Assertions.assertEquals(InboundFileState.STAGED, entry(seeded).state());
        verifyCompleted(ingestionId(tenant), 1);
    }

    @Test
    void entryInAnotherStateIsANoOp() {
        String tenant = "stage-noop";
        Seeded seeded = seed(tenant, InboundFileState.STAGED, 5);
        InboundFile before = entry(seeded);

        run(seeded);

        Assertions.assertEquals(before, entry(seeded));
        WireMockUpstreams.verify(0, WireMock.getRequestedFor(WireMock.urlPathEqualTo(rowsPath(ingestionId(tenant)))));
    }

    @Test
    void rowStreamIsReadAsItArrives() throws Exception {
        String tenant = "stage-stream";
        Seeded seeded = seed(tenant, InboundFileState.PARSED, 5);
        String body = ndjson(designFile());
        // Five slices over three seconds: rows 1 and 2 are complete after about 1.2 s.
        WireMockUpstreams.stubFor(rowsRequest(ingestionId(tenant), null)
                .willReturn(WireMock.aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/x-ndjson")
                        .withBody(body)
                        .withChunkedDribbleDelay(5, 3000)));
        stubComplete(ingestionId(tenant), 200);
        List<Long> checkpointedAt = new CopyOnWriteArrayList<>();
        Mockito.doAnswer(invocation -> {
                    checkpointedAt.add(System.nanoTime());
                    return invocation.callRealMethod();
                })
                .when(stagingLifecycle)
                .chunkCheckpoint(
                        ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any());

        try (InputStream probe = fileIngestion.rows(tenant, "vendor-ingestion", ingestionId(tenant), null)) {
            Assertions.assertFalse(
                    probe instanceof ByteArrayInputStream,
                    "the client hands over the network stream, not a buffered copy: "
                            + probe.getClass().getName());
        }
        run(seeded);
        long finished = System.nanoTime();

        Assertions.assertEquals(InboundFileState.STAGED, entry(seeded).state());
        Assertions.assertEquals(3, checkpointedAt.size());
        long leadMillis = (finished - checkpointedAt.get(0)) / 1_000_000;
        Assertions.assertTrue(
                leadMillis > 1000,
                "the first chunk was checkpointed " + leadMillis + " ms before the body ended; a buffered"
                        + " body would checkpoint every chunk after the last byte");
    }
}
