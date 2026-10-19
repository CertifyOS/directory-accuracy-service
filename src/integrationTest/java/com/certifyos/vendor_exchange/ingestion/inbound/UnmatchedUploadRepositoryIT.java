package com.certifyos.vendor_exchange.ingestion.inbound;

import com.certifyos.vendor_exchange.ApiTestProfile;
import com.certifyos.vendor_exchange.MongoResource;
import com.certifyos.vendor_exchange.persistence.Transactions;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unmatched vendor uploads: the insert that a redelivered event cannot repeat, the one-shot
 * resolution, and the open list. Every folder tenant here starts with {@code unmrepo-}; the
 * unfiltered reads are shared with other tests, so they assert on this test's own ids only.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(ApiTestProfile.class)
public class UnmatchedUploadRepositoryIT {

    private static final Instant NOW = Instant.parse("2026-10-19T10:00:00Z");

    @Inject
    UnmatchedUploadRepository unmatched;

    @Inject
    Transactions transactions;

    @Test
    void secondInsertOfSameUploadIsFalse() {
        String tenant = tenant("insert");
        UnmatchedUpload parsedUpload = parsed(tenant, NOW);
        UnmatchedUpload unparsed = unparsed(tenant, NOW.plusSeconds(1));

        Assertions.assertTrue(insert(parsedUpload));
        Assertions.assertFalse(insert(parsedUpload));
        Assertions.assertTrue(insert(unparsed));

        Assertions.assertEquals(
                parsedUpload, unmatched.find(parsedUpload.id()).orElseThrow(), "a parsed upload round trips");
        UnmatchedUpload back = unmatched.find(unparsed.id()).orElseThrow();
        Assertions.assertEquals(unparsed, back, "an unparsed upload round trips");
        Assertions.assertNull(back.parsed());
        Assertions.assertNull(back.resolution());
        Assertions.assertTrue(unmatched.find("nobody|to/x|1").isEmpty());
    }

    @Test
    void resolveOnlyOnce() {
        String tenant = tenant("resolve");
        UnmatchedUpload upload = parsed(tenant, NOW);
        insert(upload);
        UnmatchedUpload.Resolution attached =
                new UnmatchedUpload.Resolution("ATTACHED", tenant + "-candor-2026-10-001", null, "op-1", NOW);
        UnmatchedUpload.Resolution discarded =
                new UnmatchedUpload.Resolution("DISCARDED", null, "vendor test file", "op-2", NOW.plusSeconds(5));

        Assertions.assertTrue(resolve(upload.id(), attached));
        Assertions.assertFalse(resolve(upload.id(), discarded), "a resolved upload is not resolved again");
        Assertions.assertFalse(resolve("nobody|to/x|1", attached));

        Assertions.assertEquals(
                attached, unmatched.find(upload.id()).orElseThrow().resolution());
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new UnmatchedUpload.Resolution("DELETED", null, null, "op-1", NOW));
    }

    @Test
    void listOpenExcludesResolved() {
        String tenant = tenant("list");
        String other = tenant("list-other");
        UnmatchedUpload oldest = parsed(tenant, NOW);
        UnmatchedUpload middle = unparsed(tenant, NOW.plusSeconds(10));
        UnmatchedUpload resolved = parsed(tenant, NOW.plusSeconds(20));
        UnmatchedUpload newest = parsed(tenant, NOW.plusSeconds(30));
        UnmatchedUpload foreign = parsed(other, NOW.plusSeconds(5));
        for (UnmatchedUpload upload : List.of(newest, resolved, middle, oldest, foreign)) {
            insert(upload);
        }
        long openBefore = unmatched.countOpen();
        resolve(resolved.id(), new UnmatchedUpload.Resolution("DISCARDED", null, "noise", "op-1", NOW));

        Assertions.assertEquals(
                List.of(oldest.id(), middle.id(), newest.id()),
                unmatched.listOpen(tenant, 10).stream().map(UnmatchedUpload::id).toList(),
                "open rows of the tenant, oldest first, resolved and foreign rows left out");
        Assertions.assertEquals(
                List.of(oldest.id(), middle.id()),
                unmatched.listOpen(tenant, 2).stream().map(UnmatchedUpload::id).toList(),
                "the limit cuts the list");
        Assertions.assertEquals(
                List.of(foreign.id()),
                unmatched.listOpen(other, 10).stream().map(UnmatchedUpload::id).toList());
        Assertions.assertTrue(unmatched.listOpen(tenant("empty"), 10).isEmpty());
        Assertions.assertEquals(openBefore - 1, unmatched.countOpen(), "resolving one row lowers the open count");
        Assertions.assertTrue(unmatched.listOpen(null, 1000).size() >= 4, "no tenant lists every folder");
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> unmatched.listOpen(tenant, 0),
                "0 would read as no limit in the driver, so it is refused");
    }

    private boolean insert(UnmatchedUpload upload) {
        return transactions.run(session -> unmatched.insertIfAbsent(session, upload));
    }

    private boolean resolve(String uploadId, UnmatchedUpload.Resolution resolution) {
        return transactions.run(session -> unmatched.resolve(session, uploadId, resolution));
    }

    private static String tenant(String suffix) {
        return "unmrepo-" + suffix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private static UnmatchedUpload parsed(String tenant, Instant receivedAt) {
        String name = "to/" + tenant + "/" + tenant + "_" + tenant + "-candor-2026-10-001_V77_20261019.csv";
        InboundNames.Parsed parts = InboundNames.parse(name).orElseThrow();
        return new UnmatchedUpload(
                "vendor-bucket|" + name + "|" + UUID.randomUUID(),
                tenant,
                parts,
                InboundNames.UNKNOWN_BATCH,
                receivedAt,
                null);
    }

    private static UnmatchedUpload unparsed(String tenant, Instant receivedAt) {
        return new UnmatchedUpload(
                "vendor-bucket|to/" + tenant + "/notes.txt|" + UUID.randomUUID(),
                tenant,
                null,
                InboundNames.UNPARSEABLE_NAME,
                receivedAt,
                null);
    }
}
