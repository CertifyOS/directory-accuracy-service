package com.certifyos.directory_accuracy.ingestion.review;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.ingestion.ReviewFixtures;
import com.certifyos.directory_accuracy.ingestion.staging.FieldRecommendation;
import com.certifyos.directory_accuracy.persistence.Collections;
import com.certifyos.directory_accuracy.persistence.Ids;
import com.certifyos.directory_accuracy.persistence.Transactions;
import com.mongodb.client.model.Filters;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Instant;
import java.time.YearMonth;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * {@code vendor_rejected_values}: one row per distinct normalised value of one practitioner,
 * location and field across cycles, upserted on each rejection. Every tenant here starts with
 * {@code rejrepo-}.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(ApiTestProfile.class)
class RejectedValueRepositoryIT {

    static final Instant OCTOBER = Instant.parse("2026-10-20T14:02:11Z");
    static final Instant DECEMBER = Instant.parse("2026-12-18T10:41:05Z");

    @Inject
    RejectedValueRepository repository;

    @Inject
    Transactions transactions;

    @Inject
    Collections collections;

    @Test
    void secondRejectionOfSameValueIsOneRowWithCountTwo() {
        String tenant = "rejrepo-twice";
        FieldRecommendation october = phone(tenant, 10, "(303) 679-0177");
        FieldRecommendation december = phone(tenant, 12, "1-303-679-0177");

        RejectedValueRepository.Upserted first =
                upsert(october, "user:reviewer-7", "Office confirmed old number", OCTOBER);
        RejectedValueRepository.Upserted second = upsert(december, "user:reviewer-3", "Still the old number", DECEMBER);

        String id = Ids.rejectedValueId(tenant, "cert-000123", "loc-7781", "phone", "3036790177");
        Assertions.assertEquals(new RejectedValueRepository.Upserted(id, 1), first);
        Assertions.assertEquals(new RejectedValueRepository.Upserted(id, 2), second);
        RejectedValue row = repository.find(tenant, id).orElseThrow();
        Assertions.assertEquals(tenant, row.tenantId());
        Assertions.assertEquals("cert-000123", row.certifyPractitionerId());
        Assertions.assertEquals("loc-7781", row.certifyLocationId());
        Assertions.assertEquals("phone", row.field());
        Assertions.assertEquals("3036790177", row.normalizedValue());
        Assertions.assertEquals(1, row.normalizationVersion());
        Assertions.assertEquals("(303) 679-0177", row.rawValue(), "the value as first received");
        Assertions.assertEquals(2L, row.rejectionCount());
        Assertions.assertEquals(OCTOBER, row.firstRejectedAt());
        Assertions.assertEquals(DECEMBER, row.lastRejectedAt());
        Assertions.assertEquals(
                Instant.parse("2027-12-18T10:41:05Z"), row.expiresAt(), "twelve months after the latest");
        Assertions.assertEquals(
                List.of(
                        new RejectedValue.Rejection(
                                october.exportBatchId(), "user:reviewer-7", OCTOBER, "Office confirmed old number"),
                        new RejectedValue.Rejection(
                                december.exportBatchId(), "user:reviewer-3", DECEMBER, "Still the old number")),
                row.rejections());
        Assertions.assertEquals(1, collections.rejectedValues().countDocuments(Filters.eq("tenantId", tenant)));
        Assertions.assertTrue(repository.find("rejrepo-other", id).isEmpty(), "reads are tenant-scoped");
    }

    @Test
    void differentValueIsASecondRow() {
        String tenant = "rejrepo-second";
        upsert(phone(tenant, 10, "(303) 679-0177"), "user:a", "old", OCTOBER);
        upsert(phone(tenant, 12, "(303) 679-0199"), "user:a", "also old", DECEMBER);
        FieldRecommendation practitionerLevel = ReviewFixtures.pending(
                tenant, batch(tenant, 12), "cert-000123", "1003002890", null, "last_name", "Smith-Jones", DECEMBER);
        RejectedValueRepository.Upserted name =
                upsert(practitionerLevel, "user:a", "not her name", DECEMBER.plusSeconds(60));

        Assertions.assertTrue(name.id().startsWith(tenant + "|cert-000123|-|last_name|"), name.id());
        Assertions.assertNull(repository.find(tenant, name.id()).orElseThrow().certifyLocationId());
        List<RejectedValue> rows = repository.forPractitioner(tenant, "cert-000123", 10);
        Assertions.assertEquals(3, rows.size());
        Assertions.assertEquals(
                List.of("SMITHJONES", "3036790199", "3036790177"),
                rows.stream().map(RejectedValue::normalizedValue).toList(),
                "newest rejection first");
        Assertions.assertEquals(
                1, repository.forPractitioner(tenant, "cert-000123", 1).size(), "the limit holds");
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> repository.forPractitioner(tenant, "cert-000123", 0));
    }

    @Test
    void historyKeepsTheLastTwentyRejections() {
        String tenant = "rejrepo-slice";
        FieldRecommendation doc = phone(tenant, 10, "(303) 679-0177");
        RejectedValueRepository.Upserted last = null;
        for (int index = 0; index < 22; index++) {
            last = upsert(doc, "user:r" + index, "again", OCTOBER.plusSeconds(index));
        }

        RejectedValue row = repository.find(tenant, last.id()).orElseThrow();
        Assertions.assertEquals(22L, row.rejectionCount());
        Assertions.assertEquals(20, row.rejections().size());
        Assertions.assertEquals("user:r2", row.rejections().get(0).by(), "the oldest two fell off");
        Assertions.assertEquals("user:r21", row.rejections().get(19).by());
    }

    /** A removal (the vendor sent no value) is remembered too, under the no-value key. */
    @Test
    void aRejectedRemovalIsRememberedUnderTheNoValueKey() {
        String tenant = "rejrepo-removal";
        FieldRecommendation removal = phone(tenant, 10, "  ");

        RejectedValueRepository.Upserted upserted = upsert(removal, "user:a", "the number is still in use", OCTOBER);

        Assertions.assertEquals(
                Ids.rejectedValueId(tenant, "cert-000123", "loc-7781", "phone", RejectedValue.NO_VALUE), upserted.id());
        RejectedValue row = repository.find(tenant, upserted.id()).orElseThrow();
        Assertions.assertEquals(RejectedValue.NO_VALUE, row.normalizedValue());
        Assertions.assertEquals("  ", row.rawValue());
        Document stored = collections
                .rejectedValues()
                .find(Filters.eq("_id", upserted.id()))
                .first();
        Assertions.assertNotNull(stored);
        Assertions.assertInstanceOf(java.util.Date.class, stored.get("expiresAt"), "the TTL index needs a date");
    }

    // helpers

    static String batch(String tenant, int month) {
        return Ids.batchId(tenant, "candor", YearMonth.of(2026, month), 1);
    }

    static FieldRecommendation phone(String tenant, int month, String vendorValue) {
        return ReviewFixtures.pending(
                tenant, batch(tenant, month), "cert-000123", "1003002890", "loc-7781", "phone", vendorValue, OCTOBER);
    }

    RejectedValueRepository.Upserted upsert(FieldRecommendation doc, String actor, String reason, Instant at) {
        return transactions.run(session -> repository.upsert(session, doc, actor, reason, at, 12));
    }
}
