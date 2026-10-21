package com.certifyos.directory_accuracy.persistence;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.mongodb.client.model.Filters;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** A thrown body rolls back every write in it; a returning body commits all of them. */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(ApiTestProfile.class)
public class TransactionsIT {

    @Inject
    Transactions transactions;

    @Inject
    Collections collections;

    @Test
    void thrownBodyRollsBackBothWrites() {
        IllegalStateException boom = new IllegalStateException("after two writes");
        IllegalStateException seen = Assertions.assertThrows(
                IllegalStateException.class,
                () -> transactions.run(session -> {
                    collections
                            .batches()
                            .insertOne(
                                    session,
                                    new Document("_id", "tx-rollback-batch").append("tenantId", "tx-rollback"));
                    collections
                            .events()
                            .insertOne(session, new Document("exportBatchId", "tx-rollback-batch").append("type", "X"));
                    throw boom;
                }));
        Assertions.assertSame(boom, seen);
        Assertions.assertNull(collections
                .batches()
                .find(Filters.eq("_id", "tx-rollback-batch"))
                .first());
        Assertions.assertEquals(
                0, collections.events().countDocuments(Filters.eq("exportBatchId", "tx-rollback-batch")));
    }

    @Test
    void returningBodyCommitsBothWrites() {
        String result = transactions.run(session -> {
            collections
                    .batches()
                    .insertOne(session, new Document("_id", "tx-commit-batch").append("tenantId", "tx-commit"));
            collections
                    .events()
                    .insertOne(session, new Document("exportBatchId", "tx-commit-batch").append("type", "X"));
            return "done";
        });
        Assertions.assertEquals("done", result);
        Assertions.assertNotNull(
                collections.batches().find(Filters.eq("_id", "tx-commit-batch")).first());
        Assertions.assertEquals(1, collections.events().countDocuments(Filters.eq("exportBatchId", "tx-commit-batch")));
    }
}
