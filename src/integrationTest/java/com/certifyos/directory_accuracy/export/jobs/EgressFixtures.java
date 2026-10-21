package com.certifyos.directory_accuracy.export.jobs;

import com.certifyos.directory_accuracy.export.batch.BatchState;
import com.certifyos.directory_accuracy.export.batch.EgressDetails;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.batch.ExportBatchRepository;
import com.certifyos.directory_accuracy.persistence.Transactions;
import com.mongodb.client.model.Updates;
import java.time.Instant;
import java.time.YearMonth;
import java.util.List;
import java.util.UUID;

/** Batches in the states the completion tests start from, with the egress details the request job would have written. */
public final class EgressFixtures {

    public static final String FILTER_ID = "flt-direct";
    public static final YearMonth PERIOD = YearMonth.of(2026, 10);
    public static final String BUCKET = "test-vendor-bucket";

    private EgressFixtures() {}

    /** The egress details after a request at {@code requestedAt}, with the first deadline check id. */
    public static EgressDetails requested(String tenant, String batchId, int attempt, Instant requestedAt) {
        String correlationId = batchId + "-r" + attempt;
        return new EgressDetails(
                correlationId,
                "practitioner:" + tenant + ":" + correlationId,
                "tpl-1",
                3,
                "gs://egress-templates/" + tenant + "/tpl-1/v3/mappings.csv",
                ";",
                "csv",
                List.of("locations"),
                new EgressDetails.Destination(
                        BUCKET, "from/" + tenant + "/" + tenant + "_" + batchId + "_20261001.csv"),
                requestedAt,
                JobIds.deadline(batchId, attempt, 1).toString(),
                null,
                null,
                null);
    }

    /**
     * Inserts a batch and walks it to {@code target} with the egress details set; {@code file.name}
     * and {@code file.path} as the pin writes them. {@code SCHEDULED} is the pinned batch the
     * request job sees on a retry; from {@code EGRESS_COMPLETED} on, egress's counts are recorded
     * with {@code selected} as {@code selection.distinct}, unless it is negative (the event carried
     * no counts).
     */
    public static ExportBatch batchIn(
            Transactions transactions,
            ExportBatchRepository batches,
            String tenant,
            BatchState target,
            int selected,
            Instant requestedAt) {
        return batchIn(transactions, batches, tenant, "candor", target, selected, requestedAt);
    }

    /** As {@link #batchIn(Transactions, ExportBatchRepository, String, BatchState, int, Instant)}, for a vendor. */
    public static ExportBatch batchIn(
            Transactions transactions,
            ExportBatchRepository batches,
            String tenant,
            String vendor,
            BatchState target,
            int selected,
            Instant requestedAt) {
        ExportBatch batch = ExportBatch.scheduled(tenant, vendor, PERIOD, 1, FILTER_ID, requestedAt.minusSeconds(60));
        EgressDetails egress = requested(tenant, batch.id(), 1, requestedAt);
        transactions.run(session -> {
            batches.insert(session, batch);
            Instant then = requestedAt.minusSeconds(30);
            batches.updateInState(
                    session,
                    batch.id(),
                    BatchState.SCHEDULED,
                    then,
                    Updates.combine(
                            Updates.set("egress", egress.toDocument()),
                            Updates.set("file.name", tenant + "_" + batch.id() + "_20261001.csv"),
                            Updates.set(
                                    "file.path",
                                    "gs://" + BUCKET + "/"
                                            + egress.destination().objectName())));
            if (target == BatchState.SCHEDULED) {
                return null;
            }
            batches.transition(
                    session, batch.id(), BatchState.SCHEDULED, BatchState.EGRESS_REQUESTED, requestedAt, null);
            if (target == BatchState.EGRESS_COMPLETED) {
                Instant completedAt = requestedAt.plusSeconds(600);
                List<org.bson.conversions.Bson> updates = new java.util.ArrayList<>(List.of(
                        Updates.set("egress.completedAt", completedAt),
                        Updates.set("egress.completionSource", "EVENT")));
                if (selected >= 0) {
                    updates.add(Updates.set("selection.requested", (long) selected));
                    updates.add(Updates.set("selection.distinct", (long) selected));
                    updates.add(Updates.set("selection.completedAt", completedAt));
                }
                batches.transition(
                        session,
                        batch.id(),
                        BatchState.EGRESS_REQUESTED,
                        BatchState.EGRESS_COMPLETED,
                        completedAt,
                        Updates.combine(updates));
            }
            return null;
        });
        return batches.find(batch.id()).orElseThrow();
    }

    /** The first deadline check's id for a fixture batch. */
    public static UUID deadlineId(ExportBatch batch) {
        return UUID.fromString(batch.egress().deadlineJobId());
    }
}
