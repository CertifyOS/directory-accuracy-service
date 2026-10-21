package com.certifyos.directory_accuracy.export.jobs;

import com.certifyos.directory_accuracy.export.batch.BatchState;
import com.certifyos.directory_accuracy.export.batch.EgressDetails;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.batch.ExportBatchRepository;
import com.certifyos.directory_accuracy.export.batch.ExportNpi;
import com.certifyos.directory_accuracy.export.batch.ExportNpiRepository;
import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria;
import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria.Clause;
import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria.Operator;
import com.certifyos.directory_accuracy.persistence.Transactions;
import com.mongodb.client.model.Updates;
import java.time.Instant;
import java.time.YearMonth;
import java.util.List;
import java.util.UUID;

/** Batches in the states the completion tests start from, with the egress details the request job would have written. */
public final class EgressFixtures {

    public static final SelectionCriteria CRITERIA =
            new SelectionCriteria(List.of(new Clause("data.delegationStatus", Operator.IN, List.of("Direct"))));
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
     * Inserts a batch, registers {@code npiCount} NPIs and walks it to {@code target} with the
     * egress details set; {@code file.name} and {@code file.path} as the pin writes them.
     */
    public static ExportBatch batchIn(
            Transactions transactions,
            ExportBatchRepository batches,
            ExportNpiRepository npis,
            String tenant,
            BatchState target,
            int npiCount,
            Instant requestedAt) {
        return batchIn(transactions, batches, npis, tenant, "candor", target, npiCount, requestedAt);
    }

    /** As {@link #batchIn(Transactions, ExportBatchRepository, ExportNpiRepository, String, BatchState, int, Instant)}, for a vendor. */
    public static ExportBatch batchIn(
            Transactions transactions,
            ExportBatchRepository batches,
            ExportNpiRepository npis,
            String tenant,
            String vendor,
            BatchState target,
            int npiCount,
            Instant requestedAt) {
        ExportBatch batch = ExportBatch.scheduled(tenant, vendor, PERIOD, 1, CRITERIA, requestedAt.minusSeconds(60));
        EgressDetails egress = requested(tenant, batch.id(), 1, requestedAt);
        transactions.run(session -> {
            batches.insert(session, batch);
            List<ExportNpi> rows = new java.util.ArrayList<>();
            for (int index = 0; index < npiCount; index++) {
                rows.add(ExportNpi.of(
                        batch.id(),
                        tenant,
                        String.format(java.util.Locale.ROOT, "%010d", 1000000000L + index),
                        "p-" + index,
                        requestedAt));
            }
            npis.upsertAll(session, rows);
            Instant then = requestedAt.minusSeconds(30);
            batches.transition(session, batch.id(), BatchState.SCHEDULED, BatchState.NPIS_SELECTED, then, null);
            batches.updateInState(
                    session,
                    batch.id(),
                    BatchState.NPIS_SELECTED,
                    then,
                    Updates.combine(
                            Updates.set("egress", egress.toDocument()),
                            Updates.set("file.name", tenant + "_" + batch.id() + "_20261001.csv"),
                            Updates.set(
                                    "file.path",
                                    "gs://" + BUCKET + "/"
                                            + egress.destination().objectName())));
            if (target == BatchState.NPIS_SELECTED) {
                return null;
            }
            batches.transition(
                    session, batch.id(), BatchState.NPIS_SELECTED, BatchState.EGRESS_REQUESTED, requestedAt, null);
            if (target == BatchState.EGRESS_COMPLETED) {
                batches.transition(
                        session,
                        batch.id(),
                        BatchState.EGRESS_REQUESTED,
                        BatchState.EGRESS_COMPLETED,
                        requestedAt.plusSeconds(600),
                        Updates.combine(
                                Updates.set("egress.completedAt", requestedAt.plusSeconds(600)),
                                Updates.set("egress.completionSource", "EVENT")));
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
