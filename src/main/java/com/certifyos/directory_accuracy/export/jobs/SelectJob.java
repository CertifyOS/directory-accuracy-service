package com.certifyos.directory_accuracy.export.jobs;

import com.certifyos.directory_accuracy.clients.ApiLayerClient;
import com.certifyos.directory_accuracy.clients.PagedPractitioners;
import com.certifyos.directory_accuracy.clients.PractitionerRef;
import com.certifyos.directory_accuracy.export.batch.BatchLifecycle;
import com.certifyos.directory_accuracy.export.batch.BatchState;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.batch.ExportNpi;
import com.certifyos.directory_accuracy.export.batch.ExportNpiRepository;
import com.certifyos.directory_accuracy.export.schedule.SelectionPreview;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;
import org.jobrunr.jobs.annotations.Job;
import org.jobrunr.jobs.lambdas.JobRequestHandler;

/**
 * Lifecycle step 2: page api-layer for the practitioners the batch's criteria select, register
 * them, move the batch to {@code NPIS_SELECTED} (or {@code EMPTY}). Talks to one external system,
 * api-layer. Each page is one transaction (rows upserted, cursor saved, {@code NPIS_REGISTERED}),
 * so a failure mid-way costs one page: the retry resumes after the saved cursor and the upsert
 * makes a repeated page harmless. api-layer errors propagate, which is what makes JobRunr retry.
 * A practitioner without a ten-digit NPI cannot be sent to the vendor and is skipped and counted.
 */
@ApplicationScoped
public class SelectJob implements JobRequestHandler<SelectJobRequest> {

    /** Job name used in ids and logs. */
    public static final String NAME = "select";

    /** api-layer page size, from the design. */
    static final int PAGE_SIZE = 100;

    private static final Pattern NPI = Pattern.compile("\\d{10}");
    private static final Logger LOG = Logger.getLogger(SelectJob.class);

    private final BatchJobSupport support;
    private final BatchLifecycle lifecycle;
    private final ExportNpiRepository npis;
    private final ApiLayerClient apiLayer;
    private final SelectionPreview filters;
    private final Clock clock;

    public SelectJob(
            BatchJobSupport support,
            BatchLifecycle lifecycle,
            ExportNpiRepository npis,
            @RestClient ApiLayerClient apiLayer,
            SelectionPreview filters,
            Clock clock) {
        this.support = support;
        this.lifecycle = lifecycle;
        this.npis = npis;
        this.apiLayer = apiLayer;
        this.filters = filters;
        this.clock = clock;
    }

    /**
     * Runs the job for one batch.
     *
     * @param request the batch, expected in {@code SCHEDULED}, and the attempt
     */
    @Override
    @Job(name = "select %0")
    public void run(SelectJobRequest request) {
        try (JobLogContext ignored = JobLogContext.open(request.exportBatchId(), JobLogContext.currentJob())) {
            Optional<ExportBatch> loaded = support.loadExpecting(NAME, request.exportBatchId(), BatchState.SCHEDULED);
            if (loaded.isEmpty()) {
                return;
            }
            select(loaded.get());
        }
    }

    private void select(ExportBatch batch) {
        Instant started = clock.instant();
        String filter = filters.filterJson(batch.selection().criteria());
        Integer cursor = batch.selection().page();
        int page = cursor == null ? 0 : cursor + 1;
        if (cursor != null) {
            LOG.infof("%s resumes after page %d", batch.id(), cursor);
        }
        while (true) {
            PagedPractitioners result = apiLayer.practitionerFindMany(batch.tenantId(), filter, page, PAGE_SIZE);
            Instant now = clock.instant();
            PageRows built = rows(batch, result.data(), now);
            List<ExportNpi> rows = built.rows();
            int skipped = built.skipped();
            if (skipped > 0) {
                LOG.warnf("%s page %d: %d practitioners without a ten-digit NPI skipped", batch.id(), page, skipped);
            }
            long inserted = lifecycle.registerPage(batch, page, rows, skipped, now);
            LOG.infof("%s page %d: %d rows, %d new", batch.id(), page, rows.size(), inserted);
            if (isLast(result, page)) {
                break;
            }
            page++;
        }
        long registered = npis.countForBatch(batch.id());
        long durationMs = clock.millis() - started.toEpochMilli();
        if (registered == 0) {
            lifecycle.markEmpty(batch, clock.instant());
        } else {
            lifecycle.completeSelection(batch, page + 1, registered, durationMs, clock.instant());
        }
    }

    /** One page's registry rows and the count of practitioners without a usable NPI. */
    private record PageRows(List<ExportNpi> rows, int skipped) {}

    /**
     * Turns one api-layer page into registry rows: practitioners without a ten-digit NPI are
     * skipped and counted, and an NPI is kept once (two practitioners sharing an NPI on one page
     * would otherwise be two upserts of one {@code _id} in a single bulk write).
     */
    private static PageRows rows(ExportBatch batch, List<PractitionerRef> data, Instant now) {
        List<ExportNpi> rows = new ArrayList<>(data.size());
        java.util.Set<String> seen = new java.util.HashSet<>();
        int skipped = 0;
        for (PractitionerRef ref : data) {
            if (ref.npi() == null || !NPI.matcher(ref.npi()).matches()) {
                skipped++;
            } else if (seen.add(ref.npi())) {
                rows.add(ExportNpi.of(batch.id(), batch.tenantId(), ref.npi(), ref.id(), now));
            }
        }
        return new PageRows(rows, skipped);
    }

    private static boolean isLast(PagedPractitioners result, int page) {
        if (result.data().size() < PAGE_SIZE) {
            return true;
        }
        Long total = result.totalCount();
        return total != null && (long) (page + 1) * PAGE_SIZE >= total;
    }
}
