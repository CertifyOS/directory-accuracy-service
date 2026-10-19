package com.certifyos.vendor_exchange.ingestion.jobs;

import com.certifyos.vendor_exchange.export.jobs.JobLogContext;
import com.certifyos.vendor_exchange.ingestion.release.ReadBackService;
import com.certifyos.vendor_exchange.ingestion.release.ReleaseLifecycle;
import com.certifyos.vendor_exchange.ingestion.staging.FieldLevel;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.WebApplicationException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jboss.logging.Logger;
import org.jobrunr.jobs.annotations.Job;
import org.jobrunr.jobs.context.JobContext;
import org.jobrunr.jobs.lambdas.JobRequestHandler;

/**
 * The read-back deadline (plan P13): scheduled by the release job at {@code writtenAt +
 * releaseReadBackDeadlineMinutes}, deleted by the {@code mdm.slice.ov_generated} handler once the
 * release is read back. When it runs, the event was lost: it reads each entity of the release that
 * still has {@code RELEASING} fields once ({@code completionSource = DEADLINE}) and finishes only this
 * release's fields there; a read that fails here marks those fields {@code RELEASED UNKNOWN} with a
 * warning (alert A14).
 */
@ApplicationScoped
public class ReleaseReadBackDeadlineJob implements JobRequestHandler<ReleaseReadBackDeadlineJobRequest> {

    /** Job name used in ids and logs. */
    public static final String NAME = "release-readback";

    private static final Logger LOG = Logger.getLogger(ReleaseReadBackDeadlineJob.class);

    private final ReleaseLifecycle lifecycle;
    private final ReadBackService readBack;
    private final Clock clock;

    public ReleaseReadBackDeadlineJob(ReleaseLifecycle lifecycle, ReadBackService readBack, Clock clock) {
        this.lifecycle = lifecycle;
        this.readBack = readBack;
        this.clock = clock;
    }

    /** One Golden entity of the release: the practitioner, or one location. */
    record Entity(FieldLevel level, String certifyId) {}

    /**
     * Runs the deadline for one release.
     *
     * @param request the tenant, the practitioner and the release
     */
    @Override
    @Job(name = "release-readback %0")
    public void run(ReleaseReadBackDeadlineJobRequest request) {
        JobContext job = JobLogContext.currentJob();
        try (IngestionLogContext ignored =
                IngestionLogContext.openRelease(request.tenantId(), request.releaseId(), job)) {
            if (!lifecycle.config().enabled()) {
                throw new IllegalStateException(
                        "service disabled; release read-back deadline " + request + " deferred to a retry");
            }
            List<FieldRecommendation> releasing = lifecycle.releasing(request.tenantId(), request.releaseId());
            if (releasing.isEmpty()) {
                LOG.infof("release %s: nothing RELEASING at the deadline, already read back", request.releaseId());
                return;
            }
            Instant now = clock.instant();
            byEntity(releasing).forEach((entity, fields) -> readOnce(request, entity, fields, now));
        }
    }

    private void readOnce(
            ReleaseReadBackDeadlineJobRequest request, Entity entity, List<FieldRecommendation> fields, Instant now) {
        try {
            ReadBackService.Result result = readBack.atDeadline(
                    request.tenantId(), entity.certifyId(), entity.level(), request.releaseId(), now);
            LOG.infof(
                    "release %s %s %s: deadline read-back %s",
                    request.releaseId(), entity.level(), entity.certifyId(), result);
        } catch (WebApplicationException | ProcessingException failure) {
            LOG.warnf(
                    "release %s %s %s: the operational value could not be read at the deadline (%s); %d field(s)"
                            + " marked UNKNOWN",
                    request.releaseId(),
                    entity.level(),
                    entity.certifyId(),
                    failure.getClass().getSimpleName(),
                    fields.size());
            readBack.markUnknown(request.tenantId(), fields, ReleaseLifecycle.DEADLINE, now);
        }
    }

    private static Map<Entity, List<FieldRecommendation>> byEntity(List<FieldRecommendation> fields) {
        Map<Entity, List<FieldRecommendation>> groups = new LinkedHashMap<>();
        for (FieldRecommendation field : fields) {
            Entity entity = field.level() == FieldLevel.LOCATION
                    ? new Entity(FieldLevel.LOCATION, field.location().certifyLocationId())
                    : new Entity(FieldLevel.PRACTITIONER, field.practitioner().certifyPractitionerId());
            groups.computeIfAbsent(entity, key -> new ArrayList<>()).add(field);
        }
        return groups;
    }
}
