package com.certifyos.vendor_exchange.ingestion.jobs;

import com.certifyos.vendor_exchange.clients.DalBatchClient;
import com.certifyos.vendor_exchange.clients.DalBatchRequest;
import com.certifyos.vendor_exchange.clients.DalOperation;
import com.certifyos.vendor_exchange.clients.DalOperationResult;
import com.certifyos.vendor_exchange.clients.DalOperationalValueClient;
import com.certifyos.vendor_exchange.clients.DalSliceClient;
import com.certifyos.vendor_exchange.clients.OperationalValue;
import com.certifyos.vendor_exchange.clients.Slice;
import com.certifyos.vendor_exchange.export.jobs.JobLogContext;
import com.certifyos.vendor_exchange.ingestion.release.Crosswalks;
import com.certifyos.vendor_exchange.ingestion.release.NameSound;
import com.certifyos.vendor_exchange.ingestion.release.RankingGuard;
import com.certifyos.vendor_exchange.ingestion.release.ReleaseLifecycle;
import com.certifyos.vendor_exchange.ingestion.release.SliceMerge;
import com.certifyos.vendor_exchange.ingestion.staging.FieldLevel;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.Response;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;
import org.jobrunr.jobs.annotations.Job;
import org.jobrunr.jobs.context.JobContext;
import org.jobrunr.jobs.lambdas.JobRequestHandler;

/**
 * Writes one release's approved slice fields to the Golden record (design step 8, D18, D21), in the
 * design's phase order: claim, ranking check, resolve the Golden practitioner and locations, the name
 * guard, read our own slices, merge, one transactional DAL {@code /batch} of whole-slice {@code UPSERT}s,
 * record the write, schedule the read-back deadline. A refusal (ranking, unknown entity, name sound, a
 * DAL {@code 400} or {@code 422}) writes nothing and puts the fields back to {@code APPROVED}; any other
 * DAL answer ({@code 401}, {@code 403}, {@code 408}, {@code 409}, {@code 429}, a {@code 5xx}) or a
 * connection failure propagates for JobRunr's retry with the fields still {@code RELEASING}. A retry
 * that finds the write recorded only schedules the deadline. This job never marks {@code RELEASED}:
 * the read-back does.
 */
@ApplicationScoped
public class ReleaseJob implements JobRequestHandler<ReleaseJobRequest> {

    /** Job name used in ids and logs. */
    public static final String NAME = "release";

    static final String RANKING_MISSING = "RANKING_MISSING";
    static final String UNKNOWN_ENTITY = "UNKNOWN_ENTITY";
    static final String NAME_SOUND_CHANGED = "NAME_SOUND_CHANGED";
    static final String DAL_REJECTED = "DAL_REJECTED";
    static final String CORE_PRACTITIONER = "CorePractitioner";
    static final String CORE_LOCATION = "CoreLocation";
    static final String CORE_ENTITY_ADDRESS = "CoreEntityAddress";
    static final String LOCATION_ENTITY_ADDRESS = "LocationEntityAddress";
    static final String UPSERT = "UPSERT";
    static final int BODY_MAX = 400;

    /** The DAL answers that refuse the batch itself; every other 4xx is operational and retried. */
    static final Set<Integer> REFUSALS = Set.of(400, 422);

    private static final GenericType<List<DalOperationResult>> RESULTS = new GenericType<>() {};
    private static final Logger LOG = Logger.getLogger(ReleaseJob.class);

    private final ReleaseLifecycle lifecycle;
    private final RankingGuard ranking;
    private final DalSliceClient slices;
    private final DalOperationalValueClient operationalValues;
    private final DalBatchClient batch;
    private final IngestionJobEnqueuer enqueuer;
    private final Clock clock;

    public ReleaseJob(
            ReleaseLifecycle lifecycle,
            RankingGuard ranking,
            @RestClient DalSliceClient slices,
            @RestClient DalOperationalValueClient operationalValues,
            @RestClient DalBatchClient batch,
            IngestionJobEnqueuer enqueuer,
            Clock clock) {
        this.lifecycle = lifecycle;
        this.ranking = ranking;
        this.slices = slices;
        this.operationalValues = operationalValues;
        this.batch = batch;
        this.enqueuer = enqueuer;
        this.clock = clock;
    }

    /** A refusal: nothing is written and the release fails back to {@code APPROVED}. */
    static final class Refused extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private final String reason;

        Refused(String reason, String detail) {
            super(detail);
            this.reason = reason;
        }

        String reason() {
            return reason;
        }
    }

    /** One slice write still to be recorded: its alias, resource, crosswalk, fields and the read that finds its id. */
    private record Pending(
            String alias, String resource, String crosswalkId, List<String> fieldIds, Supplier<Slice> readBack) {}

    /** The batch to send and the writes to record once it is accepted. */
    private record Plan(List<DalOperation> operations, List<Pending> pending) {}

    /**
     * Runs one release.
     *
     * @param request the tenant, the practitioner and the release
     */
    @Override
    @Job(name = "release %0")
    public void run(ReleaseJobRequest request) {
        JobContext job = JobLogContext.currentJob();
        try (IngestionLogContext ignored =
                IngestionLogContext.openRelease(request.tenantId(), request.releaseId(), job)) {
            if (!lifecycle.config().enabled()) {
                // Not a silent no-op: the release would be stranded RELEASE_REQUESTED. JobRunr retries it.
                throw new IllegalStateException("service disabled; release " + request + " deferred to a retry");
            }
            release(request, InboundJobSupport.jobAttempts(job));
        }
    }

    private void release(ReleaseJobRequest request, int attempts) {
        Optional<ReleaseLifecycle.Claimed> claimed = lifecycle.claim(
                request.tenantId(), request.certifyPractitionerId(), request.releaseId(), clock.instant());
        if (claimed.isEmpty()) {
            return;
        }
        if (claimed.get().written()) {
            Instant writtenAt = claimed.get().fields().stream()
                    .map(field -> field.goldenRecordUpdate().writtenAt())
                    .min(Comparator.naturalOrder())
                    .orElseThrow();
            LOG.infof(
                    "release %s: written by an earlier run, scheduling the read-back deadline only",
                    request.releaseId());
            scheduleDeadline(request, writtenAt);
            return;
        }
        try {
            if (!ranking.ranksCandor(request.tenantId())) {
                throw new Refused(RANKING_MISSING, "the tenant's survivorship ranking has no candor entry");
            }
            write(request, plan(request, claimed.get().fields()));
        } catch (Refused refused) {
            LOG.warnf("release %s refused: %s", request.releaseId(), refused.reason());
            lifecycle.failed(
                    request.tenantId(),
                    request.releaseId(),
                    refused.reason(),
                    refused.getMessage(),
                    true,
                    attempts,
                    clock.instant());
        }
    }

    /** Resolve, name guard, read our slices, merge: the operations of one transactional batch. */
    private Plan plan(ReleaseJobRequest request, List<FieldRecommendation> fields) {
        String tenantId = request.tenantId();
        String sourceType = Crosswalks.sourceType(tenantId);
        OperationalValue golden = resolve(
                () -> operationalValues.practitioner(tenantId, request.certifyPractitionerId(), false),
                "practitioner " + request.certifyPractitionerId());
        List<FieldRecommendation> atPractitioner = new ArrayList<>();
        Map<String, List<FieldRecommendation>> byLocation = new LinkedHashMap<>();
        for (FieldRecommendation field : fields) {
            if (field.level() == FieldLevel.LOCATION) {
                byLocation
                        .computeIfAbsent(field.location().certifyLocationId(), id -> new ArrayList<>())
                        .add(field);
            } else {
                atPractitioner.add(field);
            }
        }
        Map<String, OperationalValue> goldenLocations = new LinkedHashMap<>();
        Map<String, Map<String, Object>> serviceAddresses = new LinkedHashMap<>();
        for (String locationId : byLocation.keySet()) {
            OperationalValue location =
                    resolve(() -> operationalValues.location(tenantId, locationId, false), "location " + locationId);
            goldenLocations.put(locationId, location);
            serviceAddresses.put(
                    locationId,
                    SliceMerge.serviceAddress(location)
                            .orElseThrow(() ->
                                    new Refused(UNKNOWN_ENTITY, "location " + locationId + " has no service address")));
        }
        nameGuard(golden, atPractitioner);
        List<DalOperation> operations = new ArrayList<>();
        List<Pending> pending = new ArrayList<>();
        if (!atPractitioner.isEmpty()) {
            String goldenNpi = SliceMerge.practitionerValue(golden, "npi");
            String npi =
                    goldenNpi == null ? atPractitioner.get(0).practitioner().npi() : goldenNpi;
            if (npi == null || npi.isBlank()) {
                throw new Refused(UNKNOWN_ENTITY, "practitioner " + request.certifyPractitionerId() + " has no NPI");
            }
            String crosswalk = Crosswalks.practitioner(npi);
            Supplier<Slice> read = () -> slices.practitioner(tenantId, sourceType, crosswalk);
            SliceMerge.PractitionerSlice merged = SliceMerge.practitioner(golden, own(read), valuesOf(atPractitioner));
            operations.add(upsert(CORE_PRACTITIONER, "practitioner", sourceType, crosswalk, merged.data()));
            pending.add(new Pending("practitioner", CORE_PRACTITIONER, crosswalk, ids(atPractitioner), read));
        }
        int index = 0;
        for (Map.Entry<String, List<FieldRecommendation>> entry : byLocation.entrySet()) {
            index++;
            OperationalValue location = goldenLocations.get(entry.getKey());
            Map<String, Object> address = serviceAddresses.get(entry.getKey());
            String crosswalk = SliceMerge.locationCrosswalk(location, address);
            Supplier<Slice> read = () -> slices.location(tenantId, sourceType, crosswalk);
            Slice own = own(read);
            SliceMerge.LocationSlices merged = SliceMerge.location(location, address, own, valuesOf(entry.getValue()));
            String locationAlias = "location" + index;
            String addressAlias = "address" + index;
            operations.add(
                    upsert(CORE_LOCATION, locationAlias, sourceType, merged.locationCrosswalk(), merged.location()));
            operations.add(
                    upsert(CORE_ENTITY_ADDRESS, addressAlias, sourceType, merged.addressCrosswalk(), merged.address()));
            if (own == null) {
                operations.add(link("link" + index, locationAlias, addressAlias));
            }
            pending.add(
                    new Pending(locationAlias, CORE_LOCATION, merged.locationCrosswalk(), ids(entry.getValue()), read));
        }
        return new Plan(operations, pending);
    }

    /** One DAL batch; on success the write is recorded and the read-back deadline scheduled. */
    private void write(ReleaseJobRequest request, Plan plan) {
        List<DalOperationResult> results;
        Instant sentAt = clock.instant();
        try (Response response = batch.execute(request.tenantId(), new DalBatchRequest(true, plan.operations()))) {
            results = response.readEntity(RESULTS);
        } catch (WebApplicationException failure) {
            int status =
                    failure.getResponse() == null ? 500 : failure.getResponse().getStatus();
            if (REFUSALS.contains(status)) {
                throw new Refused(DAL_REJECTED, "HTTP " + status + ": " + bodyOf(failure));
            }
            // 401/403 (the IAP token), 408, 409, 429 and every 5xx are operational: JobRunr retries.
            throw failure;
        }
        List<ReleaseLifecycle.Write> writes = new ArrayList<>();
        for (Pending pending : plan.pending()) {
            String sliceId = sliceIdOf(results, pending.alias());
            if (sliceId == null) {
                Slice read = own(pending.readBack());
                sliceId = read == null ? null : read.certifyId();
            }
            if (sliceId == null) {
                // Recording a write without its slice id would leave the fields RELEASING for good: the
                // read-back cannot match them and a retry would skip to the deadline. Retry the job.
                throw new IllegalStateException("release " + request.releaseId() + ": the DAL accepted the "
                        + pending.resource() + " write but no slice id could be read for " + pending.alias());
            }
            writes.add(
                    new ReleaseLifecycle.Write(pending.fieldIds(), pending.resource(), pending.crosswalkId(), sliceId));
        }
        Instant writtenAt = clock.instant();
        lifecycle.written(
                request.tenantId(),
                request.releaseId(),
                writes,
                sentAt,
                writtenAt,
                IngestionJobEnqueuer.readBackDeadlineId(request.releaseId()));
        scheduleDeadline(request, writtenAt);
        LOG.infof(
                "release %s: %d operation(s) written for %d slice(s)",
                request.releaseId(), plan.operations().size(), writes.size());
    }

    private void scheduleDeadline(ReleaseJobRequest request, Instant writtenAt) {
        enqueuer.readBackDeadline(
                request.tenantId(),
                request.certifyPractitionerId(),
                request.releaseId(),
                writtenAt.plus(Duration.ofMinutes(lifecycle.config().ingestion().releaseReadBackDeadlineMinutes())));
    }

    private static void nameGuard(OperationalValue golden, List<FieldRecommendation> fields) {
        for (FieldRecommendation field : fields) {
            String current = SliceMerge.practitionerValue(golden, field.field());
            if (!NameSound.same(current, field.vendorValue())) {
                throw new Refused(
                        NAME_SOUND_CHANGED, field.field() + " sounds different from the Golden record's (ask 13)");
            }
        }
    }

    /** A Golden entity by its echoed id; a 404 is a refusal, any other failure propagates. */
    private static OperationalValue resolve(Supplier<OperationalValue> read, String what) {
        try {
            OperationalValue value = read.get();
            if (value == null) {
                throw new Refused(UNKNOWN_ENTITY, what + " not found");
            }
            return value;
        } catch (WebApplicationException failure) {
            if (failure.getResponse() != null && failure.getResponse().getStatus() == 404) {
                throw new Refused(UNKNOWN_ENTITY, what + " not found");
            }
            throw failure;
        }
    }

    /** Our slice as read, or null when the DAL has none yet (404). */
    private static Slice own(Supplier<Slice> read) {
        try {
            return read.get();
        } catch (WebApplicationException failure) {
            if (failure.getResponse() != null && failure.getResponse().getStatus() == 404) {
                return null;
            }
            throw failure;
        }
    }

    private static DalOperation upsert(
            String resource, String alias, String sourceType, String crosswalkId, Map<String, Object> data) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("sourceType", sourceType);
        body.put("crosswalkId", crosswalkId);
        body.put("data", data);
        return new DalOperation(resource, alias, UPSERT, body, Map.of(), List.of());
    }

    /** The service link between our location and address slices, on our location slice's first write. */
    private static DalOperation link(String alias, String locationAlias, String addressAlias) {
        Map<String, Object> keys = new LinkedHashMap<>();
        keys.put("locationId", "${" + locationAlias + ".certifyLocationId}");
        keys.put("entityAddressId", "${" + addressAlias + ".certifyId}");
        Map<String, Object> body = new LinkedHashMap<>(keys);
        body.put("data", Map.of("addressType", "service"));
        return new DalOperation(LOCATION_ENTITY_ADDRESS, alias, UPSERT, body, keys, List.of());
    }

    /** The slice id an operation's result names: {@code certifyId}, or {@code certifyLocationId} on a location. */
    static String sliceIdOf(List<DalOperationResult> results, String alias) {
        if (results == null) {
            return null;
        }
        for (DalOperationResult result : results) {
            if (alias.equals(result.alias()) && result.result() instanceof Map<?, ?> map) {
                Object id = map.get("certifyId") != null ? map.get("certifyId") : map.get("certifyLocationId");
                return id == null ? null : id.toString();
            }
        }
        return null;
    }

    private static Map<String, String> valuesOf(List<FieldRecommendation> fields) {
        Map<String, String> values = new LinkedHashMap<>();
        fields.forEach(field -> values.put(field.field(), field.vendorValue()));
        return values;
    }

    private static List<String> ids(List<FieldRecommendation> fields) {
        return fields.stream().map(FieldRecommendation::id).toList();
    }

    private static String bodyOf(WebApplicationException failure) {
        try {
            String body = failure.getResponse().readEntity(String.class);
            if (body == null) {
                return "";
            }
            return body.length() <= BODY_MAX ? body : body.substring(0, BODY_MAX);
        } catch (RuntimeException unreadable) {
            return "";
        }
    }
}
