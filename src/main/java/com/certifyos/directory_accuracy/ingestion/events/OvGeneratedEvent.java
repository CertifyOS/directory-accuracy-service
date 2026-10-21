package com.certifyos.directory_accuracy.ingestion.events;

import com.certifyos.directory_accuracy.export.events.PubSubPushMessage;
import com.certifyos.directory_accuracy.ingestion.release.Crosswalks;
import com.certifyos.directory_accuracy.ingestion.release.ReadBackService;
import com.certifyos.directory_accuracy.ingestion.staging.FieldLevel;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * Survivorship's {@code mdm.slice.ov_generated} event: an {@code MDMEnvelope} around an {@code
 * OVCompletionPayload}, base64 in the push message's {@code data} (design step 8). The message
 * attributes {@code entityType} and {@code triggeringSourceTypes} (comma-joined) fill what the payload
 * lacks. Provisional until a live event is read (ask 18): a payload sent without the envelope is read
 * too.
 *
 * @param eventId the envelope's event id
 * @param serviceSource who published it
 * @param payload the operational value that was written and the slices that triggered the run
 * @param triggeringSourceTypes the {@code triggeringSourceTypes} attribute, split
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record OvGeneratedEvent(
        String eventId, String serviceSource, Payload payload, List<String> triggeringSourceTypes) {

    public OvGeneratedEvent {
        triggeringSourceTypes = triggeringSourceTypes == null ? List.of() : List.copyOf(triggeringSourceTypes);
    }

    /**
     * The operational value survivorship wrote.
     *
     * @param certifyId the entity's certify id (a practitioner's, or a location's)
     * @param tenantId the tenant
     * @param entityType {@code practitioner}, {@code location}, or another entity
     * @param triggeringCrosswalks the slices whose change triggered the run
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Payload(String certifyId, String tenantId, String entityType, List<Crosswalk> triggeringCrosswalks) {
        public Payload {
            triggeringCrosswalks = triggeringCrosswalks == null ? List.of() : List.copyOf(triggeringCrosswalks);
        }
    }

    /**
     * One triggering slice.
     *
     * @param certifyId the slice's certify id
     * @param crosswalkId its crosswalk id
     * @param sourceType its source type
     * @param updatedAt when it changed
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Crosswalk(String certifyId, String crosswalkId, String sourceType, String updatedAt) {}

    /**
     * Reads a push message.
     *
     * @param message the Pub/Sub message
     * @param mapper the service's mapper
     * @return the event, or empty when the data is missing, not base64, not the envelope, or names no
     *     entity or tenant
     */
    public static Optional<OvGeneratedEvent> fromMessage(PubSubPushMessage.Message message, ObjectMapper mapper) {
        if (message.data() == null || message.data().isBlank()) {
            return Optional.empty();
        }
        byte[] json;
        try {
            json = Base64.getDecoder().decode(message.data());
        } catch (IllegalArgumentException notBase64) {
            return Optional.empty();
        }
        OvGeneratedEvent read;
        try {
            read = mapper.readValue(json, OvGeneratedEvent.class);
            if (read.payload() == null) {
                read = new OvGeneratedEvent(null, null, mapper.readValue(json, Payload.class), List.of());
            }
        } catch (IOException unreadable) {
            return Optional.empty();
        }
        Payload payload = read.payload();
        if (blank(payload.certifyId()) || blank(payload.tenantId())) {
            return Optional.empty();
        }
        String entityType = blank(payload.entityType()) ? message.attributes().get("entityType") : payload.entityType();
        String sourceTypes = message.attributes().getOrDefault("triggeringSourceTypes", "");
        return Optional.of(new OvGeneratedEvent(
                read.eventId(),
                read.serviceSource(),
                new Payload(payload.certifyId(), payload.tenantId(), entityType, payload.triggeringCrosswalks()),
                Arrays.stream(sourceTypes.split(","))
                        .map(String::trim)
                        .filter(type -> !type.isEmpty())
                        .toList()));
    }

    /**
     * Whether one of our candor slices triggered the run, by the payload's crosswalks or the attribute.
     *
     * @return true when a triggering source type starts with {@code candor:}
     */
    public boolean triggeredByCandor() {
        return payload.triggeringCrosswalks().stream().anyMatch(crosswalk -> ours(crosswalk.sourceType()))
                || triggeringSourceTypes.stream().anyMatch(OvGeneratedEvent::ours);
    }

    /**
     * Our candor slices among the triggering crosswalks, as the read-back matches them against the
     * recorded writes; the change time is null when absent or not an ISO instant.
     *
     * @return the triggers, possibly empty when only the attribute named a candor source
     */
    public List<ReadBackService.Trigger> ourTriggers() {
        return payload.triggeringCrosswalks().stream()
                .filter(crosswalk -> ours(crosswalk.sourceType()))
                .map(crosswalk -> new ReadBackService.Trigger(
                        crosswalk.certifyId(), crosswalk.crosswalkId(), instantOf(crosswalk.updatedAt())))
                .toList();
    }

    private static Instant instantOf(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException unparsed) {
            return null;
        }
    }

    /**
     * Which of our field levels the entity is.
     *
     * @return {@code PRACTITIONER} or {@code LOCATION}, or null for any other entity type
     */
    public FieldLevel level() {
        String type = payload.entityType() == null ? "" : payload.entityType().toLowerCase(Locale.ROOT);
        return switch (type) {
            case "practitioner" -> FieldLevel.PRACTITIONER;
            case "location" -> FieldLevel.LOCATION;
            default -> null;
        };
    }

    private static boolean ours(String sourceType) {
        return sourceType != null && sourceType.startsWith(Crosswalks.SOURCE_PREFIX);
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }
}
