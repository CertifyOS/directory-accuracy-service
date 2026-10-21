package com.certifyos.vendor_exchange.ingestion.events;

import com.certifyos.vendor_exchange.auth.PubSubPush;
import com.certifyos.vendor_exchange.export.events.PubSubPushMessage;
import com.certifyos.vendor_exchange.http.Problem;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.jboss.logging.Logger;

/**
 * Where Pub/Sub pushes survivorship's {@code mdm.slice.ov_generated} events (subscription {@code
 * vendor-ingestion-ov-generated}, filtered to runs a candor slice triggered). The identity token is
 * verified by the bound filter first. Every handler outcome answers 204; an envelope with no message is
 * acknowledged too. An exception from the handler (an unreadable operational value, or fields not yet
 * marked written) reaches the 500 mapper and Pub/Sub redelivers.
 */
@Path("/internal/vendor-ingestion/ov-generated")
@PubSubPush
@Tag(name = "internal")
public class OvGeneratedResource {

    private static final Logger LOG = Logger.getLogger(OvGeneratedResource.class);

    private final OvGeneratedHandler handler;

    public OvGeneratedResource(OvGeneratedHandler handler) {
        this.handler = handler;
    }

    /**
     * Receives one push delivery.
     *
     * @param envelope the Pub/Sub envelope
     * @return 204, which Pub/Sub reads as an acknowledgement
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Receive a survivorship operational-value event",
            description = "Pub/Sub push endpoint for mdm.slice.ov_generated. Requires the subscription's OIDC identity "
                    + "token; reads the operational value back for fields released through a candor slice and "
                    + "answers 204 to acknowledge every outcome, including an event that is not ours; answers 503 "
                    + "while the service is switched off so the message is redelivered.")
    @APIResponse(responseCode = "204", description = "Acknowledged")
    @APIResponse(
            responseCode = "503",
            description = "The service is switched off (SERVICE_DISABLED); Pub/Sub redelivers the message",
            content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
    @APIResponse(
            responseCode = "401",
            description =
                    "Missing, rejected or not yet configured push identity (PUSH_TOKEN_REQUIRED, PUSH_TOKEN_REJECTED, PUSH_NOT_CONFIGURED)",
            content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
    public Response receive(PubSubPushMessage envelope) {
        if (envelope == null || envelope.message() == null) {
            LOG.warn("ov-generated push without a message, acknowledged and dropped");
            return Response.noContent().build();
        }
        OvGeneratedHandler.Outcome outcome = handler.handle(envelope.message());
        LOG.infof("ov-generated %s: %s", envelope.message().messageId(), outcome);
        return Response.noContent().build();
    }
}
