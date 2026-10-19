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
 * Where Pub/Sub pushes the vendor bucket's storage notifications. The identity token is verified by
 * the bound filter before this code runs. Every handler outcome answers 204 (a switched-off service
 * answers 503 through the handler's exception); an envelope with no
 * message is acknowledged too, since it will not improve on redelivery. A storage notification may
 * carry no {@code data} at all (payload format NONE), so only the message is required. An exception
 * from the handler reaches the 500 mapper and Pub/Sub redelivers.
 */
@Path("/internal/vendor-ingestion/arrivals")
@PubSubPush
@Tag(name = "internal")
public class ArrivalResource {

    private static final Logger LOG = Logger.getLogger(ArrivalResource.class);

    private final ArrivalHandler handler;

    public ArrivalResource(ArrivalHandler handler) {
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
            summary = "Receive a vendor bucket storage notification",
            description = "Pub/Sub push endpoint for object notifications under the vendor bucket. Requires the "
                    + "subscription's OIDC identity token; reads the message attributes only and answers 204 "
                    + "to acknowledge every outcome, including an event it ignores or cannot match; answers 503 while "
                    + "the service is switched off so the message is redelivered.")
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
            LOG.warn("arrival push without a message, acknowledged and dropped");
            return Response.noContent().build();
        }
        ArrivalHandler.Outcome outcome = handler.handle(envelope.message());
        LOG.infof("arrival %s: %s", envelope.message().messageId(), outcome);
        return Response.noContent().build();
    }
}
