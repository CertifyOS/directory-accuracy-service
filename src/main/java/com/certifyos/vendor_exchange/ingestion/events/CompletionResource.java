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
 * Where Pub/Sub pushes file-ingestion's completion events for this consumer. The identity token is
 * verified by the bound filter before this code runs. Every handler outcome answers 204 but one: an
 * entry whose ingestion request is not committed yet answers 503, so Pub/Sub redelivers. An envelope
 * with no message is acknowledged, since it will not improve on redelivery. Any other exception
 * from the handler reaches the 500 mapper and Pub/Sub redelivers.
 */
@Path("/internal/vendor-ingestion/completions")
@PubSubPush
@Tag(name = "internal")
public class CompletionResource {

    private static final Logger LOG = Logger.getLogger(CompletionResource.class);

    private final CompletionHandler handler;

    public CompletionResource(CompletionHandler handler) {
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
            summary = "Receive a file-ingestion completion event",
            description = "Pub/Sub push endpoint for file-ingestion's completion events on this consumer's topic. "
                    + "Requires the subscription's OIDC identity token; reads the base64 event body, finds the "
                    + "inbound file by externalRef and records the parse outcome. Answers 204 to acknowledge "
                    + "every outcome, including an event it cannot read or match, and 503 while the ingestion "
                    + "request for the file is not committed yet, so the event is delivered again.")
    @APIResponse(responseCode = "204", description = "Acknowledged")
    @APIResponse(
            responseCode = "401",
            description =
                    "Missing, rejected or not yet configured push identity (PUSH_TOKEN_REQUIRED, PUSH_TOKEN_REJECTED, PUSH_NOT_CONFIGURED)",
            content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
    @APIResponse(
            responseCode = "503",
            description =
                    "The inbound file's ingestion request is not committed yet; redeliver (ARCHIVING_NOT_COMMITTED)",
            content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
    public Response receive(PubSubPushMessage envelope) {
        if (envelope == null || envelope.message() == null) {
            LOG.warn("completion push without a message, acknowledged and dropped");
            return Response.noContent().build();
        }
        CompletionHandler.Outcome outcome = handler.handle(envelope.message());
        LOG.infof("completion %s: %s", envelope.message().messageId(), outcome);
        return Response.noContent().build();
    }
}
