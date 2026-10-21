package com.certifyos.directory_accuracy.export.events;

import com.certifyos.directory_accuracy.auth.PubSubPush;
import com.certifyos.directory_accuracy.http.Problem;
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
 * Where Pub/Sub pushes the egress completion event. The identity token is verified by the bound
 * filter before this code runs. Today the resource parses and logs the event and acknowledges it;
 * matching it to a batch and moving the state machine is the export lane's next piece. A malformed
 * message is acknowledged too: Pub/Sub would otherwise redeliver it until the dead-letter limit,
 * and a body that cannot be parsed will not parse better the fifth time.
 */
@Path("/internal/vendor-exports/egress-events")
@PubSubPush
@Tag(name = "internal")
public class EgressEventResource {

    private static final Logger LOG = Logger.getLogger(EgressEventResource.class);

    private final EgressEventHandler handler;

    public EgressEventResource(EgressEventHandler handler) {
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
            summary = "Receive an egress completion event",
            description = "Pub/Sub push endpoint. Requires the subscription's OIDC identity token; "
                    + "answers 204 to acknowledge, including for a message it cannot parse.")
    @APIResponse(responseCode = "204", description = "Acknowledged")
    @APIResponse(
            responseCode = "401",
            description =
                    "Missing, rejected or not yet configured push identity (PUSH_TOKEN_REQUIRED, PUSH_TOKEN_REJECTED, PUSH_NOT_CONFIGURED)",
            content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
    public Response receive(PubSubPushMessage envelope) {
        if (envelope == null || envelope.message() == null || envelope.message().data() == null) {
            LOG.warn("egress event push without message data, acknowledged and dropped");
            return Response.noContent().build();
        }
        EgressEventHandler.Outcome outcome = handler.handle(envelope.message());
        LOG.infof("egress event %s: %s", envelope.message().messageId(), outcome);
        return Response.noContent().build();
    }
}
