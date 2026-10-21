package com.certifyos.directory_accuracy.export.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Map;

/**
 * The envelope Pub/Sub wraps a push delivery in: the message (base64 data, id, publish time,
 * attributes) and the subscription name.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record PubSubPushMessage(Message message, String subscription) {

    /** The message inside the envelope. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Message(String data, String messageId, String publishTime, Map<String, String> attributes) {
        public Message {
            attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
        }
    }
}
