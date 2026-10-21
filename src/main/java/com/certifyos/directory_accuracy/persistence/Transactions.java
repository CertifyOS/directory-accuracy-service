package com.certifyos.directory_accuracy.persistence;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.function.Function;

/**
 * One MongoDB transaction around a body of writes. The driver's {@code withTransaction} re-runs the
 * whole body on a transient transaction error, so the body must be safe to run more than once: only
 * MongoDB writes through the given session. Never enqueue a job, publish a message or call another
 * service inside it; do those after this method returns.
 */
@ApplicationScoped
public class Transactions {

    private final MongoClient client;

    public Transactions(MongoClient client) {
        this.client = client;
    }

    /**
     * Runs {@code body} inside one transaction and returns its result.
     *
     * @param body MongoDB writes through the session; re-run as a whole on a transient error
     * @param <T> the body's result type
     * @return the body's result once the transaction committed
     */
    public <T> T run(Function<ClientSession, T> body) {
        try (ClientSession session = client.startSession()) {
            return session.withTransaction(() -> body.apply(session));
        }
    }
}
