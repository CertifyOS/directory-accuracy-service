package com.certifyos.directory_accuracy;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * One MongoDB 7 replica set (Testcontainers) per test profile; transactions need a replica set.
 * The image is pinned by digest (mongo:7.0 as resolved on 2026-10-01). This resource supplies
 * config values only. It cannot select the active profile; that is what {@link ApiTestProfile}
 * and {@link WorkerTestProfile} are for.
 */
public class MongoResource implements QuarkusTestResourceLifecycleManager {

    static final DockerImageName MONGO_IMAGE = DockerImageName.parse(
                    "mongo@sha256:9854f7139445d766a9523571d6f047530c45547460ffcf8259eb2bf4264632ca")
            .asCompatibleSubstituteFor("mongo");

    private static final MongoDBContainer MONGO = new MongoDBContainer(MONGO_IMAGE);

    @Override
    public Map<String, String> start() {
        MONGO.start();
        return Map.of("quarkus.mongodb.connection-string", MONGO.getReplicaSetUrl("directory_accuracy"));
    }

    @Override
    public void stop() {
        MONGO.stop();
    }
}
