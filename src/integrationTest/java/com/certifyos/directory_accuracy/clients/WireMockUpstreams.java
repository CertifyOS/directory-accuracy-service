package com.certifyos.directory_accuracy.clients;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;

/**
 * One WireMock standing in for egress, api-layer and file-ingestion (their paths do not overlap).
 * Same classloader caveat as the DAL stand-in: the port travels through a system property.
 */
public class WireMockUpstreams implements QuarkusTestResourceLifecycleManager {

    static final String PORT_PROPERTY = "upstream-wiremock-test-port";

    private WireMockServer server;

    @Override
    public Map<String, String> start() {
        server = new WireMockServer(0);
        server.start();
        System.setProperty(PORT_PROPERTY, String.valueOf(server.port()));
        return Map.of(
                "quarkus.rest-client.egress.url", server.baseUrl(),
                "quarkus.rest-client.api-layer.url", server.baseUrl(),
                "quarkus.rest-client.file-ingestion.url", server.baseUrl());
    }

    @Override
    public void stop() {
        if (server != null) {
            server.stop();
        }
        System.clearProperty(PORT_PROPERTY);
    }

    private static WireMock client() {
        return new WireMock("localhost", Integer.parseInt(System.getProperty(PORT_PROPERTY)));
    }

    /** Registers a stub. */
    public static void stubFor(MappingBuilder mapping) {
        client().register(mapping);
    }

    /** Clears the request journal, so a test's counts do not include earlier tests' calls. */
    public static void resetRequests() {
        client().resetRequests();
    }

    /** Asserts how often a request matched. */
    public static void verify(int count, RequestPatternBuilder pattern) {
        client().verifyThat(count, pattern);
    }
}
