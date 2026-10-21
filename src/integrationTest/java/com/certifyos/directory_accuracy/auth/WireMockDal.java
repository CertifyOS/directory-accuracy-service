package com.certifyos.directory_accuracy.auth;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;

/**
 * A WireMock standing in for the DAL. The lifecycle manager runs in a different classloader from
 * the test class, so a static server field is not visible from the tests; the port travels through
 * a system property (shared JVM-wide) and every helper builds a fresh admin client from it. The
 * property name must not start with {@code vendor-exchange.}, which is a closed config mapping.
 */
public class WireMockDal implements QuarkusTestResourceLifecycleManager {

    static final String PORT_PROPERTY = "dal-wiremock-test-port";

    private WireMockServer server;

    @Override
    public Map<String, String> start() {
        server = new WireMockServer(0);
        server.start();
        System.setProperty(PORT_PROPERTY, String.valueOf(server.port()));
        return Map.of("quarkus.rest-client.dal.url", server.baseUrl());
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

    /** A DAL answer for a user with the given permissions in one tenant. */
    public static String userJson(String id, String email, String tenant, String resource, String actions) {
        return "{\"id\":\"" + id + "\",\"email\":\"" + email + "\",\"permissions\":{\"" + tenant
                + "\":[{\"resource\":\"" + resource + "\",\"actions\":[" + actions + "]}]}}";
    }

    /** Stubs a member of {@code tenant} with read and manage on vendor-export. */
    public static void stubMember(String email, String tenant) {
        stubFor(WireMock.get(WireMock.urlPathEqualTo("/users/by-email"))
                .withQueryParam("email", WireMock.equalTo(email))
                .willReturn(WireMock.okJson(
                        userJson("u-" + tenant, email, tenant, "vendor-export", "\"read\",\"manage\""))));
    }
}
