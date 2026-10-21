package com.certifyos.directory_accuracy.http;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ProblemTest {

    @Test
    void derivesTypeAndTitleFromCodeAndStatus() {
        Problem problem = Problem.of(403, "TENANT_FORBIDDEN", "user holds no role in tenant", "/v1/vendor-exports");
        Assertions.assertEquals("urn:certifyos:vendor-exchange:problem:tenant-forbidden", problem.type());
        Assertions.assertEquals("Forbidden", problem.title());
        Assertions.assertEquals(403, problem.status());
        Assertions.assertEquals("TENANT_FORBIDDEN", problem.code());
        Assertions.assertEquals("/v1/vendor-exports", problem.instance());
    }

    @Test
    void refusesNonErrorStatus() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> Problem.of(200, "OK", null, null));
    }

    @Test
    void mapperCodesCoverTheStatusesQuarkusRaises() {
        Assertions.assertEquals("NOT_FOUND", ProblemMappers.codeFor(404));
        Assertions.assertEquals("METHOD_NOT_ALLOWED", ProblemMappers.codeFor(405));
        Assertions.assertEquals("UNSUPPORTED_MEDIA_TYPE", ProblemMappers.codeFor(415));
        Assertions.assertEquals("HTTP_418", ProblemMappers.codeFor(418));
    }
}
