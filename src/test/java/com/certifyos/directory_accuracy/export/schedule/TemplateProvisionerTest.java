package com.certifyos.directory_accuracy.export.schedule;

import com.certifyos.directory_accuracy.clients.ApiLayerClient;
import com.certifyos.directory_accuracy.clients.ApiLayerNotConfiguredException;
import com.certifyos.directory_accuracy.clients.EgressTemplate;
import com.certifyos.directory_accuracy.clients.EgressTemplateList;
import com.certifyos.directory_accuracy.http.ProblemException;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

class TemplateProvisionerTest {

    static final String TENANT = "org-1";

    ApiLayerClient apiLayer;
    TemplateProvisioner provisioner;

    private static EgressTemplate template(String id, String name, int version) {
        return new EgressTemplate(id, TENANT, name, "practitioner", "active", version, null, ",", "csv", null);
    }

    private void listAnswers(EgressTemplate... templates) {
        Mockito.when(apiLayer.listEgressTemplates(
                        TENANT, TemplateProvisioner.TEMPLATE_NAME, "active", "practitioner", 0, 10))
                .thenReturn(new EgressTemplateList(List.of(templates), templates.length, 0, 10));
    }

    @BeforeEach
    void before() {
        apiLayer = Mockito.mock(ApiLayerClient.class);
        provisioner = new TemplateProvisioner(apiLayer);
    }

    @Test
    void oneExactNameMatchIsReused() {
        listAnswers(template("tpl-1", TemplateProvisioner.TEMPLATE_NAME, 3));

        TemplateProvisioner.Provisioned found = provisioner.provision(TENANT, Optional.empty());

        Assertions.assertEquals(new TemplateProvisioner.Provisioned("tpl-1", 3, false), found);
        Mockito.verify(apiLayer, Mockito.never())
                .createEgressTemplate(
                        ArgumentMatchers.any(),
                        ArgumentMatchers.any(),
                        ArgumentMatchers.any(),
                        ArgumentMatchers.any(),
                        ArgumentMatchers.any(),
                        ArgumentMatchers.any(),
                        ArgumentMatchers.any(),
                        ArgumentMatchers.any(),
                        ArgumentMatchers.any());
    }

    @Test
    void substringMatchesDoNotCountOnlyTheExactName() {
        // api-layer's search is contains(); a template someone named with a suffix is not ours.
        listAnswers(template("tpl-other", TemplateProvisioner.TEMPLATE_NAME + " copy", 1));
        Mockito.when(apiLayer.createEgressTemplate(
                        ArgumentMatchers.eq(TENANT),
                        ArgumentMatchers.any(byte[].class),
                        ArgumentMatchers.eq(TemplateProvisioner.TEMPLATE_NAME),
                        ArgumentMatchers.anyString(),
                        ArgumentMatchers.eq("practitioner"),
                        ArgumentMatchers.eq("csv"),
                        ArgumentMatchers.eq(","),
                        ArgumentMatchers.eq(TemplateProvisioner.ROW_EXPANSION_KEYS),
                        ArgumentMatchers.eq("active")))
                .thenReturn(template("tpl-new", TemplateProvisioner.TEMPLATE_NAME, 1));

        TemplateProvisioner.Provisioned created = provisioner.provision(TENANT, Optional.empty());

        Assertions.assertEquals(new TemplateProvisioner.Provisioned("tpl-new", 1, true), created);
    }

    @Test
    void twoExactMatchesAreAConflictForAnOperator() {
        listAnswers(
                template("tpl-1", TemplateProvisioner.TEMPLATE_NAME, 1),
                template("tpl-2", TemplateProvisioner.TEMPLATE_NAME, 1));

        ProblemException refused =
                Assertions.assertThrows(ProblemException.class, () -> provisioner.provision(TENANT, Optional.empty()));

        Assertions.assertEquals(409, refused.status());
        Assertions.assertEquals("TEMPLATE_AMBIGUOUS", refused.code());
    }

    @Test
    void overrideIsVerifiedToBelongToTheTenantAndBePractitioner() {
        Mockito.when(apiLayer.getEgressTemplate(TENANT, "tpl-x")).thenReturn(template("tpl-x", "theirs", 7));
        Assertions.assertEquals(
                new TemplateProvisioner.Provisioned("tpl-x", 7, false),
                provisioner.provision(TENANT, Optional.of("tpl-x")));

        Mockito.when(apiLayer.getEgressTemplate(TENANT, "tpl-404"))
                .thenThrow(new WebApplicationException(Response.status(404).build()));
        ProblemException missing = Assertions.assertThrows(
                ProblemException.class, () -> provisioner.provision(TENANT, Optional.of("tpl-404")));
        Assertions.assertEquals("TEMPLATE_NOT_FOUND", missing.code());

        Mockito.when(apiLayer.getEgressTemplate(TENANT, "tpl-other"))
                .thenReturn(new EgressTemplate(
                        "tpl-other", "org-2", "x", "practitioner", "active", 1, null, ",", "csv", null));
        Assertions.assertEquals(
                "TEMPLATE_NOT_FOUND",
                Assertions.assertThrows(
                                ProblemException.class, () -> provisioner.provision(TENANT, Optional.of("tpl-other")))
                        .code(),
                "another tenant's template reads as not found, never as a hint that it exists");

        Mockito.when(apiLayer.getEgressTemplate(TENANT, "tpl-group"))
                .thenReturn(new EgressTemplate("tpl-group", TENANT, "x", "group", "active", 1, null, ",", "csv", null));
        Assertions.assertEquals(
                "TEMPLATE_WRONG_ENTITY",
                Assertions.assertThrows(
                                ProblemException.class, () -> provisioner.provision(TENANT, Optional.of("tpl-group")))
                        .code());
    }

    @Test
    void anHttpErrorFromListOrCreateIs503ApiLayerUnavailable() {
        Mockito.when(apiLayer.listEgressTemplates(
                        TENANT, TemplateProvisioner.TEMPLATE_NAME, "active", "practitioner", 0, 10))
                .thenThrow(new WebApplicationException(Response.status(502).build()));
        ProblemException list =
                Assertions.assertThrows(ProblemException.class, () -> provisioner.provision(TENANT, Optional.empty()));
        Assertions.assertEquals(503, list.status());
        Assertions.assertEquals("API_LAYER_UNAVAILABLE", list.code());

        Mockito.reset(apiLayer);
        listAnswers();
        Mockito.when(apiLayer.createEgressTemplate(
                        ArgumentMatchers.any(),
                        ArgumentMatchers.any(),
                        ArgumentMatchers.any(),
                        ArgumentMatchers.any(),
                        ArgumentMatchers.any(),
                        ArgumentMatchers.any(),
                        ArgumentMatchers.any(),
                        ArgumentMatchers.any(),
                        ArgumentMatchers.any()))
                .thenThrow(new WebApplicationException(Response.status(400).build()));
        ProblemException create =
                Assertions.assertThrows(ProblemException.class, () -> provisioner.provision(TENANT, Optional.empty()));
        Assertions.assertEquals("API_LAYER_UNAVAILABLE", create.code());
        Assertions.assertTrue(create.getMessage().contains("400"), create.getMessage());
    }

    @Test
    void unreachableOrUnconfiguredApiLayerIs503() {
        Mockito.when(apiLayer.listEgressTemplates(
                        ArgumentMatchers.anyString(),
                        ArgumentMatchers.anyString(),
                        ArgumentMatchers.anyString(),
                        ArgumentMatchers.anyString(),
                        ArgumentMatchers.anyInt(),
                        ArgumentMatchers.anyInt()))
                .thenThrow(new ProcessingException("connection refused"));
        ProblemException down =
                Assertions.assertThrows(ProblemException.class, () -> provisioner.provision(TENANT, Optional.empty()));
        Assertions.assertEquals(503, down.status());
        Assertions.assertEquals("API_LAYER_UNAVAILABLE", down.code());

        Mockito.when(apiLayer.getEgressTemplate(TENANT, "tpl")).thenThrow(new ApiLayerNotConfiguredException());
        Assertions.assertEquals(
                "API_LAYER_NOT_CONFIGURED",
                Assertions.assertThrows(ProblemException.class, () -> provisioner.provision(TENANT, Optional.of("tpl")))
                        .code());
    }
}
