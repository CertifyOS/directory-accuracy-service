package com.certifyos.directory_accuracy.export.schedule;

import com.certifyos.directory_accuracy.clients.ApiLayerClient;
import com.certifyos.directory_accuracy.clients.ApiLayerNotConfiguredException;
import com.certifyos.directory_accuracy.clients.EgressTemplate;
import com.certifyos.directory_accuracy.clients.EgressTemplateList;
import com.certifyos.directory_accuracy.http.ProblemException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.WebApplicationException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;

/**
 * The tenant's vendor template in egress, found or created through api-layer at schedule
 * creation. The template is this service's {@code certify-export-v1} mappings CSV, a resource of
 * this repository, uploaded under a fixed name. Found by that name: one match is reused, none is
 * created, more than one is a conflict for an operator to resolve. An operator may name an
 * existing template instead; it must belong to the tenant.
 */
@ApplicationScoped
public class TemplateProvisioner {

    /** The outbound file contract version the mappings CSV implements; recorded on every delivered file. */
    public static final String SCHEMA_VERSION = "certify-export-v1";

    /** The template name this service assigns; the list search matches on it. */
    public static final String TEMPLATE_NAME = "directory-accuracy candor certify-export-v1";

    static final String MAPPINGS_RESOURCE = "/vendor/candor/certify-export-v1.mappings.csv";
    static final String ENTITY_TYPE = "practitioner";
    static final String STATUS_ACTIVE = "active";
    static final String ROW_EXPANSION_KEYS = "[\"locations\"]";
    private static final Logger LOG = Logger.getLogger(TemplateProvisioner.class);

    private final ApiLayerClient apiLayer;

    public TemplateProvisioner(@RestClient ApiLayerClient apiLayer) {
        this.apiLayer = apiLayer;
    }

    /** The template id and version to store on the schedule. */
    public record Provisioned(String templateId, Integer version, boolean created) {}

    /**
     * Finds or creates the tenant's vendor template, or verifies an operator-named one.
     *
     * @param tenantId the tenant
     * @param override an existing template id named by the operator, or empty
     * @return the template to store on the schedule
     * @throws ProblemException 400 when the override is unknown or another tenant's, 409 when
     *     more than one template carries the name, 503 when api-layer cannot be reached
     */
    public Provisioned provision(String tenantId, Optional<String> override) {
        try {
            return override.isPresent() ? verify(tenantId, override.get()) : findOrCreate(tenantId);
        } catch (ApiLayerNotConfiguredException notConfigured) {
            throw ProblemException.unavailable("API_LAYER_NOT_CONFIGURED", notConfigured.getMessage());
        } catch (ProcessingException unreachable) {
            LOG.warnf(
                    "api-layer unreachable while provisioning a template for %s: %s", tenantId, unreachable.toString());
            throw ProblemException.unavailable("API_LAYER_UNAVAILABLE", "api-layer could not be reached");
        } catch (WebApplicationException refused) {
            int status =
                    refused.getResponse() == null ? 0 : refused.getResponse().getStatus();
            LOG.warnf("api-layer answered %d while provisioning a template for %s", status, tenantId);
            throw ProblemException.unavailable(
                    "API_LAYER_UNAVAILABLE", "api-layer answered " + status + " to the template list or create");
        }
    }

    private Provisioned verify(String tenantId, String templateId) {
        EgressTemplate template;
        try {
            template = apiLayer.getEgressTemplate(tenantId, templateId);
        } catch (WebApplicationException refused) {
            int status =
                    refused.getResponse() == null ? 0 : refused.getResponse().getStatus();
            if (status == 404) {
                throw ProblemException.badRequest("TEMPLATE_NOT_FOUND", "no egress template " + templateId);
            }
            throw ProblemException.unavailable("API_LAYER_UNAVAILABLE", "api-layer answered " + status);
        }
        if (template.tenantId() != null && !tenantId.equals(template.tenantId())) {
            throw ProblemException.badRequest("TEMPLATE_NOT_FOUND", "no egress template " + templateId);
        }
        if (!ENTITY_TYPE.equals(template.entityType())) {
            throw ProblemException.badRequest(
                    "TEMPLATE_WRONG_ENTITY", "template " + templateId + " is not a practitioner template");
        }
        return new Provisioned(template.id(), template.version(), false);
    }

    private Provisioned findOrCreate(String tenantId) {
        EgressTemplateList page =
                apiLayer.listEgressTemplates(tenantId, TEMPLATE_NAME, STATUS_ACTIVE, ENTITY_TYPE, 0, 10);
        List<EgressTemplate> matches = page.templates().stream()
                .filter(template -> TEMPLATE_NAME.equals(template.templateName()))
                .toList();
        if (matches.size() > 1) {
            throw ProblemException.conflict(
                    "TEMPLATE_AMBIGUOUS", matches.size() + " active templates are named '" + TEMPLATE_NAME + "'");
        }
        if (matches.size() == 1) {
            EgressTemplate found = matches.get(0);
            LOG.infof("tenant %s reuses egress template %s v%s", tenantId, found.id(), found.version());
            return new Provisioned(found.id(), found.version(), false);
        }
        EgressTemplate created = apiLayer.createEgressTemplate(
                tenantId,
                mappingsCsv(),
                TEMPLATE_NAME,
                "Directory accuracy vendor export, certify-export-v1, provisioned by directory-accuracy-service",
                ENTITY_TYPE,
                "csv",
                ",",
                ROW_EXPANSION_KEYS,
                STATUS_ACTIVE);
        LOG.infof("tenant %s got egress template %s v%s", tenantId, created.id(), created.version());
        return new Provisioned(created.id(), created.version(), true);
    }

    /**
     * The mappings CSV shipped with this service.
     *
     * @return the file bytes
     */
    public static byte[] mappingsCsv() {
        try (InputStream in = TemplateProvisioner.class.getResourceAsStream(MAPPINGS_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("missing resource " + MAPPINGS_RESOURCE);
            }
            return in.readAllBytes();
        } catch (IOException failure) {
            throw new IllegalStateException("cannot read " + MAPPINGS_RESOURCE, failure);
        }
    }
}
