package com.certifyos.directory_accuracy.clients;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;
import org.jboss.resteasy.reactive.PartFilename;
import org.jboss.resteasy.reactive.PartType;
import org.jboss.resteasy.reactive.RestForm;

/**
 * api-layer, called with this service's Auth0 machine token, for the tenant's egress templates:
 * the list and read that find and pin a batch's file definition, and the upload that provisions a
 * tenant's vendor template. Every call names the tenant in the {@code tenant-id} header. Practitioner
 * selection is egress's, from the schedule's saved filter; this service never lists practitioners.
 */
@RegisterRestClient(configKey = "api-layer")
@RegisterProvider(ApiLayerIapFilter.class)
@RegisterProvider(ApiLayerAuthFilter.class)
@Produces(MediaType.APPLICATION_JSON)
public interface ApiLayerClient {

    /** The tenant header api-layer requires. */
    String TENANT_HEADER = "tenant-id";

    /**
     * Lists a tenant's egress templates. {@code search} is a case-insensitive substring match on
     * the template name.
     *
     * @param tenantId the tenant
     * @param search the name substring, or null
     * @param status {@code active}, {@code draft}, {@code inactive}, or null
     * @param entityType {@code practitioner}, or null
     * @param page the page number, from 0
     * @param size the page size
     * @return the page
     */
    @GET
    @Path("/api/v1/egress-templates")
    EgressTemplateList listEgressTemplates(
            @HeaderParam(TENANT_HEADER) String tenantId,
            @QueryParam("search") String search,
            @QueryParam("status") String status,
            @QueryParam("entityType") String entityType,
            @QueryParam("page") int page,
            @QueryParam("size") int size);

    /**
     * Reads a tenant's egress template.
     *
     * @param tenantId the tenant
     * @param templateId the template
     * @return the template
     */
    @GET
    @Path("/api/v1/egress-templates/{templateId}")
    EgressTemplate getEgressTemplate(
            @HeaderParam(TENANT_HEADER) String tenantId, @PathParam("templateId") String templateId);

    /**
     * Creates a tenant's egress template from a mappings CSV (api-layer's multipart upload flow).
     *
     * @param tenantId the tenant
     * @param mappingsCsv the mappings CSV content
     * @param templateName the display name
     * @param description a description
     * @param entityType {@code practitioner}
     * @param outputFormat {@code csv}
     * @param separator the column separator
     * @param rowExpansionKeys JSON array of expansion keys, for example {@code ["locations"]}
     * @param status {@code active}
     * @return the created template
     */
    @POST
    @Path("/api/v1/egress-templates")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    EgressTemplate createEgressTemplate(
            @HeaderParam(TENANT_HEADER) String tenantId,
            @RestForm("file") @PartType("text/csv") @PartFilename("mappings.csv") byte[] mappingsCsv,
            @RestForm("templateName") @PartType(MediaType.TEXT_PLAIN) String templateName,
            @RestForm("description") @PartType(MediaType.TEXT_PLAIN) String description,
            @RestForm("entityType") @PartType(MediaType.TEXT_PLAIN) String entityType,
            @RestForm("outputFormat") @PartType(MediaType.TEXT_PLAIN) String outputFormat,
            @RestForm("separator") @PartType(MediaType.TEXT_PLAIN) String separator,
            @RestForm("rowExpansionKeys") @PartType(MediaType.TEXT_PLAIN) String rowExpansionKeys,
            @RestForm("status") @PartType(MediaType.TEXT_PLAIN) String status);
}
