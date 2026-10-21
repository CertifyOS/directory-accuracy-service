package com.certifyos.directory_accuracy.export.schedule;

import com.certifyos.directory_accuracy.clients.ApiLayerClient;
import com.certifyos.directory_accuracy.clients.ApiLayerNotConfiguredException;
import com.certifyos.directory_accuracy.clients.PagedPractitioners;
import com.certifyos.directory_accuracy.http.ProblemException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.WebApplicationException;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;

/** The selection as api-layer sees it: the {@code filter} parameter and a count with page size 1. */
@ApplicationScoped
public class SelectionPreview {

    private static final Logger LOG = Logger.getLogger(SelectionPreview.class);

    private final ApiLayerClient apiLayer;
    private final ObjectMapper mapper;

    public SelectionPreview(@RestClient ApiLayerClient apiLayer, ObjectMapper mapper) {
        this.apiLayer = apiLayer;
        this.mapper = mapper;
    }

    /**
     * How many practitioners a selection matches today.
     *
     * @param tenantId the tenant
     * @param criteria the selection
     * @return api-layer's {@code totalCount}
     * @throws ProblemException 503 when api-layer is unconfigured or cannot answer
     */
    public long count(String tenantId, SelectionCriteria criteria) {
        try {
            PagedPractitioners page = apiLayer.practitionerFindMany(tenantId, filterJson(criteria), 0, 1);
            return page.totalCount() == null ? 0 : page.totalCount();
        } catch (ApiLayerNotConfiguredException notConfigured) {
            throw ProblemException.unavailable("API_LAYER_NOT_CONFIGURED", notConfigured.getMessage());
        } catch (WebApplicationException | ProcessingException failure) {
            LOG.warnf("preview for %s failed: %s", tenantId, failure.toString());
            throw ProblemException.unavailable("API_LAYER_UNAVAILABLE", "api-layer could not answer the preview");
        }
    }

    /**
     * The selection as api-layer's {@code filter} query parameter.
     *
     * @param criteria the selection
     * @return JSON
     */
    public String filterJson(SelectionCriteria criteria) {
        try {
            return mapper.writeValueAsString(criteria.toFilter());
        } catch (JsonProcessingException impossible) {
            throw new IllegalStateException("selection could not be serialised", impossible);
        }
    }
}
