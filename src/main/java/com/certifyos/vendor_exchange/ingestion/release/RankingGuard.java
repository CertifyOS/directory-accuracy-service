package com.certifyos.vendor_exchange.ingestion.release;

import com.certifyos.vendor_exchange.clients.DalTenantConfigClient;
import com.certifyos.vendor_exchange.clients.TenantConfigurationPage;
import com.certifyos.vendor_exchange.http.ProblemException;
import io.quarkus.cache.CacheKey;
import io.quarkus.cache.CacheResult;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.WebApplicationException;
import java.util.List;
import java.util.Map;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;

/**
 * The ranking guardrail (design D20, plan P8): a tenant whose survivorship configuration does not rank
 * candor cannot release, because an unranked source falls to rank 997 and never wins. Reads the
 * tenant's configurations from the DAL page by page. The answer, either way, is cached per tenant for
 * {@code vendor-exchange.auth.cache-ttl}; a failed read is not cached.
 *
 * <p>Provisional until Gate 7 reads a live configuration: a ranking rule is any object with {@code
 * type = sourceRanking} (survivorship's {@code SurvivorshipTenantConfig.getSourceRankingRule}), or a
 * configuration entry whose {@code configurationTypeId} is {@code sourceRanking} or {@code
 * source_ranking}; it names candor when any text inside it is {@code candor} or starts with {@code
 * candor:}. A disabled entry ({@code isEnabled = false}) is skipped.
 */
@ApplicationScoped
public class RankingGuard {

    /** The cache name, configured under {@code quarkus.cache.caffeine."candor-ranking"}. */
    public static final String CACHE = "candor-ranking";

    static final int PAGE_SIZE = 50;
    static final int MAX_PAGES = 20;
    static final String SOURCE = "candor";

    private static final Logger LOG = Logger.getLogger(RankingGuard.class);

    private final DalTenantConfigClient dal;

    public RankingGuard(@RestClient DalTenantConfigClient dal) {
        this.dal = dal;
    }

    /**
     * Whether the tenant's survivorship ranking names candor.
     *
     * @param tenantId the tenant
     * @return true when a ranking rule names candor
     * @throws ProblemException 503 {@code DAL_UNAVAILABLE} when the configuration cannot be read
     */
    @CacheResult(cacheName = CACHE)
    public boolean ranksCandor(@CacheKey String tenantId) {
        try {
            for (int page = 0; page < MAX_PAGES; page++) {
                TenantConfigurationPage found = dal.list(tenantId, tenantId, page, PAGE_SIZE);
                for (Map<String, Object> entry : found.data()) {
                    if (!Boolean.FALSE.equals(entry.get("isEnabled")) && rankingNamesCandor(entry)) {
                        return true;
                    }
                }
                long total = found.totalCount() == null ? 0 : found.totalCount();
                if (found.data().isEmpty() || (long) (page + 1) * PAGE_SIZE >= total) {
                    break;
                }
            }
            LOG.infof("tenant %s: no survivorship ranking names %s", tenantId, SOURCE);
            return false;
        } catch (WebApplicationException | ProcessingException failure) {
            LOG.warnf(
                    "tenant %s: the DAL tenant configuration could not be read: %s",
                    tenantId, failure.getClass().getSimpleName());
            throw ProblemException.unavailable(
                    "DAL_UNAVAILABLE", "the tenant's survivorship configuration could not be read");
        }
    }

    /** Whether the entry is, or holds, a ranking rule that names candor. */
    static boolean rankingNamesCandor(Map<?, ?> entry) {
        Object typeId = entry.get("configurationTypeId");
        if ("sourceRanking".equals(typeId) || "source_ranking".equals(typeId)) {
            return namesCandor(entry.get("configuration"));
        }
        return findRule(entry);
    }

    private static boolean findRule(Object node) {
        if (node instanceof Map<?, ?> map) {
            if ("sourceRanking".equals(map.get("type"))) {
                return namesCandor(map);
            }
            return map.values().stream().anyMatch(RankingGuard::findRule);
        }
        if (node instanceof List<?> list) {
            return list.stream().anyMatch(RankingGuard::findRule);
        }
        return false;
    }

    private static boolean namesCandor(Object node) {
        if (node instanceof String text) {
            return text.equals(SOURCE) || text.startsWith(SOURCE + ":");
        }
        if (node instanceof Map<?, ?> map) {
            return map.values().stream().anyMatch(RankingGuard::namesCandor);
        }
        if (node instanceof List<?> list) {
            return list.stream().anyMatch(RankingGuard::namesCandor);
        }
        return false;
    }
}
