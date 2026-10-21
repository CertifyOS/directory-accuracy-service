package com.certifyos.directory_accuracy.ingestion.release;

import com.certifyos.directory_accuracy.clients.DalTenantConfigClient;
import com.certifyos.directory_accuracy.clients.TenantConfigurationPage;
import com.certifyos.directory_accuracy.http.ProblemException;
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
 * type = sourceRanking} (survivorship's {@code SurvivorshipTenantConfig.getSourceRankingRule}), and
 * it ranks candor when a field rule's {@code sourcePriority[].sourceType} is {@code candor} or starts
 * with {@code candor:} (survivorship's {@code SourceRankingRule}, {@code FieldRule}, {@code
 * SourcePriority}); candor named only in {@code excludedSources} or a description does not count. A
 * configuration entry whose {@code configurationTypeId} is {@code sourceRanking} or {@code
 * source_ranking} with a {@code configuration.ranking[]} list naming candor counts too. A disabled
 * entry ({@code isEnabled = false}) is skipped.
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

    /** Whether the entry is, or holds, a ranking rule that ranks candor. */
    static boolean rankingNamesCandor(Map<?, ?> entry) {
        Object typeId = entry.get("configurationTypeId");
        boolean rankingEntry = "sourceRanking".equals(typeId) || "source_ranking".equals(typeId);
        if (rankingEntry
                && entry.get("configuration") instanceof Map<?, ?> configuration
                && configuration.get("ranking") instanceof List<?> ranking
                && ranking.stream().anyMatch(RankingGuard::isCandor)) {
            return true;
        }
        return findRule(entry);
    }

    private static boolean findRule(Object node) {
        if (node instanceof Map<?, ?> map) {
            if ("sourceRanking".equals(map.get("type"))) {
                return ranks(map);
            }
            return map.values().stream().anyMatch(RankingGuard::findRule);
        }
        if (node instanceof List<?> list) {
            return list.stream().anyMatch(RankingGuard::findRule);
        }
        return false;
    }

    /** Whether a {@code sourceRanking} rule lists candor in a field rule's {@code sourcePriority}. */
    private static boolean ranks(Map<?, ?> rule) {
        if (!(rule.get("fieldRules") instanceof List<?> fieldRules)) {
            return false;
        }
        for (Object fieldRule : fieldRules) {
            if (fieldRule instanceof Map<?, ?> map && map.get("sourcePriority") instanceof List<?> priorities) {
                for (Object priority : priorities) {
                    if (priority instanceof Map<?, ?> source && isCandor(source.get("sourceType"))) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static boolean isCandor(Object sourceType) {
        return sourceType instanceof String text && (text.equals(SOURCE) || text.startsWith(SOURCE + ":"));
    }
}
