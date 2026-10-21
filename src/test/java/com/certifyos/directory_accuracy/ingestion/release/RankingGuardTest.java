package com.certifyos.directory_accuracy.ingestion.release;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Which configuration entries rank candor, in survivorship's {@code sourceRanking} rule shape. */
class RankingGuardTest {

    static Map<String, Object> survivorship(Map<String, Object> fieldRule) {
        return Map.of(
                "configurationTypeId",
                "survivorship",
                "configuration",
                Map.of("rules", List.of(Map.of("type", "sourceRanking", "fieldRules", List.of(fieldRule)))));
    }

    @Test
    void candorInSourcePriorityRanks() {
        Assertions.assertTrue(RankingGuard.rankingNamesCandor(survivorship(Map.of(
                "fieldPattern",
                "*",
                "sourcePriority",
                List.of(
                        Map.of("sourceType", "roster", "priority", 3),
                        Map.of("sourceType", "candor", "priority", 1))))));
        Assertions.assertTrue(RankingGuard.rankingNamesCandor(survivorship(
                Map.of("sourcePriority", List.of(Map.of("sourceType", "candor:org-xyz", "priority", 1))))));
    }

    @Test
    void candorOnlyExcludedOrDescribedDoesNotRank() {
        Assertions.assertFalse(RankingGuard.rankingNamesCandor(survivorship(Map.of(
                "description",
                "candor",
                "excludedSources",
                List.of("candor"),
                "sourcePriority",
                List.of(Map.of("sourceType", "roster", "priority", 3))))));
        Assertions.assertFalse(RankingGuard.rankingNamesCandor(
                survivorship(Map.of("sourcePriority", List.of(Map.of("sourceType", "candorx", "priority", 1))))));
        Assertions.assertFalse(RankingGuard.rankingNamesCandor(Map.of(
                "configurationTypeId",
                "survivorship",
                "configuration",
                Map.of("rules", List.of(Map.of("type", "aggregation", "sourcePriority", List.of("candor")))))));
    }

    @Test
    void aRankingEntryWithARankingList() {
        Assertions.assertTrue(RankingGuard.rankingNamesCandor(Map.of(
                "configurationTypeId", "source_ranking", "configuration", Map.of("ranking", List.of("candor:org-a")))));
        Assertions.assertFalse(RankingGuard.rankingNamesCandor(Map.of(
                "configurationTypeId", "source_ranking", "configuration", Map.of("ranking", List.of("roster:org-a")))));
    }
}
