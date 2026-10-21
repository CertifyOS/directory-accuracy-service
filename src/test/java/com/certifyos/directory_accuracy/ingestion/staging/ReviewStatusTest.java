package com.certifyos.directory_accuracy.ingestion.staging;

import com.certifyos.directory_accuracy.ingestion.staging.ReviewStatus.Action;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ReviewStatusTest {

    /** Every action and the states it may start from; every other pair is refused. */
    private static final Map<Action, Set<ReviewStatus>> FROM = Map.of(
            Action.APPROVE, EnumSet.of(ReviewStatus.PENDING, ReviewStatus.SKIPPED),
            Action.REJECT, EnumSet.of(ReviewStatus.PENDING, ReviewStatus.SKIPPED),
            Action.SKIP, EnumSet.of(ReviewStatus.PENDING),
            Action.ROLLBACK, EnumSet.of(ReviewStatus.APPROVED),
            Action.RELEASE, EnumSet.of(ReviewStatus.APPROVED, ReviewStatus.RELEASE_FAILED));

    @Test
    void movesMatchTheDesign() {
        Assertions.assertEquals(EnumSet.allOf(Action.class), FROM.keySet());
        for (ReviewStatus status : ReviewStatus.values()) {
            for (Action action : Action.values()) {
                Assertions.assertEquals(
                        FROM.get(action).contains(status), status.allows(action), status + " " + action);
            }
        }
        Assertions.assertEquals(EnumSet.of(ReviewStatus.PENDING, ReviewStatus.SKIPPED), ReviewStatus.open());
        Assertions.assertEquals(8, ReviewStatus.values().length);
        Assertions.assertEquals(5, RowDisposition.values().length);
    }
}
