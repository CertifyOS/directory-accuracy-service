package com.certifyos.directory_accuracy.ingestion.jobs;

import com.certifyos.directory_accuracy.ingestion.inbound.InboundFileState;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import org.jobrunr.jobs.states.FailedState;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** The pure rules of the final-failure filter: the reason, the step and which states a job owns. */
class IngestionFinalFailureFilterTest {

    private static FailedState failedWith(Exception exception) {
        return new FailedState("job failed", exception);
    }

    @Test
    void aRegisterClientErrorIsSourceUnreadable() {
        RegisterClientErrorException refused = new RegisterClientErrorException(
                400, new WebApplicationException(Response.status(400).build()));

        Assertions.assertEquals(
                IngestionFinalFailureFilter.SOURCE_UNREADABLE,
                IngestionFinalFailureFilter.reasonOf(failedWith(refused)));
        Assertions.assertEquals(400, refused.getResponse().getStatus(), "the refused status is kept");
        Assertions.assertFalse(refused.getMessage().contains("{"), "no problem body in the message");
    }

    @Test
    void everythingElseIsRetriesExhausted() {
        Assertions.assertEquals(
                IngestionFinalFailureFilter.JOB_RETRIES_EXHAUSTED,
                IngestionFinalFailureFilter.reasonOf(failedWith(new IllegalStateException("archive store down"))));
        Assertions.assertEquals(
                IngestionFinalFailureFilter.JOB_RETRIES_EXHAUSTED,
                IngestionFinalFailureFilter.reasonOf(failedWith(
                        new WebApplicationException(Response.status(503).build()))));
    }

    @Test
    void theStepFollowsTheJobAndHowFarTheRequestGot() {
        String request = RequestIngestionJob.class.getName();
        Assertions.assertEquals(
                RequestIngestionJob.STEP_ARCHIVE,
                IngestionFinalFailureFilter.stepOf(request, false, IngestionFinalFailureFilter.JOB_RETRIES_EXHAUSTED));
        Assertions.assertEquals(
                RequestIngestionJob.STEP_REGISTER,
                IngestionFinalFailureFilter.stepOf(request, true, IngestionFinalFailureFilter.JOB_RETRIES_EXHAUSTED));
        Assertions.assertEquals(
                RequestIngestionJob.STEP_REGISTER,
                IngestionFinalFailureFilter.stepOf(request, false, IngestionFinalFailureFilter.SOURCE_UNREADABLE));
        Assertions.assertEquals(
                IngestionDeadlineCheckJob.STEP_PARSE,
                IngestionFinalFailureFilter.stepOf(
                        IngestionDeadlineCheckJob.class.getName(),
                        true,
                        IngestionFinalFailureFilter.JOB_RETRIES_EXHAUSTED));
        Assertions.assertEquals(
                StageRowsJob.STEP_STAGE,
                IngestionFinalFailureFilter.stepOf(
                        StageRowsJob.class.getName(), true, IngestionFinalFailureFilter.JOB_RETRIES_EXHAUSTED));
    }

    @Test
    void onlyIngestionRequestsAreOurs() {
        Assertions.assertTrue(IngestionFinalFailureFilter.isIngestionRequest(new StageRowsJobRequest("b", "u", 1)));
        Assertions.assertFalse(IngestionFinalFailureFilter.isIngestionRequest(null), "a recurring job has none");
        Assertions.assertFalse(
                IngestionFinalFailureFilter.isIngestionRequest(
                        new com.certifyos.directory_accuracy.export.jobs.RequestEgressJobRequest("b", 1, 0)),
                "the export lane's filter owns its jobs");
    }

    @Test
    void aJobFailsOnlyTheStatesItWorksFrom() {
        String request = RequestIngestionJob.class.getName();
        String deadline = IngestionDeadlineCheckJob.class.getName();
        String stage = StageRowsJob.class.getName();

        Assertions.assertTrue(IngestionFinalFailureFilter.owns(request, InboundFileState.RECEIVED));
        Assertions.assertTrue(IngestionFinalFailureFilter.owns(request, InboundFileState.ARCHIVING));
        Assertions.assertFalse(IngestionFinalFailureFilter.owns(request, InboundFileState.INGESTION_REQUESTED));
        Assertions.assertTrue(IngestionFinalFailureFilter.owns(deadline, InboundFileState.INGESTION_REQUESTED));
        Assertions.assertFalse(
                IngestionFinalFailureFilter.owns(deadline, InboundFileState.ARCHIVING),
                "the request job may still commit; the reconciler covers a lost check");
        Assertions.assertFalse(IngestionFinalFailureFilter.owns(deadline, InboundFileState.PARSED));
        Assertions.assertTrue(IngestionFinalFailureFilter.owns(stage, InboundFileState.PARSED));
        Assertions.assertTrue(IngestionFinalFailureFilter.owns(stage, InboundFileState.STAGING));
        Assertions.assertFalse(IngestionFinalFailureFilter.owns(stage, InboundFileState.STAGED));
        Assertions.assertFalse(IngestionFinalFailureFilter.owns(stage, InboundFileState.FAILED));
        Assertions.assertFalse(IngestionFinalFailureFilter.owns("com.example.OtherJob", InboundFileState.RECEIVED));
    }
}
