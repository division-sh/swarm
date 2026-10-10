package pipeline_test

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestUpdateEntityState_LogsMutationRowForStateTransition(t *testing.T) {
	verifyNativeActivityBothStores(t, pipeline.VerifyUpdateEntityStateLogsMutationRowForStateTransitionForTest)
}
func TestWorkflowInstanceStore_UpsertTracksFieldsGatesAndAccumulatorInMutationLog(t *testing.T) {
	verifyNativeActivityBothStores(t, pipeline.VerifyWorkflowInstanceStoreUpsertTracksFieldsGatesAndAccumulatorForTest)
}
func TestWorkflowInstanceStore_ReplaysContainedStateMapListProjection(t *testing.T) {
	verifyNativeActivityBothStores(t, pipeline.VerifyWorkflowInstanceStoreReplaysContainedStateMapListProjectionForTest)
}
func TestApplyWorkflowGateMutation_LogsMutationRow(t *testing.T) {
	verifyNativeActivityBothStores(t, pipeline.VerifyApplyWorkflowGateMutationLogsMutationRowForTest)
}
func TestAccumulatorAppend_LogsMutationRow(t *testing.T) {
	verifyNativeActivityBothStores(t, pipeline.VerifyAccumulatorAppendLogsMutationRowForTest)
}
func TestMutationLogSchemaRejectsMissingDomainPath(t *testing.T) {
	verifyNativeActivityBothStores(t, pipeline.VerifyMutationLogSchemaRejectsMissingDomainPathForTest)
}
func TestMutationLoggedPipelineWritesFailClosedWithoutEntityMutationsTable(t *testing.T) {
	verifyNativeActivityBothStores(t, pipeline.VerifyMutationLoggedPipelineWritesFailClosedWithoutHistoryForTest)
}

func TestQ10NativeFieldlessNoStagePrivateHistoryBothStores(t *testing.T) {
	verifyNativeActivityBothStores(t, pipeline.VerifyQ10NativeFieldlessNoStagePrivateHistoryForTest)
}
