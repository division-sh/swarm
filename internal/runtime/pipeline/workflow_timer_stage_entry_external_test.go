package pipeline_test

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestStageTimerAcceptedOccurrenceRequiresCurrentEntryBothStores(t *testing.T) {
	pipeline.VerifyStageTimerAcceptedOccurrenceRequiresCurrentEntryBothStoresForTest(t, pipelineDeliveryNativeFixture)
}

func TestStageTimerUnacknowledgedCurrentEntryRecoveryBothStores(t *testing.T) {
	pipeline.VerifyStageTimerUnacknowledgedCurrentEntryRecoveryBothStoresForTest(t, pipelineDeliveryNativeFixture)
}

func TestStageTimerPreparedEntryCASBothStores(t *testing.T) {
	pipeline.VerifyStageTimerPreparedEntryCASBothStoresForTest(t, pipelineDeliveryNativeFixture)
}

func TestStageTimerSiblingApplicabilityBothStores(t *testing.T) {
	pipeline.VerifyStageTimerSiblingApplicabilityBothStoresForTest(t, pipelineDeliveryNativeFixture)
}

func TestStageTimerEntryReadFailuresConserveRowsBothStores(t *testing.T) {
	pipeline.VerifyStageTimerEntryReadFailuresConserveRowsBothStoresForTest(t, pipelineDeliveryNativeFixture)
}

func TestStageTimerConnectedEntryUsesOriginalPublicationBothStores(t *testing.T) {
	pipeline.VerifyStageTimerConnectedEntryUsesOriginalPublicationBothStoresForTest(t, pipelineDeliveryNativeFixture)
}
