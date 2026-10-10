package pipeline_test

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestPipelineInterceptionRetainsCommittedStageBothStores(t *testing.T) {
	pipeline.VerifyNativePipelineInterceptionRetainsCommittedStageForTest(t, pipelineDeliveryNativeFixture)
}
