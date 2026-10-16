package pipeline_test

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestQ10NativeFieldlessNoStagePrivateHistoryBothStores(t *testing.T) {
	pipeline.VerifyQ10NativeFieldlessNoStagePrivateHistoryForTest(t, pipelineDeliveryNativeFixture)
}
