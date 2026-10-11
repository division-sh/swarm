package pipeline_test

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestQ10NativeFieldlessNoStagePrivateHistoryBothStores(t *testing.T) {
	verifyNativeActivityBothStores(t, pipeline.VerifyQ10NativeFieldlessNoStagePrivateHistoryForTest)
}
