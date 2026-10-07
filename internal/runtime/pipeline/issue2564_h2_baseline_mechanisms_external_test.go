package pipeline_test

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestIssue2564H2BaselineMechanismsBothStores(t *testing.T) {
	pipeline.VerifyIssue2564H2BaselineMechanismsForTest(t, newTimerCauseReplayNativeFixture)
}
