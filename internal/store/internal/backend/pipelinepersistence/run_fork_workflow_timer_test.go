package pipelinepersistence

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func TestRunForkWorkflowTimerBridgeRequiresExistingNativeFrame(t *testing.T) {
	for _, postgres := range []bool{true, false} {
		for _, attempt := range []*mutationprotocol.Attempt{nil, new(mutationprotocol.Attempt)} {
			ctx := context.Background()
			if err := materializeRunForkWorkflowTimer(ctx, attempt, nil, postgres, pipeline.WorkflowTimerActivation{}, false); err == nil {
				t.Fatal("timer materialization borrowed or fabricated native frame")
			}
			if err := requireRunForkWorkflowTimer(ctx, attempt, nil, postgres, pipeline.WorkflowTimerActivation{}, true); err == nil {
				t.Fatal("timer reuse borrowed or fabricated native frame")
			}
		}
	}
}
