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
			if inventory, err := readRunForkWorkflowTimerInventory(ctx, attempt, nil, postgres, "11111111-1111-4111-8111-111111111111"); err == nil || inventory != nil {
				t.Fatal("timer inventory borrowed or fabricated native frame")
			}
		}
	}
}
