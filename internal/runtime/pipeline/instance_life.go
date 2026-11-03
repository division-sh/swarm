package pipeline

import (
	"fmt"
	"math"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

// FlowInstanceRestart captures the finished life observed by selection. It is
// a compare-and-write precondition, not permission to skip native admission.
type FlowInstanceRestart struct {
	Life       int64
	Revision   int64
	FinalStage contracts.StoredStageRef
}

func (r FlowInstanceRestart) Validate(next WorkflowInstance) error {
	if r.Life <= 0 || r.Life == math.MaxInt64 || r.Revision <= 0 || !r.FinalStage.IsFinal() || next.Life != r.Life+1 {
		return fmt.Errorf("instance restart requires its exact finished life, revision and compiled final stage")
	}
	return nil
}

func InstanceRestarted(owner flowidentity.RunScopedFlowInstance, expected, current int64) error {
	return failures.New(failures.ClassStaleArrival, "instance_restarted", "flow-instance", "admit", map[string]any{
		"flow_instance": owner.Route.InstancePath, "life": expected, "current_life": current,
	})
}
