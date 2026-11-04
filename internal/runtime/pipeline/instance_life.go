package pipeline

import (
	"fmt"
	"math"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

const instanceLifeStartedAtKey = "instance_life_started_at"

// The first life starts at physical birth; later starts are retained in the
// existing revisioned bookkeeping, independently of later stage-entry clocks.
func (i WorkflowInstance) LifeStartedAt() (time.Time, error) {
	if i.Life <= 0 {
		return time.Time{}, fmt.Errorf("instance life start requires positive life")
	}
	if i.Life == 1 {
		if i.CreatedAt.IsZero() {
			return time.Time{}, fmt.Errorf("first instance life requires its physical birth")
		}
		return i.CreatedAt, nil
	}
	raw, ok := i.Bookkeeping[instanceLifeStartedAtKey].(string)
	started, err := time.Parse(time.RFC3339Nano, raw)
	if !ok || err != nil || started.IsZero() || raw != started.UTC().Format(time.RFC3339Nano) {
		return time.Time{}, fmt.Errorf("restarted instance requires its exact retained life start")
	}
	return started, nil
}

func retainInstanceLifeStart(instance *WorkflowInstance, started time.Time) error {
	if instance.Life <= 1 {
		return nil
	}
	if instance.Bookkeeping == nil {
		instance.Bookkeeping = map[string]any{}
	}
	if _, present := instance.Bookkeeping[instanceLifeStartedAtKey]; present {
		retained, err := instance.LifeStartedAt()
		if err != nil || !retained.Equal(started) {
			return fmt.Errorf("initial entry cannot replace its retained instance life start")
		}
		return nil
	}
	instance.Bookkeeping[instanceLifeStartedAtKey] = started.UTC().Format(time.RFC3339Nano)
	return nil
}

// FlowInstanceRestart captures the finished life observed by selection. It is
// a compare-and-write precondition, not permission to skip native admission.
type FlowInstanceRestart struct {
	Life       int64
	Revision   int64
	FinalStage contracts.StoredStageRef
}

func (r FlowInstanceRestart) Validate(next WorkflowInstance) error {
	if r.Life <= 0 || r.Life == math.MaxInt64 || r.Revision <= 0 || !r.FinalStage.IsFinal() || r.FinalStage.FlowID() != next.WorkflowName || next.Life != r.Life+1 {
		return fmt.Errorf("instance restart requires its exact finished life, revision and compiled final stage")
	}
	return nil
}

func InstanceRestarted(owner flowidentity.RunScopedFlowInstance, expected, current int64) error {
	return failures.New(failures.ClassStaleArrival, "instance_restarted", "flow-instance", "admit", map[string]any{
		"flow_instance": owner.Route.InstancePath, "life": expected, "current_life": current,
	})
}
