package pipeline

import (
	"context"

	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

type PipelineRecoveryOwner interface {
	SweepUndispatched(context.Context, int) (int, error)
}

type RecoveryManager struct {
	owner PipelineRecoveryOwner
	limit int
}

func NewRecoveryManager() *RecoveryManager {
	return &RecoveryManager{limit: 5000}
}

func NewRecoveryManagerWith(owner PipelineRecoveryOwner) *RecoveryManager {
	rm := NewRecoveryManager()
	rm.owner = owner
	return rm
}

func (r *RecoveryManager) Recover(ctx context.Context) error {
	if r == nil || r.owner == nil {
		return nil
	}
	ctx = runtimepipelineobligation.WithStartupRecoveryDiagnostics(ctx)
	limit := r.limit
	if limit <= 0 {
		limit = 500
	}
	for {
		processed, err := r.owner.SweepUndispatched(ctx, limit)
		if err != nil {
			return err
		}
		if processed < limit {
			return nil
		}
	}
}
