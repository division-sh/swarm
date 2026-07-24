package pipeline

import (
	"context"
	"errors"

	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

type PipelineRecoveryOwner interface {
	SweepPipelineObligations(context.Context, int) (runtimepipelineobligation.SweepResult, error)
}

type RecoveryManager struct {
	owner PipelineRecoveryOwner
	limit int
}

func NewRecoveryManagerWith(owner PipelineRecoveryOwner) *RecoveryManager {
	return NewRecoveryManagerWithLimit(owner, 5000)
}

func NewRecoveryManagerWithLimit(owner PipelineRecoveryOwner, limit int) *RecoveryManager {
	if owner == nil {
		panic("pipeline recovery owner is required")
	}
	if limit <= 0 {
		panic("pipeline recovery limit must be positive")
	}
	return &RecoveryManager{owner: owner, limit: limit}
}

func (r *RecoveryManager) Recover(ctx context.Context) error {
	if r == nil || r.owner == nil {
		return errors.New("pipeline recovery owner is required")
	}
	ctx = runtimepipelineobligation.WithStartupRecoveryDiagnostics(ctx)
	limit := r.limit
	if limit <= 0 {
		limit = 500
	}
	for {
		result, err := r.owner.SweepPipelineObligations(ctx, limit)
		if err != nil {
			return err
		}
		if result.Blocked || result.Exhausted {
			return nil
		}
	}
}
