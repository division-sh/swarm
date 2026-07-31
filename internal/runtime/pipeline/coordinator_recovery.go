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
	return r.recover(ctx, false)
}

// RecoverToExhaustion is the startup admission form. A locally blocked pass
// cannot be treated as recovery completion because delivery enumeration must
// observe the pipeline handoff committed by every eligible event first.
func (r *RecoveryManager) RecoverToExhaustion(ctx context.Context) error {
	return r.recover(ctx, true)
}

func (r *RecoveryManager) recover(ctx context.Context, requireExhaustion bool) error {
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
		if result.Blocked && requireExhaustion {
			return errors.New("pipeline recovery blocked before explicit exhaustion")
		}
		if result.Blocked || result.Exhausted {
			return nil
		}
	}
}
