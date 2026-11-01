package destructivereset

import (
	"context"
	"errors"
	"sync"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
)

// PendingRecovery retains reset serialization between pre-ingestion effect
// settlement and process-local startup convergence. Close never completes a
// receipt: an interrupted boot remains recoverable on the next startup.
type PendingRecovery struct {
	mu          sync.Mutex
	coordinator *Coordinator
	operation   Operation
	outcome     ExecutionResult
	runtime     RuntimeReset
	lease       LockLease
	closed      bool
	completed   bool
}

// SourceSet returns only the admitted sources retained by this reset, never
// paths supplied by the restarting process. The returned plan is independent
// of the immutable journal evidence owned by this continuation.
func (r *PendingRecovery) SourceSet() (agenttopology.SourceSetPlan, error) {
	if r == nil || r.operation.Request.IncludeSourceArtifacts || r.operation.SourceSet == nil {
		return agenttopology.NewSourceSetPlan(nil, nil)
	}
	plan, err := agenttopology.NewSourceSetPlan(r.operation.SourceSet.Sources, r.operation.SourceSet.Agents)
	if err != nil {
		return agenttopology.SourceSetPlan{}, err
	}
	if len(plan.Sources) == 0 || (len(plan.Sources) == 1 && len(r.operation.Request.SourceProjections) == 0) {
		return plan, nil
	}
	if len(r.operation.Request.SourceProjections) != 1 {
		return agenttopology.SourceSetPlan{}, errors.New("reset recovery requires one captured loaded bundle")
	}
	loadedHash := r.operation.Request.SourceProjections[0].Cleanup.BundleHash
	for _, source := range plan.Sources {
		if source.BundleHash == loadedHash {
			return agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{source}, plan.Agents)
		}
	}
	return agenttopology.SourceSetPlan{}, errors.New("reset recovery loaded bundle is absent from its admitted source set")
}

func (r *PendingRecovery) Complete(ctx context.Context) error {
	if r == nil {
		return errors.New("reset recovery is unavailable")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errors.New("reset recovery is closed")
	}
	if r.completed {
		return nil
	}
	if _, err := r.coordinator.completeOperation(ctx, r.operation, r.outcome, r.runtime); err != nil {
		return err
	}
	r.completed = true
	return nil
}

func (r *PendingRecovery) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	r.runtime.Release()
	return r.lease.Release(context.WithoutCancel(ctx))
}
