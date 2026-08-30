package serveapp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/division-sh/swarm/internal/runtime"
	runtimebundledelete "github.com/division-sh/swarm/internal/runtime/bundledelete"
)

type bundleDeleteRuntimeQuiescer struct {
	contexts   *runtime.RuntimeContextManager
	supervisor *processLifecycleSupervisor
}

func (q bundleDeleteRuntimeQuiescer) QuiesceBundleRuntime(ctx context.Context, bundleHash string) (runtimebundledelete.RuntimeQuiescence, error) {
	if q.contexts == nil || q.supervisor == nil {
		return nil, errors.New("bundle delete requires runtime context quiescence ownership")
	}
	operation := &bundleDeleteRuntimeOperation{supervisor: q.supervisor}
	q.supervisor.operationMu.Lock()
	retained := false
	defer func() {
		if !retained {
			operation.release()
		}
	}()
	bundleHash = strings.TrimSpace(bundleHash)
	if !q.contexts.LookupBundleHashStatus(bundleHash).Loaded() {
		retained = true
		return &inertBundleDeleteRuntimeQuiescence{operation: operation}, nil
	}
	withdrawn, err := q.contexts.BeginBundleRuntimeQuiescence(ctx, bundleHash)
	if err != nil {
		return nil, err
	}
	predecessor := withdrawn.Context
	if predecessor.Runtime == nil {
		return nil, errors.New("bundle delete quiescence lost the predecessor runtime")
	}
	if err := q.supervisor.stopRuntime(context.Background(), predecessor.Runtime, q.supervisor.shutdownOptions); err != nil {
		restoreErr := q.supervisor.compensateBundleDeletePredecessor(context.WithoutCancel(ctx), q.contexts, predecessor, withdrawn.RuntimeGeneration)
		return nil, errors.Join(fmt.Errorf("quiesce bundle runtime: %w", err), restoreErr)
	}
	retained = true
	return &bundleDeleteRuntimeQuiescence{
		contexts: q.contexts, supervisor: q.supervisor, predecessor: predecessor,
		predecessorGeneration: withdrawn.RuntimeGeneration, operation: operation,
	}, nil
}

type bundleDeleteRuntimeOperation struct {
	once       sync.Once
	supervisor *processLifecycleSupervisor
}

func (o *bundleDeleteRuntimeOperation) release() {
	if o == nil || o.supervisor == nil {
		return
	}
	o.once.Do(o.supervisor.operationMu.Unlock)
}

type bundleDeleteRuntimeQuiescence struct {
	mu                    sync.Mutex
	contexts              *runtime.RuntimeContextManager
	supervisor            *processLifecycleSupervisor
	predecessor           runtime.BundleContext
	predecessorGeneration uint64
	operation             *bundleDeleteRuntimeOperation
	restored              bool
	committed             bool
}

func (q *bundleDeleteRuntimeQuiescence) Restore(ctx context.Context) error {
	if q == nil {
		return nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.committed || q.restored {
		q.operation.release()
		return nil
	}
	if err := q.supervisor.compensateBundleDeletePredecessor(ctx, q.contexts, q.predecessor, q.predecessorGeneration); err != nil {
		return err
	}
	q.restored = true
	q.operation.release()
	return nil
}

func (q *bundleDeleteRuntimeQuiescence) Commit() {
	if q == nil {
		return
	}
	q.mu.Lock()
	if q.committed {
		q.mu.Unlock()
		q.operation.release()
		return
	}
	q.committed = true
	q.mu.Unlock()
	next, surviving := q.contexts.CommitBundleRuntimeRemoval(q.predecessor.BundleHash())
	q.supervisor.mu.Lock()
	if q.supervisor.currentRT == q.predecessor.Runtime {
		q.supervisor.currentRT = nil
		q.supervisor.currentBundleSourceFact = next.BundleSourceFact
		if surviving {
			q.supervisor.currentRT = next.Runtime
		}
		if q.supervisor.ready != nil {
			q.supervisor.ready.Store(surviving && next.Runtime != nil)
		}
	}
	q.supervisor.mu.Unlock()
	q.operation.release()
}

type inertBundleDeleteRuntimeQuiescence struct {
	operation *bundleDeleteRuntimeOperation
}

func (q *inertBundleDeleteRuntimeQuiescence) Restore(context.Context) error {
	q.operation.release()
	return nil
}

func (q *inertBundleDeleteRuntimeQuiescence) Commit() {
	q.operation.release()
}
