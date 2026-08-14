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
	supervisor *runtimeProjectSupervisor
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
	if err := q.supervisor.completePendingReplacementRollback(ctx); err != nil {
		return nil, fmt.Errorf("finalize pending runtime replacement rollback before bundle delete: %w", err)
	}
	if err := q.supervisor.completePendingSourceSetRollback(ctx); err != nil {
		return nil, fmt.Errorf("finalize pending runtime source-set rollback before bundle delete: %w", err)
	}
	if err := q.supervisor.completePendingReplacement(); err != nil {
		return nil, fmt.Errorf("complete pending runtime restoration before bundle delete: %w", err)
	}
	bundleHash = strings.TrimSpace(bundleHash)
	if _, loaded := q.contexts.LookupBundleHash(bundleHash); !loaded {
		retained = true
		return &inertBundleDeleteRuntimeQuiescence{operation: operation}, nil
	}
	predecessor, err := q.contexts.BeginBundleRuntimeQuiescence(ctx, bundleHash)
	if err != nil {
		return nil, err
	}
	if predecessor.Runtime == nil {
		return nil, errors.New("bundle delete quiescence lost the predecessor runtime")
	}
	if err := q.supervisor.quiesceCurrentRuntimeWithOptions(context.Background(), predecessor.Runtime, q.supervisor.replacementShutdown); err != nil {
		restoreErr := q.supervisor.completeFailedQuiescenceAndRestore(ctx, q.contexts, predecessor, predecessor.Runtime, nil)
		return nil, errors.Join(fmt.Errorf("quiesce bundle runtime: %w", err), restoreErr)
	}
	retained = true
	return &bundleDeleteRuntimeQuiescence{
		contexts: q.contexts, supervisor: q.supervisor, predecessor: predecessor, operation: operation,
	}, nil
}

type bundleDeleteRuntimeOperation struct {
	once       sync.Once
	supervisor *runtimeProjectSupervisor
}

func (o *bundleDeleteRuntimeOperation) release() {
	if o == nil || o.supervisor == nil {
		return
	}
	o.once.Do(o.supervisor.operationMu.Unlock)
}

type bundleDeleteRuntimeQuiescence struct {
	mu                  sync.Mutex
	contexts            *runtime.RuntimeContextManager
	supervisor          *runtimeProjectSupervisor
	predecessor         runtime.BundleContext
	operation           *bundleDeleteRuntimeOperation
	publicationRetained bool
	restored            bool
	committed           bool
}

func (q *bundleDeleteRuntimeQuiescence) Restore(ctx context.Context) error {
	if q == nil {
		return nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	defer q.operation.release()
	if q.committed || q.restored {
		return nil
	}
	var err error
	if q.publicationRetained {
		err = q.supervisor.completePendingReplacement()
	} else {
		err = q.supervisor.restoreQuiescedPredecessor(ctx, q.contexts, q.predecessor, q.predecessor.Runtime, nil)
		q.supervisor.mu.RLock()
		q.publicationRetained = q.supervisor.pendingReplacement != nil
		q.supervisor.mu.RUnlock()
	}
	if err != nil {
		return err
	}
	q.restored = true
	return nil
}

func (q *bundleDeleteRuntimeQuiescence) Commit() {
	if q == nil {
		return
	}
	q.mu.Lock()
	q.committed = true
	q.mu.Unlock()
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
