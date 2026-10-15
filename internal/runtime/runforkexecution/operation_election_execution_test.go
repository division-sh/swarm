package runforkexecution

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	"github.com/google/uuid"
)

type competingPreparationLoader struct {
	SelectedContractSourceLoader
	loads atomic.Int32
	ready chan struct{}
}

func (l *competingPreparationLoader) LoadRunForkSelectedContractSourceForRequest(ctx context.Context, request SelectedContractSourceLoadRequest) (LoadedSelectedContractSource, error) {
	count := l.loads.Add(1)
	if count == 2 {
		close(l.ready)
	}
	if count <= 2 {
		select {
		case <-l.ready:
		case <-ctx.Done():
			return LoadedSelectedContractSource{}, ctx.Err()
		}
	}
	return l.SelectedContractSourceLoader.LoadRunForkSelectedContractSourceForRequest(ctx, request)
}

type competingMaterializationProbe struct {
	SelectedContractForkLifecycle
	attempts atomic.Int32
}

func (p *competingMaterializationProbe) MaterializeRunForkForSelectedContractExecution(ctx context.Context, request runforkreadiness.MaterializeRequest) (runfork.RunForkMaterialization, error) {
	p.attempts.Add(1)
	return p.SelectedContractForkLifecycle.MaterializeRunForkForSelectedContractExecution(ctx, request)
}

func TestSelectedForkCompetingPreparationsJoinNativeElectionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, request := ownerlessSelectedOperationFixture(t, backend)
			request.SourceLoader = &competingPreparationLoader{SelectedContractSourceLoader: request.SourceLoader, ready: make(chan struct{})}
			fork := &competingMaterializationProbe{SelectedContractForkLifecycle: request.Owner.ports.fork}
			runtime := &liveWinnerExecutionProbe{SelectedContractRuntimeExecutionLifecycle: request.Owner.ports.runtimeExecution,
				entered: make(chan struct{}), allow: make(chan struct{})}
			close(runtime.allow)
			request.Owner.ports.fork, request.Owner.ports.runtimeExecution = fork, runtime
			type outcome struct {
				result SelectedContractExecutionResult
				err    error
			}
			results := make(chan outcome, 2)
			for range 2 {
				invocation := request
				operation := *request.ForkOperation
				operation.OperationID = uuid.NewString()
				invocation.ForkOperation = &operation
				go func() {
					result, err := ExecuteSelectedContractRunFork(ctx, invocation)
					results <- outcome{result, err}
				}()
			}
			first, second := <-results, <-results
			if first.err != nil || second.err != nil || !first.result.Activation.Activated || !second.result.Activation.Activated ||
				first.result.Materialization.ForkRunID == "" || first.result.Materialization.ForkRunID != second.result.Materialization.ForkRunID ||
				fork.attempts.Load() != 2 || runtime.issues.Load() != 1 {
				t.Fatalf("competing preparations did not consume one native election: first=%+v/%v second=%+v/%v attempts=%d issues=%d",
					first.result, first.err, second.result, second.err, fork.attempts.Load(), runtime.issues.Load())
			}
			request.Owner.ports.contexts.mu.Lock()
			remaining := len(request.Owner.ports.contexts.entries)
			request.Owner.ports.contexts.mu.Unlock()
			if remaining != 1 {
				t.Fatalf("losing preparation leaked: got %d entries, want retained winner only", remaining)
			}
		})
	}
}
