package runforkexecution

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type competingPreparationLoader struct {
	SelectedContractSourceLoader
	loads atomic.Int32
	ready chan struct{}
}

func TestSelectedForkIndependentOperationsExecuteDistinctChildrenBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, keyed := range []bool{true, false} {
			name := backend + "/keyless"
			if keyed {
				name = backend + "/independent_keys"
			}
			t.Run(name, func(t *testing.T) {
				ctx, request := ownerlessSelectedOperationFixture(t, backend)
				if !keyed {
					request.ForkOperation.IdempotencyKey = ""
				}
				selected := request.SourceLoader.(SourceArtifactSelectedContractSourceLoader).Store
				advanced := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(uuid.NewString(), "item.received", "source-runtime", "", []byte(`{}`), 0,
					request.SourceRunID, events.EventEnvelope{}, events.NoRoutingSource(), time.Unix(1700002300, 0).UTC(), executionmode.Mock)
				storetest.CommitSemanticEventWithRoutes(t, ctx, selected, advanced,
					[]events.DeliveryRoute{selectedExecutionEntitylessNodeRoute("source-only-node")}, pipelineobligation.ScopeSubscribed)
				storetest.CaptureRunForkSnapshot(t, ctx, selected, request.SourceRunID)
				execution := &liveWinnerExecutionProbe{SelectedContractRuntimeExecutionLifecycle: request.Owner.ports.runtimeExecution,
					entered: make(chan struct{}), allow: make(chan struct{})}
				close(execution.allow)
				request.Owner.ports.runtimeExecution = execution
				first, err := ExecuteSelectedContractRunFork(ctx, request)
				if err != nil || !first.Activation.Activated || first.Activation.SourceFrozen || !first.Activation.SourceAdvancedAfterFork {
					t.Fatalf("first invocation failed: %+v err=%v", first, err)
				}
				next := *request.ForkOperation
				next.OperationID = uuid.NewString()
				if keyed {
					next.IdempotencyKey = "independent-second-operation"
				}
				request.ForkOperation = &next
				second, err := ExecuteSelectedContractRunFork(ctx, request)
				if err != nil || !second.Activation.Activated || first.Materialization.ForkRunID == second.Materialization.ForkRunID ||
					!first.Materialization.ForkPoint.SameIdentity(second.Materialization.ForkPoint) || execution.issues.Load() != 2 {
					t.Fatalf("independent invocation rediscovered a cut sibling: first=%+v second=%+v issues=%d err=%v", first, second, execution.issues.Load(), err)
				}
				for _, child := range []string{first.Materialization.ForkRunID, second.Materialization.ForkRunID} {
					stored, err := storetest.ReadSelectedExecutionStorage(ctx, selected, child)
					if err != nil || stored.Occurrences != 1 || stored.RunStatus == "cancelled" || stored.RunStatus == "failed" {
						t.Fatalf("independent child execution count: child=%s stored=%+v err=%v", child, stored, err)
					}
				}
				if err := request.Owner.RetireSelectedContexts(ctx); err != nil {
					t.Fatalf("join independently admitted child owners: %v", err)
				}
				request.Owner.ports.contexts.mu.Lock()
				remaining := len(request.Owner.ports.contexts.entries)
				request.Owner.ports.contexts.mu.Unlock()
				if remaining != 0 {
					t.Fatalf("independent invocations leaked after joined retirement: entries=%d", remaining)
				}
			})
		}
	}
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
