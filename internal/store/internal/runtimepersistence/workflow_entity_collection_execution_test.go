package runtimepersistence

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type selectedEntityCollectionExecutionStore interface {
	QueryWorkflowEntityCollection(context.Context, runtimepipeline.WorkflowEntityCollectionOwner) ([]runtimepipeline.WorkflowEntityStatePersistenceRecord, error)
}

type selectedEntityCollectionExecutionReader struct {
	store  selectedEntityCollectionExecutionStore
	source semanticview.Source
	runID  string
}

func (r selectedEntityCollectionExecutionReader) QueryEntityCollection(ctx context.Context, flowID, entityType string) ([]map[string]any, error) {
	owner, err := runtimepipeline.AdmitWorkflowEntityCollectionOwner(r.source, flowID, entityType, r.runID)
	if err != nil {
		return nil, err
	}
	records, err := r.store.QueryWorkflowEntityCollection(ctx, owner)
	if err != nil {
		return nil, err
	}
	rows := make([]map[string]any, 0, len(records))
	for _, record := range records {
		fields := map[string]any{}
		if err := json.Unmarshal(record.Fields, &fields); err != nil {
			return nil, err
		}
		rows = append(rows, fields)
	}
	return rows, nil
}

type selectedEntityCollectionExecutionState struct{}

func (selectedEntityCollectionExecutionState) LoadState(context.Context, runtimeengine.StateAddress) (runtimeengine.StateSnapshot, bool, error) {
	return runtimeengine.StateSnapshot{}, false, nil
}

func (selectedEntityCollectionExecutionState) SaveState(context.Context, runtimeengine.StateAddress, runtimeengine.StateMutation) error {
	return nil
}

type selectedEntityCollectionExecutionMutation struct{}

func (selectedEntityCollectionExecutionMutation) CommitEngineMutation(context.Context, runtimeengine.EngineMutation) (runtimeengine.CommittedEngineMutation, error) {
	return runtimeengine.CommittedEngineMutation{}, nil
}

type selectedEntityCollectionExecutionLocker struct{}

func (selectedEntityCollectionExecutionLocker) WithEntityLock(ctx context.Context, _ runtimeidentity.EntityID, fn func(context.Context) error) error {
	return fn(ctx)
}

type selectedEntityCollectionExecutionDispatcher struct{}

func (selectedEntityCollectionExecutionDispatcher) DispatchPostCommit(context.Context, []runtimeengine.EmitIntent) error {
	return nil
}

func TestExecutorQueryEntitiesResultIncludesStateOnlyRowOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, db, baseCtx, runID := openStateOnlyAcquisitionStore(t, backend)
			store, ok := selected.(selectedEntityCollectionExecutionStore)
			if !ok {
				t.Fatalf("%s selected store has no workflow entity collection operation", backend)
			}
			source := stateOnlyAcquisitionSourceWithMode(t, "child", runtimecontracts.FlowModeTemplate)
			executor, err := runtimeengine.NewExecutor(runtimeengine.RuntimeDependencies{
				Source:            source,
				EntityCollections: selectedEntityCollectionExecutionReader{store: store, source: source, runID: runID},
				StateRepo:         selectedEntityCollectionExecutionState{},
				MutationOwner:     selectedEntityCollectionExecutionMutation{},
				Locker:            selectedEntityCollectionExecutionLocker{},
				Dispatcher:        selectedEntityCollectionExecutionDispatcher{},
			}, nil)
			if err != nil {
				t.Fatalf("new executor: %v", err)
			}
			ctx := runtimecorrelation.WithRunID(baseCtx, runID)
			execute := func(label string) int {
				t.Helper()
				node, err := runtimeidentity.AdmitExecutableNodeDeclaration(runtimeidentity.RootPackageKey, "child", "selector")
				if err != nil {
					t.Fatal(err)
				}
				result, err := executor.Execute(ctx, runtimeengine.ExecutionRequest{
					EntityID: runtimeidentity.NormalizeEntityID(uuid.NewString()), ExecutionFlowID: runtimeidentity.NormalizeFlowID("child"), Node: node,
					HandlerEventKey: "test.node_emitted",
					Event:           eventtest.ExistingRunRootIngress(uuid.NewString(), "test.node_emitted", "", "", json.RawMessage(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC()),
					Handler:         runtimecontracts.SystemNodeEventHandler{Query: &runtimecontracts.QuerySpec{Entities: "review_item", Count: true}},
					State:           runtimeengine.StateSnapshot{CurrentState: "active", WorkflowName: "child"},
				})
				if err != nil {
					t.Fatalf("execute %s: %v", label, err)
				}
				count, ok := result.Computed["query"].(int)
				if !ok {
					t.Fatalf("%s computed.query = %#v", label, result.Computed["query"])
				}
				return count
			}

			if got := execute("before state-only row"); got != 0 {
				t.Fatalf("baseline count = %d, want zero", got)
			}
			seedWorkflowEntityQueryRowAs(t, backend, db, runID, "child/state-only", "review_item", map[string]any{"account_id": "state-only"})
			if got := execute("after state-only row"); got != 1 {
				t.Fatalf("state-only count = %d, want one", got)
			}
		})
	}
}
