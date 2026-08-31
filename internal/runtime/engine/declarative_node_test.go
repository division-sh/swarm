package engine

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	stdruntime "runtime"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestNewDeclarativeNode_RequiresExecutor(t *testing.T) {
	if node := NewDeclarativeNode(testRootExecutableNode(t, "node-a"), nil); node != nil {
		t.Fatalf("expected nil node without executor, got %#v", node)
	}
}

func TestNewDeclarativeNode_StoresNodeID(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	node := NewDeclarativeNode(testRootExecutableNode(t, "node-a"), exec)
	if node == nil {
		t.Fatal("expected declarative node")
	}
	if got := node.NodeID(); got != "node-a" {
		t.Fatalf("NodeID = %q", got)
	}
}

func TestDeclarativeNode_HandleResolvesHandlerFromSemanticSource(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.completed": {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"node-a": {
				ID: "node-a",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"task.completed": {AdvancesTo: "done"},
				},
			},
		},
	})
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        source,
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	executableNode := testRootExecutableNode(t, "node-a")
	node := NewDeclarativeNode(executableNode, exec)
	result, err := node.Handle(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     executableNode,
		Event:    eventtest.RunCreatingRootIngress("evt-1", "task.completed", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
		State:    StateSnapshot{CurrentState: "pending"},
	})
	if err != nil {
		t.Fatalf("Handle error: %v", err)
	}
	if result.NextState != "done" {
		t.Fatalf("NextState = %q", result.NextState)
	}
}

func TestDeclarativeNode_HandleRequiresHandlerWhenNotResolvable(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	executableNode := testRootExecutableNode(t, "node-a")
	node := NewDeclarativeNode(executableNode, exec)
	_, err = node.Handle(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     executableNode,
		Event:    eventtest.RunCreatingRootIngress("", "task.completed", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
	})
	if err != ErrMissingNodeHandler {
		t.Fatalf("Handle error = %v, want %v", err, ErrMissingNodeHandler)
	}
}

func TestDeclarativeNode_HandleUsesExplicitHandlerWithoutLookup(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	executableNode := testRootExecutableNode(t, "node-a")
	node := NewDeclarativeNode(executableNode, exec)
	result, err := node.Handle(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     executableNode,
		Event:    eventtest.RunCreatingRootIngress("", "task.completed", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler:  runtimecontracts.SystemNodeEventHandler{ClearGates: []string{"gate_a"}},
		State:    StateSnapshot{StateCarrier: NewStateCarrier(nil, map[string]bool{"gate_a": true}, nil)},
	})
	if err != nil {
		t.Fatalf("Handle error: %v", err)
	}
	if !reflect.DeepEqual(result.ClearGates, []string{"gate_a"}) {
		t.Fatalf("ClearGates = %#v", result.ClearGates)
	}
}

func TestResolvedExecutionHandlerRejectsQualifiedExactRawBundleFallback(t *testing.T) {
	handler := runtimecontracts.SystemNodeEventHandler{AdvancesTo: "done"}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"listener": {ID: "listener", EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"child/task.done": handler}},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
			"listener": {"child/task.done": handler},
		}},
	}
	if resolved := resolvedExecutionHandler(semanticview.Wrap(bundle), testRootExecutableNode(t, "listener"), "child/task.done"); resolved.matched {
		t.Fatalf("qualified exact handler reached engine fallback: %#v", resolved)
	}
}

func engineRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := stdruntime.Caller(0)
	if !ok {
		t.Fatal("resolve runtime caller")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

func writeEngineFixtureFile(t *testing.T, path string, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
