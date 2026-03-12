package engine

import (
	"context"
	"reflect"
	"testing"

	"empireai/internal/events"
	runtimecontracts "empireai/internal/runtime/contracts"
	"empireai/internal/runtime/semanticview"
)

func TestNewDeclarativeNode_RequiresExecutor(t *testing.T) {
	if node := NewDeclarativeNode("node-a", nil); node != nil {
		t.Fatalf("expected nil node without executor, got %#v", node)
	}
}

func TestNewDeclarativeNode_StoresNodeID(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     stubSource(),
		StateRepo:  stubStateRepo{},
		TxRunner:   stubRunner{},
		Locker:     stubLocker{},
		Outbox:     stubOutbox{},
		Dispatcher: stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	node := NewDeclarativeNode("node-a", exec)
	if node == nil {
		t.Fatal("expected declarative node")
	}
	if got := node.NodeID(); got != "node-a" {
		t.Fatalf("NodeID = %q", got)
	}
}

func TestDeclarativeNode_HandleResolvesHandlerFromSemanticSource(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"node-a": {
				ID: "node-a",
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"node-a": {
					"scan.completed": {
						AdvancesTo: "done",
					},
				},
			},
		},
	})
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     source,
		StateRepo:  stubStateRepo{},
		TxRunner:   stubRunner{},
		Locker:     stubLocker{},
		Outbox:     stubOutbox{},
		Dispatcher: stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	node := NewDeclarativeNode("node-a", exec)
	result, err := node.Handle(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		FlowID:   "flow-1",
		Event:    events.Event{ID: "evt-1", Type: "scan.completed"},
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
		Source:     stubSource(),
		StateRepo:  stubStateRepo{},
		TxRunner:   stubRunner{},
		Locker:     stubLocker{},
		Outbox:     stubOutbox{},
		Dispatcher: stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	node := NewDeclarativeNode("node-a", exec)
	_, err = node.Handle(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Event:    events.Event{Type: "scan.completed"},
	})
	if err != ErrMissingNodeHandler {
		t.Fatalf("Handle error = %v, want %v", err, ErrMissingNodeHandler)
	}
}

func TestDeclarativeNode_HandleUsesExplicitHandlerWithoutLookup(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     stubSource(),
		StateRepo:  stubStateRepo{},
		TxRunner:   stubRunner{},
		Locker:     stubLocker{},
		Outbox:     stubOutbox{},
		Dispatcher: stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	node := NewDeclarativeNode("node-a", exec)
	result, err := node.Handle(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Event:    events.Event{Type: "scan.completed"},
		Handler:  runtimecontracts.SystemNodeEventHandler{ClearGates: []string{"gate_a"}},
		State:    StateSnapshot{Metadata: map[string]any{"gates": map[string]any{"gate_a": true}}},
	})
	if err != nil {
		t.Fatalf("Handle error: %v", err)
	}
	if !reflect.DeepEqual(result.ClearGates, []string{"gate_a"}) {
		t.Fatalf("ClearGates = %#v", result.ClearGates)
	}
}
