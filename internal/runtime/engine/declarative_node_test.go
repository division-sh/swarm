package engine

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	stdruntime "runtime"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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
					"task.completed": {
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
		Event:    eventtest.RootIngress("evt-1", "task.completed", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
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
		Event:    eventtest.RootIngress("", "task.completed", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
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
		Event:    eventtest.RootIngress("", "task.completed", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
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

func TestResolvedExecutionHandler_DeniesImportBoundaryWildcardRawFallback(t *testing.T) {
	source := loadEngineImportBoundaryWildcardSource(t, "")
	resolved := resolvedExecutionHandler(source, "worker-listener", "producer/task.done")
	if resolved.matched {
		t.Fatalf("resolvedExecutionHandler matched ungranted sibling event through raw fallback: %#v", resolved)
	}
}

func TestResolvedExecutionHandler_AllowsGrantedImportBoundaryWildcard(t *testing.T) {
	source := loadEngineImportBoundaryWildcardSource(t, "      observe:\n        - source: producer\n          events: [task.done]\n")
	resolved := resolvedExecutionHandler(source, "worker-listener", "producer/task.done")
	if !resolved.matched {
		t.Fatal("resolvedExecutionHandler did not match granted sibling event")
	}
	if got := resolved.handlerEventKey; got != "**/task.done" {
		t.Fatalf("handler event key = %q, want **/task.done", got)
	}
	if !reflect.DeepEqual(resolved.handler.ClearGates, []string{"sibling_gate"}) {
		t.Fatalf("handler = %#v, want sibling_gate clear handler", resolved.handler)
	}
}

func loadEngineImportBoundaryWildcardSource(t *testing.T, observeGrant string) semanticview.Source {
	t.Helper()
	repoRoot := engineRepoRoot(t)
	root := writeEngineImportBoundaryWildcardFixture(t, observeGrant)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func engineRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := stdruntime.Caller(0)
	if !ok {
		t.Fatal("resolve runtime caller")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

func writeEngineImportBoundaryWildcardFixture(t *testing.T, observeGrant string) string {
	t.Helper()
	root := t.TempDir()
	workerBind := ""
	if strings.TrimSpace(observeGrant) != "" {
		workerBind = "    bind:\n" + observeGrant
	}
	writeEngineFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: engine-import-boundary-wildcard
version: "1.0.0"
platform_version: ">=1.6.0"
flows:
  - id: worker
    flow: worker
    mode: static
`+workerBind+`  - id: producer
    flow: producer
    mode: static
`)
	writeEngineFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: engine-import-boundary-wildcard\n")
	writeEngineFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeEngineFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeEngineFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeEngineFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeEngineFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeEngineFixtureFile(t, filepath.Join(root, "flows", "worker", "package.yaml"), "name: worker\nversion: \"1.0.0\"\n")
	writeEngineFixtureFile(t, filepath.Join(root, "flows", "worker", "schema.yaml"), `
name: worker
mode: static
initial_state: active
terminal_states: [done]
states: [active, done]
pins:
  outputs:
    events: [task.done]
`)
	writeEngineFixtureFile(t, filepath.Join(root, "flows", "worker", "policy.yaml"), "{}\n")
	writeEngineFixtureFile(t, filepath.Join(root, "flows", "worker", "agents.yaml"), "{}\n")
	writeEngineFixtureFile(t, filepath.Join(root, "flows", "worker", "events.yaml"), "task.done: {}\n")
	writeEngineFixtureFile(t, filepath.Join(root, "flows", "worker", "nodes.yaml"), `
worker-listener:
  id: worker-listener
  execution_type: system_node
  subscribes_to: ["**/task.done"]
  event_handlers:
    "**/task.done":
      clear_gates: [sibling_gate]
`)
	writeEngineFixtureFile(t, filepath.Join(root, "flows", "producer", "package.yaml"), "name: producer\nversion: \"1.0.0\"\n")
	writeEngineFixtureFile(t, filepath.Join(root, "flows", "producer", "schema.yaml"), `
name: producer
mode: static
initial_state: active
terminal_states: [done]
states: [active, done]
pins:
  outputs:
    events: [task.done]
`)
	writeEngineFixtureFile(t, filepath.Join(root, "flows", "producer", "policy.yaml"), "{}\n")
	writeEngineFixtureFile(t, filepath.Join(root, "flows", "producer", "agents.yaml"), "{}\n")
	writeEngineFixtureFile(t, filepath.Join(root, "flows", "producer", "events.yaml"), "task.done: {}\n")
	writeEngineFixtureFile(t, filepath.Join(root, "flows", "producer", "nodes.yaml"), "{}\n")
	return root
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
