package pipeline

import (
	"context"
	"path/filepath"
	"testing"

	"empireai/internal/events"
	runtimecontracts "empireai/internal/runtime/contracts"
	"empireai/internal/runtime/semanticview"
)

type recordingExecutionEngine struct {
	called  bool
	handler runtimecontracts.SystemNodeEventHandler
}

func (r *recordingExecutionEngine) ExecuteHandlerSteps(_ context.Context, handler SystemNodeEventHandler, _ Event) (*HandlerOutcome, error) {
	r.called = true
	r.handler = handler
	return &HandlerOutcome{Handled: true}, nil
}

func TestFindAccumulationTimeoutHandler_OnTimeoutFixture(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier2-accumulation", "test-accumulate-on-timeout")
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "mas-platform", "platform", "contracts", "platform-spec.yaml")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	nodeID, handler, ok := findAccumulationTimeoutHandler(semanticview.Wrap(bundle), "accumulate.timeout")
	if !ok {
		t.Fatal("expected timeout handler")
	}
	if nodeID != "test-node" {
		t.Fatalf("nodeID = %q", nodeID)
	}
	if handler.Accumulate == nil || handler.Accumulate.OnTimeout == nil {
		t.Fatalf("handler missing accumulate on_timeout: %#v", handler.Accumulate)
	}
	entry := bundle.NodeEntries()["test-node"]
	rawHandler := entry.EventHandlers["item.arrived"]
	if rawHandler.Accumulate == nil || rawHandler.Accumulate.OnTimeout == nil {
		t.Fatalf("raw handler missing accumulate on_timeout: %#v", rawHandler.Accumulate)
	}
}

func TestDeclarativeNodeHandleEvent_SelectsOnTimeoutAccumulatorHandler(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier2-accumulation", "test-accumulate-on-timeout")
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "mas-platform", "platform", "contracts", "platform-spec.yaml")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	entry := bundle.NodeEntries()["test-node"]
	engine := &recordingExecutionEngine{}
	node := NewNode(entry, engine, nil)
	handled := node.Handle(context.Background(), events.Event{Type: "accumulate.timeout"})
	if !handled {
		t.Fatal("expected timeout event to be handled")
	}
	if !engine.called {
		t.Fatal("expected execution engine to be called")
	}
}

func TestDeclarativeNodeHandleEvent_MatchesWildcardHandler(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier5-flow-lifecycle", "test-wildcard-subscription")
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "mas-platform", "platform", "contracts", "platform-spec.yaml")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	entry := bundle.NodeEntries()["test-node"]
	engine := &recordingExecutionEngine{}
	node := NewNode(entry, engine, nil)
	handled := node.Handle(context.Background(), events.Event{Type: "task.completed"}.WithEntityID("ent-1"))
	if !handled {
		t.Fatal("expected wildcard event to be handled")
	}
	if !engine.called {
		t.Fatal("expected execution engine to be called")
	}
}

func TestWorkflowNodeEventPolicy_MatchesWildcardSubscription(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier5-flow-lifecycle", "test-wildcard-subscription")
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "mas-platform", "platform", "contracts", "platform-spec.yaml")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	module, err := newPipelineFixtureWorkflowModule(bundle)
	if err != nil {
		t.Fatalf("newPipelineFixtureWorkflowModule: %v", err)
	}
	previous := defaultWorkflowModuleFactory
	SetDefaultWorkflowModuleFactory(func() WorkflowModule { return module })
	defer SetDefaultWorkflowModuleFactory(previous)

	policy, ok := workflowNodeEventPolicy("test-node", "task.completed")
	if !ok {
		t.Fatal("expected wildcard subscription policy to match")
	}
	if !policy.RequireEntity {
		t.Fatalf("policy = %#v, want require_entity=true", policy)
	}
}

func TestWorkflowMaxChainDepthPolicy_UsesFixturePolicy(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier6-event-loop", "test-chain-depth-limit")
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "mas-platform", "platform", "contracts", "platform-spec.yaml")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	got := workflowMaxChainDepthPolicy(semanticview.Wrap(bundle))
	if got != 5 {
		t.Fatalf("workflowMaxChainDepthPolicy = %d, want 5", got)
	}
}
