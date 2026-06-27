package pipeline

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type recordingExecutionEngine struct {
	called          bool
	handler         runtimecontracts.SystemNodeEventHandler
	handlerEventKey string
}

func (r *recordingExecutionEngine) ExecuteHandlerSteps(_ context.Context, handler SystemNodeEventHandler, _ Event, handlerEventKey string) (*HandlerOutcome, error) {
	r.called = true
	r.handler = handler
	r.handlerEventKey = handlerEventKey
	return &HandlerOutcome{Handled: true}, nil
}

func TestFindAccumulationTimeoutHandlerForBucket_OnTimeoutFixture(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier2-accumulation", "test-accumulate-on-timeout")
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	bucket := timeridentity.NewAccumulatorBucketRef("test-node", "item.arrived")
	handler, ok := findAccumulationTimeoutHandlerForBucket(semanticview.Wrap(bundle), bucket)
	if !ok {
		t.Fatal("expected timeout handler")
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
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	entry := bundle.NodeEntries()["test-node"]
	engine := &recordingExecutionEngine{}
	node := NewNode(entry, semanticview.Wrap(bundle), engine, nil)
	handled := node.Handle(context.Background(), eventtest.RootIngress("", "accumulate.timeout", "", "", mustJSON(map[string]any{
		"timer_handle": map[string]any{
			"kind": "accumulation_timeout",
			"bucket": map[string]any{
				"node_id":    "test-node",
				"event_type": "item.arrived",
			},
		},
	}), 0, "", "", events.EventEnvelope{}, time.Time{}))
	if !handled {
		t.Fatal("expected timeout event to be handled")
	}
	if !engine.called {
		t.Fatal("expected execution engine to be called")
	}
	if got := engine.handlerEventKey; got != "item.arrived" {
		t.Fatalf("handler event key = %q, want item.arrived", got)
	}
}

func TestDeclarativeNodeHandleEvent_MatchesWildcardHandler(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier5-flow-lifecycle", "test-wildcard-subscription")
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	entry := bundle.NodeEntries()["test-node"]
	engine := &recordingExecutionEngine{}
	node := NewNode(entry, semanticview.Wrap(bundle), engine, nil)
	handled := node.Handle(context.Background(), eventtest.RootIngress("", "task.completed", "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"), time.Time{}))
	if !handled {
		t.Fatal("expected wildcard event to be handled")
	}
	if !engine.called {
		t.Fatal("expected execution engine to be called")
	}
	if got := engine.handlerEventKey; got != "*.completed" {
		t.Fatalf("handler event key = %q, want *.completed", got)
	}
}

func TestDeclarativeNodeHandleEvent_DeniesImportBoundaryWildcardRawFallback(t *testing.T) {
	source := loadPipelineImportBoundaryWildcardSource(t, "")
	entry := source.NodeEntries()["worker-listener"]
	engine := &recordingExecutionEngine{}
	node := NewNode(entry, source, engine, nil)

	handled := node.Handle(context.Background(), eventtest.RootIngress("", "producer/task.done", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
	if handled {
		t.Fatal("expected ungranted sibling event not to be handled")
	}
	if engine.called {
		t.Fatal("execution engine was called through raw wildcard fallback")
	}
}

func TestDeclarativeNodeHandleEvent_AllowsGrantedImportBoundaryWildcard(t *testing.T) {
	source := loadPipelineImportBoundaryWildcardSource(t, "      observe:\n        - source: producer\n          events: [task.done]\n")
	entry := source.NodeEntries()["worker-listener"]
	engine := &recordingExecutionEngine{}
	node := NewNode(entry, source, engine, nil)

	handled := node.Handle(context.Background(), eventtest.RootIngress("", "producer/task.done", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
	if !handled {
		t.Fatal("expected granted sibling event to be handled")
	}
	if !engine.called {
		t.Fatal("expected execution engine to be called")
	}
	if got := engine.handlerEventKey; got != "**/task.done" {
		t.Fatalf("handler event key = %q, want **/task.done", got)
	}
}

func TestWorkflowNodeEventPolicy_MatchesWildcardSubscription(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier5-flow-lifecycle", "test-wildcard-subscription")
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	module, err := newPipelineFixtureWorkflowModule(bundle)
	if err != nil {
		t.Fatalf("newPipelineFixtureWorkflowModule: %v", err)
	}
	policy, ok := workflowNodeEventPolicy(module.WorkflowNodes(), "test-node", "task.completed")
	if !ok {
		t.Fatal("expected wildcard subscription policy to match")
	}
	if !policy.RequireEntity {
		t.Fatalf("policy = %#v, want require_entity=true", policy)
	}
}

func TestDeclarativeNodeHandleEvent_MatchesDeepWildcardChildFlowHandler(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-wildcard-deep-subscription")
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	entry := bundle.NodeEntries()["collector"]
	engine := &recordingExecutionEngine{}
	node := NewNode(entry, semanticview.Wrap(bundle), engine, nil)
	handled := node.Handle(context.Background(), eventtest.RootIngress("", "child/grandchild/task.done", "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"), time.Time{}))
	if !handled {
		t.Fatal("expected deep wildcard event to be handled")
	}
	if !engine.called {
		t.Fatal("expected execution engine to be called")
	}
	if got := engine.handlerEventKey; got != "**/task.done" {
		t.Fatalf("handler event key = %q, want **/task.done", got)
	}
}

func TestWorkflowMaxChainDepthPolicy_UsesFixturePolicy(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier6-event-loop", "test-chain-depth-limit")
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	got := workflowMaxChainDepthPolicy(semanticview.Wrap(bundle))
	if got != 5 {
		t.Fatalf("workflowMaxChainDepthPolicy = %d, want 5", got)
	}
}

func TestWorkflowHandlerRetryBase_UsesFixturePolicyDefault(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	if got := workflowHandlerRetryBase(source); got != time.Second {
		t.Fatalf("workflowHandlerRetryBase default = %s, want 1s", got)
	}

	bundle := &runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{
			Values: map[string]runtimecontracts.PolicyValue{
				"handler_retry_base_seconds": {Value: 60},
			},
		},
	}
	if got := workflowHandlerRetryBase(semanticview.Wrap(bundle)); got != 60*time.Second {
		t.Fatalf("workflowHandlerRetryBase policy = %s, want 1m0s", got)
	}
}
