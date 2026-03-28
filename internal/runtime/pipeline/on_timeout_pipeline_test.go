package pipeline

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimeengine "swarm/internal/runtime/engine"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/testutil"
)

func TestExecuteAuthoritativeNodeHandler_OnTimeoutAdvancesPartial(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier2-accumulation", "test-accumulate-on-timeout")
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	module, err := newPipelineFixtureWorkflowModule(bundle)
	if err != nil {
		t.Fatalf("newPipelineFixtureWorkflowModule: %v", err)
	}
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc := NewPipelineCoordinatorWithOptions(noopPipelineBus{}, db, PipelineCoordinatorOptions{
		Module: module,
	})
	if pc == nil {
		t.Fatal("expected coordinator")
	}
	instance := WorkflowInstance{
		InstanceID:      "ent-001",
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "collecting",
		Metadata: map[string]any{
			"expected_count": 3,
		},
		StateBuckets: map[string]any{
			"test-node": map[string]any{
				"handler_accumulators": map[string]any{
					"test-node:item.arrived": map[string]any{
						"expected":       []string{},
						"expected_count": 3,
						"received": map[string]any{
							"evt-a": true,
							"evt-b": true,
						},
						"items": []map[string]any{
							{"event_id": "evt-a"},
							{"event_id": "evt-b"},
						},
						"started_at":       time.Now().UTC().Format(time.RFC3339Nano),
						"last_event_id":    "evt-b",
						"last_event_type":  "item.arrived",
						"last_received_at": time.Now().UTC().Format(time.RFC3339Nano),
						"last_source":      "cataloge2e",
					},
				},
			},
		},
	}
	if err := pc.workflowStore.Upsert(context.Background(), instance); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	timeoutEvt := events.Event{
		ID:        "timeout-1",
		Type:      events.EventType("accumulate.timeout"),
		CreatedAt: time.Now().UTC(),
	}.WithEntityID("ent-001")
	result, err := pc.executeAuthoritativeNodeHandler(context.Background(), timeoutEvt, workflowTriggerContext{
		Event: timeoutEvt,
		State: pc.currentWorkflowState(context.Background(), "ent-001"),
	})
	if err != nil {
		t.Fatalf("executeAuthoritativeNodeHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected timeout handler to be handled")
	}
	if result.Outcome == nil {
		t.Fatal("expected timeout outcome")
	}
	if result.Outcome.AdvancesTo != "partial" {
		t.Fatalf("Outcome.AdvancesTo = %q", result.Outcome.AdvancesTo)
	}
	if len(result.Outcome.Emits) != 2 || result.Outcome.Emits[0] != "collection.done" || result.Outcome.Emits[1] != "collection.partial" {
		t.Fatalf("Outcome.Emits = %#v", result.Outcome.Emits)
	}
}

type noopPipelineBus struct{}

func (noopPipelineBus) Subscribe(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}
func (noopPipelineBus) Publish(context.Context, events.Event) error { return nil }
func (noopPipelineBus) PublishDirect(context.Context, events.Event, []string) error {
	return nil
}
func (noopPipelineBus) ResolveSubscribedRecipients(string) []string { return nil }
func (noopPipelineBus) LogRuntime(context.Context, RuntimeLogEntry) {}
func (noopPipelineBus) EngineOutbox() runtimeengine.OutboxWriter {
	return noOpEngineOutbox{}
}
func (noopPipelineBus) EngineDispatcher() runtimeengine.PostCommitDispatcher {
	return noOpEngineDispatcher{}
}

func newPipelineFixtureWorkflowModule(bundle *runtimecontracts.WorkflowContractBundle) (WorkflowModule, error) {
	source := semanticview.Wrap(bundle)
	workflow, err := LoadWorkflowDefinition(source)
	if err != nil {
		return nil, err
	}
	workflowNodes, err := LoadWorkflowNodes(source)
	if err != nil {
		return nil, err
	}
	return &pipelineFixtureWorkflowModule{
		source:         source,
		workflow:       workflow,
		workflowNodes:  workflowNodes,
		guardRegistry:  NewContractGuardRegistry(source),
		actionRegistry: NewContractActionRegistry(source),
	}, nil
}

type pipelineFixtureWorkflowModule struct {
	source         semanticview.Source
	workflow       *WorkflowDefinition
	workflowNodes  []WorkflowNode
	guardRegistry  GuardRegistry
	actionRegistry ActionRegistry
}

func (m *pipelineFixtureWorkflowModule) SemanticSource() semanticview.Source { return m.source }
func (m *pipelineFixtureWorkflowModule) WorkflowDefinition() *WorkflowDefinition {
	return m.workflow
}
func (m *pipelineFixtureWorkflowModule) WorkflowNodes() []WorkflowNode {
	return append([]WorkflowNode(nil), m.workflowNodes...)
}
func (m *pipelineFixtureWorkflowModule) GuardRegistry() GuardRegistry   { return m.guardRegistry }
func (m *pipelineFixtureWorkflowModule) ActionRegistry() ActionRegistry { return m.actionRegistry }
