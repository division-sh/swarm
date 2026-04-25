package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/core/paths"
	"swarm/internal/runtime/semanticview"
)

func TestCreateFlowInstanceResolvesInstanceIDFromPayloadPath(t *testing.T) {
	var captured FlowInstanceActivationRequest
	pc := &PipelineCoordinator{
		instanceActivator: func(_ context.Context, req FlowInstanceActivationRequest) error {
			captured = req
			return nil
		},
	}
	trigger := (events.Event{
		Type:    events.EventType("custom.triggered"),
		Payload: []byte(`{"entity_id":"ent-1","desired_instance_id":"inst-42","name":"alpha"}`),
	}).WithEntityID("ent-1")

	ok := pc.createFlowInstance(context.Background(), workflowTriggerContext{Event: trigger}, handlerExecutionPlan{
		Template:       "review",
		InstanceIDFrom: "payload.desired_instance_id",
		InstanceIDPath: paths.Parse("payload.desired_instance_id"),
		ConfigFrom: &runtimecontracts.ConfigFromSpec{
			Bindings: map[string]string{
				"name": "payload.name",
			},
		},
	})
	if ok != nil {
		t.Fatalf("expected createFlowInstance to succeed: %v", ok)
	}
	if captured.Instance.InstanceID != "inst-42" {
		t.Fatalf("instance id = %q, want inst-42", captured.Instance.InstanceID)
	}
	if captured.Instance.InstancePath != "review/inst-42" {
		t.Fatalf("instance path = %q, want review/inst-42", captured.Instance.InstancePath)
	}
	if captured.Instance.EntityID != FlowInstanceEntityID("review/inst-42") {
		t.Fatalf("entity id = %q, want %q", captured.Instance.EntityID, FlowInstanceEntityID("review/inst-42"))
	}
}

func TestCreateFlowInstanceResolvesConfigFromBindings(t *testing.T) {
	var captured FlowInstanceActivationRequest
	pc := &PipelineCoordinator{
		instanceActivator: func(_ context.Context, req FlowInstanceActivationRequest) error {
			captured = req
			return nil
		},
	}
	trigger := (events.Event{
		Type:    events.EventType("spawn.requested"),
		Payload: []byte(`{"entity_id":"ent-1","instance_id":"inst-42","name":"alpha","priority":1}`),
	}).WithEntityID("ent-1")

	err := pc.createFlowInstance(context.Background(), workflowTriggerContext{Event: trigger}, handlerExecutionPlan{
		Template:       "review",
		InstanceIDFrom: "payload.instance_id",
		InstanceIDPath: paths.Parse("payload.instance_id"),
		ConfigFrom: &runtimecontracts.ConfigFromSpec{
			Bindings: map[string]string{
				"name":     "payload.name",
				"priority": "payload.priority",
			},
		},
	})
	if err != nil {
		t.Fatalf("expected createFlowInstance to succeed: %v", err)
	}
	if captured.Config["name"] != "alpha" {
		t.Fatalf("config name = %#v, want alpha", captured.Config["name"])
	}
	if captured.Config["priority"] != float64(1) && captured.Config["priority"] != 1 {
		t.Fatalf("config priority = %#v, want 1", captured.Config["priority"])
	}
	if captured.Instance.SubjectID != "" {
		t.Fatalf("subject id = %q, want empty", captured.Instance.SubjectID)
	}
	if captured.Instance.ParentEntityID != "ent-1" {
		t.Fatalf("parent entity id = %q, want ent-1", captured.Instance.ParentEntityID)
	}
}

func TestCreateFlowInstanceRejectsMissingRequiredSiblingFields(t *testing.T) {
	pc := &PipelineCoordinator{
		instanceActivator: func(_ context.Context, req FlowInstanceActivationRequest) error {
			t.Fatalf("unexpected activation request: %#v", req)
			return nil
		},
	}
	trigger := (events.Event{
		Type:    events.EventType("spawn.requested"),
		Payload: []byte(`{"entity_id":"ent-1","instance_id":"inst-42","name":"alpha"}`),
	}).WithEntityID("ent-1")

	err := pc.createFlowInstance(context.Background(), workflowTriggerContext{Event: trigger}, handlerExecutionPlan{
		Template: "review",
	})
	if err == nil || !strings.Contains(err.Error(), "requires non-empty instance_id_from and config_from") {
		t.Fatalf("createFlowInstance error = %v, want missing required siblings", err)
	}
}

func TestCreateFlowInstanceRejectsGeneratedFallbackWithoutInstanceIDFrom(t *testing.T) {
	pc := &PipelineCoordinator{
		instanceActivator: func(_ context.Context, req FlowInstanceActivationRequest) error {
			t.Fatalf("unexpected activation request: %#v", req)
			return nil
		},
	}
	trigger := (events.Event{
		Type:    events.EventType("spawn.requested"),
		Payload: []byte(`{"entity_id":"ent-1","instance_id":"inst-42","name":"alpha"}`),
	}).WithEntityID("ent-1")

	err := pc.createFlowInstance(context.Background(), workflowTriggerContext{Event: trigger}, handlerExecutionPlan{
		Template: "review",
		ConfigFrom: &runtimecontracts.ConfigFromSpec{
			Bindings: map[string]string{
				"name": "payload.name",
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "requires non-empty instance_id_from and config_from") {
		t.Fatalf("createFlowInstance error = %v, want missing instance_id_from failure", err)
	}
}

func TestCreateFlowInstanceRejectsEmptyConfigFromBindings(t *testing.T) {
	pc := &PipelineCoordinator{
		instanceActivator: func(_ context.Context, req FlowInstanceActivationRequest) error {
			t.Fatalf("unexpected activation request: %#v", req)
			return nil
		},
	}
	trigger := (events.Event{
		Type:    events.EventType("spawn.requested"),
		Payload: []byte(`{"entity_id":"ent-1","desired_instance_id":"inst-42"}`),
	}).WithEntityID("ent-1")

	err := pc.createFlowInstance(context.Background(), workflowTriggerContext{Event: trigger}, handlerExecutionPlan{
		Template:       "review",
		InstanceIDFrom: "payload.desired_instance_id",
		InstanceIDPath: paths.Parse("payload.desired_instance_id"),
		ConfigFrom: &runtimecontracts.ConfigFromSpec{
			Bindings: map[string]string{},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "requires non-empty instance_id_from and config_from") {
		t.Fatalf("createFlowInstance error = %v, want missing config_from failure", err)
	}
}

func TestCreateFlowInstanceRejectsEmptyResolvedConfig(t *testing.T) {
	pc := &PipelineCoordinator{
		instanceActivator: func(_ context.Context, req FlowInstanceActivationRequest) error {
			t.Fatalf("unexpected activation request: %#v", req)
			return nil
		},
	}
	trigger := (events.Event{
		Type:    events.EventType("spawn.requested"),
		Payload: []byte(`{"entity_id":"ent-1","desired_instance_id":"inst-42"}`),
	}).WithEntityID("ent-1")

	err := pc.createFlowInstance(context.Background(), workflowTriggerContext{Event: trigger}, handlerExecutionPlan{
		Template:       "review",
		InstanceIDFrom: "payload.desired_instance_id",
		InstanceIDPath: paths.Parse("payload.desired_instance_id"),
		ConfigFrom: &runtimecontracts.ConfigFromSpec{
			Bindings: map[string]string{
				"name": "payload.missing_name",
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "config_from resolved empty") {
		t.Fatalf("createFlowInstance error = %v, want empty resolved config failure", err)
	}
}

func TestHandlerEmitEnvelope_KeepsLocalEntityAcrossOutputBoundaries(t *testing.T) {
	bundle := loadWorkflowFixtureBundle(t, "test-child-flow-local-events")
	module, err := newPipelineFixtureWorkflowModule(bundle)
	if err != nil {
		t.Fatalf("newPipelineFixtureWorkflowModule: %v", err)
	}
	pc := &PipelineCoordinator{module: module}
	trigger := workflowTriggerContext{
		Event: events.Event{
			Type:    events.EventType("child/child.start"),
			Payload: []byte(`{"entity_id":"ent-child"}`),
		}.WithEntityID("ent-child"),
		State: WorkflowState{
			EntityID: "ent-child",
			Metadata: map[string]any{
				"flow_path":        "child/inst-1",
				"subject_id":       "ent-parent",
				"parent_entity_id": "ent-parent",
			},
		},
	}

	internalPayload := pc.handlerEmitEnvelope(withPipelineFlowScope(context.Background(), "child"), trigger, "child/child.internal")
	if got := asString(internalPayload["entity_id"]); got != "ent-child" {
		t.Fatalf("internal payload entity_id = %q, want ent-child", got)
	}

	outputPayload := pc.handlerEmitEnvelope(withPipelineFlowScope(context.Background(), "child"), trigger, "child/child.done")
	if got := asString(outputPayload["entity_id"]); got != "ent-child" {
		t.Fatalf("output payload entity_id = %q, want ent-child", got)
	}

	pinBundle := loadWorkflowFixtureBundle(t, "test-child-flow-pin-wiring")
	pinModule, err := newPipelineFixtureWorkflowModule(pinBundle)
	if err != nil {
		t.Fatalf("newPipelineFixtureWorkflowModule(pin wiring): %v", err)
	}
	pinPC := &PipelineCoordinator{module: pinModule}
	pinPayload := pinPC.handlerEmitEnvelope(withPipelineFlowScope(context.Background(), "child"), trigger, "child/work.completed")
	if got := asString(pinPayload["entity_id"]); got != "ent-child" {
		t.Fatalf("pin output payload entity_id = %q, want ent-child", got)
	}
}

func TestHandlerEmitEnvelope_RootFlowOutputUsesLocalEntity(t *testing.T) {
	source := loadWorkflowTempSource(t, map[string]string{
		"package.yaml": `name: test
version: 1.0.0
flows:
  - id: scoring
    flow: scoring
    mode: static
`,
		"flows/scoring/schema.yaml": `name: scoring
pins:
  outputs:
    events:
      - scoring.requested
`,
		"flows/scoring/nodes.yaml": `scoring-node:
  id: scoring-node
  execution_type: system_node
  event_handlers: {}
`,
	})
	pc := &PipelineCoordinator{module: staticSemanticWorkflowModule{source: source}}
	trigger := workflowTriggerContext{
		Event: events.Event{
			Type:    events.EventType("vertical.discovered"),
			Payload: []byte(`{"entity_id":"ent-root"}`),
		}.WithEntityID("ent-root"),
		State: WorkflowState{
			EntityID: "ent-child",
			Metadata: map[string]any{
				"subject_id":       "ent-root",
				"parent_entity_id": "ent-root",
			},
		},
	}

	payload := pc.handlerEmitEnvelope(withPipelineFlowScope(context.Background(), "scoring"), trigger, "scoring/scoring.requested")
	if got := asString(payload["entity_id"]); got != "ent-child" {
		t.Fatalf("root flow output payload entity_id = %q, want ent-child", got)
	}
}

func loadWorkflowFixtureSource(t *testing.T, fixture string) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(loadWorkflowFixtureBundle(t, fixture))
}

func loadWorkflowFixtureBundle(t *testing.T, fixture string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", fixture)
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load fixture bundle: %v", err)
	}
	return bundle
}

func loadWorkflowTempSource(t *testing.T, files map[string]string) semanticview.Source {
	t.Helper()
	root := t.TempDir()
	for rel, body := range files {
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", path, err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, platformSpec)
	if err != nil {
		t.Fatalf("load temp bundle: %v", err)
	}
	return semanticview.Wrap(bundle)
}

type staticSemanticWorkflowModule struct {
	source semanticview.Source
}

func (m staticSemanticWorkflowModule) SemanticSource() semanticview.Source   { return m.source }
func (staticSemanticWorkflowModule) WorkflowDefinition() *WorkflowDefinition { return nil }
func (staticSemanticWorkflowModule) WorkflowNodes() []WorkflowNode           { return nil }
func (staticSemanticWorkflowModule) GuardRegistry() GuardRegistry            { return nil }
func (staticSemanticWorkflowModule) ActionRegistry() ActionRegistry          { return nil }
