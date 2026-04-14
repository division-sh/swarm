package pipeline

import (
	"context"
	"os"
	"path/filepath"
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
		Payload: []byte(`{"entity_id":"ent-1","desired_instance_id":"inst-42"}`),
	}).WithEntityID("ent-1")

	ok := pc.createFlowInstance(context.Background(), workflowTriggerContext{Event: trigger}, handlerExecutionPlan{
		Template:       "review",
		InstanceIDFrom: "payload.desired_instance_id",
		InstanceIDPath: paths.Parse("payload.desired_instance_id"),
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
	if captured.Instance.SubjectID != "ent-1" {
		t.Fatalf("subject id = %q, want ent-1", captured.Instance.SubjectID)
	}
}

func TestWorkflowEmitTargetsParentEntity_OutputPin(t *testing.T) {
	source := loadWorkflowFixtureSource(t, "test-child-flow-local-events")
	if !workflowEmitTargetsParentEntity(source, "child", "child/child.done") {
		t.Fatal("expected child/child.done to target the parent entity")
	}
	if !workflowEmitTargetsParentEntity(source, "child", "child.done") {
		t.Fatal("expected local child.done to target the parent entity through the same canonical proof")
	}
	if workflowEmitTargetsParentEntity(source, "child", "child/child.internal") {
		t.Fatal("did not expect child/child.internal to target the parent entity")
	}
	if workflowEmitTargetsParentEntity(source, "child", "child.internal") {
		t.Fatal("did not expect local child.internal to target the parent entity")
	}

	pinSource := loadWorkflowFixtureSource(t, "test-child-flow-pin-wiring")
	if !workflowEmitTargetsParentEntity(pinSource, "child", "child/work.completed") {
		t.Fatal("expected child/work.completed to target the parent entity")
	}
	if !workflowEmitTargetsParentEntity(pinSource, "child", "work.completed") {
		t.Fatal("expected local work.completed to target the parent entity through the same canonical proof")
	}
}

func TestWorkflowEmitTargetsParentEntity_DoesNotScanOtherFlows(t *testing.T) {
	source := loadWorkflowFixtureSource(t, "test-child-flow-sibling-isolation")
	if workflowEmitTargetsParentEntity(source, "flow-a", "flow-b/work.done") {
		t.Fatal("did not expect flow-b output event to retarget when executing inside flow-a")
	}
	if workflowEmitTargetsParentEntity(source, "", "flow-a/work.done") {
		t.Fatal("did not expect root/no-flow context to retarget from a child flow output declaration")
	}
}

func TestHandlerEmitPayload_UsesParentEntityForOutputPinsAndLocalEntityForInternalEvents(t *testing.T) {
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

	internalPayload := pc.handlerEmitPayload(withPipelineFlowScope(context.Background(), "child"), trigger, "child/child.internal")
	if got := asString(internalPayload["entity_id"]); got != "ent-child" {
		t.Fatalf("internal payload entity_id = %q, want ent-child", got)
	}

	outputPayload := pc.handlerEmitPayload(withPipelineFlowScope(context.Background(), "child"), trigger, "child/child.done")
	if got := asString(outputPayload["entity_id"]); got != "ent-parent" {
		t.Fatalf("output payload entity_id = %q, want ent-parent", got)
	}

	pinBundle := loadWorkflowFixtureBundle(t, "test-child-flow-pin-wiring")
	pinModule, err := newPipelineFixtureWorkflowModule(pinBundle)
	if err != nil {
		t.Fatalf("newPipelineFixtureWorkflowModule(pin wiring): %v", err)
	}
	pinPC := &PipelineCoordinator{module: pinModule}
	pinPayload := pinPC.handlerEmitPayload(withPipelineFlowScope(context.Background(), "child"), trigger, "child/work.completed")
	if got := asString(pinPayload["entity_id"]); got != "ent-parent" {
		t.Fatalf("pin output payload entity_id = %q, want ent-parent", got)
	}
}

func TestHandlerEmitPayload_RootFlowOutputUsesLocalEntity(t *testing.T) {
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

	payload := pc.handlerEmitPayload(withPipelineFlowScope(context.Background(), "scoring"), trigger, "scoring/scoring.requested")
	if got := asString(payload["entity_id"]); got != "ent-child" {
		t.Fatalf("root flow output payload entity_id = %q, want ent-child", got)
	}
}

func TestWorkflowEmitTargetsParentEntity_ValidationOutputsSplitTargetEntityAndBackprop(t *testing.T) {
	source := loadValidationOutputBoundarySource(t)
	for _, eventType := range []string{
		"validation/validation.started",
		"validation/validation.package_ready",
		"validation/brand.requested",
		"validation/cto.spec_review_requested",
		"validation/spec.revision_requested",
	} {
		if workflowEmitTargetsParentEntity(source, "validation", eventType) {
			t.Fatalf("did not expect %s to retarget to the parent entity", eventType)
		}
	}
	if !workflowEmitTargetsParentEntity(source, "validation", "validation/vertical.killed_backprop") {
		t.Fatal("expected validation/vertical.killed_backprop to remain parent-targeted")
	}
}

func TestWorkflowEmitTargetsParentEntity_ScoringOutputsRemainParentTargetedDespiteSameFlowConsumers(t *testing.T) {
	source := loadScoringSameFlowConsumerOutputSource(t)
	for _, eventType := range []string{
		"scoring/scoring.requested",
		"scoring/scoring.derived_ready",
	} {
		if !workflowEmitTargetsParentEntity(source, "scoring", eventType) {
			t.Fatalf("expected %s to remain parent-targeted outside the validation-only child", eventType)
		}
	}
}

func loadValidationOutputBoundarySource(t *testing.T) semanticview.Source {
	t.Helper()
	return loadWorkflowTempSource(t, map[string]string{
		"package.yaml": `name: test
version: 1.0.0
flows:
  - id: validation
    flow: validation
    mode: template
`,
		"flows/validation/schema.yaml": `name: validation
pins:
  inputs:
    events:
      - vertical.shortlisted
      - vertical.resumed
  outputs:
    events:
      - brand.requested
      - cto.spec_review_requested
      - spec.revision_requested
      - validation.package_ready
      - validation.started
      - vertical.killed_backprop
required_agents:
  - role: validation-coordinator
    subscribes_to:
      - validation.package_ready
  - role: business-research-agent
    subscribes_to:
      - validation.started
  - role: factory-cto
    subscribes_to:
      - cto.spec_review_requested
  - role: pre-brand-agent
    subscribes_to:
      - brand.requested
`,
		"flows/validation/events.yaml": `brand.requested:
  payload:
    entity_id: string
cto.spec_review_requested:
  payload:
    entity_id: string
spec.requested:
  payload:
    entity_id: string
spec.revision_requested:
  payload:
    entity_id: string
validation.package_ready:
  payload:
    entity_id: string
validation.started:
  payload:
    entity_id: string
    source_scoring_entity_id: string
vertical.killed_backprop:
  payload:
    entity_id: string
    vertical_id: string
vertical.shortlisted:
  payload:
    entity_id: string
vertical.resumed:
  payload:
    entity_id: string
`,
		"flows/validation/nodes.yaml": `validation-orchestrator:
  id: validation-orchestrator
  execution_type: system_node
  subscribes_to:
    - spec.revision_requested
  event_handlers:
    spec.revision_requested:
      emits: spec.requested
    research.vertical_rejected:
      emits:
        - vertical.killed_backprop
`,
	})
}

func loadScoringSameFlowConsumerOutputSource(t *testing.T) semanticview.Source {
	t.Helper()
	return loadWorkflowTempSource(t, map[string]string{
		"package.yaml": `name: test
version: 1.0.0
flows:
  - id: scoring
    flow: scoring
    mode: template
`,
		"flows/scoring/schema.yaml": `name: scoring
pins:
  inputs:
    events:
      - vertical.discovered
  outputs:
    events:
      - scoring.requested
      - scoring.derived_ready
required_agents:
  - role: scorer
    subscribes_to:
      - scoring.requested
  - role: scoring-followup
    subscribes_to:
      - scoring.derived_ready
`,
		"flows/scoring/events.yaml": `scoring.requested:
  payload:
    entity_id: string
scoring.derived_ready:
  payload:
    entity_id: string
vertical.discovered:
  payload:
    entity_id: string
`,
		"flows/scoring/nodes.yaml": `scoring-orchestrator:
  id: scoring-orchestrator
  execution_type: system_node
  subscribes_to:
    - scoring.requested
  event_handlers:
    scoring.requested:
      emits:
        - scoring.derived_ready
`,
	})
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
