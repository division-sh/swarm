package pipeline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/core/paths"
	"swarm/internal/runtime/core/values"
	"swarm/internal/runtime/semanticview"
)

func testCreateFlowInstanceContext(trigger workflowTriggerContext) values.Context {
	payload := parsePayloadMap(trigger.Event.Payload)
	entity := map[string]any{
		"entity_id": workflowEventEntityID(trigger.Event),
	}
	return createFlowInstanceHandlerContext(trigger, payload, entity)
}

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
	triggerCtx := workflowTriggerContext{Event: trigger}

	ok := pc.createFlowInstance(context.Background(), triggerCtx, handlerExecutionPlan{
		Template:       "review",
		InstanceIDFrom: "payload.desired_instance_id",
		InstanceIDPath: paths.Parse("payload.desired_instance_id"),
		ConfigFrom: &runtimecontracts.ConfigFromSpec{
			Bindings: map[string]string{
				"name": "payload.name",
			},
		},
	}, testCreateFlowInstanceContext(triggerCtx))
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
	triggerCtx := workflowTriggerContext{Event: trigger}

	err := pc.createFlowInstance(context.Background(), triggerCtx, handlerExecutionPlan{
		Template:       "review",
		InstanceIDFrom: "payload.instance_id",
		InstanceIDPath: paths.Parse("payload.instance_id"),
		ConfigFrom: &runtimecontracts.ConfigFromSpec{
			Bindings: map[string]string{
				"name":     "payload.name",
				"priority": "payload.priority",
			},
		},
	}, testCreateFlowInstanceContext(triggerCtx))
	if err != nil {
		t.Fatalf("expected createFlowInstance to succeed: %v", err)
	}
	if captured.Config["name"] != "alpha" {
		t.Fatalf("config name = %#v, want alpha", captured.Config["name"])
	}
	if captured.Config["priority"] != float64(1) && captured.Config["priority"] != 1 {
		t.Fatalf("config priority = %#v, want 1", captured.Config["priority"])
	}
	if captured.Instance.ParentEntityID != "ent-1" {
		t.Fatalf("parent entity id = %q, want ent-1", captured.Instance.ParentEntityID)
	}
}

func TestCreateFlowInstanceResolvesConfigFromHandlerEventContext(t *testing.T) {
	var captured FlowInstanceActivationRequest
	pc := &PipelineCoordinator{
		instanceActivator: func(_ context.Context, req FlowInstanceActivationRequest) error {
			captured = req
			return nil
		},
	}
	trigger := (events.Event{
		ID:            "evt-123",
		Type:          events.EventType("spawn.requested"),
		ParentEventID: "source-evt-1",
		Payload:       []byte(`{"entity_id":"ent-1","instance_id":"inst-42","name":"alpha"}`),
	}).WithEnvelope(events.EventEnvelope{
		EntityID: "ent-1",
		Source: events.RouteIdentity{
			FlowID:       "parent-flow",
			FlowInstance: "parent-flow/source-1",
			EntityID:     "ent-parent",
		},
	})
	triggerCtx := workflowTriggerContext{Event: trigger}

	err := pc.createFlowInstance(context.Background(), triggerCtx, handlerExecutionPlan{
		Template:       "review",
		InstanceIDFrom: "payload.instance_id",
		InstanceIDPath: paths.Parse("payload.instance_id"),
		ConfigFrom: &runtimecontracts.ConfigFromSpec{
			Bindings: map[string]string{
				"source_event_id": "event.id",
				"event_type":      "event.type",
				"source_flow":     "event.source.flow_id",
				"correlation_id":  "event.source_event_id",
				"name":            "payload.name",
				"parent_entity":   "entity.entity_id",
			},
		},
	}, testCreateFlowInstanceContext(triggerCtx))
	if err != nil {
		t.Fatalf("expected createFlowInstance to succeed: %v", err)
	}
	for key, want := range map[string]any{
		"source_event_id": "evt-123",
		"event_type":      "spawn.requested",
		"source_flow":     "parent-flow",
		"correlation_id":  "source-evt-1",
		"name":            "alpha",
		"parent_entity":   "ent-1",
	} {
		if got := captured.Config[key]; got != want {
			t.Fatalf("config[%s] = %#v, want %#v", key, got, want)
		}
	}
}

func TestCreateFlowInstanceRejectsUnknownEventConfigRef(t *testing.T) {
	pc := &PipelineCoordinator{
		instanceActivator: func(_ context.Context, req FlowInstanceActivationRequest) error {
			t.Fatalf("unexpected activation request: %#v", req)
			return nil
		},
	}
	trigger := (events.Event{
		ID:      "evt-123",
		Type:    events.EventType("spawn.requested"),
		Payload: []byte(`{"entity_id":"ent-1","instance_id":"inst-42"}`),
	}).WithEntityID("ent-1")
	triggerCtx := workflowTriggerContext{Event: trigger}

	err := pc.createFlowInstance(context.Background(), triggerCtx, handlerExecutionPlan{
		Template:       "review",
		InstanceIDFrom: "payload.instance_id",
		InstanceIDPath: paths.Parse("payload.instance_id"),
		ConfigFrom: &runtimecontracts.ConfigFromSpec{
			Bindings: map[string]string{
				"missing_event": "event.missing",
			},
		},
	}, testCreateFlowInstanceContext(triggerCtx))
	var refErr flowInstanceConfigRefError
	if !errors.As(err, &refErr) {
		t.Fatalf("createFlowInstance error = %T %v, want flowInstanceConfigRefError", err, err)
	}
	for _, want := range []string{`config_from "missing_event"`, `ref "event.missing"`, "resolved empty"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("createFlowInstance error = %v, want %q", err, want)
		}
	}
}

func TestCreateFlowInstanceRejectsUnsupportedConfigRefRoot(t *testing.T) {
	pc := &PipelineCoordinator{
		instanceActivator: func(_ context.Context, req FlowInstanceActivationRequest) error {
			t.Fatalf("unexpected activation request: %#v", req)
			return nil
		},
	}
	trigger := (events.Event{
		ID:      "evt-123",
		Type:    events.EventType("spawn.requested"),
		Payload: []byte(`{"entity_id":"ent-1","instance_id":"inst-42"}`),
	}).WithEntityID("ent-1")
	triggerCtx := workflowTriggerContext{Event: trigger}

	err := pc.createFlowInstance(context.Background(), triggerCtx, handlerExecutionPlan{
		Template:       "review",
		InstanceIDFrom: "payload.instance_id",
		InstanceIDPath: paths.Parse("payload.instance_id"),
		ConfigFrom: &runtimecontracts.ConfigFromSpec{
			Bindings: map[string]string{
				"policy_value": "policy.value",
			},
		},
	}, testCreateFlowInstanceContext(triggerCtx))
	var refErr flowInstanceConfigRefError
	if !errors.As(err, &refErr) {
		t.Fatalf("createFlowInstance error = %T %v, want flowInstanceConfigRefError", err, err)
	}
	for _, want := range []string{`config_from "policy_value"`, `ref "policy.value"`, `unsupported root "policy"`} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("createFlowInstance error = %v, want %q", err, want)
		}
	}
}

func TestCreateFlowInstanceDoesNotResolveInstanceIDFromEventRef(t *testing.T) {
	pc := &PipelineCoordinator{
		instanceActivator: func(_ context.Context, req FlowInstanceActivationRequest) error {
			t.Fatalf("unexpected activation request: %#v", req)
			return nil
		},
	}
	trigger := (events.Event{
		ID:      "evt-123",
		Type:    events.EventType("spawn.requested"),
		Payload: []byte(`{"entity_id":"ent-1","name":"alpha"}`),
	}).WithEntityID("ent-1")
	triggerCtx := workflowTriggerContext{Event: trigger}

	err := pc.createFlowInstance(context.Background(), triggerCtx, handlerExecutionPlan{
		Template:       "review",
		InstanceIDFrom: "event.id",
		InstanceIDPath: paths.Parse("event.id"),
		ConfigFrom: &runtimecontracts.ConfigFromSpec{
			Bindings: map[string]string{
				"name": "payload.name",
			},
		},
	}, testCreateFlowInstanceContext(triggerCtx))
	if err == nil || !strings.Contains(err.Error(), "create_flow_instance instance_id_from resolved empty") {
		t.Fatalf("createFlowInstance error = %v, want instance_id_from split behavior", err)
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
	triggerCtx := workflowTriggerContext{Event: trigger}

	err := pc.createFlowInstance(context.Background(), triggerCtx, handlerExecutionPlan{
		Template: "review",
	}, testCreateFlowInstanceContext(triggerCtx))
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
	triggerCtx := workflowTriggerContext{Event: trigger}

	err := pc.createFlowInstance(context.Background(), triggerCtx, handlerExecutionPlan{
		Template: "review",
		ConfigFrom: &runtimecontracts.ConfigFromSpec{
			Bindings: map[string]string{
				"name": "payload.name",
			},
		},
	}, testCreateFlowInstanceContext(triggerCtx))
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
	triggerCtx := workflowTriggerContext{Event: trigger}

	err := pc.createFlowInstance(context.Background(), triggerCtx, handlerExecutionPlan{
		Template:       "review",
		InstanceIDFrom: "payload.desired_instance_id",
		InstanceIDPath: paths.Parse("payload.desired_instance_id"),
		ConfigFrom: &runtimecontracts.ConfigFromSpec{
			Bindings: map[string]string{},
		},
	}, testCreateFlowInstanceContext(triggerCtx))
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
	triggerCtx := workflowTriggerContext{Event: trigger}

	err := pc.createFlowInstance(context.Background(), triggerCtx, handlerExecutionPlan{
		Template:       "review",
		InstanceIDFrom: "payload.desired_instance_id",
		InstanceIDPath: paths.Parse("payload.desired_instance_id"),
		ConfigFrom: &runtimecontracts.ConfigFromSpec{
			Bindings: map[string]string{
				"name": "payload.missing_name",
			},
		},
	}, testCreateFlowInstanceContext(triggerCtx))
	if err == nil || !strings.Contains(err.Error(), `config_from "name" ref "payload.missing_name" resolved empty`) {
		t.Fatalf("createFlowInstance error = %v, want missing config ref failure", err)
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
				"parent_entity_id": "ent-root",
			},
		},
	}

	payload := pc.handlerEmitEnvelope(withPipelineFlowScope(context.Background(), "scoring"), trigger, "scoring/scoring.requested")
	if got := asString(payload["entity_id"]); got != "ent-child" {
		t.Fatalf("root flow output payload entity_id = %q, want ent-child", got)
	}
}

func TestTemplateInstanceSystemNodeDeliveryLocalizesConcreteEventToHandlerKey(t *testing.T) {
	source := loadWorkflowTempSource(t, map[string]string{
		"package.yaml": `name: test
version: 1.0.0
flows:
  - id: operating
    flow: operating
    mode: template
`,
		"flows/operating/schema.yaml": `name: operating
initial_state: initializing
terminal_states: [ready]
states: [initializing, ready]
auto_emit_on_create:
  event: opco.product_initialization_requested
`,
		"flows/operating/events.yaml": `opco.product_initialization_requested:
  entity_id: string
opco.ceo_ready:
  entity_id: string
`,
		"flows/operating/nodes.yaml": `lifecycle-orchestrator:
  id: lifecycle-orchestrator
  execution_type: system_node
  subscribes_to: [opco.product_initialization_requested]
  produces: [opco.ceo_ready]
  event_handlers:
    opco.product_initialization_requested:
      emit: opco.ceo_ready
`,
	})
	evt := (events.Event{
		Type:    events.EventType("operating/inst-1/opco.product_initialization_requested"),
		Payload: []byte(`{"entity_id":"ent-operating"}`),
	}).WithEntityID("ent-operating").WithFlowInstance("operating/inst-1")
	if _, ok := source.NodeEventHandler("lifecycle-orchestrator", string(evt.Type)); ok {
		t.Fatal("raw bundle handler lookup unexpectedly matched concrete instance event without delivery localization")
	}
	if _, ok := workflowNodeEventHandlerForDelivery(source, "lifecycle-orchestrator", evt); !ok {
		t.Fatal("expected concrete instance event to resolve to local lifecycle-orchestrator handler")
	}

	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{
		bus:            bus,
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
		module:         staticSemanticWorkflowModule{source: source},
	}
	handled, err := pc.executeNodeHandlerPlanResult(context.Background(), "lifecycle-orchestrator", evt)
	if err != nil {
		t.Fatalf("executeNodeHandlerPlanResult: %v", err)
	}
	if !handled {
		t.Fatal("executeNodeHandlerPlanResult handled = false, want true")
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("published count = %d, want 1", got)
	}
	if got := string(bus.publishedEvent(0).Type); got != "operating/opco.ceo_ready" {
		t.Fatalf("published event type = %q, want operating/opco.ceo_ready", got)
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
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load fixture bundle: %v", err)
	}
	return bundle
}

func loadWorkflowTempSource(t *testing.T, files map[string]string) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(loadWorkflowTempBundle(t, files))
}

func loadWorkflowTempBundle(t *testing.T, files map[string]string) *runtimecontracts.WorkflowContractBundle {
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
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, platformSpec)
	if err != nil {
		t.Fatalf("load temp bundle: %v", err)
	}
	return bundle
}

type staticSemanticWorkflowModule struct {
	source semanticview.Source
}

func (m staticSemanticWorkflowModule) SemanticSource() semanticview.Source   { return m.source }
func (staticSemanticWorkflowModule) WorkflowDefinition() *WorkflowDefinition { return nil }
func (staticSemanticWorkflowModule) WorkflowNodes() []WorkflowNode           { return nil }
func (staticSemanticWorkflowModule) GuardRegistry() GuardRegistry            { return nil }
func (staticSemanticWorkflowModule) ActionRegistry() ActionRegistry          { return nil }
