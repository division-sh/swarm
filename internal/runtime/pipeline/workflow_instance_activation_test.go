package pipeline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/core/values"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	"time"
)

func testCreateFlowInstanceContext(trigger workflowTriggerContext) values.Context {
	payload := parsePayloadMap(trigger.Event.Payload())
	entity := map[string]any{
		"entity_id": workflowEventEntityID(trigger.Event),
	}
	ctx := createFlowInstanceHandlerContext(trigger, payload, entity)
	ctx.PlatformEntity = values.Wrap(map[string]any{"id": workflowEventEntityID(trigger.Event)})
	return ctx
}

func TestCreateFlowInstanceResolvesInstanceIDFromPayloadPath(t *testing.T) {
	var captured FlowInstanceActivationRequest
	pc := &PipelineCoordinator{
		instanceActivator: func(_ context.Context, req FlowInstanceActivationRequest) error {
			captured = req
			return nil
		},
	}
	trigger := eventtest.RootIngress(
		"",
		events.EventType("custom.triggered"),
		testPipelineRunID,
		"",
		[]byte(`{"entity_id":"ent-1","desired_instance_id":"inst-42","name":"alpha"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Time{},
	)
	triggerCtx := workflowTriggerContext{Event: trigger}

	ok := pc.createFlowInstance(testAuthorActivityContext(context.Background()), triggerCtx, handlerExecutionPlan{
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

func TestCreateFlowInstanceArmsInitialStageTimersWithSQLiteStore(t *testing.T) {
	runID := uuid.NewString()
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	store := newSQLiteWorkflowInstanceStoreForTest(t, db)
	ensurePipelineTestRun(t, store, runID)
	source := semanticview.Wrap(stageTimerTemplateLifecycleBundle())
	pc := &PipelineCoordinator{
		module:        &pipelineFixtureWorkflowModule{source: source},
		workflowStore: store,
		instanceActivator: func(ctx context.Context, req FlowInstanceActivationRequest) error {
			return store.Upsert(ctx, WorkflowInstance{
				InstanceID:      req.Instance.InstanceID,
				StorageRef:      req.Instance.InstancePath,
				WorkflowName:    req.Instance.TemplateID,
				WorkflowVersion: "1.0.0",
				CurrentState:    "awaiting_review",
				Metadata: map[string]any{
					"entity_id":        req.Instance.EntityID,
					"instance_id":      req.Instance.InstanceID,
					"flow_path":        req.Instance.InstancePath,
					"parent_entity_id": req.Instance.ParentEntityID,
				},
			})
		},
	}
	pc.workflowTimers = newWorkflowTimerLifecycle(pc)
	trigger := eventtest.RootIngress(
		"",
		events.EventType("spawn.requested"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1","instance_id":"inst-42","name":"alpha"}`),
		0,
		runID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Time{},
	)
	triggerCtx := workflowTriggerContext{Event: trigger}
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	beforeCreate := time.Now().UTC()

	err := pc.createFlowInstance(ctx, triggerCtx, handlerExecutionPlan{
		Template:       "review",
		InstanceIDFrom: "payload.instance_id",
		InstanceIDPath: paths.Parse("payload.instance_id"),
		ConfigFrom: &runtimecontracts.ConfigFromSpec{
			Bindings: map[string]string{"name": "payload.name"},
		},
	}, testCreateFlowInstanceContext(triggerCtx))
	if err != nil {
		t.Fatalf("createFlowInstance: %v", err)
	}
	entityID := FlowInstanceEntityID("review/inst-42")
	activations, err := store.listWorkflowTimerActivations(ctx, runID, entityID, true)
	if err != nil {
		t.Fatalf("list workflow timer activations: %v", err)
	}
	if len(activations) != 1 {
		t.Fatalf("workflow timer activations = %d, want 1: %#v", len(activations), activations)
	}
	activation := activations[0]
	if activation.Ref.Declaration != "review.awaiting_review.expired" {
		t.Fatalf("timer declaration = %q, want review.awaiting_review.expired", activation.Ref.Declaration)
	}
	scheduledAt := activation.FireAt
	if scheduledAt.Before(beforeCreate.Add(2*time.Hour)) || scheduledAt.After(time.Now().UTC().Add(2*time.Hour+time.Second)) {
		t.Fatalf("schedule At = %s, want child-flow policy rendered delay near 2h after create", scheduledAt)
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
	trigger := eventtest.RootIngress(
		"",
		events.EventType("spawn.requested"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1","instance_id":"inst-42","name":"alpha","priority":1}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Time{},
	)
	triggerCtx := workflowTriggerContext{Event: trigger}

	err := pc.createFlowInstance(testAuthorActivityContext(context.Background()), triggerCtx, handlerExecutionPlan{
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

func TestCreateFlowInstancePreservesNullConfigFromValues(t *testing.T) {
	var captured FlowInstanceActivationRequest
	pc := &PipelineCoordinator{
		instanceActivator: func(_ context.Context, req FlowInstanceActivationRequest) error {
			captured = req
			return nil
		},
	}
	trigger := eventtest.RootIngress(
		"",
		events.EventType("spawn.requested"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1","instance_id":"inst-42","optional_setting":null}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Time{},
	)
	triggerCtx := workflowTriggerContext{Event: trigger}

	err := pc.createFlowInstance(testAuthorActivityContext(context.Background()), triggerCtx, handlerExecutionPlan{
		Template:       "review",
		InstanceIDFrom: "payload.instance_id",
		InstanceIDPath: paths.Parse("payload.instance_id"),
		ConfigFrom: &runtimecontracts.ConfigFromSpec{
			Bindings: map[string]string{
				"optional_setting": "payload.optional_setting",
				"bare_optional":    "optional_setting",
			},
		},
	}, testCreateFlowInstanceContext(triggerCtx))
	if err != nil {
		t.Fatalf("expected createFlowInstance to preserve explicit null config values: %v", err)
	}
	for _, key := range []string{"optional_setting", "bare_optional"} {
		value, ok := captured.Config[key]
		if !ok {
			t.Fatalf("config[%s] missing; want explicit nil value", key)
		}
		if value != nil {
			t.Fatalf("config[%s] = %#v, want nil", key, value)
		}
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
	trigger := eventtest.ChildWithLineage(
		"evt-123",
		events.EventType("spawn.requested"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1","instance_id":"inst-42","name":"alpha"}`),
		0,
		events.EventLineage{RunID: testPipelineRunID, ParentEventID: "source-evt-1", ExecutionMode: executionmode.Live},
		events.EventEnvelope{
			EntityID: "ent-1",
			Source: events.RouteIdentity{
				FlowID:       "parent-flow",
				FlowInstance: "parent-flow/source-1",
				EntityID:     "ent-parent",
			},
		},
		time.Time{},
	)

	triggerCtx := workflowTriggerContext{Event: trigger}

	err := pc.createFlowInstance(testAuthorActivityContext(context.Background()), triggerCtx, handlerExecutionPlan{
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
				"parent_entity":   "_entity.id",
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
	trigger := eventtest.RootIngress(
		"evt-123",
		events.EventType("spawn.requested"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1","instance_id":"inst-42"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Time{},
	)

	triggerCtx := workflowTriggerContext{Event: trigger}

	err := pc.createFlowInstance(testAuthorActivityContext(context.Background()), triggerCtx, handlerExecutionPlan{
		Template:       "review",
		InstanceIDFrom: "payload.instance_id",
		InstanceIDPath: paths.Parse("payload.instance_id"),
		ConfigFrom: &runtimecontracts.ConfigFromSpec{
			Bindings: map[string]string{
				"missing_payload": "payload.missing",
			},
		},
	}, testCreateFlowInstanceContext(triggerCtx))
	var refErr flowInstanceConfigRefError
	if !errors.As(err, &refErr) {
		t.Fatalf("createFlowInstance error = %T %v, want flowInstanceConfigRefError", err, err)
	}
	for _, want := range []string{`config_from "missing_payload"`, `ref "payload.missing"`, "resolved empty"} {
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
	trigger := eventtest.RootIngress(
		"evt-123",
		events.EventType("spawn.requested"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1","instance_id":"inst-42"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Time{},
	)

	triggerCtx := workflowTriggerContext{Event: trigger}

	err := pc.createFlowInstance(testAuthorActivityContext(context.Background()), triggerCtx, handlerExecutionPlan{
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
	trigger := eventtest.RootIngress(
		"evt-123",
		events.EventType("spawn.requested"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1","name":"alpha"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Time{},
	)

	triggerCtx := workflowTriggerContext{Event: trigger}

	err := pc.createFlowInstance(testAuthorActivityContext(context.Background()), triggerCtx, handlerExecutionPlan{
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
	trigger := eventtest.RootIngress(
		"",
		events.EventType("spawn.requested"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1","instance_id":"inst-42","name":"alpha"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Time{},
	)
	triggerCtx := workflowTriggerContext{Event: trigger}

	err := pc.createFlowInstance(testAuthorActivityContext(context.Background()), triggerCtx, handlerExecutionPlan{
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
	trigger := eventtest.RootIngress(
		"",
		events.EventType("spawn.requested"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1","instance_id":"inst-42","name":"alpha"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Time{},
	)
	triggerCtx := workflowTriggerContext{Event: trigger}

	err := pc.createFlowInstance(testAuthorActivityContext(context.Background()), triggerCtx, handlerExecutionPlan{
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
	trigger := eventtest.RootIngress(
		"",
		events.EventType("spawn.requested"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1","desired_instance_id":"inst-42"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Time{},
	)
	triggerCtx := workflowTriggerContext{Event: trigger}

	err := pc.createFlowInstance(testAuthorActivityContext(context.Background()), triggerCtx, handlerExecutionPlan{
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
	trigger := eventtest.RootIngress(
		"",
		events.EventType("spawn.requested"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1","desired_instance_id":"inst-42"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Time{},
	)
	triggerCtx := workflowTriggerContext{Event: trigger}

	err := pc.createFlowInstance(testAuthorActivityContext(context.Background()), triggerCtx, handlerExecutionPlan{
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
		Event: eventtest.RootIngress(
			"",
			events.EventType("child/child.start"),
			"",
			"",
			[]byte(`{"entity_id":"ent-child"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-child"),
			time.Time{},
		),

		State: WorkflowState{
			EntityID: "ent-child",
			Metadata: map[string]any{
				"flow_path":        "child/inst-1",
				"parent_entity_id": "ent-parent",
			},
		},
	}

	internalPayload := pc.handlerEmitEnvelope(withPipelineFlowScope(testAuthorActivityContext(context.Background()), "child"), trigger, "child/child.internal")
	if got := asString(internalPayload["entity_id"]); got != "ent-child" {
		t.Fatalf("internal payload entity_id = %q, want ent-child", got)
	}

	outputPayload := pc.handlerEmitEnvelope(withPipelineFlowScope(testAuthorActivityContext(context.Background()), "child"), trigger, "child/child.done")
	if got := asString(outputPayload["entity_id"]); got != "ent-child" {
		t.Fatalf("output payload entity_id = %q, want ent-child", got)
	}

	pinBundle := loadWorkflowFixtureBundle(t, "test-child-flow-pin-wiring")
	pinModule, err := newPipelineFixtureWorkflowModule(pinBundle)
	if err != nil {
		t.Fatalf("newPipelineFixtureWorkflowModule(pin wiring): %v", err)
	}
	pinPC := &PipelineCoordinator{module: pinModule}
	pinPayload := pinPC.handlerEmitEnvelope(withPipelineFlowScope(testAuthorActivityContext(context.Background()), "child"), trigger, "child/work.completed")
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
		Event: eventtest.RootIngress(
			"",
			events.EventType("vertical.discovered"),
			"",
			"",
			[]byte(`{"entity_id":"ent-root"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-root"),
			time.Time{},
		),

		State: WorkflowState{
			EntityID: "ent-child",
			Metadata: map[string]any{
				"parent_entity_id": "ent-root",
			},
		},
	}

	payload := pc.handlerEmitEnvelope(withPipelineFlowScope(testAuthorActivityContext(context.Background()), "scoring"), trigger, "scoring/scoring.requested")
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
	evt := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("operating/inst-1/opco.product_initialization_requested"),
		"",
		"",
		[]byte(`{"entity_id":"ent-operating"}`),
		0,
		testPipelineRunID,
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-operating"), "operating/inst-1"),
		time.Time{},
	)

	if _, ok := source.NodeEventHandler("lifecycle-orchestrator", string(evt.Type())); ok {
		t.Fatal("raw bundle handler lookup unexpectedly matched concrete instance event without delivery localization")
	}
	resolved := workflowNodeEventHandlerResolutionForDelivery(source, "lifecycle-orchestrator", evt)
	if !resolved.Matched {
		t.Fatal("expected concrete instance event to resolve to local lifecycle-orchestrator handler")
	}
	if got := resolved.HandlerEventKey; got != "opco.product_initialization_requested" {
		t.Fatalf("handler event key = %q, want opco.product_initialization_requested", got)
	}

	_, db, _ := testutil.StartPostgres(t)
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{
		bus:            bus,
		db:             db,
		workflowStore:  NewWorkflowInstanceStore(db),
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
		module:         staticSemanticWorkflowModule{source: source},
	}
	seedPipelineNodeDeliveryAuthority(t, db, evt, "lifecycle-orchestrator")
	handled, err := pc.executeNodeHandlerPlanResult(testPipelineCoordinatorRunContext(t, pc), "lifecycle-orchestrator", evt)
	if err != nil {
		t.Fatalf("executeNodeHandlerPlanResult: %v", err)
	}
	if !handled {
		t.Fatal("executeNodeHandlerPlanResult handled = false, want true")
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("published count = %d, want 1", got)
	}
	if got := string(bus.publishedEvent(0).Type()); got != "operating/opco.ceo_ready" {
		t.Fatalf("published event type = %q, want operating/opco.ceo_ready", got)
	}
}

func TestTemplateInstanceRecordEvidenceUsesLocalizedHandlerEvidenceTarget(t *testing.T) {
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
`,
		"flows/operating/events.yaml": `build_progress:
  entity_id: string
  summary: string
`,
		"flows/operating/nodes.yaml": `build-orchestrator:
  id: build-orchestrator
  execution_type: system_node
  subscribes_to: [build_progress]
  event_handlers:
    build_progress:
      action: record_evidence
      evidence_target: build_evidence
`,
	})
	const entityID = "11111111-1111-1111-1111-111111111111"
	evt := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("operating/inst-1/build_progress"),
		"",
		"",
		mustJSON(map[string]any{"entity_id": entityID, "summary": "compile complete"}),
		0,
		"",
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), "operating/inst-1"),
		time.Time{},
	)

	if _, ok := source.NodeEventHandler("build-orchestrator", string(evt.Type())); ok {
		t.Fatal("raw bundle handler lookup unexpectedly matched concrete instance event without delivery localization")
	}
	resolved := workflowNodeEventHandlerResolutionForDelivery(source, "build-orchestrator", evt)
	if !resolved.Matched {
		t.Fatal("expected concrete instance event to resolve to local build-orchestrator handler")
	}
	if got := resolved.HandlerEventKey; got != "build_progress" {
		t.Fatalf("handler event key = %q, want build_progress", got)
	}
	if got := strings.TrimSpace(resolved.Handler.EvidenceTarget); got != "build_evidence" {
		t.Fatalf("resolved evidence target = %q, want build_evidence", got)
	}

	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	workflowStore := NewWorkflowInstanceStore(db)
	pc := &PipelineCoordinator{
		bus:            &recordingPipelineBus{},
		db:             db,
		workflowStore:  workflowStore,
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
		module:         staticSemanticWorkflowModule{source: source},
	}
	ctx := testPipelineCoordinatorRunContext(t, pc)
	if err := workflowStore.Upsert(ctx, WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "operating",
		WorkflowVersion: "1.0.0",
		CurrentState:    "initializing",
		Metadata:        map[string]any{},
		StateBuckets:    map[string]any{},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	seedPipelineNodeDeliveryAuthority(t, db, evt, "build-orchestrator")

	handled, err := pc.executeNodeHandlerPlanResult(ctx, "build-orchestrator", evt)
	if err != nil {
		t.Fatalf("executeNodeHandlerPlanResult: %v", err)
	}
	if !handled {
		t.Fatal("executeNodeHandlerPlanResult handled = false, want true")
	}

	instance, ok, err := workflowStore.Load(ctx, entityID)
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("expected workflow instance to exist")
	}
	entries := workflowEvidenceEntries(t, instance, "build_evidence")
	if len(entries) != 1 {
		t.Fatalf("build_evidence entries = %d, want 1", len(entries))
	}
	if got := entries[0]["summary"]; got != "compile complete" {
		t.Fatalf("evidence summary = %#v, want compile complete", got)
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
