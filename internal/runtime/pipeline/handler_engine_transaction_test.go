package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimeengine "swarm/internal/runtime/engine"
	"swarm/internal/runtime/semanticview"
)

type recordingPipelineBus struct {
	mu        sync.Mutex
	publishes []events.Event
	publishErr error
}

type recordingPipelineDispatcher struct {
	bus *recordingPipelineBus
}

func (b *recordingPipelineBus) Publish(_ context.Context, evt events.Event) error {
	if b.publishErr != nil {
		return b.publishErr
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.publishes = append(b.publishes, evt)
	return nil
}

func (*recordingPipelineBus) Subscribe(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}

func (*recordingPipelineBus) PublishDirect(context.Context, events.Event, []string) error { return nil }
func (*recordingPipelineBus) ResolveSubscribedRecipients(string) []string                 { return nil }
func (*recordingPipelineBus) LogRuntime(context.Context, RuntimeLogEntry)                 {}
func (*recordingPipelineBus) EngineOutbox() runtimeengine.OutboxWriter                    { return noOpEngineOutbox{} }
func (b *recordingPipelineBus) EngineDispatcher() runtimeengine.PostCommitDispatcher {
	return recordingPipelineDispatcher{bus: b}
}

func (d recordingPipelineDispatcher) DispatchPostCommit(ctx context.Context, intents []runtimeengine.EmitIntent) error {
	if CollectPipelineEmitIntents(ctx, intents) {
		return nil
	}
	for _, intent := range intents {
		if len(intent.Recipients) > 0 {
			if err := d.bus.PublishDirect(ctx, intent.Event, intent.Recipients); err != nil {
				return err
			}
			continue
		}
		if err := d.bus.Publish(ctx, intent.Event); err != nil {
			return err
		}
	}
	return nil
}

func (b *recordingPipelineBus) publishedCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.publishes)
}

func (b *recordingPipelineBus) publishedEvent(i int) events.Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.publishes[i]
}

func TestPipelineCoordinatorPublish_ReturnsBusPublishError(t *testing.T) {
	wantErr := errors.New("bus publish failed")
	pc := &PipelineCoordinator{
		bus: &recordingPipelineBus{publishErr: wantErr},
	}

	err := pc.publish(context.Background(), "custom.emitted", "ent-1", map[string]any{"ok": true})
	if !errors.Is(err, wantErr) {
		t.Fatalf("publish error = %v, want %v", err, wantErr)
	}
}

func TestExecuteNodeContractHandlerFlushesCollectedEventsToParentCollector(t *testing.T) {
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{
		bus:            bus,
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
	}
	parentCollector := make([]events.Event, 0, 1)
	ctx := context.WithValue(context.Background(), pipelineEmitCollectorKey{}, &parentCollector)

	result, err := pc.executeNodeContractHandler(ctx, "node-a", runtimecontracts.SystemNodeEventHandler{
		Emits: runtimecontracts.EventEmission{Single: "custom.emitted"},
	}, workflowTriggerContext{
		Event: events.Event{Type: events.EventType("custom.trigger")}.WithEntityID("ent-1"),
		State: WorkflowState{Stage: WorkflowStateID("queued"), Metadata: map[string]any{}},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected handled result")
	}
	if got := len(parentCollector); got != 1 {
		t.Fatalf("parent collector count = %d, want 1", got)
	}
	if got := string(parentCollector[0].Type); got != "custom.emitted" {
		t.Fatalf("collected event type = %q, want custom.emitted", got)
	}
	if got := bus.publishedCount(); got != 0 {
		t.Fatalf("bus published count = %d, want 0 when parent collector is present", got)
	}
}

func TestExecuteNodeContractHandlerPublishesCollectedEventsWithoutParentCollector(t *testing.T) {
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{
		bus:            bus,
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
	}

	result, err := pc.executeNodeContractHandler(context.Background(), "node-a", runtimecontracts.SystemNodeEventHandler{
		Emits: runtimecontracts.EventEmission{Single: "custom.emitted"},
	}, workflowTriggerContext{
		Event: events.Event{Type: events.EventType("custom.trigger")}.WithEntityID("ent-1"),
		State: WorkflowState{Stage: WorkflowStateID("queued"), Metadata: map[string]any{}},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected handled result")
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("bus published count = %d, want 1", got)
	}
}

func TestExecuteNodeContractHandlerMintsEntityIDForEntityMaterializingHandler(t *testing.T) {
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{
		bus:            bus,
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
		module: &previewWorkflowModule{
			bundle: &runtimecontracts.WorkflowContractBundle{
				Semantics: runtimecontracts.WorkflowSemanticView{
					EntitySchema: runtimecontracts.EntitySchema{
						Groups: []runtimecontracts.EntitySchemaGroup{
							{
								Name: "identity",
								Fields: []runtimecontracts.EntitySchemaField{
									{Name: "name", Type: "text"},
								},
							},
						},
					},
				},
			},
		},
	}

	result, err := pc.executeNodeContractHandler(context.Background(), "node-a", runtimecontracts.SystemNodeEventHandler{
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "name", Value: runtimecontracts.ExpressionValue{Literal: "Minted Entity"}},
			},
		},
		Emits: runtimecontracts.EventEmission{Single: "custom.emitted"},
	}, workflowTriggerContext{
		Event: events.Event{Type: events.EventType("custom.trigger")},
		State: WorkflowState{Stage: WorkflowStateID(""), Metadata: map[string]any{}},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected handled result")
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("bus published count = %d, want 1", got)
	}
	if got := bus.publishedEvent(0).EntityID(); got == "" {
		t.Fatal("expected emitted event to carry minted entity_id")
	}
}

func TestResolveHandlerEntityIDForFlowKeepsSameFlowEntity(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			EntitySchema: runtimecontracts.EntitySchema{
				Groups: []runtimecontracts.EntitySchemaGroup{
					{
						Name: "identity",
						Fields: []runtimecontracts.EntitySchemaField{
							{Name: "name", Type: "text"},
						},
					},
				},
			},
		},
	})
	handler := runtimecontracts.SystemNodeEventHandler{
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "name", Value: runtimecontracts.ExpressionValue{Literal: "Same Flow"}},
			},
		},
	}
	const entityID = "ent-1"
	state := WorkflowState{EntityID: entityID}

	gotID, gotEvt := resolveHandlerEntityIDForFlow(source, "scoring", handler, entityID, events.Event{
		Type: events.EventType("vertical.discovered"),
	}.WithEntityID(entityID), &state)

	if gotID != entityID {
		t.Fatalf("entityID = %q, want %q", gotID, entityID)
	}
	if got := gotEvt.EntityID(); got != entityID {
		t.Fatalf("event entity_id = %q, want %q", got, entityID)
	}
	if state.EntityID != entityID {
		t.Fatalf("state entity_id = %q, want %q", state.EntityID, entityID)
	}
}

func TestResolveHandlerEntityIDForFlowPreservesCrossFlowEntityWithoutCreateEntity(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			EntitySchema: runtimecontracts.EntitySchema{
				Groups: []runtimecontracts.EntitySchemaGroup{
					{
						Name: "identity",
						Fields: []runtimecontracts.EntitySchemaField{
							{Name: "name", Type: "text"},
						},
					},
				},
			},
		},
	})
	handler := runtimecontracts.SystemNodeEventHandler{
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "name", Value: runtimecontracts.ExpressionValue{Literal: "New Flow Entity"}},
			},
		},
	}
	const incomingEntityID = "ent-discovery"
	state := WorkflowState{EntityID: incomingEntityID}

	gotID, gotEvt := resolveHandlerEntityIDForFlow(source, "scoring", handler, incomingEntityID, events.Event{
		Type:    events.EventType("vertical.discovered"),
		Payload: mustJSON(map[string]any{"entity_id": incomingEntityID}),
	}, &state)

	if gotID != incomingEntityID {
		t.Fatalf("entityID = %q, want preserved %q", gotID, incomingEntityID)
	}
	if got := gotEvt.EntityID(); got != incomingEntityID {
		t.Fatalf("event entity_id = %q, want %q", got, incomingEntityID)
	}
	if state.EntityID != incomingEntityID {
		t.Fatalf("state entity_id = %q, want %q", state.EntityID, incomingEntityID)
	}
}

func TestResolveHandlerEntityIDForFlowCreateEntityKeepsInboundReferenceAndClearsState(t *testing.T) {
	handler := runtimecontracts.SystemNodeEventHandler{CreateEntity: true}
	const inboundEntityID = "ent-parent"
	state := WorkflowState{
		EntityID: inboundEntityID,
		Stage:    WorkflowStateID("queued"),
		Status:   "active",
		Metadata: map[string]any{"name": "Parent"},
	}
	inbound := events.Event{
		Type:    events.EventType("vertical.discovered"),
		Payload: mustJSON(map[string]any{"entity_id": inboundEntityID, "name": "Parent"}),
	}

	gotID, gotEvt := resolveHandlerEntityIDForFlow(nil, "scoring", handler, inboundEntityID, inbound, &state)

	if gotID == "" || gotID == inboundEntityID {
		t.Fatalf("entityID = %q, want fresh id", gotID)
	}
	if got := gotEvt.EntityID(); got != inboundEntityID {
		t.Fatalf("inbound event entity_id = %q, want preserved %q", got, inboundEntityID)
	}
	if state.EntityID != gotID {
		t.Fatalf("state entity_id = %q, want %q", state.EntityID, gotID)
	}
	if state.Stage != "" {
		t.Fatalf("state stage = %q, want cleared", state.Stage)
	}
	if state.Status != "" {
		t.Fatalf("state status = %q, want cleared", state.Status)
	}
	if state.Metadata != nil {
		t.Fatalf("state metadata = %#v, want nil", state.Metadata)
	}
}

func TestHandlerExecutionStateSnapshotCreateEntityStartsClean(t *testing.T) {
	snapshot := handlerExecutionStateSnapshot(runtimecontracts.SystemNodeEventHandler{CreateEntity: true}, "ent-child", WorkflowState{
		EntityID: "ent-parent",
		Stage:    WorkflowStateID("queued"),
		Metadata: map[string]any{"name": "Parent"},
	})

	if got := snapshot.EntityID.String(); got != "ent-child" {
		t.Fatalf("snapshot entity_id = %q, want ent-child", got)
	}
	if snapshot.CurrentState != "" {
		t.Fatalf("snapshot current_state = %q, want empty", snapshot.CurrentState)
	}
	if snapshot.Metadata == nil {
		t.Fatal("snapshot metadata = nil, want empty map")
	}
	if len(snapshot.Metadata) != 0 {
		t.Fatalf("snapshot metadata = %#v, want empty", snapshot.Metadata)
	}
}

func TestExecuteNodeContractHandlerReturnsTerminalRejectForTerminalEntity(t *testing.T) {
	pc := &PipelineCoordinator{
		module: &previewWorkflowModule{
			workflow: NewWorkflowDefinition("demo", []WorkflowStage{
				{Name: "queued"},
				{Name: "done", Terminal: true},
			}, nil),
		},
	}

	result, err := pc.executeNodeContractHandler(context.Background(), "node-a", runtimecontracts.SystemNodeEventHandler{}, workflowTriggerContext{
		Event: events.Event{Type: events.EventType("custom.trigger")}.WithEntityID("ent-1"),
		State: WorkflowState{Stage: WorkflowStateID("done"), Metadata: map[string]any{}},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected handled result")
	}
	if result.Outcome == nil || result.Outcome.Status != HandlerOutcomeTerminalReject {
		t.Fatalf("outcome = %#v, want terminal_reject", result.Outcome)
	}
}

func TestExecuteNodeContractHandlerAppliesPayloadTransformToEmittedEvent(t *testing.T) {
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{
		bus:            bus,
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
	}

	_, err := pc.executeNodeContractHandler(context.Background(), "node-a", runtimecontracts.SystemNodeEventHandler{
		Emits: runtimecontracts.EventEmission{Single: "custom.emitted"},
		PayloadTransform: &runtimecontracts.PayloadTransformSpec{
			Mappings: map[string]string{
				"entity_id":     "payload.entity_id",
				"summary.stage": "entity.stage",
				"flags.ready":   "true",
				"label":         `"done"`,
			},
		},
	}, workflowTriggerContext{
		Event: events.Event{
			Type:    events.EventType("custom.trigger"),
			Payload: mustJSON(map[string]any{"entity_id": "ent-1"}),
		}.WithEntityID("ent-1"),
		State: WorkflowState{Stage: WorkflowStateID("queued"), Metadata: map[string]any{"stage": "queued"}},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("bus published count = %d, want 1", got)
	}
	var payload map[string]any
	if err := json.Unmarshal(bus.publishedEvent(0).Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if got := payload["entity_id"]; got != "ent-1" {
		t.Fatalf("payload.entity_id = %#v, want ent-1", got)
	}
	summary, _ := payload["summary"].(map[string]any)
	if got := summary["stage"]; got != "queued" {
		t.Fatalf("payload.summary.stage = %#v, want queued", got)
	}
	flags, _ := payload["flags"].(map[string]any)
	if got := flags["ready"]; got != true {
		t.Fatalf("payload.flags.ready = %#v, want true", got)
	}
	if got := payload["label"]; got != "done" {
		t.Fatalf("payload.label = %#v, want done", got)
	}
}

func TestExecuteNodeContractHandlerRuleMatchAugmentsDefaultEmit(t *testing.T) {
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{
		bus:            bus,
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
	}

	_, err := pc.executeNodeContractHandler(context.Background(), "node-a", runtimecontracts.SystemNodeEventHandler{
		Emits: runtimecontracts.EventEmission{Single: "default.emitted"},
		Rules: []runtimecontracts.HandlerRuleEntry{
			{ID: "pick-rule", Condition: "true", Emits: runtimecontracts.EventEmission{Single: "rule.emitted"}},
		},
	}, workflowTriggerContext{
		Event: events.Event{
			Type:    events.EventType("custom.trigger"),
			Payload: mustJSON(map[string]any{"entity_id": "ent-1"}),
		}.WithEntityID("ent-1"),
		State: WorkflowState{Stage: WorkflowStateID("queued"), Metadata: map[string]any{}},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if got := bus.publishedCount(); got != 2 {
		t.Fatalf("bus published count = %d, want 2", got)
	}
	if got := string(bus.publishedEvent(0).Type); got != "default.emitted" {
		t.Fatalf("published type[0] = %q, want default.emitted", got)
	}
	if got := string(bus.publishedEvent(1).Type); got != "rule.emitted" {
		t.Fatalf("published type[1] = %q, want rule.emitted", got)
	}
}

func TestExecuteNodeContractHandlerExecutesHandlerActionInsideEngine(t *testing.T) {
	bus := &recordingPipelineBus{}
	pc := NewPipelineCoordinatorWithOptions(bus, nil, PipelineCoordinatorOptions{
		Module: NewGenericTestWorkflowModule(),
	})
	entityID := "ent-1"

	result, err := pc.executeNodeContractHandler(context.Background(), "node-a", runtimecontracts.SystemNodeEventHandler{
		Action: runtimecontracts.ActionSpec{ID: "increment_revision_count"},
	}, workflowTriggerContext{
		Event: events.Event{Type: events.EventType("custom.trigger")}.WithEntityID(entityID),
		State: WorkflowState{Stage: WorkflowStateID("queued"), Metadata: map[string]any{}},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected handled result")
	}
	if got := result.Outcome.ActionsExecuted; len(got) != 1 || got[0] != "increment_revision_count" {
		t.Fatalf("actions executed = %#v, want [increment_revision_count]", got)
	}
	if got := bus.publishedCount(); got != 0 {
		t.Fatalf("bus published count = %d, want 0", got)
	}
}
