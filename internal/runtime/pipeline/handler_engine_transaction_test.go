package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimeengine "swarm/internal/runtime/engine"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/testutil"
)

type recordingPipelineBus struct {
	mu         sync.Mutex
	publishes  []events.Event
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
func (*recordingPipelineBus) LogRuntime(context.Context, RuntimeLogEntry) error           { return nil }
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

func TestExecuteNodeContractHandlerUsesTypedEnvelopeIdentityOverPayload(t *testing.T) {
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{
		bus:            bus,
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
	}

	result, err := pc.executeNodeContractHandler(context.Background(), "node-a", runtimecontracts.SystemNodeEventHandler{
		Emits: runtimecontracts.EventEmission{Single: "custom.emitted"},
	}, workflowTriggerContext{
		Event: (events.Event{
			Type:      events.EventType("custom.trigger"),
			Payload:   []byte(`{"entity_id":"payload-ent"}`),
			CreatedAt: time.Now().UTC(),
		}).WithEnvelope(events.EventEnvelope{EntityID: "env-ent"}),
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
	if got := bus.publishedEvent(0).EntityID(); got != "env-ent" {
		t.Fatalf("emitted event entity_id = %q, want env-ent", got)
	}
	var payload map[string]any
	if err := json.Unmarshal(bus.publishedEvent(0).Payload, &payload); err != nil {
		t.Fatalf("unmarshal emitted payload: %v", err)
	}
	if got := payload["entity_id"]; got != "env-ent" {
		t.Fatalf("emitted payload entity_id = %#v, want env-ent", got)
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
				{TargetField: "name", Value: runtimecontracts.LiteralExpression("Minted Entity")},
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

func TestExecuteNodeContractHandlerRejectsEmitWhenPersistencePrerequisiteFieldIsMissing(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, bus := newEmitPersistenceTestCoordinator(db)
	const entityID = "11111111-1111-1111-1111-111111111111"
	if err := pc.workflowStore.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "validation",
		WorkflowVersion: "v-test",
		CurrentState:    "researching",
		Metadata: map[string]any{
			"subject_id": entityID,
		},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	_, err := pc.executeNodeContractHandler(context.Background(), "node-a", runtimecontracts.SystemNodeEventHandler{
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "business_brief"},
			},
			SourceEvent: "research.completed",
		},
		Emits:      runtimecontracts.EventEmission{Single: "spec.requested"},
		AdvancesTo: "mvp_speccing",
	}, workflowTriggerContext{
		Event: events.Event{
			Type:    events.EventType("research.completed"),
			Payload: mustJSON(map[string]any{}),
		}.WithEntityID(entityID),
		State: WorkflowState{
			EntityID: entityID,
			Stage:    WorkflowStateID("researching"),
			Metadata: map[string]any{},
		},
	}, false)
	if !errors.Is(err, runtimeengine.ErrEmitPersistencePrerequisite) {
		t.Fatalf("executeNodeContractHandler error = %v, want %v", err, runtimeengine.ErrEmitPersistencePrerequisite)
	}
	if got := bus.publishedCount(); got != 0 {
		t.Fatalf("published count = %d, want 0 when persistence prerequisite is missing", got)
	}

	instance, ok, loadErr := pc.workflowStore.Load(context.Background(), entityID)
	if loadErr != nil {
		t.Fatalf("load workflow instance: %v", loadErr)
	}
	if !ok {
		t.Fatal("expected seeded workflow instance to remain available")
	}
	if got := strings.TrimSpace(instance.CurrentState); got != "researching" {
		t.Fatalf("current_state = %q, want researching after rollback", got)
	}
	if _, exists := instance.Metadata["business_brief"]; exists {
		t.Fatalf("business_brief unexpectedly persisted after rejected emit: %#v", instance.Metadata["business_brief"])
	}
}

func TestExecuteNodeContractHandlerPublishesAfterPersistencePrerequisiteFieldSucceeds(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, bus := newEmitPersistenceTestCoordinator(db)
	const entityID = "11111111-1111-1111-1111-111111111111"
	if err := pc.workflowStore.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "validation",
		WorkflowVersion: "v-test",
		CurrentState:    "researching",
		Metadata: map[string]any{
			"subject_id": entityID,
		},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	result, err := pc.executeNodeContractHandler(context.Background(), "node-a", runtimecontracts.SystemNodeEventHandler{
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "business_brief"},
			},
			SourceEvent: "research.completed",
		},
		Emits:      runtimecontracts.EventEmission{Single: "spec.requested"},
		AdvancesTo: "mvp_speccing",
	}, workflowTriggerContext{
		Event: events.Event{
			Type: events.EventType("research.completed"),
			Payload: mustJSON(map[string]any{
				"business_brief": map[string]any{"summary": "validated"},
			}),
		}.WithEntityID(entityID),
		State: WorkflowState{
			EntityID: entityID,
			Stage:    WorkflowStateID("researching"),
			Metadata: map[string]any{},
		},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected handled result")
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("published count = %d, want 1", got)
	}
	if got := string(bus.publishedEvent(0).Type); got != "spec.requested" {
		t.Fatalf("published type = %q, want spec.requested", got)
	}

	instance, ok, loadErr := pc.workflowStore.Load(context.Background(), entityID)
	if loadErr != nil {
		t.Fatalf("load workflow instance: %v", loadErr)
	}
	if !ok {
		t.Fatal("expected workflow instance to persist")
	}
	if got := strings.TrimSpace(instance.CurrentState); got != "mvp_speccing" {
		t.Fatalf("current_state = %q, want mvp_speccing", got)
	}
	brief, ok := instance.Metadata["business_brief"].(map[string]any)
	if !ok || brief["summary"] != "validated" {
		t.Fatalf("business_brief = %#v, want persisted payload", instance.Metadata["business_brief"])
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
				{TargetField: "name", Value: runtimecontracts.LiteralExpression("Same Flow")},
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

func newEmitPersistenceTestCoordinator(db *sql.DB) (*PipelineCoordinator, *recordingPipelineBus) {
	bus := &recordingPipelineBus{}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:    "validation",
			Version: "v-test",
			EntitySchema: runtimecontracts.EntitySchema{
				Groups: []runtimecontracts.EntitySchemaGroup{
					{
						Name: "validation_phase",
						Fields: []runtimecontracts.EntitySchemaField{
							{Name: "business_brief", Type: "jsonb"},
						},
					},
				},
			},
		},
	}
	pc := &PipelineCoordinator{
		bus:            bus,
		db:             db,
		workflowStore:  NewWorkflowInstanceStore(db),
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
		module: &previewWorkflowModule{
			bundle: bundle,
			workflow: NewWorkflowDefinition("validation", []WorkflowStage{
				{Name: "researching"},
				{Name: "mvp_speccing"},
			}, []WorkflowTransition{
				{
					Name: "research_to_spec",
					From: []WorkflowStateID{"researching"},
					To:   "mvp_speccing",
				},
			}),
		},
	}
	return pc, bus
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
				{TargetField: "name", Value: runtimecontracts.LiteralExpression("New Flow Entity")},
			},
		},
	}
	const incomingEntityID = "ent-discovery"
	state := WorkflowState{EntityID: incomingEntityID}

	gotID, gotEvt := resolveHandlerEntityIDForFlow(source, "scoring", handler, incomingEntityID, events.Event{
		Type:    events.EventType("vertical.discovered"),
		Payload: mustJSON(map[string]any{"entity_id": incomingEntityID}),
	}.WithEnvelope(events.EventEnvelope{EntityID: incomingEntityID}), &state)

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

func TestResolveHandlerEntityIDForRootUsesSubjectEntityForFlowScopedInbound(t *testing.T) {
	handler := runtimecontracts.SystemNodeEventHandler{
		Emits: runtimecontracts.EventEmission{Single: "pipeline.complete"},
	}
	const inboundEntityID = "ent-child"
	const subjectEntityID = "ent-root"
	state := WorkflowState{
		EntityID: inboundEntityID,
		Metadata: map[string]any{
			"subject_id": subjectEntityID,
		},
	}

	gotID, gotEvt := resolveHandlerEntityIDForFlow(nil, "", handler, inboundEntityID, events.Event{
		Type: events.EventType("child/grandchild/task.done"),
	}.WithEntityID(inboundEntityID), &state)

	if gotID != subjectEntityID {
		t.Fatalf("entityID = %q, want subject/root %q", gotID, subjectEntityID)
	}
	if got := gotEvt.EntityID(); got != inboundEntityID {
		t.Fatalf("inbound event entity_id = %q, want preserved %q", got, inboundEntityID)
	}
	if state.EntityID != subjectEntityID {
		t.Fatalf("state entity_id = %q, want %q", state.EntityID, subjectEntityID)
	}
}

func TestResolveHandlerEntityIDForFlowUsesParentEntityForDescendantScopedInbound(t *testing.T) {
	handler := runtimecontracts.SystemNodeEventHandler{
		Emits: runtimecontracts.EventEmission{Single: "step.result"},
	}
	const inboundEntityID = "ent-grandchild"
	const parentEntityID = "ent-child"
	state := WorkflowState{
		EntityID: inboundEntityID,
		Metadata: map[string]any{
			"flow_path":        "child/grandchild/inst-1",
			"parent_entity_id": parentEntityID,
			"subject_id":       "ent-root",
		},
	}

	gotID, gotEvt := resolveHandlerEntityIDForFlow(nil, "child", handler, inboundEntityID, events.Event{
		Type: events.EventType("child/grandchild/micro.done"),
	}.WithEntityID(inboundEntityID), &state)

	if gotID != parentEntityID {
		t.Fatalf("entityID = %q, want parent %q", gotID, parentEntityID)
	}
	if got := gotEvt.EntityID(); got != inboundEntityID {
		t.Fatalf("inbound event entity_id = %q, want preserved %q", got, inboundEntityID)
	}
	if state.EntityID != parentEntityID {
		t.Fatalf("state entity_id = %q, want %q", state.EntityID, parentEntityID)
	}
}

func TestResolveHandlerEntityIDForFlowDoesNotRetargetSameFlowInstancePath(t *testing.T) {
	handler := runtimecontracts.SystemNodeEventHandler{
		Emits: runtimecontracts.EventEmission{Single: "step.result"},
	}
	const (
		entityID = "ent-child"
		rootID   = "ent-root"
	)
	state := WorkflowState{
		EntityID: entityID,
		Metadata: map[string]any{
			"flow_path":        "child/inst-1",
			"parent_entity_id": rootID,
			"subject_id":       rootID,
		},
	}

	gotID, gotEvt := resolveHandlerEntityIDForFlow(nil, "child", handler, entityID, events.Event{
		Type: events.EventType("child/grandchild/micro.done"),
	}.WithEntityID(entityID), &state)

	if gotID != entityID {
		t.Fatalf("entityID = %q, want %q", gotID, entityID)
	}
	if got := gotEvt.EntityID(); got != entityID {
		t.Fatalf("inbound event entity_id = %q, want preserved %q", got, entityID)
	}
	if state.EntityID != entityID {
		t.Fatalf("state entity_id = %q, want %q", state.EntityID, entityID)
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
	}.WithEnvelope(events.EventEnvelope{EntityID: inboundEntityID})

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
	if state.Metadata == nil {
		t.Fatal("state metadata = nil, want subject_id preserved")
	}
	if got := strings.TrimSpace(asString(state.Metadata["subject_id"])); got != inboundEntityID {
		t.Fatalf("state subject_id = %q, want %q", got, inboundEntityID)
	}
	if got := strings.TrimSpace(asString(state.Metadata["parent_entity_id"])); got != inboundEntityID {
		t.Fatalf("state parent_entity_id = %q, want %q", got, inboundEntityID)
	}
	instanceID := strings.TrimSpace(asString(state.Metadata["instance_id"]))
	if instanceID == "" {
		t.Fatal("state instance_id is empty, want generated logical instance id")
	}
	if got := strings.TrimSpace(asString(state.Metadata["flow_path"])); got != "scoring/"+instanceID {
		t.Fatalf("state flow_path = %q, want %q", got, "scoring/"+instanceID)
	}
	if got := strings.TrimSpace(asString(state.Metadata["storage_ref"])); got != "scoring/"+instanceID {
		t.Fatalf("state storage_ref = %q, want %q", got, "scoring/"+instanceID)
	}
	if wantEntityID := FlowInstanceEntityID("scoring/" + instanceID); gotID != wantEntityID {
		t.Fatalf("entityID = %q, want persisted flow entity id %q", gotID, wantEntityID)
	}
}

func TestHandlerExecutionStateSnapshotCreateEntityStartsClean(t *testing.T) {
	snapshot := handlerExecutionStateSnapshot(runtimecontracts.SystemNodeEventHandler{CreateEntity: true}, "ent-child", WorkflowState{
		EntityID: "ent-parent",
		Stage:    WorkflowStateID("queued"),
		Metadata: map[string]any{"subject_id": "ent-parent"},
	})

	if got := snapshot.EntityID.String(); got != "ent-child" {
		t.Fatalf("snapshot entity_id = %q, want ent-child", got)
	}
	if snapshot.CurrentState != "" {
		t.Fatalf("snapshot current_state = %q, want empty", snapshot.CurrentState)
	}
	if snapshot.Metadata == nil {
		t.Fatal("snapshot metadata = nil, want subject metadata")
	}
	if got := strings.TrimSpace(asString(snapshot.Metadata["subject_id"])); got != "ent-parent" {
		t.Fatalf("snapshot subject_id = %q, want ent-parent", got)
	}
}

func TestResolveHandlerEntityIDForFlowCreateEntitySeedsFirstFlowSubjectID(t *testing.T) {
	handler := runtimecontracts.SystemNodeEventHandler{CreateEntity: true}
	state := WorkflowState{}

	gotID, _ := resolveHandlerEntityIDForFlow(nil, "scoring", handler, "", events.Event{
		Type: events.EventType("vertical.discovered"),
	}, &state)

	if gotID == "" {
		t.Fatal("expected fresh entity id")
	}
	if state.Metadata == nil {
		t.Fatal("state metadata = nil, want subject_id")
	}
	if got := strings.TrimSpace(asString(state.Metadata["subject_id"])); got != gotID {
		t.Fatalf("state subject_id = %q, want %q", got, gotID)
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
				"summary.stage": "entity.current_state",
			},
			Entries: []runtimecontracts.TransformSpec{
				{Target: "flags.ready", Value: runtimecontracts.CELExpression("true")},
				{Target: "label", Value: runtimecontracts.CELExpression(`"done"`)},
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

func TestExecuteNodeHandlerPlanResult_NestedDescendantCompletionAdvancesParentFlow(t *testing.T) {
	source := loadWorkflowFixtureSource(t, "test-nested-three-levels")
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected workflow fixture bundle")
	}
	module, err := newPipelineFixtureWorkflowModule(bundle)
	if err != nil {
		t.Fatalf("newPipelineFixtureWorkflowModule: %v", err)
	}
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	store := NewWorkflowInstanceStore(db)
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{
		bus:            bus,
		db:             db,
		module:         module,
		workflowStore:  store,
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
	}

	const (
		rootEntityID = "11111111-1111-1111-1111-111111111111"
	)
	childEntityID := FlowInstanceEntityID("child/inst-1")
	grandchildEntityID := FlowInstanceEntityID("child/grandchild/inst-1")
	if err := store.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      childEntityID,
		SubjectID:       rootEntityID,
		StorageRef:      "child/inst-1",
		WorkflowName:    "child",
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "waiting",
		Metadata: map[string]any{
			"entity_id":        childEntityID,
			"flow_path":        "child/inst-1",
			"subject_id":       rootEntityID,
			"parent_entity_id": rootEntityID,
		},
	}); err != nil {
		t.Fatalf("seed child instance: %v", err)
	}
	if err := store.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      grandchildEntityID,
		SubjectID:       rootEntityID,
		StorageRef:      "child/grandchild/inst-1",
		WorkflowName:    "grandchild",
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "finished",
		Metadata: map[string]any{
			"entity_id":        grandchildEntityID,
			"flow_path":        "child/grandchild/inst-1",
			"subject_id":       rootEntityID,
			"parent_entity_id": childEntityID,
		},
	}); err != nil {
		t.Fatalf("seed grandchild instance: %v", err)
	}
	if consume, handled := pc.workflowNodeInterceptPolicy("child/grandchild/micro.done", (events.Event{
		Type: events.EventType("child/grandchild/micro.done"),
	}).WithEntityID(grandchildEntityID)); !handled {
		t.Fatalf("workflowNodeInterceptPolicy handled = %v, consume = %v, want handled", handled, consume)
	}

	handled, err := pc.dispatchWorkflowNodeEventResult(context.Background(), events.Event{
		Type: events.EventType("child/grandchild/micro.done"),
	}.WithEntityID(grandchildEntityID))
	if err != nil {
		t.Fatalf("executeNodeHandlerPlanResult: %v", err)
	}
	if !handled {
		t.Fatal("expected handler to execute")
	}
	child, found, err := store.Load(context.Background(), childEntityID)
	if err != nil {
		t.Fatalf("load child instance: %v", err)
	}
	if !found {
		t.Fatal("expected child instance")
	}
	if got := strings.TrimSpace(child.CurrentState); got != "completed" {
		t.Fatalf("child current_state = %q, want completed", got)
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("published count = %d, want 1", got)
	}
	if got := string(bus.publishedEvent(0).Type); got != "child/step.result" {
		t.Fatalf("published type = %q, want child/step.result", got)
	}
	if got := bus.publishedEvent(0).EntityID(); got != rootEntityID {
		t.Fatalf("published entity_id = %q, want %q", got, rootEntityID)
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

func TestExecuteNodeContractHandlerOnCompleteDoesNotSeeCurrentHandlerTopLevelWritesEarly(t *testing.T) {
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{
		bus:            bus,
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
	}

	result, err := pc.executeNodeContractHandler(context.Background(), "node-a", runtimecontracts.SystemNodeEventHandler{
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "branch_target", Value: runtimecontracts.LiteralExpression("handler")},
			},
		},
		OnComplete: []runtimecontracts.HandlerRuleEntry{
			{ID: "too-early", Condition: `"branch_target" in entity && entity.branch_target == "handler"`, Emits: runtimecontracts.EventEmission{Single: "branch.selected"}},
		},
	}, workflowTriggerContext{
		Event: events.Event{
			Type: events.EventType("custom.trigger"),
		}.WithEntityID("ent-1"),
		State: WorkflowState{EntityID: "ent-1", Stage: WorkflowStateID("queued"), Metadata: map[string]any{}},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected handled result")
	}
	if got := strings.TrimSpace(result.Outcome.RuleID); got != "" {
		t.Fatalf("rule_id = %q, want empty when on_complete cannot see current-handler top-level writes", got)
	}
	if got := bus.publishedCount(); got != 0 {
		t.Fatalf("published count = %d, want 0 when branch selection cannot see early top-level writes", got)
	}
}

func TestExecuteNodeContractHandlerExecutesEmitInsideEngine(t *testing.T) {
	bus := &recordingPipelineBus{}
	pc := NewPipelineCoordinatorWithOptions(bus, nil, PipelineCoordinatorOptions{
		Module: NewGenericTestWorkflowModule(),
	})
	entityID := "ent-1"

	result, err := pc.executeNodeContractHandler(context.Background(), "node-a", runtimecontracts.SystemNodeEventHandler{
		Emits: runtimecontracts.EventEmission{Single: "custom.emitted"},
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
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("bus published count = %d, want 1", got)
	}
}
