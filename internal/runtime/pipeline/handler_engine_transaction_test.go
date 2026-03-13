package pipeline

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"empireai/internal/events"
	runtimecontracts "empireai/internal/runtime/contracts"
	runtimeengine "empireai/internal/runtime/engine"
)

type recordingPipelineBus struct {
	mu        sync.Mutex
	publishes []events.Event
}

type recordingPipelineDispatcher struct {
	bus *recordingPipelineBus
}

func (b *recordingPipelineBus) Publish(_ context.Context, evt events.Event) error {
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

func TestExecuteNodeContractHandlerFlushesCollectedEventsToParentCollector(t *testing.T) {
	bus := &recordingPipelineBus{}
	pc := &FactoryPipelineCoordinator{
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
		State: WorkflowState{Stage: PipelineStage("queued"), Metadata: map[string]any{}},
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
	pc := &FactoryPipelineCoordinator{
		bus:            bus,
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
	}

	result, err := pc.executeNodeContractHandler(context.Background(), "node-a", runtimecontracts.SystemNodeEventHandler{
		Emits: runtimecontracts.EventEmission{Single: "custom.emitted"},
	}, workflowTriggerContext{
		Event: events.Event{Type: events.EventType("custom.trigger")}.WithEntityID("ent-1"),
		State: WorkflowState{Stage: PipelineStage("queued"), Metadata: map[string]any{}},
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

func TestExecuteNodeContractHandlerAppliesPayloadTransformToEmittedEvent(t *testing.T) {
	bus := &recordingPipelineBus{}
	pc := &FactoryPipelineCoordinator{
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
		State: WorkflowState{Stage: PipelineStage("queued"), Metadata: map[string]any{"stage": "queued"}},
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

func TestExecuteNodeContractHandlerRuleMatchOverridesDefaultEmit(t *testing.T) {
	bus := &recordingPipelineBus{}
	pc := &FactoryPipelineCoordinator{
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
		State: WorkflowState{Stage: PipelineStage("queued"), Metadata: map[string]any{}},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("bus published count = %d, want 1", got)
	}
	if got := string(bus.publishedEvent(0).Type); got != "rule.emitted" {
		t.Fatalf("published type = %q, want rule.emitted", got)
	}
}

func TestExecuteNodeContractHandlerExecutesHandlerActionInsideEngine(t *testing.T) {
	bus := &recordingPipelineBus{}
	pc := NewFactoryPipelineCoordinatorWithOptions(bus, nil, FactoryPipelineCoordinatorOptions{
		Module: NewGenericTestWorkflowModule(),
	})
	entityID := "ent-1"
	pc.validationGate.states[entityID] = &validationPipelineState{}

	result, err := pc.executeNodeContractHandler(context.Background(), "node-a", runtimecontracts.SystemNodeEventHandler{
		Action: runtimecontracts.ActionSpec{ID: "increment_revision_count"},
	}, workflowTriggerContext{
		Event: events.Event{Type: events.EventType("custom.trigger")}.WithEntityID(entityID),
		State: WorkflowState{Stage: PipelineStage("queued"), Metadata: map[string]any{}},
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
	snapshot := pc.validationStateSnapshot(entityID)
	if snapshot == nil || snapshot.RevisionCount != 1 {
		t.Fatalf("revision count = %#v, want 1", snapshot)
	}
	if got := bus.publishedCount(); got != 0 {
		t.Fatalf("bus published count = %d, want 0", got)
	}
}
