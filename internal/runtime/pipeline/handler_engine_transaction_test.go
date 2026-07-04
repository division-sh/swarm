package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type recordingPipelineBus struct {
	mu            sync.Mutex
	publishes     []events.Event
	runtimeLogs   []RuntimeLogEntry
	outboxIntents []runtimeengine.EmitIntent
	publishErr    error
	outboxErr     error
}

type recordingPipelineDispatcher struct {
	bus *recordingPipelineBus
}

type recordingPipelineOutbox struct {
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
func (b *recordingPipelineBus) LogRuntime(_ context.Context, entry RuntimeLogEntry) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.runtimeLogs = append(b.runtimeLogs, entry)
	return nil
}
func (b *recordingPipelineBus) EngineOutbox() runtimeengine.OutboxWriter {
	return recordingPipelineOutbox{bus: b}
}
func (b *recordingPipelineBus) EngineDispatcher() runtimeengine.PostCommitDispatcher {
	return recordingPipelineDispatcher{bus: b}
}

func (o recordingPipelineOutbox) WriteOutbox(_ context.Context, intents []runtimeengine.EmitIntent) error {
	if o.bus == nil {
		return nil
	}
	if o.bus.outboxErr != nil {
		return o.bus.outboxErr
	}
	o.bus.mu.Lock()
	defer o.bus.mu.Unlock()
	o.bus.outboxIntents = append(o.bus.outboxIntents, cloneEmitIntents(intents)...)
	return nil
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

func (b *recordingPipelineBus) outboxCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.outboxIntents)
}

func (b *recordingPipelineBus) outboxIntent(i int) runtimeengine.EmitIntent {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.outboxIntents[i]
}

func (b *recordingPipelineBus) runtimeLogEntries() []RuntimeLogEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]RuntimeLogEntry, len(b.runtimeLogs))
	copy(out, b.runtimeLogs)
	return out
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
		Emit: runtimecontracts.EmitSpec{Event: "custom.emitted"},
	}, workflowTriggerContext{
		Event: eventtest.RootIngress("", events.EventType("custom.trigger"), "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"), time.Time{}),
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
	if got := string(parentCollector[0].Type()); got != "custom.emitted" {
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

	result, err := pc.executeNodeContractHandler(testPipelineCoordinatorRunContext(t, pc), "node-a", runtimecontracts.SystemNodeEventHandler{
		Emit: runtimecontracts.EmitSpec{Event: "custom.emitted"},
	}, workflowTriggerContext{
		Event: eventtest.RootIngress("", events.EventType("custom.trigger"), "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"), time.Time{}),
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

	result, err := pc.executeNodeContractHandler(testPipelineCoordinatorRunContext(t, pc), "node-a", runtimecontracts.SystemNodeEventHandler{
		Emit: runtimecontracts.EmitSpec{Event: "custom.emitted"},
	}, workflowTriggerContext{
		Event: eventtest.RootIngress(
			"",
			events.EventType("custom.trigger"),
			"",
			"",
			[]byte(`{"entity_id":"payload-ent"}`),
			0,
			"",
			"",
			events.EventEnvelope{EntityID: "env-ent"},
			time.Now().UTC(),
		),
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
	if err := json.Unmarshal(bus.publishedEvent(0).Payload(), &payload); err != nil {
		t.Fatalf("unmarshal emitted payload: %v", err)
	}
	if _, ok := payload["entity_id"]; ok {
		t.Fatalf("emitted payload must not carry envelope entity_id: %#v", payload["entity_id"])
	}
}

func TestExecuteNodeContractHandlerMintsEntityIDForEntityMaterializingHandler(t *testing.T) {
	source := loadWorkflowTempSource(t, map[string]string{
		"package.yaml": `
name: runtime-test
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: scoring
    flow: scoring
    mode: static
`,
		"schema.yaml": "name: runtime-test\n",
		"flows/scoring/schema.yaml": `
name: scoring
mode: static
initial_state: queued
states: [queued]
`,
		"flows/scoring/entities.yaml": `
subject:
  name: text
`,
		"flows/scoring/nodes.yaml": `
node-a:
  id: node-a
  execution_type: system_node
`,
	})
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected temp workflow bundle")
	}
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{
		bus:            bus,
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
		module: &previewWorkflowModule{
			bundle: bundle,
		},
	}

	result, err := pc.executeNodeContractHandler(testPipelineCoordinatorRunContext(t, pc), "node-a", runtimecontracts.SystemNodeEventHandler{
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "name", Value: runtimecontracts.LiteralExpression("Minted Entity")},
			},
		},
		Emit: runtimecontracts.EmitSpec{Event: "custom.emitted"},
	}, workflowTriggerContext{
		Event: eventtest.RootIngress("", events.EventType("custom.trigger"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
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
		t.Fatal("expected emitted event to carry canonical entity_id")
	}
}

func TestExecuteNodeContractHandlerRejectsEmitWhenPersistencePrerequisiteFieldIsMissing(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, bus := newEmitPersistenceTestCoordinator(db)
	const entityID = "11111111-1111-1111-1111-111111111111"
	if err := pc.workflowStore.Upsert(testPipelineCoordinatorRunContext(t, pc), WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "validation",
		WorkflowVersion: "v-test",
		CurrentState:    "researching",
		Metadata:        map[string]any{},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	_, err := pc.executeNodeContractHandler(testPipelineCoordinatorRunContext(t, pc), "node-a", runtimecontracts.SystemNodeEventHandler{
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "business_brief"},
			},
			SourceEvent: "research.completed",
		},
		Emit:       runtimecontracts.EmitSpec{Event: "spec.requested"},
		AdvancesTo: "mvp_speccing",
	}, workflowTriggerContext{
		Event: eventtest.RootIngress(
			"",
			events.EventType("research.completed"),
			"",
			"",
			mustJSON(map[string]any{}),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
			time.Time{},
		),

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

	instance, ok, loadErr := pc.workflowStore.Load(testPipelineCoordinatorRunContext(t, pc), entityID)
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
	if err := pc.workflowStore.Upsert(testPipelineCoordinatorRunContext(t, pc), WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "validation",
		WorkflowVersion: "v-test",
		CurrentState:    "researching",
		Metadata:        map[string]any{},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	result, err := pc.executeNodeContractHandler(testPipelineCoordinatorRunContext(t, pc), "node-a", runtimecontracts.SystemNodeEventHandler{
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "business_brief"},
			},
			SourceEvent: "research.completed",
		},
		Emit:       runtimecontracts.EmitSpec{Event: "spec.requested"},
		AdvancesTo: "mvp_speccing",
	}, workflowTriggerContext{
		Event: eventtest.RootIngress(
			"",
			events.EventType("research.completed"),
			"",
			"",
			mustJSON(map[string]any{
				"business_brief": map[string]any{"summary": "validated"},
			}),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
			time.Time{},
		),

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
	if got := string(bus.publishedEvent(0).Type()); got != "spec.requested" {
		t.Fatalf("published type = %q, want spec.requested", got)
	}

	instance, ok, loadErr := pc.workflowStore.Load(testPipelineCoordinatorRunContext(t, pc), entityID)
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

func TestExecuteNodeContractHandlerLogsAccumulatorCompletionCommittedOutcome(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, bus := newAccumulatorOutcomeTestCoordinator(db)
	const entityID = "11111111-1111-1111-1111-111111111111"
	if err := pc.workflowStore.Upsert(testPipelineCoordinatorRunContext(t, pc), WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "validation",
		WorkflowVersion: "v-test",
		CurrentState:    "researching",
		Metadata: map[string]any{
			"expected_count": 2,
		},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	handler := runtimecontracts.SystemNodeEventHandler{
		Accumulate: &runtimecontracts.AccumulateSpec{
			ExpectedFrom: "entity.expected_count",
			Completion:   runtimecontracts.ParseAccumulateCompletion("all"),
		},
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{{TargetField: "business_brief"}},
		},
		OnComplete: []runtimecontracts.HandlerRuleEntry{
			{ID: "complete", Condition: "true", AdvancesTo: "mvp_speccing", Emit: runtimecontracts.EmitSpec{Event: "spec.requested"}},
		},
	}

	for idx := 0; idx < 2; idx++ {
		payload := map[string]any{"item_id": idx + 1}
		if idx == 1 {
			payload["business_brief"] = map[string]any{"summary": "validated"}
		}
		result, err := pc.executeNodeContractHandler(testPipelineCoordinatorRunContext(t, pc), "node-a", handler, workflowTriggerContext{
			Event: eventtest.RootIngress(
				"",
				events.EventType("research.completed"),
				"",
				"",
				mustJSON(payload),
				0,
				"",
				"",
				events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
				time.Time{},
			),

			State: pc.currentWorkflowState(testPipelineCoordinatorRunContext(t, pc), entityID),
		}, false)
		if err != nil {
			t.Fatalf("executeNodeContractHandler(%d): %v", idx, err)
		}
		if !result.Handled {
			t.Fatalf("expected handled result for event %d", idx)
		}
	}

	logs := bus.runtimeLogEntries()
	entry := findRuntimeLogByAction(t, logs, accumulatorCompletionOutcomeAction)
	if strings.TrimSpace(entry.Level.String()) != "info" {
		t.Fatalf("log level = %q, want info", entry.Level.String())
	}
	detail, _ := entry.Detail.(map[string]any)
	if got := runtimeLogDetailString(detail["decision_reason_code"]); got != "completion_committed" {
		t.Fatalf("decision_reason_code = %q, want completion_committed", got)
	}
	if got := runtimeLogDetailString(detail["evaluation_outcome"]); got != "succeeded" {
		t.Fatalf("evaluation_outcome = %q, want succeeded", got)
	}
	if got := runtimeLogDetailString(detail["commit_outcome"]); got != "committed" {
		t.Fatalf("commit_outcome = %q, want committed", got)
	}
	if got := detail["received_count"]; got != 2 && got != float64(2) {
		t.Fatalf("received_count = %#v, want 2", got)
	}

	instance, ok, err := pc.workflowStore.Load(testPipelineCoordinatorRunContext(t, pc), entityID)
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("expected workflow instance to persist")
	}
	if got := strings.TrimSpace(instance.CurrentState); got != "mvp_speccing" {
		t.Fatalf("current_state = %q, want mvp_speccing", got)
	}
}

func TestExecuteNodeContractHandlerLogsAccumulatorCompletionEvaluationFailure(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, bus := newAccumulatorOutcomeTestCoordinator(db)
	const entityID = "11111111-1111-1111-1111-111111111111"
	if err := pc.workflowStore.Upsert(testPipelineCoordinatorRunContext(t, pc), WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "validation",
		WorkflowVersion: "v-test",
		CurrentState:    "researching",
		Metadata: map[string]any{
			"expected_count": 1,
		},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	_, err := pc.executeNodeContractHandler(testPipelineCoordinatorRunContext(t, pc), "node-a", runtimecontracts.SystemNodeEventHandler{
		Accumulate: &runtimecontracts.AccumulateSpec{
			ExpectedFrom: "entity.expected_count",
			Completion:   runtimecontracts.ParseAccumulateCompletion("all"),
		},
		OnComplete: []runtimecontracts.HandlerRuleEntry{
			{ID: "bad", Condition: `entity.branch_target == "go"`, AdvancesTo: "mvp_speccing"},
		},
	}, workflowTriggerContext{
		Event: eventtest.RootIngress(
			"",
			events.EventType("research.completed"),
			"",
			"",
			mustJSON(map[string]any{"item_id": 1}),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
			time.Time{},
		),

		State: pc.currentWorkflowState(testPipelineCoordinatorRunContext(t, pc), entityID),
	}, false)
	if err == nil {
		t.Fatal("expected executeNodeContractHandler to fail on on_complete evaluation error")
	}

	entry := findRuntimeLogByAction(t, bus.runtimeLogEntries(), accumulatorCompletionOutcomeAction)
	detail, _ := entry.Detail.(map[string]any)
	if got := runtimeLogDetailString(detail["decision_reason_code"]); got != "on_complete_evaluation_failed" {
		t.Fatalf("decision_reason_code = %q, want on_complete_evaluation_failed", got)
	}
	if got := runtimeLogDetailString(detail["evaluation_outcome"]); got != "failed" {
		t.Fatalf("evaluation_outcome = %q, want failed", got)
	}
	if got := runtimeLogDetailString(detail["commit_outcome"]); got != "rolled_back" {
		t.Fatalf("commit_outcome = %q, want rolled_back", got)
	}

	instance, ok, loadErr := pc.workflowStore.Load(testPipelineCoordinatorRunContext(t, pc), entityID)
	if loadErr != nil {
		t.Fatalf("load workflow instance: %v", loadErr)
	}
	if !ok {
		t.Fatal("expected workflow instance to remain available")
	}
	if got := strings.TrimSpace(instance.CurrentState); got != "researching" {
		t.Fatalf("current_state = %q, want researching after rollback", got)
	}
}

func TestExecuteNodeContractHandlerLogsAccumulatorCompletionCommitFailure(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, bus := newAccumulatorOutcomeTestCoordinator(db)
	const entityID = "11111111-1111-1111-1111-111111111111"
	if err := pc.workflowStore.Upsert(testPipelineCoordinatorRunContext(t, pc), WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "validation",
		WorkflowVersion: "v-test",
		CurrentState:    "researching",
		Metadata: map[string]any{
			"expected_count": 1,
		},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	_, err := pc.executeNodeContractHandler(testPipelineCoordinatorRunContext(t, pc), "node-a", runtimecontracts.SystemNodeEventHandler{
		Accumulate: &runtimecontracts.AccumulateSpec{
			ExpectedFrom: "entity.expected_count",
			Completion:   runtimecontracts.ParseAccumulateCompletion("all"),
		},
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{{TargetField: "business_brief"}},
		},
		OnComplete: []runtimecontracts.HandlerRuleEntry{
			{ID: "complete", Condition: "true", AdvancesTo: "mvp_speccing", Emit: runtimecontracts.EmitSpec{Event: "spec.requested"}},
		},
	}, workflowTriggerContext{
		Event: eventtest.RootIngress(
			"",
			events.EventType("research.completed"),
			"",
			"",
			mustJSON(map[string]any{"item_id": 1}),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
			time.Time{},
		),

		State: pc.currentWorkflowState(testPipelineCoordinatorRunContext(t, pc), entityID),
	}, false)
	if !errors.Is(err, runtimeengine.ErrEmitPersistencePrerequisite) {
		t.Fatalf("executeNodeContractHandler error = %v, want %v", err, runtimeengine.ErrEmitPersistencePrerequisite)
	}

	entry := findRuntimeLogByAction(t, bus.runtimeLogEntries(), accumulatorCompletionOutcomeAction)
	detail, _ := entry.Detail.(map[string]any)
	if got := runtimeLogDetailString(detail["decision_reason_code"]); got != "transaction_commit_failed" {
		t.Fatalf("decision_reason_code = %q, want transaction_commit_failed", got)
	}
	if got := runtimeLogDetailString(detail["evaluation_outcome"]); got != "succeeded" {
		t.Fatalf("evaluation_outcome = %q, want succeeded", got)
	}
	if got := runtimeLogDetailString(detail["commit_outcome"]); got != "rolled_back" {
		t.Fatalf("commit_outcome = %q, want rolled_back", got)
	}

	instance, ok, loadErr := pc.workflowStore.Load(testPipelineCoordinatorRunContext(t, pc), entityID)
	if loadErr != nil {
		t.Fatalf("load workflow instance: %v", loadErr)
	}
	if !ok {
		t.Fatal("expected seeded workflow instance to remain available")
	}
	if got := strings.TrimSpace(instance.CurrentState); got != "researching" {
		t.Fatalf("current_state = %q, want researching after rollback", got)
	}
}

func TestExecuteNodeContractHandlerPersistsArithmeticDataAccumulationExpression(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc := NewPipelineCoordinatorWithOptions(&recordingPipelineBus{}, db, PipelineCoordinatorOptions{
		Module: &previewWorkflowModule{bundle: &runtimecontracts.WorkflowContractBundle{
			Semantics: runtimecontracts.WorkflowSemanticView{
				Name:    "validation",
				Version: "v-test",
				EntitySchema: runtimecontracts.EntitySchema{
					Groups: []runtimecontracts.EntitySchemaGroup{
						{
							Name: "tracking",
							Fields: []runtimecontracts.EntitySchemaField{
								{Name: "revision_count", Type: "integer"},
							},
						},
					},
				},
			},
		}},
	})
	const entityID = "11111111-1111-1111-1111-111111111111"
	if err := pc.workflowStore.Upsert(testPipelineCoordinatorRunContext(t, pc), WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "validation",
		WorkflowVersion: "v-test",
		CurrentState:    "queued",
		Metadata: map[string]any{
			"revision_count": 0,
		},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	_, err := pc.executeNodeContractHandler(testPipelineCoordinatorRunContext(t, pc), "node-a", runtimecontracts.SystemNodeEventHandler{
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "revision_count", Value: runtimecontracts.CELExpression("entity.revision_count + 1")},
			},
		},
	}, workflowTriggerContext{
		Event: eventtest.RootIngress(
			"",
			events.EventType("validation.spec_requested"),
			"",
			"",
			nil,
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
			time.Time{},
		),
		State: pc.currentWorkflowState(testPipelineCoordinatorRunContext(t, pc), entityID),
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}

	instance, ok, err := pc.workflowStore.Load(testPipelineCoordinatorRunContext(t, pc), entityID)
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("workflow instance missing after declarative write")
	}
	switch got := instance.Metadata["revision_count"].(type) {
	case int:
		if got != 1 {
			t.Fatalf("revision_count = %d, want 1", got)
		}
	case float64:
		if got != 1 {
			t.Fatalf("revision_count = %v, want 1", got)
		}
	default:
		t.Fatalf("revision_count = %#v (%T), want 1", instance.Metadata["revision_count"], instance.Metadata["revision_count"])
	}
}

func TestExecuteNodeContractHandlerFailsClosedOnDataAccumulationCELRuntimeError(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc := NewPipelineCoordinatorWithOptions(&recordingPipelineBus{}, db, PipelineCoordinatorOptions{
		Module: &previewWorkflowModule{bundle: &runtimecontracts.WorkflowContractBundle{
			Semantics: runtimecontracts.WorkflowSemanticView{
				Name:    "validation",
				Version: "v-test",
				EntitySchema: runtimecontracts.EntitySchema{
					Groups: []runtimecontracts.EntitySchemaGroup{
						{
							Name: "tracking",
							Fields: []runtimecontracts.EntitySchemaField{
								{Name: "revision_count", Type: "integer"},
							},
						},
					},
				},
			},
		}},
	})
	const entityID = "11111111-1111-1111-1111-111111111111"
	if err := pc.workflowStore.Upsert(testPipelineCoordinatorRunContext(t, pc), WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "validation",
		WorkflowVersion: "v-test",
		CurrentState:    "queued",
		Metadata:        map[string]any{},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	_, err := pc.executeNodeContractHandler(testPipelineCoordinatorRunContext(t, pc), "node-a", runtimecontracts.SystemNodeEventHandler{
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "revision_count", Value: runtimecontracts.CELExpression("entity.revision_count + 1")},
			},
		},
	}, workflowTriggerContext{
		Event: eventtest.RootIngress(
			"",
			events.EventType("validation.spec_requested"),
			"",
			"",
			nil,
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
			time.Time{},
		),
		State: pc.currentWorkflowState(testPipelineCoordinatorRunContext(t, pc), entityID),
	}, false)
	if err == nil {
		t.Fatal("expected executeNodeContractHandler to fail on data accumulation CEL runtime error")
	}
	if !strings.Contains(err.Error(), "data_accumulation target revision_count") && !strings.Contains(err.Error(), "data_accumulation target entity.revision_count") {
		t.Fatalf("error = %v, want data_accumulation target context", err)
	}

	instance, ok, loadErr := pc.workflowStore.Load(testPipelineCoordinatorRunContext(t, pc), entityID)
	if loadErr != nil {
		t.Fatalf("load workflow instance: %v", loadErr)
	}
	if !ok {
		t.Fatal("expected workflow instance to remain available")
	}
	if _, exists := instance.Metadata["revision_count"]; exists {
		t.Fatalf("revision_count unexpectedly persisted after CEL runtime error: %#v", instance.Metadata["revision_count"])
	}
}

func TestExecuteNodeContractHandlerPersistsNullPresenceCheckDataAccumulationExpression(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc := NewPipelineCoordinatorWithOptions(&recordingPipelineBus{}, db, PipelineCoordinatorOptions{
		Module: &previewWorkflowModule{bundle: &runtimecontracts.WorkflowContractBundle{
			Semantics: runtimecontracts.WorkflowSemanticView{
				Name:    "validation",
				Version: "v-test",
				EntitySchema: runtimecontracts.EntitySchema{
					Groups: []runtimecontracts.EntitySchemaGroup{
						{
							Name: "tracking",
							Fields: []runtimecontracts.EntitySchemaField{
								{Name: "kill_reason", Type: "text"},
								{Name: "kill_reason_missing", Type: "boolean"},
							},
						},
					},
				},
			},
		}},
	})
	const entityID = "11111111-1111-1111-1111-111111111111"
	if err := pc.workflowStore.Upsert(testPipelineCoordinatorRunContext(t, pc), WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "validation",
		WorkflowVersion: "v-test",
		CurrentState:    "queued",
		Metadata:        map[string]any{},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	_, err := pc.executeNodeContractHandler(testPipelineCoordinatorRunContext(t, pc), "node-a", runtimecontracts.SystemNodeEventHandler{
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "kill_reason_missing", Value: runtimecontracts.CELExpression("entity.kill_reason == null")},
			},
		},
	}, workflowTriggerContext{
		Event: eventtest.RootIngress(
			"",
			events.EventType("validation.spec_requested"),
			"",
			"",
			nil,
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
			time.Time{},
		),
		State: pc.currentWorkflowState(testPipelineCoordinatorRunContext(t, pc), entityID),
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}

	instance, ok, err := pc.workflowStore.Load(testPipelineCoordinatorRunContext(t, pc), entityID)
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("workflow instance missing after declarative write")
	}
	if got := instance.Metadata["kill_reason_missing"]; got != true {
		t.Fatalf("kill_reason_missing = %#v, want true", got)
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

	gotID, gotEvt := resolveHandlerEntityIDForFlow(source, "scoring", handler, entityID, eventtest.RootIngress(
		"",
		events.EventType("vertical.discovered"),
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
		time.Time{},
	),
		&state)

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

func newAccumulatorOutcomeTestCoordinator(db *sql.DB) (*PipelineCoordinator, *recordingPipelineBus) {
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
							{Name: "expected_count", Type: "integer"},
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

func findRuntimeLogByAction(t *testing.T, logs []RuntimeLogEntry, action string) RuntimeLogEntry {
	t.Helper()
	for _, entry := range logs {
		if strings.TrimSpace(entry.Action) == strings.TrimSpace(action) {
			return entry
		}
	}
	t.Fatalf("missing runtime log action %q in %#v", action, logs)
	return RuntimeLogEntry{}
}

func runtimeLogDetailString(v any) string {
	text, _ := v.(string)
	return strings.TrimSpace(text)
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

	gotID, gotEvt := resolveHandlerEntityIDForFlow(source, "scoring", handler, incomingEntityID, eventtest.RootIngress(
		"",
		events.EventType("vertical.discovered"),
		"",
		"",
		mustJSON(map[string]any{"entity_id": incomingEntityID}),
		0,
		"",
		"",
		events.EventEnvelope{EntityID: incomingEntityID},
		time.Time{},
	),
		&state)

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

func TestResolveHandlerEntityIDForRootKeepsFlowScopedInboundEntity(t *testing.T) {
	handler := runtimecontracts.SystemNodeEventHandler{
		Emit: runtimecontracts.EmitSpec{Event: "pipeline.complete"},
	}
	const inboundEntityID = "ent-child"
	state := WorkflowState{
		EntityID: inboundEntityID,
		Metadata: map[string]any{},
	}

	gotID, gotEvt := resolveHandlerEntityIDForFlow(nil, "", handler, inboundEntityID, eventtest.RootIngress(
		"",
		events.EventType("child/grandchild/task.done"),
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, inboundEntityID),
		time.Time{},
	),
		&state)

	if gotID != inboundEntityID {
		t.Fatalf("entityID = %q, want inbound %q", gotID, inboundEntityID)
	}
	if got := gotEvt.EntityID(); got != inboundEntityID {
		t.Fatalf("inbound event entity_id = %q, want preserved %q", got, inboundEntityID)
	}
	if state.EntityID != inboundEntityID {
		t.Fatalf("state entity_id = %q, want %q", state.EntityID, inboundEntityID)
	}
}

func TestResolveHandlerEntityIDForFlowKeepsInboundEntityForDescendantScopedInbound(t *testing.T) {
	handler := runtimecontracts.SystemNodeEventHandler{
		Emit: runtimecontracts.EmitSpec{Event: "step.result"},
	}
	const inboundEntityID = "ent-grandchild"
	const parentEntityID = "ent-child"
	state := WorkflowState{
		EntityID: inboundEntityID,
		Metadata: map[string]any{
			"flow_path":        "child/grandchild/inst-1",
			"parent_entity_id": parentEntityID,
		},
	}

	gotID, gotEvt := resolveHandlerEntityIDForFlow(nil, "child", handler, inboundEntityID, eventtest.RootIngress(
		"",
		events.EventType("child/grandchild/micro.done"),
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, inboundEntityID),
		time.Time{},
	),
		&state)

	if gotID != inboundEntityID {
		t.Fatalf("entityID = %q, want inbound %q", gotID, inboundEntityID)
	}
	if got := gotEvt.EntityID(); got != inboundEntityID {
		t.Fatalf("inbound event entity_id = %q, want preserved %q", got, inboundEntityID)
	}
	if state.EntityID != inboundEntityID {
		t.Fatalf("state entity_id = %q, want %q", state.EntityID, inboundEntityID)
	}
}

func TestResolveHandlerEntityIDForFlowDoesNotRetargetSameFlowInstancePath(t *testing.T) {
	handler := runtimecontracts.SystemNodeEventHandler{
		Emit: runtimecontracts.EmitSpec{Event: "step.result"},
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
		},
	}

	gotID, gotEvt := resolveHandlerEntityIDForFlow(nil, "child", handler, entityID, eventtest.RootIngress(
		"",
		events.EventType("child/grandchild/micro.done"),
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
		time.Time{},
	),
		&state)

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

func TestResolveHandlerEntityIDForFlowCreateEntitySeedsInitialStateAndSchemaDefaults(t *testing.T) {
	source := loadWorkflowTempSource(t, map[string]string{
		"package.yaml": `
name: runtime-test
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: scoring
    flow: scoring
    mode: static
`,
		"schema.yaml": "name: runtime-test\n",
		"flows/scoring/schema.yaml": `
name: scoring
mode: static
initial_state: queued
states: [queued]
`,
		"flows/scoring/entities.yaml": `
vertical:
  revision_count:
    type: integer
    initial: 0
  is_duplicate:
    type: boolean
    initial: false
`,
	})
	handler := runtimecontracts.SystemNodeEventHandler{CreateEntity: true}
	const inboundEntityID = "ent-parent"
	state := WorkflowState{
		EntityID: inboundEntityID,
		Stage:    WorkflowStateID("queued"),
		Status:   "active",
		Metadata: map[string]any{"name": "Parent"},
	}
	inbound := eventtest.RootIngress(
		"",
		events.EventType("vertical.discovered"),
		"",
		"",
		mustJSON(map[string]any{"entity_id": inboundEntityID, "name": "Parent"}),
		0,
		"",
		"",
		events.EventEnvelope{EntityID: inboundEntityID},
		time.Time{},
	)

	gotID, gotEvt := resolveHandlerEntityIDForFlow(source, "scoring", handler, inboundEntityID, inbound, &state)

	if gotID != FlowInstanceEntityID("scoring/scoring") {
		t.Fatalf("entityID = %q, want canonical flow primary %q", gotID, FlowInstanceEntityID("scoring/scoring"))
	}
	if got := gotEvt.EntityID(); got != inboundEntityID {
		t.Fatalf("inbound event entity_id = %q, want preserved %q", got, inboundEntityID)
	}
	if state.EntityID != gotID {
		t.Fatalf("state entity_id = %q, want %q", state.EntityID, gotID)
	}
	if state.Stage != "queued" {
		t.Fatalf("state stage = %q, want queued", state.Stage)
	}
	if state.Status != "" {
		t.Fatalf("state status = %q, want cleared", state.Status)
	}
	if state.Metadata == nil {
		t.Fatal("state metadata = nil, want create entity metadata")
	}
	if got := strings.TrimSpace(asString(state.Metadata["subject_id"])); got != "" {
		t.Fatalf("state subject_id = %q, want empty", got)
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
	if got := strings.TrimSpace(asString(state.Metadata["entity_type"])); got != "vertical" {
		t.Fatalf("state entity_type = %q, want vertical", got)
	}
	if got := state.Metadata["revision_count"]; !isZeroIntegerValue(got) {
		t.Fatalf("state revision_count = %#v, want 0", got)
	}
	if got := state.Metadata["is_duplicate"]; got != false {
		t.Fatalf("state is_duplicate = %#v, want false", got)
	}
	if wantEntityID := FlowInstanceEntityID("scoring/" + instanceID); gotID != wantEntityID {
		t.Fatalf("entityID = %q, want persisted flow entity id %q", gotID, wantEntityID)
	}
}

func TestHandlerExecutionStateSnapshotCreateEntityIncludesInitialStateAndDefaults(t *testing.T) {
	snapshot, err := handlerExecutionStateSnapshot(runtimecontracts.SystemNodeEventHandler{CreateEntity: true}, "ent-child", WorkflowState{
		EntityID: "ent-parent",
		Stage:    WorkflowStateID("queued"),
		Metadata: map[string]any{
			"revision_count": 0,
			"is_duplicate":   false,
		},
	}, "validation", "v-test")
	if err != nil {
		t.Fatalf("handlerExecutionStateSnapshot: %v", err)
	}

	if got := snapshot.EntityID.String(); got != "ent-child" {
		t.Fatalf("snapshot entity_id = %q, want ent-child", got)
	}
	if snapshot.CurrentState != "queued" {
		t.Fatalf("snapshot current_state = %q, want queued", snapshot.CurrentState)
	}
	if snapshot.WorkflowName != "validation" {
		t.Fatalf("snapshot workflow_name = %q, want validation", snapshot.WorkflowName)
	}
	if snapshot.WorkflowVersion != "v-test" {
		t.Fatalf("snapshot workflow_version = %q, want v-test", snapshot.WorkflowVersion)
	}
	if snapshot.Metadata == nil {
		t.Fatal("snapshot metadata = nil, want persisted metadata")
	}
	if got := snapshot.Metadata["revision_count"]; got != 0 {
		t.Fatalf("snapshot revision_count = %#v, want 0", got)
	}
	if got := snapshot.Metadata["is_duplicate"]; got != false {
		t.Fatalf("snapshot is_duplicate = %#v, want false", got)
	}
}

func TestExecuteNodeContractHandlerRejectsMalformedPersistedGateShape(t *testing.T) {
	pc := &PipelineCoordinator{
		bus:            &recordingPipelineBus{},
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
	}

	_, err := pc.executeNodeContractHandler(testPipelineCoordinatorRunContext(t, pc), "node-a", runtimecontracts.SystemNodeEventHandler{}, workflowTriggerContext{
		Event: eventtest.RootIngress("", events.EventType("custom.trigger"), "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"), time.Time{}),
		State: WorkflowState{
			Stage:    WorkflowStateID("queued"),
			Metadata: map[string]any{"gates": "invalid"},
		},
	}, false)
	if err == nil {
		t.Fatal("expected malformed persisted gates to fail closed")
	}
	if !strings.Contains(err.Error(), "metadata.gates") {
		t.Fatalf("error = %v, want metadata.gates context", err)
	}
}

func TestResolveHandlerEntityIDForFlowCreateEntityDoesNotSeedSubjectID(t *testing.T) {
	handler := runtimecontracts.SystemNodeEventHandler{CreateEntity: true}
	state := WorkflowState{}

	gotID, _ := resolveHandlerEntityIDForFlow(nil, "scoring", handler, "", eventtest.RootIngress("", events.EventType("vertical.discovered"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}), &state)

	if want := FlowInstanceEntityID("scoring/scoring"); gotID != want {
		t.Fatalf("entityID = %q, want canonical flow primary %q", gotID, want)
	}
	if state.Metadata == nil {
		t.Fatal("state metadata = nil, want create entity metadata")
	}
	if got := strings.TrimSpace(asString(state.Metadata["subject_id"])); got != "" {
		t.Fatalf("state subject_id = %q, want empty", got)
	}
}

func TestExecuteNodeContractHandlerCreateEntityPersistsSchemaInitialValuesBeforeGuardReads(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	bus := &recordingPipelineBus{}
	source := loadWorkflowTempSource(t, map[string]string{
		"package.yaml": `
name: runtime-test
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: validation
    flow: validation
    mode: static
`,
		"schema.yaml": "name: runtime-test\n",
		"flows/validation/schema.yaml": `
name: validation
mode: static
initial_state: queued
states: [queued]
`,
		"flows/validation/entities.yaml": `
validation_entity:
  revision_count:
    type: integer
    initial: 0
  kill_reason: text
`,
		"flows/validation/nodes.yaml": `
node-a:
  id: node-a
  execution_type: system_node
`,
	})
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected temp workflow bundle")
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
				{Name: "queued"},
			}, nil),
		},
	}

	result, err := pc.executeNodeContractHandler(testPipelineCoordinatorRunContext(t, pc), "node-a", runtimecontracts.SystemNodeEventHandler{
		CreateEntity: true,
		Guard:        &runtimecontracts.GuardSpec{Check: `entity.revision_count == 0 && entity.kill_reason == ""`},
		Emit: runtimecontracts.EmitSpec{
			Event: "entity.created",
			Fields: map[string]runtimecontracts.ExpressionValue{
				"revision_count": runtimecontracts.CELExpression("entity.revision_count"),
			},
		},
	}, workflowTriggerContext{
		Event: eventtest.RootIngress("", events.EventType("candidate.discovered"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
		State: WorkflowState{},
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
	emitted := bus.publishedEvent(0)
	entityID := emitted.EntityID()
	if entityID == "" {
		t.Fatal("expected emitted event to carry created entity id")
	}
	var payload map[string]any
	if err := json.Unmarshal(emitted.Payload(), &payload); err != nil {
		t.Fatalf("unmarshal emitted payload: %v", err)
	}
	if got := payload["revision_count"]; got != float64(0) && got != 0 {
		t.Fatalf("emitted payload revision_count = %#v, want 0", got)
	}

	instance, ok, err := pc.workflowStore.Load(testPipelineCoordinatorRunContext(t, pc), entityID)
	if err != nil {
		t.Fatalf("workflowStore.Load: %v", err)
	}
	if !ok {
		t.Fatal("expected created entity to persist")
	}
	if got := instance.Metadata["revision_count"]; got != float64(0) && got != 0 {
		t.Fatalf("persisted revision_count = %#v, want 0", got)
	}
	assertCreatedChildFlowIdentityCoherent(t, db, "validation", entityID, emitted, instance)

	rows, err := db.QueryContext(context.Background(), `
		SELECT field, COALESCE(writer_type, ''), COALESCE(writer_id, ''), COALESCE(handler_step, '')
		FROM entity_mutations
		WHERE entity_id = $1::uuid
		ORDER BY field, created_at
	`, entityID)
	if err != nil {
		t.Fatalf("query entity_mutations: %v", err)
	}
	defer rows.Close()

	initialMutations := map[string][3]string{}
	for rows.Next() {
		var field, writerType, writerID, handlerStep string
		if err := rows.Scan(&field, &writerType, &writerID, &handlerStep); err != nil {
			t.Fatalf("scan entity_mutations: %v", err)
		}
		if writerType == "platform" && writerID == "entity_initial_value" {
			initialMutations[field] = [3]string{writerType, writerID, handlerStep}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows error: %v", err)
	}
	if got, ok := initialMutations["revision_count"]; !ok {
		t.Fatalf("expected initial-value mutation for revision_count, got %#v", initialMutations)
	} else if got[2] != "create_entity" {
		t.Fatalf("revision_count initial mutation metadata = %#v, want handler_step create_entity", got)
	}
}

func TestExecuteNodeContractHandlerQueryEntitiesGuardUsesWorkflowContext(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	bus := &recordingPipelineBus{}
	source := loadWorkflowTempSource(t, map[string]string{
		"package.yaml": `
name: runtime-test
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: validation
    flow: validation
    mode: static
`,
		"schema.yaml": "name: runtime-test\n",
		"flows/validation/schema.yaml": `
name: validation
mode: static
initial_state: queued
states: [queued]
`,
		"flows/validation/entities.yaml": `
validation_request:
  request_id: text
`,
		"flows/validation/nodes.yaml": `
node-a:
  id: node-a
  execution_type: system_node
`,
	})
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected temp workflow bundle")
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
				{Name: "queued"},
			}, nil),
		},
	}
	ctx := testPipelineCoordinatorRunContext(t, pc)
	const otherRunID = "88888888-8888-8888-8888-888888888888"
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO runs (run_id, status)
		VALUES ($1::uuid, 'running')
		ON CONFLICT (run_id) DO NOTHING
	`, otherRunID); err != nil {
		t.Fatalf("seed other run: %v", err)
	}
	otherCtx := runtimecorrelation.WithRunID(context.Background(), otherRunID)
	seedQueryEntitiesGuardInstance(t, pc.workflowStore, ctx, "11111111-1111-1111-1111-111111111111", "validation/existing-same", "req-existing")
	seedQueryEntitiesGuardInstance(t, pc.workflowStore, otherCtx, "22222222-2222-2222-2222-222222222222", "validation/existing-other", "req-cross-run")

	handler := runtimecontracts.SystemNodeEventHandler{
		CreateEntity: true,
		Guard:        &runtimecontracts.GuardSpec{Check: `query_entities(request_id == payload.request_id).count == 0`},
		Emit: runtimecontracts.EmitSpec{
			Event: "request.accepted",
			Fields: map[string]runtimecontracts.ExpressionValue{
				"request_id": runtimecontracts.RefExpression("payload.request_id"),
			},
		},
	}
	runHandler := func(entityID, requestID string) error {
		_, err := pc.executeNodeContractHandler(ctx, "node-a", handler, workflowTriggerContext{
			Event: eventtest.RootIngress(
				"evt-"+requestID,
				events.EventType("request.received"),
				"",
				"",
				mustJSON(map[string]any{"request_id": requestID}),
				0,
				testPipelineRunID,
				"",
				events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
				time.Time{},
			),

			State: WorkflowState{EntityID: entityID, Stage: WorkflowStateID("queued"), Metadata: map[string]any{}},
		}, false)
		return err
	}

	if err := runHandler("33333333-3333-3333-3333-333333333333", "req-new"); err != nil {
		t.Fatalf("execute handler for new request: %v", err)
	}
	if err := runHandler("44444444-4444-4444-4444-444444444444", "req-cross-run"); err != nil {
		t.Fatalf("execute handler for cross-run request: %v", err)
	}
	if got := bus.publishedCount(); got != 2 {
		t.Fatalf("published count after accepted requests = %d, want 2", got)
	}
	if err := runHandler("55555555-5555-5555-5555-555555555555", "req-existing"); err != nil {
		t.Fatalf("execute handler for duplicate request: %v", err)
	}
	if got := bus.publishedCount(); got != 2 {
		t.Fatalf("published count after duplicate request = %d, want unchanged 2", got)
	}
}

func seedQueryEntitiesGuardInstance(t *testing.T, store *WorkflowInstanceStore, ctx context.Context, entityID, storageRef, requestID string) {
	t.Helper()
	if err := store.Upsert(ctx, WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      storageRef,
		WorkflowName:    "validation",
		WorkflowVersion: "1.0.0",
		CurrentState:    "queued",
		Metadata: map[string]any{
			"entity_id":  entityID,
			"request_id": requestID,
		},
	}); err != nil {
		t.Fatalf("seed query_entities guard instance %s: %v", entityID, err)
	}
}

func TestExecuteNodeContractHandlerCreateEntityPersistsNonValidationChildFlowIdentity(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	bus := &recordingPipelineBus{}
	source := loadWorkflowTempSource(t, map[string]string{
		"package.yaml": `
name: runtime-test
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: review
    flow: review
    mode: static
`,
		"schema.yaml": "name: runtime-test\n",
		"flows/review/schema.yaml": `
name: review
mode: static
initial_state: queued
states: [queued]
`,
		"flows/review/entities.yaml": `
review_entity:
  status:
    type: text
    initial: pending
`,
		"flows/review/nodes.yaml": `
node-a:
  id: node-a
  execution_type: system_node
`,
	})
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected temp workflow bundle")
	}
	pc := &PipelineCoordinator{
		bus:            bus,
		db:             db,
		workflowStore:  NewWorkflowInstanceStore(db),
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
		module: &previewWorkflowModule{
			bundle: bundle,
			workflow: NewWorkflowDefinition("review", []WorkflowStage{
				{Name: "queued"},
			}, nil),
		},
	}

	result, err := pc.executeNodeContractHandler(testPipelineCoordinatorRunContext(t, pc), "node-a", runtimecontracts.SystemNodeEventHandler{
		CreateEntity: true,
		Guard:        &runtimecontracts.GuardSpec{Check: `entity.status == "pending"`},
		Emit: runtimecontracts.EmitSpec{
			Event: "review.created",
			Fields: map[string]runtimecontracts.ExpressionValue{
				"status": runtimecontracts.CELExpression("entity.status"),
			},
		},
	}, workflowTriggerContext{
		Event: eventtest.RootIngress("", events.EventType("candidate.ready"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
		State: WorkflowState{},
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
	emitted := bus.publishedEvent(0)
	entityID := emitted.EntityID()
	if entityID == "" {
		t.Fatal("expected emitted event to carry created entity id")
	}
	instance, ok, err := pc.workflowStore.Load(testPipelineCoordinatorRunContext(t, pc), entityID)
	if err != nil {
		t.Fatalf("workflowStore.Load: %v", err)
	}
	if !ok {
		t.Fatal("expected created entity to persist")
	}
	if got := instance.Metadata["status"]; got != "pending" {
		t.Fatalf("persisted status = %#v, want pending", got)
	}
	assertCreatedChildFlowIdentityCoherent(t, db, "review", entityID, emitted, instance)
}

func assertCreatedChildFlowIdentityCoherent(t *testing.T, db *sql.DB, flowID, entityID string, emitted events.Event, instance WorkflowInstance) {
	t.Helper()
	instanceID := strings.TrimSpace(asString(instance.Metadata["instance_id"]))
	if instanceID == "" {
		t.Fatalf("created %s entity %s missing instance_id metadata: %#v", flowID, entityID, instance.Metadata)
	}
	flowPath := flowID + "/" + instanceID
	if got := strings.TrimSpace(instance.StorageRef); got != flowPath {
		t.Fatalf("created %s entity storage_ref = %q, want %q", flowID, got, flowPath)
	}
	if got := strings.TrimSpace(asString(instance.Metadata["storage_ref"])); got != flowPath {
		t.Fatalf("created %s entity metadata storage_ref = %q, want %q", flowID, got, flowPath)
	}
	if got := strings.TrimSpace(asString(instance.Metadata["flow_path"])); got != flowPath {
		t.Fatalf("created %s entity flow_path = %q, want %q", flowID, got, flowPath)
	}
	if got := entityID; got != FlowInstanceEntityID(flowPath) {
		t.Fatalf("created %s entity id = %q, want %q for flow path %q", flowID, got, FlowInstanceEntityID(flowPath), flowPath)
	}
	if got := emitted.FlowInstance(); got != flowPath {
		t.Fatalf("created %s emitted flow_instance = %q, want %q", flowID, got, flowPath)
	}
	var rowOwner string
	if err := db.QueryRowContext(context.Background(), `
		SELECT flow_instance
		FROM entity_state
		WHERE entity_id = $1::uuid
	`, entityID).Scan(&rowOwner); err != nil {
		t.Fatalf("query created %s entity_state owner: %v", flowID, err)
	}
	if got := strings.TrimSpace(rowOwner); got != flowPath {
		t.Fatalf("created %s entity_state.flow_instance = %q, want %q", flowID, got, flowPath)
	}
}

func TestExecuteNodeContractHandlerCreateEntityAllowsLaterClearOfSchemaInitialValue(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	bus := &recordingPipelineBus{}
	source := loadWorkflowTempSource(t, map[string]string{
		"package.yaml": `
name: runtime-test
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: validation
    flow: validation
    mode: static
`,
		"schema.yaml": "name: runtime-test\n",
		"flows/validation/schema.yaml": `
name: validation
mode: static
initial_state: queued
states: [queued]
`,
		"flows/validation/entities.yaml": `
validation_entity:
  revision_count:
    type: integer
    initial: 0
`,
		"flows/validation/nodes.yaml": `
node-a:
  id: node-a
  execution_type: system_node
`,
	})
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected temp workflow bundle")
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
				{Name: "queued"},
			}, nil),
		},
	}

	result, err := pc.executeNodeContractHandler(testPipelineCoordinatorRunContext(t, pc), "node-a", runtimecontracts.SystemNodeEventHandler{
		CreateEntity: true,
		Clear:        &runtimecontracts.ClearSpec{Targets: []string{"entity.revision_count"}},
		Emit:         runtimecontracts.EmitSpec{Event: "entity.created"},
	}, workflowTriggerContext{
		Event: eventtest.RootIngress("", events.EventType("candidate.discovered"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
		State: WorkflowState{},
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
	entityID := bus.publishedEvent(0).EntityID()
	if entityID == "" {
		t.Fatal("expected emitted event to carry created entity id")
	}

	instance, ok, err := pc.workflowStore.Load(testPipelineCoordinatorRunContext(t, pc), entityID)
	if err != nil {
		t.Fatalf("workflowStore.Load: %v", err)
	}
	if !ok {
		t.Fatal("expected created entity to persist")
	}
	if _, ok := instance.Metadata["revision_count"]; ok {
		t.Fatalf("persisted revision_count = %#v, want field cleared", instance.Metadata["revision_count"])
	}

	rows, err := db.QueryContext(context.Background(), `
		SELECT field, COALESCE(writer_type, ''), COALESCE(writer_id, ''), COALESCE(handler_step, '')
		FROM entity_mutations
		WHERE entity_id = $1::uuid AND field = 'revision_count'
		ORDER BY created_at
	`, entityID)
	if err != nil {
		t.Fatalf("query entity_mutations: %v", err)
	}
	defer rows.Close()

	var sawInitial bool
	for rows.Next() {
		var field, writerType, writerID, handlerStep string
		if err := rows.Scan(&field, &writerType, &writerID, &handlerStep); err != nil {
			t.Fatalf("scan entity_mutations: %v", err)
		}
		if writerType == "platform" && writerID == "entity_initial_value" && handlerStep == "create_entity" {
			sawInitial = true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows error: %v", err)
	}
	if !sawInitial {
		t.Fatal("expected initial-value mutation for revision_count before later clear")
	}
}

func TestPreviewContractHandlerExecutionShowsInitialValuesMaterialized(t *testing.T) {
	source := loadWorkflowTempSource(t, map[string]string{
		"package.yaml": `
name: runtime-test
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: validation
    flow: validation
    mode: static
`,
		"schema.yaml": "name: runtime-test\n",
		"flows/validation/schema.yaml": `
name: validation
mode: static
initial_state: queued
states: [queued]
`,
		"flows/validation/entities.yaml": `
validation_entity:
  revision_count:
    type: integer
    initial: 0
  is_duplicate:
    type: boolean
    initial: false
`,
		"flows/validation/nodes.yaml": `
node-a:
  id: node-a
  execution_type: system_node
  event_handlers:
    candidate.discovered:
      create_entity: true
      guard:
        check: entity.revision_count == 0
      emit: entity.created
`,
	})
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected temp workflow bundle")
	}

	preview, err := PreviewContractHandlerExecution(context.Background(), bundle, "node-a", eventtest.RootIngress("", events.EventType("candidate.discovered"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}), WorkflowState{}, nil)
	if err != nil {
		t.Fatalf("PreviewContractHandlerExecution: %v", err)
	}
	if got := preview.Metadata["revision_count"]; !isZeroIntegerValue(got) {
		t.Fatalf("preview revision_count = %#v, want 0", got)
	}
	if got := preview.InitialValues["revision_count"]; !isZeroIntegerValue(got) {
		t.Fatalf("preview initial revision_count = %#v, want 0", got)
	}
	if got := preview.InitialValues["is_duplicate"]; got != false {
		t.Fatalf("preview initial is_duplicate = %#v, want false", got)
	}
}

func isZeroIntegerValue(v any) bool {
	switch typed := v.(type) {
	case int:
		return typed == 0
	case int8:
		return typed == 0
	case int16:
		return typed == 0
	case int32:
		return typed == 0
	case int64:
		return typed == 0
	case uint:
		return typed == 0
	case uint8:
		return typed == 0
	case uint16:
		return typed == 0
	case uint32:
		return typed == 0
	case uint64:
		return typed == 0
	case float32:
		return typed == 0
	case float64:
		return typed == 0
	default:
		return false
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

	result, err := pc.executeNodeContractHandler(testPipelineCoordinatorRunContext(t, pc), "node-a", runtimecontracts.SystemNodeEventHandler{}, workflowTriggerContext{
		Event: eventtest.RootIngress("", events.EventType("custom.trigger"), "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"), time.Time{}),
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

func TestExecuteNodeContractHandlerAppliesEmitFieldsToEmittedEvent(t *testing.T) {
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{
		bus:            bus,
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
	}

	_, err := pc.executeNodeContractHandler(testPipelineCoordinatorRunContext(t, pc), "node-a", runtimecontracts.SystemNodeEventHandler{
		Emit: runtimecontracts.EmitSpec{
			Event: "custom.emitted",
			Fields: map[string]runtimecontracts.ExpressionValue{
				"summary.entity_id": runtimecontracts.CELExpression("_entity.id"),
				"summary.stage":     runtimecontracts.CELExpression("_entity.current_state"),
				"flags.ready":       runtimecontracts.CELExpression("true"),
				"label":             runtimecontracts.CELExpression(`"done"`),
			},
		},
	}, workflowTriggerContext{
		Event: eventtest.RootIngress(
			"",
			events.EventType("custom.trigger"),
			"",
			"",
			mustJSON(map[string]any{"entity_id": "ent-1"}),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
			time.Time{},
		),

		State: WorkflowState{Stage: WorkflowStateID("queued"), Metadata: map[string]any{"stage": "queued"}},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("bus published count = %d, want 1", got)
	}
	var payload map[string]any
	if err := json.Unmarshal(bus.publishedEvent(0).Payload(), &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	summary, _ := payload["summary"].(map[string]any)
	if got := summary["entity_id"]; got != "ent-1" {
		t.Fatalf("payload.summary.entity_id = %#v, want ent-1", got)
	}
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
	if _, ok := payload["entity_id"]; ok {
		t.Fatalf("payload must not carry envelope entity_id: %#v", payload["entity_id"])
	}
}

func TestExecuteNodeContractHandlerAppliesEmitFieldsSparseFieldPresenceCheck(t *testing.T) {
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{
		bus:            bus,
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
	}

	_, err := pc.executeNodeContractHandler(testPipelineCoordinatorRunContext(t, pc), "node-a", runtimecontracts.SystemNodeEventHandler{
		Emit: runtimecontracts.EmitSpec{
			Event: "custom.emitted",
			Fields: map[string]runtimecontracts.ExpressionValue{
				"kill_reason_missing": runtimecontracts.CELExpression("entity.kill_reason == null"),
			},
		},
	}, workflowTriggerContext{
		Event: eventtest.RootIngress(
			"",
			events.EventType("custom.trigger"),
			"",
			"",
			mustJSON(map[string]any{"entity_id": "ent-1"}),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
			time.Time{},
		),

		State: WorkflowState{Stage: WorkflowStateID("queued"), Metadata: map[string]any{}},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("bus published count = %d, want 1", got)
	}
	var payload map[string]any
	if err := json.Unmarshal(bus.publishedEvent(0).Payload(), &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if got := payload["kill_reason_missing"]; got != true {
		t.Fatalf("payload.kill_reason_missing = %#v, want true", got)
	}
}

func TestExecuteNodeContractHandlerEmitFieldsEntityPresenceCheckMintsEntityID(t *testing.T) {
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{
		bus:            bus,
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
		module: &previewWorkflowModule{
			bundle: func() *runtimecontracts.WorkflowContractBundle {
				source := loadWorkflowTempSource(t, map[string]string{
					"package.yaml": `
name: runtime-test
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: scoring
    flow: scoring
    mode: static
`,
					"schema.yaml": "name: runtime-test\n",
					"flows/scoring/schema.yaml": `
name: scoring
mode: static
initial_state: queued
states: [queued]
`,
					"flows/scoring/entities.yaml": `
subject:
  kill_reason: text
`,
					"flows/scoring/nodes.yaml": `
node-a:
  id: node-a
  execution_type: system_node
`,
				})
				bundle, ok := semanticview.Bundle(source)
				if !ok {
					t.Fatal("expected temp workflow bundle")
				}
				return bundle
			}(),
		},
	}

	_, err := pc.executeNodeContractHandler(testPipelineCoordinatorRunContext(t, pc), "node-a", runtimecontracts.SystemNodeEventHandler{
		Emit: runtimecontracts.EmitSpec{
			Event: "custom.emitted",
			Fields: map[string]runtimecontracts.ExpressionValue{
				"label": runtimecontracts.CELExpression(`entity.kill_reason != "" ? entity.kill_reason : payload.reason`),
			},
		},
	}, workflowTriggerContext{
		Event: eventtest.RootIngress("", events.EventType("custom.trigger"), "", "", mustJSON(map[string]any{"reason": "active"}), 0, "", "", events.EventEnvelope{}, time.Time{}),

		State: WorkflowState{Stage: WorkflowStateID("queued"), Metadata: map[string]any{}},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("bus published count = %d, want 1", got)
	}
	emitted := bus.publishedEvent(0)
	if got := emitted.EntityID(); got == "" {
		t.Fatal("expected emit.fields entity reference to mint entity_id")
	}
	var payload map[string]any
	if err := json.Unmarshal(emitted.Payload(), &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if got := payload["label"]; got != "active" {
		t.Fatalf("payload.label = %#v, want active", got)
	}
}

func TestExecuteNodeHandlerPlanResult_NestedDescendantCompletionDoesNotBackPropagateToRoot(t *testing.T) {
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
		bus:                     bus,
		db:                      db,
		module:                  module,
		workflowStore:           store,
		expressionEval:          newWorkflowExpressionEvaluator(),
		entityLocks:             map[string]*sync.Mutex{},
		eventReceiptsCapability: eventReceiptsCapabilityStub{enabled: true}.resolve,
	}

	const (
		rootEntityID = "11111111-1111-1111-1111-111111111111"
	)
	childEntityID := FlowInstanceEntityID("child/inst-1")
	grandchildEntityID := FlowInstanceEntityID("child/grandchild/inst-1")
	if err := store.Upsert(testWorkflowStoreRunContext(t, store), WorkflowInstance{
		InstanceID:      childEntityID,
		StorageRef:      "child/inst-1",
		WorkflowName:    "child",
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "waiting",
		Metadata: map[string]any{
			"entity_id":        childEntityID,
			"flow_path":        "child/inst-1",
			"parent_entity_id": rootEntityID,
		},
	}); err != nil {
		t.Fatalf("seed child instance: %v", err)
	}
	if err := store.Upsert(testWorkflowStoreRunContext(t, store), WorkflowInstance{
		InstanceID:      grandchildEntityID,
		StorageRef:      "child/grandchild/inst-1",
		WorkflowName:    "grandchild",
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "finished",
		Metadata: map[string]any{
			"entity_id":        grandchildEntityID,
			"flow_path":        "child/grandchild/inst-1",
			"parent_entity_id": childEntityID,
		},
	}); err != nil {
		t.Fatalf("seed grandchild instance: %v", err)
	}
	if consume, handled := pc.workflowNodeInterceptPolicy(context.Background(), "child/grandchild/micro.done", eventtest.RootIngress(
		"",
		events.EventType("child/grandchild/micro.done"),
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, grandchildEntityID),
		time.Time{},
	)); !handled {
		t.Fatalf("workflowNodeInterceptPolicy handled = %v, consume = %v, want handled", handled, consume)
	}

	evt := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("child/grandchild/micro.done"),
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, grandchildEntityID),
		time.Time{},
	)

	seedPipelineNodeDeliveryAuthority(t, db, evt, "root-collector")
	handled, err := pc.dispatchWorkflowNodeEventResult(testWorkflowStoreRunContext(t, store), evt)
	if err != nil {
		t.Fatalf("executeNodeHandlerPlanResult: %v", err)
	}
	if !handled {
		t.Fatal("expected handler to execute")
	}
	child, found, err := store.Load(testWorkflowStoreRunContext(t, store), childEntityID)
	if err != nil {
		t.Fatalf("load child instance: %v", err)
	}
	if !found {
		t.Fatal("expected child instance")
	}
	if got := strings.TrimSpace(child.CurrentState); got != "waiting" {
		t.Fatalf("child current_state = %q, want waiting", got)
	}
	if got := bus.publishedCount(); got != 0 {
		t.Fatalf("published count = %d, want 0 without subject-link back-propagation", got)
	}
}

func TestExecuteNodeContractHandlerRejectsAmbiguousHandlerTopLevelEmitWithRules(t *testing.T) {
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{
		bus:            bus,
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
	}

	_, err := pc.executeNodeContractHandler(testPipelineCoordinatorRunContext(t, pc), "node-a", runtimecontracts.SystemNodeEventHandler{
		Emit: runtimecontracts.EmitSpec{Event: "default.emitted"},
		Rules: []runtimecontracts.HandlerRuleEntry{
			{ID: "pick-rule", Condition: "true", Emit: runtimecontracts.EmitSpec{Event: "rule.emitted"}},
		},
	}, workflowTriggerContext{
		Event: eventtest.RootIngress(
			"",
			events.EventType("custom.trigger"),
			"",
			"",
			mustJSON(map[string]any{"entity_id": "ent-1"}),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
			time.Time{},
		),

		State: WorkflowState{Stage: WorkflowStateID("queued"), Metadata: map[string]any{}},
	}, false)
	if err == nil {
		t.Fatal("expected ambiguous handler-level emit config to be rejected")
	}
	if !strings.Contains(err.Error(), "handler-top-level emit is only allowed on single-emit handlers") {
		t.Fatalf("error = %v", err)
	}
}

func TestExecuteNodeContractHandlerRejectsAmbiguousHandlerTopLevelEmitWithRulesWithoutRuleEmit(t *testing.T) {
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{
		bus:            bus,
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
	}

	_, err := pc.executeNodeContractHandler(testPipelineCoordinatorRunContext(t, pc), "node-a", runtimecontracts.SystemNodeEventHandler{
		Emit: runtimecontracts.EmitSpec{Event: "default.emitted"},
		Rules: []runtimecontracts.HandlerRuleEntry{
			{ID: "pick-rule", Condition: "true", AdvancesTo: "done"},
		},
	}, workflowTriggerContext{
		Event: eventtest.RootIngress(
			"",
			events.EventType("custom.trigger"),
			"",
			"",
			mustJSON(map[string]any{"entity_id": "ent-1"}),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
			time.Time{},
		),

		State: WorkflowState{Stage: WorkflowStateID("queued"), Metadata: map[string]any{}},
	}, false)
	if err == nil {
		t.Fatal("expected ambiguous handler-level emit config to be rejected")
	}
	if !strings.Contains(err.Error(), "handler-top-level emit is only allowed on single-emit handlers") {
		t.Fatalf("error = %v", err)
	}
}

func TestExecuteNodeContractHandlerOnCompleteDoesNotSeeCurrentHandlerTopLevelWritesEarly(t *testing.T) {
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{
		bus:            bus,
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
	}

	_, err := pc.executeNodeContractHandler(testPipelineCoordinatorRunContext(t, pc), "node-a", runtimecontracts.SystemNodeEventHandler{
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "branch_target", Value: runtimecontracts.LiteralExpression("handler")},
			},
		},
		OnComplete: []runtimecontracts.HandlerRuleEntry{
			{ID: "too-early", Condition: `"branch_target" in entity && entity.branch_target == "handler"`, Emit: runtimecontracts.EmitSpec{Event: "branch.selected"}},
		},
	}, workflowTriggerContext{
		Event: eventtest.RootIngress("", events.EventType("custom.trigger"), "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"), time.Time{}),

		State: WorkflowState{EntityID: "ent-1", Stage: WorkflowStateID("queued"), Metadata: map[string]any{}},
	}, false)
	if err == nil {
		t.Fatal("expected missing early entity field to fail closed at runtime")
	}
	if !strings.Contains(err.Error(), "entity.branch_target") {
		t.Fatalf("error = %v, want missing entity.branch_target context", err)
	}
}

func TestExecuteNodeContractHandlerExecutesEmitInsideEngine(t *testing.T) {
	bus := &recordingPipelineBus{}
	pc := NewPipelineCoordinatorWithOptions(bus, nil, PipelineCoordinatorOptions{
		Module: NewGenericTestWorkflowModule(),
	})
	entityID := "ent-1"

	result, err := pc.executeNodeContractHandler(testPipelineCoordinatorRunContext(t, pc), "node-a", runtimecontracts.SystemNodeEventHandler{
		Emit: runtimecontracts.EmitSpec{Event: "custom.emitted"},
	}, workflowTriggerContext{
		Event: eventtest.RootIngress("", events.EventType("custom.trigger"), "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Time{}),
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

func TestExecuteNodeContractHandlerOnSuccessRulesEmitsBothInOrder(t *testing.T) {
	bus := &recordingPipelineBus{}
	pc := NewPipelineCoordinatorWithOptions(bus, nil, PipelineCoordinatorOptions{
		Module: &previewWorkflowModule{bundle: additiveOnSuccessContractBundle()},
	})
	entityID := "ent-1"

	result, err := pc.executeNodeContractHandler(testPipelineCoordinatorRunContext(t, pc), "node-a", runtimecontracts.SystemNodeEventHandler{
		OnSuccess: runtimecontracts.HandlerOnSuccessSpec{Emit: runtimecontracts.EmitSpec{Event: "handler.succeeded"}},
		Rules: []runtimecontracts.HandlerRuleEntry{
			{ID: "pick-rule", Condition: "true", Emit: runtimecontracts.EmitSpec{Event: "rule.emitted"}},
		},
	}, workflowTriggerContext{
		Event: eventtest.RootIngress("", events.EventType("custom.trigger"), "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Time{}),
		State: WorkflowState{Stage: WorkflowStateID("queued"), Metadata: map[string]any{}},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected handled result")
	}
	if got := bus.publishedCount(); got != 2 {
		t.Fatalf("bus published count = %d, want 2", got)
	}
	if got := []events.EventType{bus.publishes[0].Type(), bus.publishes[1].Type()}; !reflect.DeepEqual(got, []events.EventType{"rule.emitted", "handler.succeeded"}) {
		t.Fatalf("published order = %#v", got)
	}
}

func TestExecuteNodeContractHandlerRulesEmitTemplatePublishesOneMergedEvent(t *testing.T) {
	bus := &recordingPipelineBus{}
	pc := NewPipelineCoordinatorWithOptions(bus, nil, PipelineCoordinatorOptions{
		Module: &previewWorkflowModule{bundle: rulesEmitTemplateContractBundle()},
	})
	entityID := "ent-1"

	result, err := pc.executeNodeContractHandler(testPipelineCoordinatorRunContext(t, pc), "node-a", runtimecontracts.SystemNodeEventHandler{
		Emit: runtimecontracts.EmitSpec{
			Event: "account.bucketed",
			Fields: map[string]runtimecontracts.ExpressionValue{
				"account_id": runtimecontracts.CELExpression("payload.account_id"),
				"score":      runtimecontracts.CELExpression("payload.score"),
			},
		},
		Rules: []runtimecontracts.HandlerRuleEntry{
			{
				ID:        "high",
				Condition: "payload.score >= 80",
				Emit: runtimecontracts.EmitSpec{Fields: map[string]runtimecontracts.ExpressionValue{
					"bucket": runtimecontracts.CELExpression(`"high"`),
				}},
			},
			{
				ID:        "medium",
				Condition: "payload.score >= 40",
				Emit: runtimecontracts.EmitSpec{Fields: map[string]runtimecontracts.ExpressionValue{
					"bucket": runtimecontracts.CELExpression(`"medium"`),
				}},
			},
			{
				ID:        "low",
				Condition: "else",
				Emit: runtimecontracts.EmitSpec{Fields: map[string]runtimecontracts.ExpressionValue{
					"bucket": runtimecontracts.CELExpression(`"low"`),
				}},
			},
		},
	}, workflowTriggerContext{
		Event: eventtest.RootIngress(
			"",
			events.EventType("account.scored"),
			"",
			"",
			mustJSON(map[string]any{"account_id": "acct-1", "score": 91}),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
			time.Time{},
		),
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
	emitted := bus.publishedEvent(0)
	if got := emitted.Type(); got != events.EventType("account.bucketed") {
		t.Fatalf("published event = %q, want account.bucketed", got)
	}
	var payload map[string]any
	if err := json.Unmarshal(emitted.Payload(), &payload); err != nil {
		t.Fatalf("json.Unmarshal payload: %v", err)
	}
	if got := payload["account_id"]; got != "acct-1" {
		t.Fatalf("account_id = %#v, want acct-1", got)
	}
	if got := payload["bucket"]; got != "high" {
		t.Fatalf("bucket = %#v, want high", got)
	}
	if got := int(payload["score"].(float64)); got != 91 {
		t.Fatalf("score = %#v, want 91", payload["score"])
	}
}

func TestExecuteNodeContractHandlerOnSuccessOutboxFailureDoesNotPartiallyPublish(t *testing.T) {
	bus := &recordingPipelineBus{outboxErr: errors.New("outbox failed")}
	pc := NewPipelineCoordinatorWithOptions(bus, nil, PipelineCoordinatorOptions{
		Module: &previewWorkflowModule{bundle: additiveOnSuccessContractBundle()},
	})

	_, err := pc.executeNodeContractHandler(testPipelineCoordinatorRunContext(t, pc), "node-a", runtimecontracts.SystemNodeEventHandler{
		AdvancesTo: "done",
		OnSuccess:  runtimecontracts.HandlerOnSuccessSpec{Emit: runtimecontracts.EmitSpec{Event: "handler.succeeded"}},
		Rules: []runtimecontracts.HandlerRuleEntry{
			{ID: "pick-rule", Condition: "true", Emit: runtimecontracts.EmitSpec{Event: "rule.emitted"}},
		},
	}, workflowTriggerContext{
		Event: eventtest.RootIngress("", events.EventType("custom.trigger"), "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"), time.Time{}),
		State: WorkflowState{Stage: WorkflowStateID("queued"), Metadata: map[string]any{}},
	}, false)
	if err == nil || !strings.Contains(err.Error(), "outbox failed") {
		t.Fatalf("executeNodeContractHandler error = %v, want outbox failed", err)
	}
	if got := len(bus.outboxIntents); got != 0 {
		t.Fatalf("outbox intents len = %d, want 0 after outbox failure", got)
	}
	if got := bus.publishedCount(); got != 0 {
		t.Fatalf("bus published count = %d, want 0 after outbox failure", got)
	}
}

func declarativeEmitContractTestBundle(eventType string) *runtimecontracts.WorkflowContractBundle {
	return declarativeEmitContractTestBundleWithEntry(eventType, runtimecontracts.EventCatalogEntry{
		Payload: runtimecontracts.EventPayloadSpec{
			Properties: map[string]runtimecontracts.EventFieldSpec{
				"label": {Type: "string"},
			},
		},
		Required: []string{"label"},
	})
}

func additiveOnSuccessContractBundle() *runtimecontracts.WorkflowContractBundle {
	return &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:    "test",
			Version: "v-test",
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"rule.emitted":      {},
			"handler.succeeded": {},
		},
	}
}

func rulesEmitTemplateContractBundle() *runtimecontracts.WorkflowContractBundle {
	return &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:    "test",
			Version: "v-test",
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"account.scored": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"account_id": {Type: "string"},
						"score":      {Type: "number"},
					},
				},
				Required: []string{"account_id", "score"},
			},
			"account.bucketed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"account_id": {Type: "string"},
						"score":      {Type: "number"},
						"bucket":     {Type: "string"},
					},
				},
				Required: []string{"account_id", "score", "bucket"},
			},
		},
	}
}

func declarativeEmitContractTestBundleWithEntry(eventType string, entry runtimecontracts.EventCatalogEntry) *runtimecontracts.WorkflowContractBundle {
	eventType = strings.TrimSpace(eventType)
	return &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:    "test",
			Version: "v-test",
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			eventType: entry,
		},
	}
}

func newDeclarativeEmitContractCoordinator(eventType string) (*PipelineCoordinator, *recordingPipelineBus) {
	return newDeclarativeEmitContractCoordinatorWithBundle(declarativeEmitContractTestBundle(eventType))
}

func newDeclarativeEmitContractCoordinatorWithBundle(bundle *runtimecontracts.WorkflowContractBundle) (*PipelineCoordinator, *recordingPipelineBus) {
	bus := &recordingPipelineBus{}
	return &PipelineCoordinator{
		bus:            bus,
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
		module: &previewWorkflowModule{
			bundle: bundle,
		},
	}, bus
}

func TestExecuteNodeContractHandler_UsesEmitFieldsAsOnlyBusinessPayloadSource(t *testing.T) {
	pc, bus := newDeclarativeEmitContractCoordinator("custom.emitted")

	_, err := pc.executeNodeContractHandler(testPipelineCoordinatorRunContext(t, pc), "node-a", runtimecontracts.SystemNodeEventHandler{
		Emit: runtimecontracts.EmitSpec{
			Event: "custom.emitted",
			Fields: map[string]runtimecontracts.ExpressionValue{
				"label": runtimecontracts.CELExpression(`"done"`),
			},
		},
	}, workflowTriggerContext{
		Event: eventtest.RootIngress(
			"",
			events.EventType("custom.trigger"),
			"",
			"",
			mustJSON(map[string]any{"entity_id": "ent-1", "legacy": "should-not-pass"}),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
			time.Time{},
		),

		State: WorkflowState{EntityID: "ent-1", Stage: WorkflowStateID("queued"), Metadata: map[string]any{"legacy_entity": "should-not-pass"}},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("bus published count = %d, want 1", got)
	}
	var payload map[string]any
	if err := json.Unmarshal(bus.publishedEvent(0).Payload(), &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if got := payload["label"]; got != "done" {
		t.Fatalf("payload.label = %#v, want done", got)
	}
	if _, ok := payload["entity_id"]; ok {
		t.Fatalf("payload must not carry envelope entity_id: %#v", payload["entity_id"])
	}
	if _, ok := payload["trigger_event_type"]; ok {
		t.Fatalf("payload must not carry envelope trigger_event_type: %#v", payload["trigger_event_type"])
	}
	if _, ok := payload["current_state"]; ok {
		t.Fatalf("payload must not carry envelope current_state: %#v", payload["current_state"])
	}
	if _, ok := payload["legacy"]; ok {
		t.Fatalf("legacy trigger payload leaked into emitted payload: %#v", payload["legacy"])
	}
	if _, ok := payload["legacy_entity"]; ok {
		t.Fatalf("entity metadata leaked into emitted payload: %#v", payload["legacy_entity"])
	}
	if got := bus.publishedEvent(0).EntityID(); got != "ent-1" {
		t.Fatalf("emitted event entity_id = %q, want ent-1", got)
	}
	if got := string(bus.publishedEvent(0).Type()); got != "custom.emitted" {
		t.Fatalf("emitted event type = %q, want custom.emitted", got)
	}
}

func TestExecuteNodeContractHandler_GuardEscalateUsesOnlyRuntimeOwnedEnvelope(t *testing.T) {
	pc, bus := newDeclarativeEmitContractCoordinatorWithBundle(declarativeEmitContractTestBundleWithEntry("guard.failed", runtimecontracts.EventCatalogEntry{
		Payload: runtimecontracts.EventPayloadSpec{},
	}))

	_, err := pc.executeNodeContractHandler(testPipelineCoordinatorRunContext(t, pc), "node-a", runtimecontracts.SystemNodeEventHandler{
		Guard: &runtimecontracts.GuardSpec{
			Check:  "payload.score >= 70",
			OnFail: "escalate:guard.failed",
		},
	}, workflowTriggerContext{
		Event: eventtest.RootIngress(
			"",
			events.EventType("custom.trigger"),
			"",
			"",
			mustJSON(map[string]any{"entity_id": "ent-1", "score": 50, "legacy": "should-not-pass"}),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
			time.Time{},
		),

		State: WorkflowState{EntityID: "ent-1", Stage: WorkflowStateID("queued"), Metadata: map[string]any{"legacy_entity": "should-not-pass"}},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("bus published count = %d, want 1", got)
	}
	var payload map[string]any
	if err := json.Unmarshal(bus.publishedEvent(0).Payload(), &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if _, ok := payload["entity_id"]; ok {
		t.Fatalf("payload must not carry envelope entity_id: %#v", payload["entity_id"])
	}
	if _, ok := payload["trigger_event_type"]; ok {
		t.Fatalf("payload must not carry envelope trigger_event_type: %#v", payload["trigger_event_type"])
	}
	if _, ok := payload["current_state"]; ok {
		t.Fatalf("payload must not carry envelope current_state: %#v", payload["current_state"])
	}
	if _, ok := payload["score"]; ok {
		t.Fatalf("guard escalation leaked trigger payload into emitted payload: %#v", payload["score"])
	}
	if _, ok := payload["legacy"]; ok {
		t.Fatalf("guard escalation leaked legacy trigger payload into emitted payload: %#v", payload["legacy"])
	}
	if got := bus.publishedEvent(0).EntityID(); got != "ent-1" {
		t.Fatalf("guard escalation event entity_id = %q, want ent-1", got)
	}
	if _, ok := payload["legacy_entity"]; ok {
		t.Fatalf("guard escalation leaked entity metadata into emitted payload: %#v", payload["legacy_entity"])
	}
}

func TestExecuteNodeContractHandler_GuardEscalateObjectFieldsUseExplicitPayloadOnly(t *testing.T) {
	pc, bus := newDeclarativeEmitContractCoordinatorWithBundle(declarativeEmitContractTestBundleWithEntry("guard.failed", runtimecontracts.EventCatalogEntry{
		Payload: runtimecontracts.EventPayloadSpec{
			Properties: map[string]runtimecontracts.EventFieldSpec{
				"score":  {Type: "number"},
				"reason": {Type: "string"},
			},
			Required: []string{"score", "reason"},
		},
	}))

	_, err := pc.executeNodeContractHandler(testPipelineCoordinatorRunContext(t, pc), "node-a", runtimecontracts.SystemNodeEventHandler{
		Guard: &runtimecontracts.GuardSpec{
			Check: "payload.score >= 70",
			OnFailSpec: runtimecontracts.GuardFailureSpec{
				Action: runtimecontracts.GuardFailureActionEscalate,
				Escalation: runtimecontracts.EmitSpec{
					Event: "guard.failed",
					Fields: map[string]runtimecontracts.ExpressionValue{
						"score":  runtimecontracts.CELExpression("payload.score"),
						"reason": runtimecontracts.CELExpression(`"score_below_threshold"`),
					},
				},
			},
		},
	}, workflowTriggerContext{
		Event: eventtest.RootIngress(
			"",
			events.EventType("custom.trigger"),
			"",
			"",
			mustJSON(map[string]any{"entity_id": "ent-1", "score": 50, "legacy": "should-not-pass"}),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
			time.Time{},
		),
		State: WorkflowState{EntityID: "ent-1", Stage: WorkflowStateID("queued"), Metadata: map[string]any{"legacy_entity": "should-not-pass"}},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("bus published count = %d, want 1", got)
	}
	var payload map[string]any
	if err := json.Unmarshal(bus.publishedEvent(0).Payload(), &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if got := payload["score"]; got != float64(50) {
		t.Fatalf("guard escalation score payload = %#v, want 50", got)
	}
	if got := payload["reason"]; got != "score_below_threshold" {
		t.Fatalf("guard escalation reason payload = %#v, want score_below_threshold", got)
	}
	if _, ok := payload["entity_id"]; ok {
		t.Fatalf("payload must not carry envelope entity_id: %#v", payload["entity_id"])
	}
	if _, ok := payload["legacy"]; ok {
		t.Fatalf("guard escalation leaked unmapped trigger payload: %#v", payload["legacy"])
	}
	if _, ok := payload["legacy_entity"]; ok {
		t.Fatalf("guard escalation leaked entity metadata: %#v", payload["legacy_entity"])
	}
}

func TestExecuteNodeContractHandler_RejectsUndeclaredBusinessPayloadAcrossSupportedEmitSites(t *testing.T) {
	tests := []struct {
		name    string
		event   events.Event
		state   WorkflowState
		handler runtimecontracts.SystemNodeEventHandler
	}{
		{
			name:  "handler top level",
			event: eventtest.RootIngress("", events.EventType("custom.trigger"), "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"), time.Time{}),
			state: WorkflowState{EntityID: "ent-1", Stage: WorkflowStateID("queued"), Metadata: map[string]any{}},
			handler: runtimecontracts.SystemNodeEventHandler{
				Emit: runtimecontracts.EmitSpec{
					Event: "custom.emitted",
					Fields: map[string]runtimecontracts.ExpressionValue{
						"label": runtimecontracts.CELExpression(`"ok"`),
						"extra": runtimecontracts.CELExpression(`"bad"`),
					},
				},
			},
		},
		{
			name:  "rules",
			event: eventtest.RootIngress("", events.EventType("custom.trigger"), "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"), time.Time{}),
			state: WorkflowState{EntityID: "ent-1", Stage: WorkflowStateID("queued"), Metadata: map[string]any{}},
			handler: runtimecontracts.SystemNodeEventHandler{
				Rules: []runtimecontracts.HandlerRuleEntry{{
					ID:        "pick",
					Condition: "true",
					Emit: runtimecontracts.EmitSpec{
						Event: "custom.emitted",
						Fields: map[string]runtimecontracts.ExpressionValue{
							"label": runtimecontracts.CELExpression(`"ok"`),
							"extra": runtimecontracts.CELExpression(`"bad"`),
						},
					},
				}},
			},
		},
		{
			name:  "on_complete",
			event: eventtest.RootIngress("", events.EventType("custom.trigger"), "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"), time.Time{}),
			state: WorkflowState{EntityID: "ent-1", Stage: WorkflowStateID("queued"), Metadata: map[string]any{}},
			handler: runtimecontracts.SystemNodeEventHandler{
				OnComplete: []runtimecontracts.HandlerRuleEntry{{
					ID:        "complete",
					Condition: "true",
					Emit: runtimecontracts.EmitSpec{
						Event: "custom.emitted",
						Fields: map[string]runtimecontracts.ExpressionValue{
							"label": runtimecontracts.CELExpression(`"ok"`),
							"extra": runtimecontracts.CELExpression(`"bad"`),
						},
					},
				}},
			},
		},
		{
			name: "accumulate.on_timeout",
			event: eventtest.RootIngress(
				"",
				events.EventType("accumulate.timeout"),
				"",
				"",
				mustJSON(map[string]any{
					"timer_handle": map[string]any{
						"kind": "accumulation_timeout",
						"bucket": map[string]any{
							"node_id":    "node-a",
							"event_type": "item.arrived",
						},
					},
				}),
				0,
				"",
				"",
				events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
				time.Time{},
			),

			state: WorkflowState{EntityID: "ent-1", Stage: WorkflowStateID("collecting"), Metadata: map[string]any{"expected_count": 2}},
			handler: runtimecontracts.SystemNodeEventHandler{
				Accumulate: &runtimecontracts.AccumulateSpec{
					ExpectedFrom: "entity.expected_count",
					Completion: runtimecontracts.AccumulateCompletion{
						Mode: runtimecontracts.AccumulateModeAll,
					},
					OnTimeout: &runtimecontracts.HandlerRuleEntry{
						Emit: runtimecontracts.EmitSpec{
							Event: "custom.emitted",
							Fields: map[string]runtimecontracts.ExpressionValue{
								"label": runtimecontracts.CELExpression(`"ok"`),
								"extra": runtimecontracts.CELExpression(`"bad"`),
							},
						},
					},
				},
			},
		},
		{
			name: "fan_out",
			event: eventtest.RootIngress(
				"",
				events.EventType("batch.submitted"),
				"",
				"",
				mustJSON(map[string]any{"items": []any{map[string]any{"label": "x"}}}),
				0,
				"",
				"",
				events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
				time.Time{},
			),

			state: WorkflowState{EntityID: "ent-1", Stage: WorkflowStateID("queued"), Metadata: map[string]any{}},
			handler: runtimecontracts.SystemNodeEventHandler{
				FanOut: &runtimecontracts.FanOutSpec{
					ItemsFrom: "payload.items",
					Emit: runtimecontracts.EmitSpec{
						Event: "custom.emitted",
						Fields: map[string]runtimecontracts.ExpressionValue{
							"label": runtimecontracts.CELExpression(`"ok"`),
							"extra": runtimecontracts.CELExpression(`"bad"`),
						},
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pc, bus := newDeclarativeEmitContractCoordinator("custom.emitted")
			_, err := pc.executeNodeContractHandler(testPipelineCoordinatorRunContext(t, pc), "node-a", tc.handler, workflowTriggerContext{
				Event: tc.event,
				State: tc.state,
			}, false)
			if err == nil {
				t.Fatal("expected undeclared business payload to fail closed")
			}
			if !errors.Is(err, runtimeengine.ErrEmitPayloadContractViolation) {
				t.Fatalf("error = %v, want %v", err, runtimeengine.ErrEmitPayloadContractViolation)
			}
			if got := bus.publishedCount(); got != 0 {
				t.Fatalf("bus published count = %d, want 0", got)
			}
		})
	}
}
