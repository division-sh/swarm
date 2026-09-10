package pipeline

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func compiledLoopEvidenceSource(t *testing.T) semanticview.Source {
	t.Helper()
	return loadWorkflowTempSource(t, map[string]string{
		"schema.yaml": `name: loop-evidence
stages:
  waiting: {initial: true}
  drafting: {}
  review: {}
  done: {terminal: true}
  escaped: {terminal: true}
loops:
  revision:
    revision_field: revision_id
    max_attempts: 2
    escape:
      advances_to: escaped
`,
		"entities.yaml": "test_entity: {}\n",
		"events.yaml":   "loop.start: {}\nloop.rule:\n  revision_id: text\nloop.complete:\n  revision_id: text\nloop.repeat:\n  revision_id: text\nloop.close:\n  revision_id: text\n",
		"nodes.yaml": `starter:
  event_handlers:
    loop.start:
      loop: {start: revision, from: waiting}
      advances_to: drafting
reviewer:
  event_handlers:
    loop.rule:
      loop: {admit: revision, from: drafting}
      rules:
        review:
          condition: else
          advances_to: review
collector:
  event_handlers:
    loop.complete:
      loop: {admit: revision, from: drafting}
      on_complete:
        - id: review
          condition: else
          advances_to: review
repeater:
  event_handlers:
    loop.repeat:
      loop: {repeat: revision, from: review}
      advances_to: drafting
closer:
  event_handlers:
    loop.close:
      loop: {close: revision, from: review}
      advances_to: done
`,
	})
}

type compiledLoopEvidenceHarness struct {
	t        *testing.T
	ctx      context.Context
	pc       *PipelineCoordinator
	store    *workflowInstanceStore
	source   semanticview.Source
	route    flowidentity.Route
	entityID string
	clock    time.Time
}

func newCompiledLoopEvidenceHarness(t *testing.T, storeCase workflowJoinStoreCase) *compiledLoopEvidenceHarness {
	t.Helper()
	store, ctx := storeCase.open(t)
	source := compiledLoopEvidenceSource(t)
	pc := newWorkflowJoinPipelineCoordinator(&recordingPipelineBus{}, store.testDB(), PipelineCoordinatorOptions{
		Module: &pipelineFixtureWorkflowModule{source: source}, Persistence: workflowPersistenceForTest(store),
	})
	runID := runtimecorrelation.RunIDFromContext(ctx)
	entityID := uuid.NewString()
	now := canonicalWorkflowTimerTime(time.Now().UTC())
	instance := materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: ".", WorkflowVersion: "1",
		CurrentState: "waiting", CreatedAt: now, EnteredStageAt: now, EntityType: "test_entity", Fields: map[string]any{},
	})
	if err := store.upsert(ctx, instance); err != nil {
		t.Fatal(err)
	}
	return &compiledLoopEvidenceHarness{t: t, ctx: ctx, pc: pc, store: store, source: source, route: testWorkflowInstanceRoute(runID), entityID: entityID, clock: now}
}

func (h *compiledLoopEvidenceHarness) load() WorkflowInstance {
	h.t.Helper()
	instance, found, err := h.store.Load(h.ctx, h.route)
	if err != nil || !found {
		h.t.Fatalf("load loop instance: found=%v err=%v", found, err)
	}
	return instance
}

func (h *compiledLoopEvidenceHarness) activation() loopruntime.Activation {
	h.t.Helper()
	carrier, err := workflowInstanceStateCarrier(h.load())
	if err != nil {
		h.t.Fatal(err)
	}
	activation, found, err := loopruntime.Load(carrier.StateBuckets, ".", "revision")
	if err != nil || !found {
		h.t.Fatalf("load loop activation: found=%v err=%v", found, err)
	}
	return activation
}

func (h *compiledLoopEvidenceHarness) execute(nodeID, eventType, revision string) (contractHandlerExecutionResult, events.Event, error) {
	h.t.Helper()
	h.clock = h.clock.Add(time.Second)
	eventID := uuid.NewString()
	payload, err := json.Marshal(map[string]any{"revision_id": revision})
	if err != nil {
		h.t.Fatal(err)
	}
	runID := runtimecorrelation.RunIDFromContext(h.ctx)
	event := eventtest.RunCreatingRootIngress(eventID, events.EventType(eventType), "operator", "", payload, 0, runID, "", handlerTestWorkflowEnvelope(".", runID, h.entityID), h.clock)
	persistWorkflowTimerEvent(h.t, h.store, h.ctx, eventID, eventType, runID, h.entityID, payload, h.clock)
	node := pipelineSourceNode(h.t, h.source, ".", nodeID)
	handler, ok := h.source.ExecutableNodeEventHandlers(node)[eventType]
	if !ok {
		h.t.Fatalf("source lacks %s/%s", node.Key(), eventType)
	}
	result, err := h.pc.executeNodeContractHandler(h.ctx, node, handler, workflowTriggerContext{
		Event: event, HandlerEventKey: eventType, State: mustCurrentWorkflowState(h.t, h.pc, h.ctx, h.route, h.entityID),
	}, false)
	return result, event, err
}

func (h *compiledLoopEvidenceHarness) advance(nodeID, eventType, revision, from, to string, operation contracts.LoopOperationKind, carrier contracts.HandlerAdvanceCarrierKind) WorkflowTransitionRecord {
	h.t.Helper()
	before := h.load()
	result, event, err := h.execute(nodeID, eventType, revision)
	if err != nil || !result.Handled {
		h.t.Fatalf("execute %s: handled=%v err=%v", eventType, result.Handled, err)
	}
	after := h.load()
	if after.CurrentState != to || after.Revision != before.Revision+1 || len(after.TransitionHistory) != len(before.TransitionHistory)+1 {
		h.t.Fatalf("%s did not commit one transition: before=%#v after=%#v", eventType, before, after)
	}
	record := after.TransitionHistory[len(after.TransitionHistory)-1]
	compiled, ok := record.Evidence.Compiled()
	node := pipelineSourceNode(h.t, h.source, ".", nodeID)
	if !ok || compiled.FlowID() != "." || !compiled.Edge().Node.Equal(node) || compiled.Edge().HandlerEvent != eventType || compiled.Edge().LoopID != "revision" || compiled.Edge().LoopOperation != operation || compiled.Edge().AdvanceCarrier != carrier || record.From != from || record.To != to {
		h.t.Fatalf("%s exact carrier lost: %#v", eventType, record)
	}
	if record.TriggerEventID != event.ID() || record.TransitionID != record.Evidence.ID() || !record.Evidence.RuleSelection().Equal(result.RuleSelection) {
		h.t.Fatalf("%s execution/history disagree: record=%#v result=%#v", eventType, record, result.RuleSelection)
	}
	graph, ok := semanticview.WorkflowStageTopology(h.source, ".")
	if !ok {
		h.t.Fatal("source graph missing")
	}
	if err := record.Evidence.ValidateAgainst(graph); err != nil {
		h.t.Fatal(err)
	}
	if !after.EnteredStageAt.Equal(event.CreatedAt()) {
		h.t.Fatalf("%s entry time = %s, want accepted event %s", eventType, after.EnteredStageAt, event.CreatedAt())
	}
	assertCompiledLifecycleHistoryRoundTrip(h.t, record)
	return record
}

func TestPipelineCompiledLoopCarrierSourceAdmissionOnBothStores(t *testing.T) {
	for _, storeCase := range workflowJoinStoreCases() {
		for _, tc := range []struct {
			node, event string
			carrier     contracts.HandlerAdvanceCarrierKind
			context     handlerselection.Context
		}{
			{"reviewer", "loop.rule", contracts.HandlerAdvanceCarrierRules, handlerselection.ContextRules},
			{"collector", "loop.complete", contracts.HandlerAdvanceCarrierOnComplete, handlerselection.ContextOnComplete},
		} {
			t.Run(storeCase.name+"/"+tc.event, func(t *testing.T) {
				h := newCompiledLoopEvidenceHarness(t, storeCase)
				h.advance("starter", "loop.start", "", "waiting", "drafting", contracts.LoopOperationStart, contracts.HandlerAdvanceCarrierHandler)
				revision := h.activation().RevisionID
				record := h.advance(tc.node, tc.event, revision, "drafting", "review", contracts.LoopOperationAdmit, tc.carrier)
				compiled, _ := record.Evidence.Compiled()
				handler := h.source.ExecutableNodeEventHandlers(pipelineSourceNode(t, h.source, ".", tc.node))[tc.event]
				rules := handler.Rules
				if tc.carrier == contracts.HandlerAdvanceCarrierOnComplete {
					rules = handler.OnComplete
				}
				expectedRef, qualified := rules[0].DeclarationIdentity()
				if !qualified || record.Evidence.RuleSelection().Context() != tc.context || !compiled.Edge().RuleRef.Equal(expectedRef) || !record.Evidence.RuleSelection().Ref().Equal(expectedRef) {
					t.Fatalf("loop admit lost underlying rule: %#v", record)
				}
				before := h.load()
				_, _, err := h.execute(tc.node, tc.event, revision)
				if envelope, ok := failures.As(err); !ok || envelope.Failure.Class != failures.ClassEarlyArrival {
					t.Fatalf("wrong loop source error = %v, want early_arrival", err)
				}
				if after := h.load(); !reflect.DeepEqual(before, after) {
					t.Fatalf("wrong-source admission mutated instance: before=%#v after=%#v", before, after)
				}
			})
		}
	}
}

func TestPipelineCompiledLoopOperationEvidenceOnBothStores(t *testing.T) {
	for _, storeCase := range workflowJoinStoreCases() {
		for _, outcome := range []string{"close", "escape"} {
			t.Run(storeCase.name+"/"+outcome, func(t *testing.T) {
				h := newCompiledLoopEvidenceHarness(t, storeCase)
				h.advance("starter", "loop.start", "", "waiting", "drafting", contracts.LoopOperationStart, contracts.HandlerAdvanceCarrierHandler)
				first := h.activation()
				h.advance("reviewer", "loop.rule", first.RevisionID, "drafting", "review", contracts.LoopOperationAdmit, contracts.HandlerAdvanceCarrierRules)
				h.advance("repeater", "loop.repeat", first.RevisionID, "review", "drafting", contracts.LoopOperationRepeat, contracts.HandlerAdvanceCarrierHandler)
				second := h.activation()
				if second.RevisionID == first.RevisionID || second.Attempt != 2 {
					t.Fatalf("repeat lost generation: first=%#v second=%#v", first, second)
				}
				h.advance("collector", "loop.complete", second.RevisionID, "drafting", "review", contracts.LoopOperationAdmit, contracts.HandlerAdvanceCarrierOnComplete)
				var record WorkflowTransitionRecord
				if outcome == "close" {
					record = h.advance("closer", "loop.close", second.RevisionID, "review", "done", contracts.LoopOperationClose, contracts.HandlerAdvanceCarrierHandler)
				} else {
					record = h.advance("repeater", "loop.repeat", second.RevisionID, "review", "escaped", contracts.LoopOperationRepeat, "")
				}
				compiled, _ := record.Evidence.Compiled()
				if compiled.Edge().RuleRef.Valid() || record.Evidence.RuleSelection().Ref().Valid() {
					t.Fatalf("%s inherited collector rule: %#v", outcome, record)
				}
				closed := h.activation()
				if closed.Status != loopruntime.StatusClosed || closed.RevisionID != second.RevisionID || closed.Attempt != 2 {
					t.Fatalf("%s lost closed generation: %#v", outcome, closed)
				}
				if outcome == "escape" && (compiled.Edge().Source != "loop.escape" || closed.CloseReason != loopruntime.CloseReasonEscaped) {
					t.Fatalf("wrong escape cause: %#v / %#v", record, closed)
				}
				if outcome == "close" && closed.CloseReason != loopruntime.CloseReasonCompleted {
					t.Fatalf("ordinary close has wrong reason: %#v", closed)
				}
			})
		}
	}
}
