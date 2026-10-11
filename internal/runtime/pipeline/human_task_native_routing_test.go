package pipeline

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/decisioncardtest"
	"github.com/google/uuid"
)

// Only the adversarial read differs. Every card/continuation mutation still
// reaches the original selected-store owner, not this read collaborator.
type foreignHumanTaskRequesterForTest struct {
	decisioncard.HumanTaskStore
	flowInstanceOnly bool
}

func (s foreignHumanTaskRequesterForTest) LoadHumanTaskContinuation(ctx context.Context, id string) (decisioncard.HumanTaskContinuation, error) {
	continuation, err := s.HumanTaskStore.LoadHumanTaskContinuation(ctx, id)
	if err == nil {
		if s.flowInstanceOnly {
			continuation.RequesterRoute.FlowInstance = "provider/foreign"
		} else {
			continuation.RequesterRoute.EntityID = uuid.NewString()
		}
	}
	return continuation, err
}

func nativeHumanTaskRoutingFixtureForTest(t *testing.T, backend string, global bool, open pipelineDeliveryNativeOpenerForTest) (*PipelineDeliveryNativeFixtureForTest, *PipelineCoordinator, context.Context, decisioncard.Card, decisioncard.HumanTaskContinuation, *nativePipelineDeliveryBusObservationForTest) {
	t.Helper()
	bundle := loadWorkflowTempBundle(t, map[string]string{
		"schema.yaml":   "name: human-task-native\nstages:\n  active: {}\n",
		"entities.yaml": "test_entity: {}\n",
		"events.yaml":   "provider.requested:\nmailbox.card_decided:\nmailbox.card_deferred:\nmailbox.card_expired:\nhuman_task.approved:\nhuman_task.rejected:\nhuman_task.deferred:\nhuman_task.expired:\n",
	})
	fixture, pc, ctx := nativePilotPipelineForTest(t, backend, bundle, open)
	if err := fixture.Construct(ctx, constructedScenarioInstanceForTest(t, pc.SemanticSource(), ctx, ".")); err != nil {
		t.Fatal(err)
	}
	run := runtimeRunID(ctx)
	fixture.ObserveHumanRequester(ctx, "requester-agent")
	source, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: ".", FlowInstance: run, EntityID: run})
	if err != nil {
		t.Fatal(err)
	}
	scope := decisioncard.Scope{Kind: decisioncard.ScopeFlow, FlowInstance: run}
	if global {
		scope = decisioncard.Scope{Kind: decisioncard.ScopeGlobal}
	}
	anchor, err := decisioncard.NewHumanTaskAnchor(decisioncard.HumanTaskAnchor{RequesterAgentID: "requester-agent", OperationID: decisioncardtest.HumanOperation(t, run, "provider-turn/tool-call-1"), Category: "review", Scope: scope, Source: source})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := decisioncard.FreezeSnapshot("human_task", "Review provider result", map[string]any{"summary": "ready"}, map[string]runtimecontracts.WorkflowGateOutcomePlan{
		"approve": {Verdict: "approve", Label: "Approve"},
		"reject":  {Verdict: "reject", Label: "Reject", Input: map[string]runtimecontracts.WorkflowGateInputField{"reason": {Type: "text", Required: true}}, InputOrder: []string{"reason"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	fact, found := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	if !found {
		t.Fatal("native human task requires its admitted source")
	}
	now := decisioncard.CanonicalTimestamp(time.Now().UTC())
	card, err := decisioncard.New(decisioncard.Card{CardID: uuid.NewString(), RunID: run, Anchor: anchor, Snapshot: snapshot, ExecutionMode: "live", BundleHash: fact.BundleHash(), CreatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	continuation := decisioncard.HumanTaskContinuation{CardID: card.CardID, RunID: run, RequesterRoute: source.Route(), ReplyContextID: "reply-context-a", SourceEventID: uuid.NewString(), DeadlineAt: now.Add(24 * time.Hour), BudgetBundleHash: card.BundleHash, BudgetWindowStart: now, BudgetWindowEnd: now.Add(7 * 24 * time.Hour), State: decisioncard.HumanTaskContinuationPending, CreatedAt: now, UpdatedAt: now}
	if err := fixture.CreateHumanReply(ctx, run, continuation.SourceEventID, continuation.ReplyContextID); err != nil {
		t.Fatal(err)
	}
	bus := observeNativePipelineDeliveryBusForTest(t, pc)
	return fixture, pc, ctx, card, continuation, bus
}

func observeNativeHumanTaskDispatchForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, bus *nativePipelineDeliveryBusObservationForTest, continuation decisioncard.HumanTaskContinuation) *int {
	t.Helper()
	count := new(int)
	bus.beforeDispatch = func(_ context.Context, intents []engine.EmitIntent) error {
		if fixture.Transactions().Active != 0 {
			return fmt.Errorf("human-task dispatch began before selected transaction completion")
		}
		if len(intents) != 1 || len(intents[0].Recipients) != 1 || intents[0].Recipients[0] != "requester-agent" || intents[0].Context.ReplyContextID() != continuation.ReplyContextID || intents[0].Event.TargetRoute().Normalized() != continuation.RequesterRoute {
			return fmt.Errorf("native direct requester route/context changed: %+v", intents)
		}
		*count++
		return nil
	}
	return count
}

func nativeHumanTaskLifecycleParentForTest(t *testing.T, card decisioncard.Card, id string, kind events.EventType, at time.Time) events.Event {
	t.Helper()
	payload, err := canonicaljson.Bytes(map[string]any{"card_id": card.CardID})
	if err != nil {
		t.Fatal(err)
	}
	return eventtest.RuntimeControl(id, kind, "platform", "", payload, 0, card.RunID, "", events.EnvelopeForFlowInstance(events.EventEnvelope{}, card.RunID), at)
}

func VerifyNativeHumanTaskDecisionRoutesDirectlyToRequesterInOneMutationOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, global := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/global=%t", backend, global), func(t *testing.T) {
				fixture, pc, ctx, card, continuation, bus := nativeHumanTaskRoutingFixtureForTest(t, backend, global, open)
				if err := fixture.HumanTasks.CreateHumanTaskCard(ctx, card, continuation); err != nil {
					t.Fatal(err)
				}
				fields, err := canonicaljson.FromGo(map[string]any{"reason": "Needs source evidence"})
				if err != nil {
					t.Fatal(err)
				}
				decisionID := uuid.NewString()
				decided, err := fixture.ApplyDecision(ctx, decisioncard.DecideRequest{CardID: card.CardID, Verdict: "reject", Fields: fields, PrincipalID: "operator-a", ObservedContentHash: card.CardContentHash, DecisionEventID: decisionID, Now: card.CreatedAt.Add(time.Minute)})
				if err != nil || decided.Card.Status != decisioncard.StatusDecided {
					t.Fatalf("native decision=%+v err=%v", decided, err)
				}
				parent := nativeHumanTaskLifecycleParentForTest(t, decided.Card, decisionID, workflowGateDecisionEventType, decided.Card.DecidedAt)
				fixture.PublishDirect(ctx, parent)
				before := fixture.Transactions()
				previous, err := fixture.HumanTasks.LoadHumanTaskContinuation(ctx, card.CardID)
				if err != nil {
					t.Fatal(err)
				}
				dispatches := observeNativeHumanTaskDispatchForTest(t, fixture, bus, continuation)
				pc.humanTasks = foreignHumanTaskRequesterForTest{HumanTaskStore: fixture.HumanTasks}
				pass, emitted, _, err := pc.Intercept(ctx, parent)
				if err == nil || pass || len(emitted) != 0 || bus.committedCount() != 0 || *dispatches != 0 {
					t.Fatalf("foreign requester escaped: pass=%t emitted=%v err=%v", pass, emitted, err)
				}
				assertNativeHumanTaskRefusalConservesContinuationForTest(t, fixture, ctx, previous, before)
				pc.humanTasks = fixture.HumanTasks
				if _, _, err := pc.handleWorkflowGateDecisionEvent(ctx, parent); err != nil {
					t.Fatal(err)
				}
				persisted, err := fixture.HumanTasks.LoadHumanTaskContinuation(ctx, card.CardID)
				after := fixture.Transactions()
				if err != nil || persisted.State != decisioncard.HumanTaskContinuationOutcomeDispatched || *dispatches != 1 || bus.committedCount() != 1 || after.OtherCommits != before.OtherCommits+1 || after.Active != 0 {
					t.Fatalf("native direct route completion=%+v dispatches=%d err=%v", persisted, *dispatches, err)
				}
				if event := bus.persistedPublishedEvent(t, fixture, ctx, 0); event.Type() != "human_task.rejected" {
					t.Fatalf("direct product=%s, want rejected", event.Type())
				}
			})
		}
	}
}

func VerifyNativeHumanTaskDeferredAndExpiredOutcomesUseRequesterRouteOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, expired := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/expired=%t", backend, expired), func(t *testing.T) {
				fixture, pc, ctx, card, continuation, bus := nativeHumanTaskRoutingFixtureForTest(t, backend, true, open)
				id := uuid.NewString()
				kind, product := decisionCardDeferredEventType, events.EventType("human_task.deferred")
				card.DeferredUntil = card.CreatedAt.Add(time.Hour)
				continuation.DeferredUntil, continuation.DeferCause = card.DeferredUntil, "operator_deferred"
				if expired {
					kind, product = decisionCardExpiredEventType, "human_task.expired"
					continuation.DeadlineAt = card.CreatedAt.Add(time.Hour)
				}
				if err := fixture.HumanTasks.CreateHumanTaskCard(ctx, card, continuation); err != nil {
					t.Fatal(err)
				}
				parent := nativeHumanTaskLifecycleParentForTest(t, card, id, kind, card.CreatedAt.Add(time.Hour))
				if expired {
					parent = commitNativeHumanTaskExpiryForTest(t, fixture, bus, ctx, continuation.DeadlineAt)
				} else {
					if _, err := fixture.ApplyDeferral(ctx, decisioncard.DeferRequest{CardID: card.CardID, PrincipalID: "operator-a", Until: card.CreatedAt.Add(2 * time.Hour), Now: card.CreatedAt.Add(time.Hour)}); err != nil {
						t.Fatal(err)
					}
					fixture.PublishDirect(ctx, parent)
				}
				dispatches := observeNativeHumanTaskDispatchForTest(t, fixture, bus, continuation)
				before := fixture.Transactions()
				previous, err := fixture.HumanTasks.LoadHumanTaskContinuation(ctx, card.CardID)
				if err != nil {
					t.Fatal(err)
				}
				pc.humanTasks = foreignHumanTaskRequesterForTest{HumanTaskStore: fixture.HumanTasks, flowInstanceOnly: true}
				if expired {
					_, _, err = pc.handleDecisionCardExpiredEvent(ctx, parent)
				} else {
					_, _, err = pc.handleDecisionCardDeferredEvent(ctx, parent)
				}
				if err == nil || bus.committedCount() != 0 || *dispatches != 0 {
					t.Fatalf("foreign lifecycle requester escaped: err=%v", err)
				}
				assertNativeHumanTaskRefusalConservesContinuationForTest(t, fixture, ctx, previous, before)
				pc.humanTasks = fixture.HumanTasks
				if expired {
					_, _, err = pc.handleDecisionCardExpiredEvent(ctx, parent)
				} else {
					_, _, err = pc.handleDecisionCardDeferredEvent(ctx, parent)
				}
				after := fixture.Transactions()
				if err != nil || *dispatches != 1 || bus.committedCount() != 1 || after.OtherCommits != before.OtherCommits+1 || after.Active != 0 {
					t.Fatalf("native lifecycle route dispatches=%d err=%v", *dispatches, err)
				}
				if event := bus.persistedPublishedEvent(t, fixture, ctx, 0); event.Type() != product {
					t.Fatalf("direct product=%s want=%s", event.Type(), product)
				}
			})
		}
	}
}

func assertNativeHumanTaskRefusalConservesContinuationForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, ctx context.Context, before decisioncard.HumanTaskContinuation, counts PipelineDeliveryNativeTransactionCountsForTest) {
	t.Helper()
	after, err := fixture.HumanTasks.LoadHumanTaskContinuation(ctx, before.CardID)
	current := fixture.Transactions()
	if err != nil || !reflect.DeepEqual(before, after) || current.OtherCommits != counts.OtherCommits || current.WorkflowCommits != counts.WorkflowCommits || current.Active != 0 {
		t.Fatalf("requester refusal changed native continuation or committed a mutation: %+v -> %+v, %+v -> %+v, err=%v", before, after, counts, current, err)
	}
}

func commitNativeHumanTaskExpiryForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, bus *nativePipelineDeliveryBusObservationForTest, ctx context.Context, at time.Time) events.Event {
	t.Helper()
	due, err := fixture.HumanExpiry.ListDueHumanTaskExpiryEvents(ctx, at, 10)
	if err != nil || len(due) != 1 {
		t.Fatalf("native expiry candidates=%v err=%v", due, err)
	}
	plans, err := bus.PrepareEnginePublications(ctx, []engine.EmitIntent{{Event: due[0]}})
	if err != nil {
		t.Fatal(err)
	}
	committed, err := fixture.HumanExpiry.CommitHumanTaskExpirations(ctx, HumanTaskExpiryCommand{ObservedAt: at, Limit: 10, Publications: plans})
	if err != nil || !committed.Acknowledged || committed.Validate() != nil {
		t.Fatalf("native expiry commit=%+v err=%v", committed, err)
	}
	if err := bus.EngineMutationPublicationPlanner.FinalizeEnginePublications(ctx, committed.Publications); err != nil {
		t.Fatal(err)
	}
	return due[0]
}
