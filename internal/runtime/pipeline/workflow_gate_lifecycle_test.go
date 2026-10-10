package pipeline

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/google/uuid"
)

func TestStageGateOwnerRequiresAuthoritativeWorkflowInstance(t *testing.T) {
	entityID := uuid.NewString()
	instancePath := "telegram-ingress/standing-one"
	instance := WorkflowInstance{
		InstanceID: "standing-one", StorageRef: instancePath, EntityID: entityID, WorkflowName: "telegram-ingress",
		Fields:     map[string]any{},
		EntityType: "test_entity",
	}
	anchor := decisioncard.StageGateAnchor{
		Route:  runtimeflowidentity.StoredRoute("telegram-ingress", "standing-one", instancePath),
		FlowID: "telegram-ingress", EntityID: entityID,
	}
	activation := gateruntime.Activation{FlowID: anchor.FlowID}
	if err := validateStageGateInstanceOwner(anchor, instance, activation); err != nil {
		t.Fatalf("exact workflow instance owner rejected: %v", err)
	}
	for _, hostile := range []decisioncard.StageGateAnchor{
		{Route: anchor.Route, FlowID: "foreign-flow", EntityID: entityID},
		{Route: runtimeflowidentity.StoredRoute("telegram-ingress", "foreign", "telegram-ingress/foreign"), FlowID: anchor.FlowID, EntityID: entityID},
		{Route: anchor.Route, FlowID: anchor.FlowID, EntityID: uuid.NewString()},
	} {
		if err := validateStageGateInstanceOwner(hostile, instance, activation); err == nil {
			t.Fatalf("foreign stage-gate owner accepted: %#v", hostile)
		}
	}
}

func VerifyNativeWorkflowGateEntryUsesOneTransactionAndRollsBackOnCardFailureForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture, pc, ctx, mutations := nativeLifecycleComponentForTest(t, backend, gateLifecycleBundle(t), "drafting", open)
			run := runtimecorrelation.RunIDFromContext(ctx)
			if err := fixture.SetCardInsertFault(ctx, run, true); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := fixture.SetCardInsertFault(ctx, run, false); err != nil {
					t.Error(err)
				}
			})
			before := fixture.Transactions()
			_, err := executeNativeLifecycleTransitionForTest(t, fixture, mutations, pc, ctx, "draft.ready")
			if err == nil || !strings.Contains(err.Error(), "workflow_gate_card_insert_cut") {
				t.Fatalf("gate creation did not reach the real card insert: %v", err)
			}
			after := fixture.Transactions()
			if after.WorkflowCommits != before.WorkflowCommits || after.WorkflowRollbacks != before.WorkflowRollbacks+1 || after.Active != 0 {
				t.Fatalf("card failure escaped the original single mutation transaction: before=%+v after=%+v", before, after)
			}
			loaded, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, run))
			if err != nil || !found || loaded.CurrentState != "drafting" || len(loaded.TransitionHistory) != 0 {
				t.Fatalf("card failure changed lifecycle: found=%t error=%v state=%+v", found, err, loaded)
			}
			carrier, err := workflowInstanceStateCarrier(loaded)
			if err != nil {
				t.Fatal(err)
			}
			activations, err := gateruntime.List(carrier.StateBuckets)
			if err != nil || len(activations) != 0 {
				t.Fatalf("gate activations after rollback=%+v error=%v", activations, err)
			}
			items, _, err := fixture.Cards.ListDecisionCards(ctx, decisioncard.ListOptions{RunID: run, Limit: 10})
			if err != nil || len(items) != 0 {
				t.Fatalf("card insert survived rollback: %+v/%v", items, err)
			}
		})
	}
}

func VerifyNativeWorkflowGateEntryCreatesMatchingActivationAndCardOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture, pc, ctx, mutations := nativeLifecycleComponentForTest(t, backend, gateLifecycleBundle(t), "drafting", open)
			before := fixture.Transactions()
			if _, err := executeNativeLifecycleTransitionForTest(t, fixture, mutations, pc, ctx, "draft.ready"); err != nil {
				t.Fatal(err)
			}
			after := fixture.Transactions()
			if after.WorkflowCommits != before.WorkflowCommits+1 || after.WorkflowRollbacks != before.WorkflowRollbacks || after.Active != 0 {
				t.Fatalf("gate creation did not use one exact native commit: before=%+v after=%+v", before, after)
			}
			card := nativeGateCardForTest(t, fixture, ctx)
			loaded, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, runtimecorrelation.RunIDFromContext(ctx)))
			if err != nil || !found {
				t.Fatalf("load gate: %t/%v", found, err)
			}
			carrier, err := workflowInstanceStateCarrier(loaded)
			if err != nil {
				t.Fatal(err)
			}
			activation, found, err := gateruntime.Load(carrier.StateBuckets, ".", "launch_review")
			if err != nil || !found {
				t.Fatalf("gate activation=%+v found=%t error=%v", activation, found, err)
			}
			route, err := gateruntime.RouteFor(activation.RoutesJSON, "approve")
			if err != nil || route.EmitSchema.Len() == 0 {
				t.Fatalf("gate continuation lost frozen resolved event schema: %+v/%v", route, err)
			}
			anchor := mustStageGateAnchor(t, card)
			if activation.CardID != card.CardID || activation.ActivationID != anchor.StageActivationID || activation.Status != gateruntime.StatusOpen {
				t.Fatalf("native activation/card mismatch: activation=%+v card=%+v", activation, card)
			}
		})
	}
}

func VerifyNativeWorkflowGateDecisionRoutePublishesAtomicallyAndRecoversIdempotentlyOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			fixture, pc, ctx, _ := nativeLifecycleComponentForTest(t, tc.name, gateLifecycleBundle(t), "awaiting_review", open)
			workflowStore := pc.workflowStore
			runID := runtimecorrelation.RunIDFromContext(ctx)
			now := time.Now().UTC()
			entityID := runID
			bus := observeNativePipelineDeliveryBusForTest(t, pc)
			if err := pc.applyWorkflowGateIntents(ctx, testRunScopedWorkflowInstanceFromContext(ctx, runID).Route, entityID, "", "awaiting_review", "state:awaiting_review", time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			card := nativeGateCardForTest(t, fixture, ctx)
			decisionEventID := uuid.NewString()
			decision, err := fixture.ApplyDecision(ctx, decisioncard.DecideRequest{
				CardID: card.CardID, Verdict: "approve", PrincipalID: "operator",
				ObservedContentHash: card.CardContentHash, DecisionEventID: decisionEventID, Now: now.Add(time.Minute),
			})
			if err != nil {
				t.Fatal(err)
			}
			card = decision.Card
			if err := workflowStore.CommitDecision(ctx, card, decisionEventID, now.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			route, err := pc.loadStageGateRoute(ctx, card)
			if err != nil {
				t.Fatal(err)
			}
			parent := eventtest.RuntimeControl(decisionEventID, workflowGateDecisionEventType, "platform", "", json.RawMessage(`{"card_id":"`+card.CardID+`"}`), 0, runID, "", testWorkflowSourceEnvelope(".", runID, entityID), card.DecidedAt)
			fixture.PublishDirect(ctx, parent)
			emitted, err := workflowGateOutcomeEvent(card, parent, route)
			if err != nil || emitted == nil {
				t.Fatalf("workflowGateOutcomeEvent = %#v, %v", emitted, err)
			}
			if err := fixture.HideMutationTable(ctx, true); err != nil {
				t.Fatal(err)
			}
			hidden := true
			t.Cleanup(func() {
				if !hidden {
					return
				}
				if err := fixture.HideMutationTable(ctx, false); err != nil {
					t.Error(err)
				}
			})
			before := fixture.Transactions()
			if _, err := pc.routeWorkflowGateDecision(ctx, card, parent, route, emitted); err == nil || !strings.Contains(err.Error(), "entity_mutations") {
				t.Fatalf("native route persistence failure = %v", err)
			}
			after := fixture.Transactions()
			if after.WorkflowRollbacks != before.WorkflowRollbacks+1 || after.WorkflowCommits != before.WorkflowCommits || after.Active != 0 {
				t.Fatalf("route failure did not roll back the exact selected transaction: %+v -> %+v", before, after)
			}
			assertGateLifecycleState(t, workflowStore, ctx, entityID, "awaiting_review", gateruntime.StatusDecisionCommitted)
			if persisted := fixture.EventIDCount(ctx, emitted.ID()); persisted != 0 || bus.publishedCount() != 0 {
				t.Fatalf("rolled-back native outcomes = %d, handoffs=%d", persisted, bus.publishedCount())
			}
			if err := fixture.HideMutationTable(ctx, false); err != nil {
				t.Fatal(err)
			}
			hidden = false
			if _, err := pc.routeWorkflowGateDecision(ctx, card, parent, route, emitted); err != nil {
				t.Fatal(err)
			}
			assertGateLifecycleState(t, workflowStore, ctx, entityID, "operating", gateruntime.StatusRouted)
			if bus.publishedCount() != 1 || bus.publishedEvent(0).ID() != emitted.ID() {
				t.Fatalf("published outcomes = %#v, want one deterministic event %s", bus.published, emitted.ID())
			}
			if persisted := fixture.EventIDCount(ctx, emitted.ID()); persisted != 1 {
				t.Fatalf("committed native outcome rows = %d", persisted)
			}
			if _, err := pc.routeWorkflowGateDecision(ctx, card, parent, route, emitted); err != nil {
				t.Fatalf("idempotent route recovery: %v", err)
			}
			if bus.publishedCount() != 1 || fixture.EventIDCount(ctx, emitted.ID()) != 1 {
				t.Fatalf("idempotent recovery republished outcome: %d", bus.publishedCount())
			}
		})
	}
}

func VerifyNativeWorkflowGateCommittedDecisionWinsOrdinaryAndTimerExitRacesOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, exitKind := range []string{"ordinary", "timer"} {
		for _, tc := range workflowJoinStoreCases() {
			t.Run(tc.name+"/"+exitKind, func(t *testing.T) {
				fixture, pc, ctx, mutations := nativeLifecycleComponentForTest(t, tc.name, gateLifecycleBundle(t), "awaiting_review", open)
				workflowStore := pc.workflowStore
				runID, entityID := runtimecorrelation.RunIDFromContext(ctx), runtimecorrelation.RunIDFromContext(ctx)
				now := time.Now().UTC()
				if err := applyTestInitialEntryEffect(ctx, pc, testWorkflowInstanceRoute(runID), entityID); err != nil {
					t.Fatal(err)
				}
				card := nativeGateCardForTest(t, fixture, ctx)
				if err := workflowStore.CommitDecision(ctx, card, uuid.NewString(), now.Add(time.Minute)); err != nil {
					t.Fatal(err)
				}
				route := testRunScopedWorkflowInstanceFromContext(ctx, runID).Route
				sourceEvent := "ordinary.transition"
				if exitKind == "timer" {
					graph, found := semanticview.WorkflowStageTopology(pc.SemanticSource(), ".")
					if !found {
						t.Fatal("gate race fixture has no compiled root graph")
					}
					sourceEvent = ""
					for _, edge := range graph.Edges {
						if edge.Source == "timer" && edge.From == "awaiting_review" && edge.To == "operating" {
							sourceEvent = edge.EventType
						}
					}
					if sourceEvent == "" {
						t.Fatal("gate race fixture has no compiled timer exit")
					}
				}
				if _, err := compiledLifecycleTransitionForTest(pc, ".", "awaiting_review", "operating", sourceEvent); err != nil {
					t.Fatalf("competing exit lacks compiled evidence: %v", err)
				}
				var err error
				if exitKind == "ordinary" {
					_, err = executeNativeLifecycleTransitionForTest(t, fixture, mutations, pc, ctx, sourceEvent)
				} else {
					inbound := nativeWorkflowJoinEventForTest(ctx, ".", runID, entityID, sourceEvent, []byte("{}"), time.Now().UTC())
					fixture.PublishDirect(ctx, inbound)
					err = pc.persistWorkflowStateForTest(runtimecorrelation.WithInboundEvent(ctx, inbound), route, entityID, "operating", sourceEvent)
				}

				if err == nil || !strings.Contains(err.Error(), "has a committed verdict awaiting its frozen route") {
					t.Fatalf("competing exit rejection = %v, want committed-verdict fence", err)
				}
				assertGateLifecycleState(t, workflowStore, ctx, entityID, "awaiting_review", gateruntime.StatusDecisionCommitted)
			})
		}
	}
}

func VerifyNativeWorkflowGateDecisionWaitsForItsRecordedBundlePinOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			fixture, pc, ctx, _ := nativeLifecycleComponentForTest(t, tc.name, gateLifecycleBundle(t), "awaiting_review", open)
			workflowStore := pc.workflowStore
			runID := runtimecorrelation.RunIDFromContext(ctx)
			now := time.Now().UTC()
			entityID := runID
			if err := pc.applyWorkflowGateIntents(ctx, testRunScopedWorkflowInstanceFromContext(ctx, runID).Route, entityID, "", "awaiting_review", "state:awaiting_review", time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			decisionEventID := uuid.NewString()
			card := nativeGateCardForTest(t, fixture, ctx)
			decision, err := fixture.ApplyDecision(ctx, decisioncard.DecideRequest{
				CardID: card.CardID, Verdict: "approve", PrincipalID: "operator",
				ObservedContentHash: card.CardContentHash, DecisionEventID: decisionEventID, Now: now.Add(time.Minute),
			})
			if err != nil {
				t.Fatal(err)
			}
			card = decision.Card
			if err := workflowStore.CommitDecision(ctx, card, decisionEventID, now.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			pc.sourceArtifactFact = mustPipelineTestSourceArtifactFact("bundle-v2:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
			parent := eventtest.RuntimeControl(decisionEventID, workflowGateDecisionEventType, "platform", "", json.RawMessage(`{"card_id":"`+card.CardID+`"}`), 0, runID, "", testWorkflowSourceEnvelope(".", runID, entityID), card.DecidedAt)
			_, outcome, err := pc.handleWorkflowGateDecisionEvent(ctx, parent)
			if err != nil {
				t.Fatalf("decision deferral returned error: %v", err)
			}
			disposition, deferred := outcome.Disposition()
			if !deferred || disposition.Kind() != runtimepipelineobligation.DispositionDeferred {
				t.Fatalf("bundle-pin outcome = %#v, want recoverable pipeline deferral", outcome)
			}
			failure := disposition.Failure()
			if failure == nil || failure.Class != runtimefailures.ClassDependencyUnavailable || failure.Detail.Code != "decision_card_bundle_unavailable" || !failure.Retryable {
				t.Fatalf("bundle-pin failure = %#v, want retryable dependency-unavailable classification", failure)
			}
			assertGateLifecycleState(t, workflowStore, ctx, entityID, "awaiting_review", gateruntime.StatusDecisionCommitted)
		})
	}
}

func VerifyNativeInitialStageLifecycleArmsStandingGateOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			fixture, pc, ctx, _ := nativeLifecycleComponentForTest(t, tc.name, gateLifecycleBundle(t), "awaiting_review", open)
			workflowStore := pc.workflowStore
			runID := runtimecorrelation.RunIDFromContext(ctx)
			entityID := runID
			if err := applyTestInitialEntryEffect(ctx, pc, testRunScopedWorkflowInstanceFromContext(ctx, runID).Route, entityID); err != nil {
				t.Fatal(err)
			}
			card := nativeGateCardForTest(t, fixture, ctx)
			if mustStageGateAnchor(t, card).EntityID != entityID {
				t.Fatalf("standing initial card = %#v", card)
			}
			assertGateLifecycleState(t, workflowStore, ctx, entityID, "awaiting_review", gateruntime.StatusOpen)
		})
	}
}

func VerifyNativeWorkflowGateTerminationUsesCanonicalPersistedEntityIdentityOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			fixture, pc, ctx, _ := nativeLifecycleComponentForTest(t, tc.name, gateLifecycleBundle(t), "awaiting_review", open)
			ctx = runtimeeffects.WithExecutionMode(ctx, executionmode.Mock)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			entityID := runID
			bus := observeNativePipelineDeliveryBusForTest(t, pc)
			if err := pc.applyWorkflowGateIntents(ctx, testRunScopedWorkflowInstanceFromContext(ctx, runID).Route, entityID, "", "awaiting_review", "state:awaiting_review", time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			card := nativeGateCardForTest(t, fixture, ctx)
			if err := pc.MarkTerminated(ctx, testRunScopedWorkflowInstanceForRun(runID, runID), identity.NormalizeEntityID(entityID), time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			superseded, err := fixture.Cards.GetDecisionCard(ctx, card.CardID)
			if err != nil || superseded.Status != decisioncard.StatusSuperseded || mustStageGateAnchor(t, superseded).EntityID != entityID {
				t.Fatalf("superseded native card = %#v/%v, want canonical %s", superseded, err, entityID)
			}
			cardAnchor := mustStageGateAnchor(t, card)
			if bus.publishedCount() != 1 || bus.publishedEvent(0).FlowInstance() != cardAnchor.Route.InstancePath || bus.publishedEvent(0).EntityID() != entityID {
				t.Fatalf("terminated-flow supersession events = %#v, want card flow %q and entity %q", bus.published, cardAnchor.Route.InstancePath, entityID)
			}
			if card.ExecutionMode != executionmode.Mock || bus.publishedEvent(0).ExecutionMode() != executionmode.Mock {
				t.Fatalf("terminated-flow modes = card:%q event:%q, want mock", card.ExecutionMode, bus.publishedEvent(0).ExecutionMode())
			}
			if bus.publishedEvent(0).ParentEventID() != "" {
				t.Fatal("explicit flow termination fabricated accepted-event lineage")
			}
		})
	}
}

func VerifyNativeWorkflowGateOrdinaryExitSupersessionCarriesCardFlowIdentityOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			fixture, pc, ctx, mutations := nativeLifecycleComponentForTest(t, tc.name, gateLifecycleBundle(t), "drafting", open)
			workflowStore := pc.workflowStore
			runID, entityID := runtimecorrelation.RunIDFromContext(ctx), runtimecorrelation.RunIDFromContext(ctx)
			route := testWorkflowInstanceRoute(runID)
			bus := observeNativePipelineDeliveryBusForTest(t, pc)
			if _, err := executeNativeLifecycleTransitionForTest(t, fixture, mutations, pc, ctx, "draft.ready"); err != nil {
				t.Fatal(err)
			}
			card := nativeGateCardForTest(t, fixture, ctx)
			inbound, err := executeNativeLifecycleTransitionForTest(t, fixture, mutations, pc, ctx, "review.expired")
			if err != nil {
				t.Fatal(err)
			}
			if bus.publishedCount() != 1 {
				t.Fatalf("ordinary exit must publish exactly one supersession, got %d", bus.publishedCount())
			}
			cardAnchor := mustStageGateAnchor(t, card)
			if got := bus.publishedEvent(0); got.RunID() != runID || got.EntityID() != entityID || got.FlowInstance() != cardAnchor.Route.InstancePath {
				t.Fatalf("ordinary-exit identity = run:%q entity:%q flow:%q, want %q/%q/%q", got.RunID(), got.EntityID(), got.FlowInstance(), runID, entityID, cardAnchor.Route.InstancePath)
			}
			if bus.publishedEvent(0).ParentEventID() != inbound.ID() {
				t.Fatalf("ordinary-exit parent = %q, want exact accepted event %q", bus.publishedEvent(0).ParentEventID(), inbound.ID())
			}
			instance, found, err := workflowStore.Load(ctx, testRunScopedWorkflowRoute(ctx, route))
			if err != nil || !found {
				t.Fatalf("load committed supersession: found=%t err=%v", found, err)
			}
			carrier, err := workflowInstanceStateCarrier(instance)
			if err != nil {
				t.Fatal(err)
			}
			activation, found, err := gateruntime.Load(carrier.StateBuckets, instance.WorkflowName, card.Snapshot.Decision)
			if err != nil || !found || activation.Status != gateruntime.StatusSuperseded {
				t.Fatalf("load superseded activation: found=%t activation=%+v err=%v", found, activation, err)
			}
			transition, err := compiledLifecycleTransitionForTest(pc, instance.WorkflowName, "awaiting_review", "operating", string(inbound.Type()))
			if err != nil {
				t.Fatal(err)
			}
			accepted := func(target runtimeflowidentity.Route, entity string, mode executionmode.Mode, selected *workflowlifecycle.Transition) workflowlifecycle.Effect {
				t.Helper()
				effect, err := workflowlifecycle.NewAcceptedEvent(target, identity.NormalizeEntityID(entity), inbound.ID(), string(inbound.Type()), mode, inbound.CreatedAt(), selected)
				if err != nil {
					t.Fatal(err)
				}
				return effect
			}
			initial, err := workflowlifecycle.NewInitialEntry(route, identity.NormalizeEntityID(entityID), instance.CurrentState, card.ExecutionMode, inbound.CreatedAt())
			if err != nil {
				t.Fatal(err)
			}
			valid := accepted(route, entityID, card.ExecutionMode, transition)
			for _, hostile := range []struct {
				name   string
				run    string
				effect workflowlifecycle.Effect
			}{
				{"unknown", runID, workflowlifecycle.Effect{}},
				{"initial", runID, initial},
				{"no_transition", runID, accepted(route, entityID, card.ExecutionMode, nil)},
				{"foreign_run", uuid.NewString(), valid},
				{"foreign_route", runID, accepted(testWorkflowInstanceRoute("other-flow"), entityID, card.ExecutionMode, transition)},
				{"foreign_entity", runID, accepted(route, uuid.NewString(), card.ExecutionMode, transition)},
				{"crossed_mode", runID, accepted(route, entityID, executionmode.Mock, transition)},
			} {
				t.Run(hostile.name, func(t *testing.T) {
					if _, err := workflowGateExitSupersededEvent(card, activation, instance, hostile.run, hostile.effect); err == nil {
						t.Fatal("crossed stage-exit facts acquired causal publication authority")
					}
				})
			}
			foreign := activation
			foreign.CardID = uuid.NewString()
			if _, err := workflowGateExitSupersededEvent(card, foreign, instance, runID, valid); err == nil {
				t.Fatal("another card acquired supersession publication authority")
			}
			foreign = activation
			foreign.ActivationID = uuid.NewString()
			if _, err := workflowGateExitSupersededEvent(card, foreign, instance, runID, valid); err == nil {
				t.Fatal("another activation acquired supersession publication authority")
			}
		})
	}
}

func mustStageGateAnchor(t *testing.T, card decisioncard.Card) decisioncard.StageGateAnchor {
	t.Helper()
	anchor, err := card.Anchor.StageGate()
	if err != nil {
		t.Fatal(err)
	}
	return anchor
}

func assertGateLifecycleState(t *testing.T, store *workflowInstanceStore, ctx context.Context, entityID, stage string, status gateruntime.Status) {
	t.Helper()
	loaded, ok, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, runtimeRunID(ctx)))
	if err != nil || !ok {
		t.Fatalf("Load = %#v, %v, %v", loaded, ok, err)
	}
	carrier, err := workflowInstanceStateCarrier(loaded)
	if err != nil {
		t.Fatal(err)
	}
	activation, found, err := gateruntime.Load(carrier.StateBuckets, ".", "launch_review")
	if err != nil || !found || loaded.CurrentState != stage || activation.Status != status {
		t.Fatalf("gate state = stage:%s activation:%#v found:%v err:%v, want %s/%s", loaded.CurrentState, activation, found, err, stage, status)
	}
}

func gateLifecycleBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	return loadWorkflowTempBundle(t, map[string]string{
		"schema.yaml": `name: gate-test
stages:
  awaiting_review:
    gate:
      decision: launch_review
      outcomes:
        approve:
          advances_to: operating
          emit: {event: launch.approved}
    timers:
      - after: 1h
        advances_to: operating
  drafting: {}
  operating: {}
`,
		"entities.yaml": "test_entity: {}\n",
		"events.yaml":   "launch.approved:\ndraft.ready:\nordinary.transition:\nreview.expired:\n",
		"nodes.yaml": `reviewer:
  execution_type: system_node
  event_handlers:
    draft.ready: {advances_to: awaiting_review}
    ordinary.transition: {advances_to: operating}
    review.expired: {advances_to: operating}
`,
	})
}

func TestRootDeclarationConsumersUseAuthoredExecutionFlowNotDisplayName(t *testing.T) {
	bundle := gateLifecycleBundle(t)
	// A non-authoritative display label must not change the loaded root scope.
	bundle.Semantics.Name = "gate-test"
	source := semanticview.Wrap(bundle)
	if source.WorkflowName() != "gate-test" || semanticview.RootExecutionFlowID(source) != "." {
		t.Fatalf("test identity setup = display:%q authored:%q", source.WorkflowName(), semanticview.RootExecutionFlowID(source))
	}
	pc := &PipelineCoordinator{module: &pipelineFixtureWorkflowModule{source: source}}

	flowID, plan, ok := workflowGatePlanForInstance(pc, WorkflowInstance{WorkflowName: ".",
		EntityType: "test_entity"}, "awaiting_review")
	if !ok || flowID != "." || plan.Decision != "launch_review" {
		t.Fatalf("authored-root gate = flow:%q plan:%#v ok:%t", flowID, plan, ok)
	}
	if _, _, ok := workflowGatePlanForInstance(pc, WorkflowInstance{WorkflowName: "gate-test",
		EntityType: "test_entity"}, "awaiting_review"); ok {
		t.Fatal("display workflow name was accepted as root gate authority")
	}
	if !loopFlowIDMatches(source, ".", ".") || loopFlowIDMatches(source, ".", "gate-test") {
		t.Fatal("root loop declaration did not use authored root identity exclusively")
	}
	if !workflowTimerDeclarationOwnedByInstance(source, ".", ".") || workflowTimerDeclarationOwnedByInstance(source, "gate-test", ".") {
		t.Fatal("root timer declaration did not use authored root identity exclusively")
	}
}

func runtimeRunID(ctx context.Context) string {
	// The store test cases always stamp the run identity in context.
	return runtimecorrelation.RunIDFromContext(ctx)
}
