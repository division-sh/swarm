package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type gateLifecycleCardStore struct {
	decisioncard.Store
	createErr     error
	created       []decisioncard.Card
	createTx      []bool
	supersededFor []string
	continuations map[string]decisioncard.HumanTaskContinuation
	completedTx   []bool
}

func (s *gateLifecycleCardStore) CreateDecisionCard(ctx context.Context, card decisioncard.Card) error {
	_, tx := PipelineSQLTxFromContext(ctx)
	s.createTx = append(s.createTx, tx)
	if s.createErr != nil {
		return s.createErr
	}
	s.created = append(s.created, card)
	return nil
}

func (s *gateLifecycleCardStore) GetDecisionCard(_ context.Context, cardID string) (decisioncard.Card, error) {
	for _, card := range s.created {
		if card.CardID == cardID {
			return card, nil
		}
	}
	return decisioncard.Card{}, decisioncard.ErrNotFound
}

func (s *gateLifecycleCardStore) SupersedeDecisionCardsForStage(_ context.Context, _, entityID, _, _ string, _ time.Time) error {
	s.supersededFor = append(s.supersededFor, entityID)
	return nil
}

func (s *gateLifecycleCardStore) CreateHumanTaskCard(ctx context.Context, card decisioncard.Card, continuation decisioncard.HumanTaskContinuation) error {
	if err := s.CreateDecisionCard(ctx, card); err != nil {
		return err
	}
	if s.continuations == nil {
		s.continuations = map[string]decisioncard.HumanTaskContinuation{}
	}
	s.continuations[card.CardID] = continuation
	return nil
}

func (s *gateLifecycleCardStore) LoadHumanTaskContinuation(_ context.Context, cardID string) (decisioncard.HumanTaskContinuation, error) {
	continuation, ok := s.continuations[cardID]
	if !ok {
		return decisioncard.HumanTaskContinuation{}, decisioncard.ErrNotFound
	}
	return continuation, nil
}

func (s *gateLifecycleCardStore) CompleteHumanTaskOutcome(ctx context.Context, cardID, eventID string, at time.Time) (decisioncard.HumanTaskContinuation, error) {
	_, inMutation := PipelineSQLTxFromContext(ctx)
	s.completedTx = append(s.completedTx, inMutation)
	continuation, ok := s.continuations[cardID]
	if !ok {
		return decisioncard.HumanTaskContinuation{}, decisioncard.ErrNotFound
	}
	if continuation.OutcomeEventID != eventID {
		return decisioncard.HumanTaskContinuation{}, errors.New("human-task outcome event identity mismatch")
	}
	if continuation.State != decisioncard.HumanTaskContinuationDecisionCommitted &&
		continuation.State != decisioncard.HumanTaskContinuationExpired &&
		continuation.State != decisioncard.HumanTaskContinuationOutcomeDispatched {
		return decisioncard.HumanTaskContinuation{}, errors.New("human-task continuation is not dispatchable")
	}
	continuation.State = decisioncard.HumanTaskContinuationOutcomeDispatched
	continuation.UpdatedAt = at.UTC()
	s.continuations[cardID] = continuation
	return continuation, nil
}

func TestHumanTaskDecisionRoutesDirectlyToRequesterInOneMutationOnBothStores(t *testing.T) {
	for _, scopeCase := range []struct {
		name  string
		scope decisioncard.Scope
	}{
		{name: "flow", scope: decisioncard.Scope{Kind: decisioncard.ScopeFlow, FlowInstance: "provider/instance-a"}},
		{name: "global", scope: decisioncard.Scope{Kind: decisioncard.ScopeGlobal}},
	} {
		for _, tc := range workflowJoinStoreCases() {
			t.Run(tc.name+"/"+scopeCase.name, func(t *testing.T) {
				workflowStore, ctx := tc.open(t)
				runID := runtimeRunID(ctx)
				ensurePipelineTestRun(t, workflowStore, runID)
				now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
				decisionEventID := uuid.NewString()
				anchor, err := decisioncard.NewHumanTaskAnchor(decisioncard.HumanTaskAnchor{
					RequesterAgentID: "requester-agent", OperationID: "provider-turn/tool-call-1", Category: "review",
					Scope: scopeCase.scope,
				})
				if err != nil {
					t.Fatal(err)
				}
				snapshot, err := decisioncard.FreezeSnapshot("human_task", "Review provider result", map[string]any{"summary": "ready"}, map[string]runtimecontracts.WorkflowGateOutcomePlan{
					"approve": {Verdict: "approve", Label: "Approve"},
					"reject":  {Verdict: "reject", Label: "Reject", Input: map[string]runtimecontracts.WorkflowGateInputField{"reason": {Type: "text", Required: true}}},
				})
				if err != nil {
					t.Fatal(err)
				}
				card, err := decisioncard.New(decisioncard.Card{
					CardID: uuid.NewString(), RunID: runID, Anchor: anchor, Snapshot: snapshot,
					ExecutionMode: "live",
					BundleHash:    "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", CreatedAt: now,
				})
				if err != nil {
					t.Fatal(err)
				}
				fields, err := canonicaljson.FromGo(map[string]any{"reason": "Needs source evidence"})
				if err != nil {
					t.Fatal(err)
				}
				card.Status = decisioncard.StatusDecided
				card.Verdict = "reject"
				card.Fields = fields
				card.DecidedBy = "operator-a"
				card.DecidedAt = now.Add(time.Minute)
				card.DecisionEventID = decisionEventID
				cards := &gateLifecycleCardStore{
					created: []decisioncard.Card{card},
					continuations: map[string]decisioncard.HumanTaskContinuation{card.CardID: {
						CardID: card.CardID, RunID: runID,
						RequesterRoute: events.RouteIdentity{FlowInstance: "provider/instance-a", EntityID: "requester-entity"},
						ReplyContextID: "reply-context-a", SourceEventID: uuid.NewString(),
						DeadlineAt: now.Add(24 * time.Hour), BudgetBundleHash: card.BundleHash,
						BudgetWindowStart: now, BudgetWindowEnd: now.Add(7 * 24 * time.Hour),
						State: decisioncard.HumanTaskContinuationDecisionCommitted, OutcomeEventID: decisionEventID,
						CreatedAt: now, UpdatedAt: card.DecidedAt,
					}},
				}
				bus := &recordingPipelineBus{}
				pc := NewPipelineCoordinatorWithOptions(bus, workflowStore.db, PipelineCoordinatorOptions{
					Module:        &pipelineFixtureWorkflowModule{source: semanticview.Wrap(gateLifecycleBundle())},
					WorkflowStore: workflowStore, DecisionCards: cards, BundleHash: card.BundleHash,
				})
				payload, err := canonicaljson.Bytes(map[string]any{"card_id": card.CardID})
				if err != nil {
					t.Fatal(err)
				}
				parent := eventtest.RuntimeControl(decisionEventID, workflowGateDecisionEventType, "platform", "", payload, 0, runID, "", events.EnvelopeForFlowInstance(events.EventEnvelope{}, "provider/instance-a"), card.DecidedAt)
				if _, err := pc.handleWorkflowGateDecisionEvent(ctx, parent); err != nil {
					t.Fatal(err)
				}
				if len(cards.completedTx) != 1 || !cards.completedTx[0] {
					t.Fatalf("continuation completion transaction evidence = %#v", cards.completedTx)
				}
				if len(bus.directPublishes) != 1 || bus.directPublishes[0].Type() != events.EventType("human_task.rejected") {
					t.Fatalf("direct outcomes = %#v", bus.directPublishes)
				}
				if len(bus.directRecipients) != 1 || len(bus.directRecipients[0]) != 1 || bus.directRecipients[0][0] != "requester-agent" {
					t.Fatalf("direct recipients = %#v", bus.directRecipients)
				}
				if got := bus.directPublishes[0].TargetRoute().Normalized(); got != (events.RouteIdentity{FlowInstance: "provider/instance-a", EntityID: "requester-entity"}) {
					t.Fatalf("direct requester route = %#v", got)
				}
				if len(bus.directContexts) != 1 || bus.directContexts[0].ReplyContextID() != "reply-context-a" || !bus.directInMutation[0] {
					t.Fatalf("direct delivery evidence = contexts:%#v transactions:%#v", bus.directContexts, bus.directInMutation)
				}
				continuation, err := cards.LoadHumanTaskContinuation(ctx, card.CardID)
				if err != nil || continuation.State != decisioncard.HumanTaskContinuationOutcomeDispatched {
					t.Fatalf("dispatched continuation = %#v, %v", continuation, err)
				}
			})
		}
	}
}

func TestHumanTaskDeferredAndExpiredOutcomesUseRequesterRouteOnBothStores(t *testing.T) {
	for _, lifecycle := range []struct {
		name        string
		eventType   events.EventType
		productType events.EventType
	}{
		{name: "deferred", eventType: decisionCardDeferredEventType, productType: "human_task.deferred"},
		{name: "expired", eventType: decisionCardExpiredEventType, productType: "human_task.expired"},
	} {
		for _, tc := range workflowJoinStoreCases() {
			t.Run(tc.name+"/"+lifecycle.name, func(t *testing.T) {
				workflowStore, ctx := tc.open(t)
				runID := runtimeRunID(ctx)
				ensurePipelineTestRun(t, workflowStore, runID)
				now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
				lifecycleEventID := uuid.NewString()
				anchor, err := decisioncard.NewHumanTaskAnchor(decisioncard.HumanTaskAnchor{
					RequesterAgentID: "requester-agent", OperationID: "provider-turn/tool-call-1", Category: "review",
					Scope: decisioncard.Scope{Kind: decisioncard.ScopeGlobal},
				})
				if err != nil {
					t.Fatal(err)
				}
				snapshot, err := decisioncard.FreezeSnapshot("human_task", "Review provider result", nil, map[string]runtimecontracts.WorkflowGateOutcomePlan{
					"approve": {Verdict: "approve", Label: "Approve"},
					"reject":  {Verdict: "reject", Label: "Reject"},
				})
				if err != nil {
					t.Fatal(err)
				}
				card, err := decisioncard.New(decisioncard.Card{
					CardID: uuid.NewString(), RunID: runID, Anchor: anchor, Snapshot: snapshot,
					ExecutionMode: "live",
					BundleHash:    "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", CreatedAt: now,
				})
				if err != nil {
					t.Fatal(err)
				}
				continuation := decisioncard.HumanTaskContinuation{
					CardID: card.CardID, RunID: runID,
					RequesterRoute: events.RouteIdentity{FlowInstance: "provider/instance-a", EntityID: "requester-entity"},
					ReplyContextID: "reply-context-a", SourceEventID: uuid.NewString(),
					DeadlineAt: now.Add(24 * time.Hour), BudgetBundleHash: card.BundleHash,
					BudgetWindowStart: now, BudgetWindowEnd: now.Add(7 * 24 * time.Hour), CreatedAt: now, UpdatedAt: now,
				}
				switch lifecycle.name {
				case "deferred":
					card.DeferredUntil = now.Add(time.Hour)
					continuation.State = decisioncard.HumanTaskContinuationPending
					continuation.DeferredUntil = card.DeferredUntil
					continuation.DeferCause = "operator_deferred"
				case "expired":
					card.Status = decisioncard.StatusExpired
					card.DecidedAt = now.Add(time.Hour)
					continuation.State = decisioncard.HumanTaskContinuationExpired
					continuation.OutcomeEventID = lifecycleEventID
				}
				cards := &gateLifecycleCardStore{
					created:       []decisioncard.Card{card},
					continuations: map[string]decisioncard.HumanTaskContinuation{card.CardID: continuation},
				}
				bus := &recordingPipelineBus{}
				pc := NewPipelineCoordinatorWithOptions(bus, workflowStore.db, PipelineCoordinatorOptions{
					Module:        &pipelineFixtureWorkflowModule{source: semanticview.Wrap(gateLifecycleBundle())},
					WorkflowStore: workflowStore, DecisionCards: cards, BundleHash: card.BundleHash,
				})
				payload, err := canonicaljson.Bytes(map[string]any{"card_id": card.CardID})
				if err != nil {
					t.Fatal(err)
				}
				parent := eventtest.RuntimeControl(lifecycleEventID, lifecycle.eventType, "platform", "", payload, 0, runID, "", events.EventEnvelope{}, now.Add(time.Hour))
				switch lifecycle.name {
				case "deferred":
					_, err = pc.handleDecisionCardDeferredEvent(ctx, parent)
				case "expired":
					_, err = pc.handleDecisionCardExpiredEvent(ctx, parent)
				}
				if err != nil {
					t.Fatal(err)
				}
				if len(bus.directPublishes) != 1 || bus.directPublishes[0].Type() != lifecycle.productType {
					t.Fatalf("direct lifecycle outcomes = %#v", bus.directPublishes)
				}
				if got := bus.directPublishes[0].TargetRoute().Normalized(); got != continuation.RequesterRoute.Normalized() {
					t.Fatalf("direct requester route = %#v, want %#v", got, continuation.RequesterRoute)
				}
				if len(bus.directRecipients) != 1 || len(bus.directRecipients[0]) != 1 || bus.directRecipients[0][0] != "requester-agent" {
					t.Fatalf("direct recipients = %#v", bus.directRecipients)
				}
			})
		}
	}
}

func TestWorkflowGateEntryUsesOneTransactionAndRollsBackOnCardFailure(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			workflowStore, ctx := tc.open(t)
			ctx = runtimeeffects.WithExecutionMode(ctx, executionmode.Live)
			now := time.Date(2026, time.July, 12, 12, 0, 0, 0, time.UTC)
			entityID := uuid.NewString()
			instance := WorkflowInstance{
				InstanceID: uuid.NewString(), StorageRef: entityID, WorkflowName: "gate-test", WorkflowVersion: "1",
				CurrentState: "drafting", EnteredStageAt: now,
				Metadata: map[string]any{"entity_id": entityID, "run_id": runtimeRunID(ctx)},
			}
			if err := workflowStore.Upsert(ctx, instance); err != nil {
				t.Fatal(err)
			}
			cards := &gateLifecycleCardStore{createErr: errors.New("planted card persistence failure")}
			bundle := gateLifecycleBundle()
			pc := NewPipelineCoordinatorWithOptions(&recordingPipelineBus{}, workflowStore.db, PipelineCoordinatorOptions{
				Module: &pipelineFixtureWorkflowModule{source: semanticview.Wrap(bundle)}, WorkflowStore: workflowStore,
				DecisionCards: cards, BundleHash: "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			})

			err := pc.applyWorkflowGateIntents(ctx, entityID, "drafting", "awaiting_review", "draft.ready")
			if err == nil || err.Error() != cards.createErr.Error() {
				t.Fatalf("applyWorkflowGateIntents error = %v, want planted card failure", err)
			}
			if len(cards.createTx) != 1 || !cards.createTx[0] {
				t.Fatalf("card create transaction evidence = %#v, want active transaction", cards.createTx)
			}
			loaded, ok, err := workflowStore.Load(ctx, entityID)
			if err != nil || !ok {
				t.Fatalf("Load = %#v, %v, %v", loaded, ok, err)
			}
			carrier, err := runtimeengine.StateCarrierFromPersisted(loaded.Metadata, loaded.StateBuckets)
			if err != nil {
				t.Fatal(err)
			}
			activations, err := gateruntime.List(carrier.StateBuckets)
			if err != nil {
				t.Fatal(err)
			}
			if len(activations) != 0 {
				t.Fatalf("gate activations after rollback = %#v, want none", activations)
			}
		})
	}
}

func TestWorkflowGateEntryCreatesMatchingActivationAndCardOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			workflowStore, ctx := tc.open(t)
			now := time.Date(2026, time.July, 12, 12, 0, 0, 0, time.UTC)
			entityID := uuid.NewString()
			if err := workflowStore.Upsert(ctx, WorkflowInstance{
				InstanceID: uuid.NewString(), StorageRef: entityID, WorkflowName: "gate-test", WorkflowVersion: "1",
				CurrentState: "drafting", EnteredStageAt: now, Metadata: map[string]any{"entity_id": entityID, "run_id": runtimeRunID(ctx)},
			}); err != nil {
				t.Fatal(err)
			}
			cards := &gateLifecycleCardStore{}
			pc := NewPipelineCoordinatorWithOptions(&recordingPipelineBus{}, workflowStore.db, PipelineCoordinatorOptions{
				Module: &pipelineFixtureWorkflowModule{source: semanticview.Wrap(gateLifecycleBundle())}, WorkflowStore: workflowStore,
				DecisionCards: cards, BundleHash: "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			})
			if err := pc.applyWorkflowGateIntents(ctx, entityID, "drafting", "awaiting_review", "draft.ready"); err != nil {
				t.Fatal(err)
			}
			if len(cards.created) != 1 || len(cards.createTx) != 1 || !cards.createTx[0] {
				t.Fatalf("created cards/transaction = %#v/%#v", cards.created, cards.createTx)
			}
			loaded, ok, err := workflowStore.Load(ctx, entityID)
			if err != nil || !ok {
				t.Fatalf("Load = %#v, %v, %v", loaded, ok, err)
			}
			carrier, err := runtimeengine.StateCarrierFromPersisted(loaded.Metadata, loaded.StateBuckets)
			if err != nil {
				t.Fatal(err)
			}
			activation, found, err := gateruntime.Load(carrier.StateBuckets, "", "launch_review")
			if err != nil || !found {
				t.Fatalf("gate activation = %#v, %v, %v", activation, found, err)
			}
			route, err := gateruntime.RouteFor(activation.RoutesJSON, "approve")
			if err != nil || route.EmitSchema.Len() == 0 {
				t.Fatalf("gate continuation did not freeze the resolved outcome event schema: %#v, %v", route, err)
			}
			cardAnchor := mustStageGateAnchor(t, cards.created[0])
			if activation.CardID != cards.created[0].CardID || activation.ActivationID != cardAnchor.StageActivationID || activation.Status != gateruntime.StatusOpen {
				t.Fatalf("activation/card mismatch: activation=%#v card=%#v", activation, cards.created[0])
			}
		})
	}
}

func TestWorkflowGateDecisionRoutePublishesAtomicallyAndRecoversIdempotentlyOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			workflowStore, ctx := tc.open(t)
			runID := runtimeRunID(ctx)
			ensurePipelineTestRun(t, workflowStore, runID)
			now := time.Date(2026, time.July, 12, 12, 0, 0, 0, time.UTC)
			entityID := uuid.NewString()
			if err := workflowStore.Upsert(ctx, WorkflowInstance{
				InstanceID: "human-readable-instance", StorageRef: entityID, WorkflowName: "gate-test", WorkflowVersion: "1",
				CurrentState: "awaiting_review", EnteredStageAt: now, Metadata: map[string]any{"entity_id": entityID, "run_id": runID},
			}); err != nil {
				t.Fatal(err)
			}
			cards := &gateLifecycleCardStore{}
			bus := &recordingPipelineBus{}
			if _, err := workflowStore.db.ExecContext(ctx, `CREATE TABLE gate_outcome_atomic_probe (event_id TEXT PRIMARY KEY)`); err != nil {
				t.Fatal(err)
			}
			bus.publishInMutationHook = func(txctx context.Context, evt events.Event) error {
				tx, ok := PipelineSQLTxFromContext(txctx)
				if !ok {
					return errors.New("missing pipeline transaction")
				}
				placeholder := "?"
				if !workflowStore.isSQLite() {
					placeholder = "$1"
				}
				if _, err := tx.ExecContext(txctx, `INSERT INTO gate_outcome_atomic_probe (event_id) VALUES (`+placeholder+`)`, evt.ID()); err != nil {
					return err
				}
				if bus.publishErr != nil {
					return bus.publishErr
				}
				bus.mu.Lock()
				bus.publishes = append(bus.publishes, evt)
				bus.mu.Unlock()
				return nil
			}
			bundleHash := "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			pc := NewPipelineCoordinatorWithOptions(bus, workflowStore.db, PipelineCoordinatorOptions{
				Module: &pipelineFixtureWorkflowModule{source: semanticview.Wrap(gateLifecycleBundle())}, WorkflowStore: workflowStore,
				DecisionCards: cards, BundleHash: bundleHash,
			})
			if err := pc.applyWorkflowGateIntents(ctx, entityID, "", "awaiting_review", "state:awaiting_review"); err != nil {
				t.Fatal(err)
			}
			card := cards.created[0]
			decisionEventID := uuid.NewString()
			if err := workflowStore.CommitDecision(ctx, card, decisionEventID, now.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			card.Status = decisioncard.StatusDecided
			card.Verdict = "approve"
			card.DecisionEventID = decisionEventID
			card.DecidedAt = now.Add(time.Minute)
			route, err := pc.loadStageGateRoute(ctx, card)
			if err != nil {
				t.Fatal(err)
			}
			parent := eventtest.RuntimeControl(decisionEventID, workflowGateDecisionEventType, "platform", "", json.RawMessage(`{"card_id":"`+card.CardID+`"}`), 0, runID, "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), card.DecidedAt)
			emitted, err := workflowGateOutcomeEvent(card, parent, route)
			if err != nil || emitted == nil {
				t.Fatalf("workflowGateOutcomeEvent = %#v, %v", emitted, err)
			}
			bus.publishErr = errors.New("planted outcome persistence failure")
			if err := pc.routeWorkflowGateDecision(ctx, card, parent, route, emitted); !errors.Is(err, bus.publishErr) {
				t.Fatalf("route failure = %v", err)
			}
			assertGateLifecycleState(t, workflowStore, ctx, entityID, "awaiting_review", gateruntime.StatusDecisionCommitted)
			var persisted int
			if err := workflowStore.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM gate_outcome_atomic_probe`).Scan(&persisted); err != nil || persisted != 0 {
				t.Fatalf("rolled-back outcome rows = %d, %v", persisted, err)
			}
			bus.publishErr = nil
			if err := pc.routeWorkflowGateDecision(ctx, card, parent, route, emitted); err != nil {
				t.Fatal(err)
			}
			assertGateLifecycleState(t, workflowStore, ctx, entityID, "operating", gateruntime.StatusRouted)
			if len(bus.publishes) != 1 || bus.publishes[0].ID() != emitted.ID() {
				t.Fatalf("published outcomes = %#v, want one deterministic event %s", bus.publishes, emitted.ID())
			}
			if err := workflowStore.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM gate_outcome_atomic_probe`).Scan(&persisted); err != nil || persisted != 1 {
				t.Fatalf("committed outcome rows = %d, %v", persisted, err)
			}
			if err := pc.routeWorkflowGateDecision(ctx, card, parent, route, emitted); err != nil {
				t.Fatalf("idempotent route recovery: %v", err)
			}
			if len(bus.publishes) != 1 {
				t.Fatalf("idempotent recovery republished outcome: %d", len(bus.publishes))
			}
		})
	}
}

func TestWorkflowGateCommittedDecisionWinsOrdinaryAndTimerExitRacesOnBothStores(t *testing.T) {
	for _, sourceEvent := range []string{"ordinary.transition", "timer:awaiting_review.expired"} {
		for _, tc := range workflowJoinStoreCases() {
			t.Run(tc.name+"/"+sourceEvent, func(t *testing.T) {
				workflowStore, ctx := tc.open(t)
				runID := runtimeRunID(ctx)
				ensurePipelineTestRun(t, workflowStore, runID)
				now := time.Date(2026, time.July, 12, 12, 0, 0, 0, time.UTC)
				entityID := uuid.NewString()
				if err := workflowStore.Upsert(ctx, WorkflowInstance{InstanceID: uuid.NewString(), StorageRef: entityID, WorkflowName: "gate-test", WorkflowVersion: "1", CurrentState: "awaiting_review", EnteredStageAt: now, Metadata: map[string]any{"entity_id": entityID, "run_id": runID}}); err != nil {
					t.Fatal(err)
				}
				cards := &gateLifecycleCardStore{}
				pc := NewPipelineCoordinatorWithOptions(&recordingPipelineBus{}, workflowStore.db, PipelineCoordinatorOptions{Module: &pipelineFixtureWorkflowModule{source: semanticview.Wrap(gateLifecycleBundle())}, WorkflowStore: workflowStore, DecisionCards: cards, BundleHash: "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
				if err := pc.applyWorkflowGateIntents(ctx, entityID, "", "awaiting_review", "state:awaiting_review"); err != nil {
					t.Fatal(err)
				}
				card := cards.created[0]
				if err := workflowStore.CommitDecision(ctx, card, uuid.NewString(), now.Add(time.Minute)); err != nil {
					t.Fatal(err)
				}
				err := workflowStore.RunPipelineMutation(ctx, func(txctx context.Context) error {
					if err := workflowStore.MutateE(txctx, entityID, func(instance *WorkflowInstance) error {
						instance.CurrentState = "operating"
						return nil
					}); err != nil {
						return err
					}
					return pc.applyWorkflowGateIntents(txctx, entityID, "awaiting_review", "operating", sourceEvent)
				})
				if err == nil {
					t.Fatal("competing exit beat a committed verdict")
				}
				assertGateLifecycleState(t, workflowStore, ctx, entityID, "awaiting_review", gateruntime.StatusDecisionCommitted)
			})
		}
	}
}

func TestWorkflowGateDecisionWaitsForItsRecordedBundlePinOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			workflowStore, ctx := tc.open(t)
			runID := runtimeRunID(ctx)
			ensurePipelineTestRun(t, workflowStore, runID)
			now := time.Date(2026, time.July, 12, 12, 0, 0, 0, time.UTC)
			entityID := uuid.NewString()
			if err := workflowStore.Upsert(ctx, WorkflowInstance{InstanceID: uuid.NewString(), StorageRef: entityID, WorkflowName: "gate-test", WorkflowVersion: "1", CurrentState: "awaiting_review", EnteredStageAt: now, Metadata: map[string]any{"entity_id": entityID, "run_id": runID}}); err != nil {
				t.Fatal(err)
			}
			cards := &gateLifecycleCardStore{}
			pc := NewPipelineCoordinatorWithOptions(&recordingPipelineBus{}, workflowStore.db, PipelineCoordinatorOptions{Module: &pipelineFixtureWorkflowModule{source: semanticview.Wrap(gateLifecycleBundle())}, WorkflowStore: workflowStore, DecisionCards: cards, BundleHash: "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
			if err := pc.applyWorkflowGateIntents(ctx, entityID, "", "awaiting_review", "state:awaiting_review"); err != nil {
				t.Fatal(err)
			}
			decisionEventID := uuid.NewString()
			card := cards.created[0]
			if err := workflowStore.CommitDecision(ctx, card, decisionEventID, now.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			card.Status, card.Verdict, card.DecisionEventID, card.DecidedAt = decisioncard.StatusDecided, "approve", decisionEventID, now.Add(time.Minute)
			cards.created[0] = card
			pc.bundleHash = "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
			parent := eventtest.RuntimeControl(decisionEventID, workflowGateDecisionEventType, "platform", "", json.RawMessage(`{"card_id":"`+card.CardID+`"}`), 0, runID, "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), card.DecidedAt)
			if _, err := pc.handleWorkflowGateDecisionEvent(ctx, parent); err == nil {
				t.Fatal("decision routed under an unavailable bundle pin")
			} else {
				if !IsPipelineReceiptDeferred(err) {
					t.Fatalf("bundle-pin error = %T %v, want recoverable pipeline deferral", err, err)
				}
				failure := runtimefailures.Normalize(err, runtimeWorkflowID, "route_gate_decision")
				if failure.Class != runtimefailures.ClassDependencyUnavailable || failure.Detail.Code != "decision_card_bundle_unavailable" || !failure.Retryable {
					t.Fatalf("bundle-pin failure = %#v, want retryable dependency-unavailable classification", failure)
				}
			}
			assertGateLifecycleState(t, workflowStore, ctx, entityID, "awaiting_review", gateruntime.StatusDecisionCommitted)
		})
	}
}

func TestInitialStageLifecycleArmsStandingGateOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			workflowStore, ctx := tc.open(t)
			ctx = runtimeeffects.WithExecutionMode(ctx, executionmode.Live)
			runID := runtimeRunID(ctx)
			ensurePipelineTestRun(t, workflowStore, runID)
			entityID := uuid.NewString()
			if err := workflowStore.Upsert(ctx, WorkflowInstance{InstanceID: "standing-readable-id", StorageRef: entityID, WorkflowName: "gate-test", WorkflowVersion: "1", CurrentState: "awaiting_review", EnteredStageAt: time.Now().UTC(), Metadata: map[string]any{"entity_id": entityID, "run_id": runID, "activation": "standing"}}); err != nil {
				t.Fatal(err)
			}
			cards := &gateLifecycleCardStore{}
			pc := NewPipelineCoordinatorWithOptions(&recordingPipelineBus{}, workflowStore.db, PipelineCoordinatorOptions{Module: &pipelineFixtureWorkflowModule{source: semanticview.Wrap(gateLifecycleBundle())}, WorkflowStore: workflowStore, DecisionCards: cards, BundleHash: "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
			if err := workflowStore.RunPipelineMutation(ctx, func(txctx context.Context) error {
				return pc.ArmFlowInstanceInitialStageLifecycle(txctx, entityID)
			}); err != nil {
				t.Fatal(err)
			}
			if len(cards.created) != 1 || mustStageGateAnchor(t, cards.created[0]).EntityID != entityID {
				t.Fatalf("standing initial cards = %#v", cards.created)
			}
			assertGateLifecycleState(t, workflowStore, ctx, entityID, "awaiting_review", gateruntime.StatusOpen)
		})
	}
}

func TestWorkflowGateTerminationUsesCanonicalPersistedEntityIdentityOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			workflowStore, ctx := tc.open(t)
			runID := runtimeRunID(ctx)
			ensurePipelineTestRun(t, workflowStore, runID)
			entityID := uuid.NewString()
			if err := workflowStore.Upsert(ctx, WorkflowInstance{InstanceID: "display-instance-id", StorageRef: entityID, WorkflowName: "gate-test", WorkflowVersion: "1", CurrentState: "awaiting_review", EnteredStageAt: time.Now().UTC(), Metadata: map[string]any{"entity_id": entityID, "run_id": runID}}); err != nil {
				t.Fatal(err)
			}
			cards := &gateLifecycleCardStore{}
			bus := &recordingPipelineBus{}
			pc := NewPipelineCoordinatorWithOptions(bus, workflowStore.db, PipelineCoordinatorOptions{Module: &pipelineFixtureWorkflowModule{source: semanticview.Wrap(gateLifecycleBundle())}, WorkflowStore: workflowStore, DecisionCards: cards, BundleHash: "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
			if err := pc.applyWorkflowGateIntents(ctx, entityID, "", "awaiting_review", "state:awaiting_review"); err != nil {
				t.Fatal(err)
			}
			if err := workflowStore.MarkTerminated(ctx, entityID, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			if len(cards.supersededFor) != 1 || cards.supersededFor[0] != entityID {
				t.Fatalf("supersession entity identities = %#v, want canonical %s", cards.supersededFor, entityID)
			}
			cardAnchor := mustStageGateAnchor(t, cards.created[0])
			if len(bus.publishes) != 1 || bus.publishes[0].FlowInstance() != cardAnchor.FlowInstance || bus.publishes[0].EntityID() != entityID {
				t.Fatalf("terminated-flow supersession events = %#v, want card flow %q and entity %q", bus.publishes, cardAnchor.FlowInstance, entityID)
			}
		})
	}
}

func TestWorkflowGateOrdinaryExitSupersessionCarriesCardFlowIdentityOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			workflowStore, ctx := tc.open(t)
			runID := runtimeRunID(ctx)
			ensurePipelineTestRun(t, workflowStore, runID)
			entityID := uuid.NewString()
			if err := workflowStore.Upsert(ctx, WorkflowInstance{InstanceID: "child/review-1", StorageRef: entityID, WorkflowName: "gate-test", WorkflowVersion: "1", CurrentState: "drafting", EnteredStageAt: time.Now().UTC(), Metadata: map[string]any{"entity_id": entityID, "run_id": runID}}); err != nil {
				t.Fatal(err)
			}
			cards := &gateLifecycleCardStore{}
			bus := &recordingPipelineBus{}
			pc := NewPipelineCoordinatorWithOptions(bus, workflowStore.db, PipelineCoordinatorOptions{Module: &pipelineFixtureWorkflowModule{source: semanticview.Wrap(gateLifecycleBundle())}, WorkflowStore: workflowStore, DecisionCards: cards, BundleHash: "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
			if err := pc.applyWorkflowGateIntents(ctx, entityID, "drafting", "awaiting_review", "draft.ready"); err != nil {
				t.Fatal(err)
			}
			if err := workflowStore.RunPipelineMutation(ctx, func(txctx context.Context) error {
				if err := workflowStore.MutateE(txctx, entityID, func(instance *WorkflowInstance) error {
					instance.CurrentState = "operating"
					return nil
				}); err != nil {
					return err
				}
				return pc.applyWorkflowGateIntents(txctx, entityID, "awaiting_review", "operating", "review.expired")
			}); err != nil {
				t.Fatal(err)
			}
			if len(bus.publishes) != 1 || len(cards.created) != 1 {
				t.Fatalf("ordinary-exit supersession events = %#v cards = %#v", bus.publishes, cards.created)
			}
			cardAnchor := mustStageGateAnchor(t, cards.created[0])
			if got := bus.publishes[0]; got.RunID() != runID || got.EntityID() != entityID || got.FlowInstance() != cardAnchor.FlowInstance {
				t.Fatalf("ordinary-exit identity = run:%q entity:%q flow:%q, want %q/%q/%q", got.RunID(), got.EntityID(), got.FlowInstance(), runID, entityID, cardAnchor.FlowInstance)
			}
		})
	}
}

func ensurePipelineTestRun(t *testing.T, store *WorkflowInstanceStore, runID string) {
	t.Helper()
	query := `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running') ON CONFLICT (run_id) DO NOTHING`
	if store.isSQLite() {
		query = `INSERT OR IGNORE INTO runs (run_id, status) VALUES (?, 'running')`
	}
	if _, err := store.db.ExecContext(testAuthorActivityContext(t, context.Background()), query, runID); err != nil {
		t.Fatal(err)
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

func assertGateLifecycleState(t *testing.T, store *WorkflowInstanceStore, ctx context.Context, entityID, stage string, status gateruntime.Status) {
	t.Helper()
	loaded, ok, err := store.Load(ctx, entityID)
	if err != nil || !ok {
		t.Fatalf("Load = %#v, %v, %v", loaded, ok, err)
	}
	carrier, err := runtimeengine.StateCarrierFromPersisted(loaded.Metadata, loaded.StateBuckets)
	if err != nil {
		t.Fatal(err)
	}
	activation, found, err := gateruntime.Load(carrier.StateBuckets, "", "launch_review")
	if err != nil || !found || loaded.CurrentState != stage || activation.Status != status {
		t.Fatalf("gate state = stage:%s activation:%#v found:%v err:%v, want %s/%s", loaded.CurrentState, activation, found, err, stage, status)
	}
}

func gateLifecycleBundle() *runtimecontracts.WorkflowContractBundle {
	gates := []runtimecontracts.WorkflowGatePlan{{
		Stage: "awaiting_review", Decision: "launch_review", Outcomes: map[string]runtimecontracts.WorkflowGateOutcomePlan{
			"approve": {Verdict: "approve", AdvancesTo: "operating", Emit: runtimecontracts.EmitSpec{Event: "launch.approved"}},
		},
	}}
	return &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"launch.approved": {Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{}}},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name: "gate-test", Version: "1", InitialStage: "drafting", Gates: gates,
		},
	}
}

func runtimeRunID(ctx context.Context) string {
	// The store test cases always stamp the run identity in context.
	return runtimecorrelation.RunIDFromContext(ctx)
}
