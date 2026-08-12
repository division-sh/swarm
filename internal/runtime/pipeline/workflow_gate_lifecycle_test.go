package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

const gateLifecycleRequesterEntityID = "11111111-1111-4111-8111-111111111111"

type gateLifecycleCardStore struct {
	decisioncard.Store
	createErr     error
	created       []decisioncard.Card
	createTx      []bool
	supersededFor []string
	continuations map[string]decisioncard.HumanTaskContinuation
	completedTx   []bool
}

func newGateLifecyclePipelineCoordinator(bus *recordingPipelineBus, db *sql.DB, opts PipelineCoordinatorOptions) *PipelineCoordinator {
	opts.PipelineObligations = unavailablePipelineTestObligationOwner{}
	if opts.Persistence.store != nil {
		if runner, ok := opts.Persistence.store.engineMutations.(*recordingRuntimeMutationRunner); ok {
			runner.decisionCards = opts.DecisionCards
		}
		if runner, ok := opts.Persistence.store.decisionRoutes.(*recordingRuntimeMutationRunner); ok {
			runner.decisionCards = opts.DecisionCards
		}
	}
	return newDurablePipelineCoordinatorForTest(bus, db, opts)
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

func TestStageGateOwnerRequiresAuthoritativeWorkflowInstance(t *testing.T) {
	entityID := uuid.NewString()
	instancePath := "telegram-ingress/standing-one"
	instance := WorkflowInstance{
		InstanceID: "standing-one", StorageRef: instancePath, WorkflowName: "telegram-ingress",
		Metadata: map[string]any{"flow_path": instancePath, "instance_id": "standing-one", "entity_id": entityID},
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
				source := eventtest.ConcreteTemplateRoutingSource("provider", "provider/instance-a", "11111111-1111-1111-1111-111111111111")
				anchor, err := decisioncard.NewHumanTaskAnchor(decisioncard.HumanTaskAnchor{
					RequesterAgentID: "requester-agent", OperationID: "provider-turn/tool-call-1", Category: "review",
					Scope: scopeCase.scope, Source: source,
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
						RequesterRoute: source.Route(),
						ReplyContextID: "reply-context-a", SourceEventID: uuid.NewString(),
						DeadlineAt: now.Add(24 * time.Hour), BudgetBundleHash: card.BundleHash,
						BudgetWindowStart: now, BudgetWindowEnd: now.Add(7 * 24 * time.Hour),
						State: decisioncard.HumanTaskContinuationDecisionCommitted, OutcomeEventID: decisionEventID,
						CreatedAt: now, UpdatedAt: card.DecidedAt,
					}},
				}
				bus := &recordingPipelineBus{}
				pc := newGateLifecyclePipelineCoordinator(bus, workflowStore.testDB(), PipelineCoordinatorOptions{
					Module:      &pipelineFixtureWorkflowModule{source: semanticview.Wrap(gateLifecycleBundle())},
					Persistence: workflowPersistenceForTest(workflowStore), DecisionCards: cards, HumanTasks: cards, BundleSourceFact: mustPipelineTestBundleSourceFact(card.BundleHash),
				})
				payload, err := canonicaljson.Bytes(map[string]any{"card_id": card.CardID})
				if err != nil {
					t.Fatal(err)
				}
				parent := eventtest.RuntimeControl(decisionEventID, workflowGateDecisionEventType, "platform", "", payload, 0, runID, "", events.EnvelopeForFlowInstance(events.EventEnvelope{}, "provider/instance-a"), card.DecidedAt)
				hostile := cards.continuations[card.CardID]
				hostile.RequesterRoute.EntityID = "foreign-requester"
				cards.continuations[card.CardID] = hostile
				if _, _, err := pc.handleWorkflowGateDecisionEvent(ctx, parent); err == nil {
					t.Fatal("human-task decision accepted a foreign requester route")
				}
				if len(bus.directPublishes) != 0 || len(cards.completedTx) != 0 {
					t.Fatalf("foreign requester mutated outcome state: publishes=%d completions=%#v", len(bus.directPublishes), cards.completedTx)
				}
				hostile.RequesterRoute = source.Route()
				cards.continuations[card.CardID] = hostile
				if _, _, err := pc.handleWorkflowGateDecisionEvent(ctx, parent); err != nil {
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
				if got := bus.directPublishes[0].TargetRoute().Normalized(); got != source.Route() {
					t.Fatalf("direct requester route = %#v", got)
				}
				if len(bus.directContexts) != 1 || bus.directContexts[0].ReplyContextID() != "reply-context-a" || bus.directInMutation[0] {
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
				source := eventtest.ConcreteTemplateRoutingSource("provider", "provider/instance-a", "11111111-1111-1111-1111-111111111111")
				anchor, err := decisioncard.NewHumanTaskAnchor(decisioncard.HumanTaskAnchor{
					RequesterAgentID: "requester-agent", OperationID: "provider-turn/tool-call-1", Category: "review",
					Scope: decisioncard.Scope{Kind: decisioncard.ScopeGlobal}, Source: source,
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
					RequesterRoute: source.Route(),
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
				pc := newGateLifecyclePipelineCoordinator(bus, workflowStore.testDB(), PipelineCoordinatorOptions{
					Module:      &pipelineFixtureWorkflowModule{source: semanticview.Wrap(gateLifecycleBundle())},
					Persistence: workflowPersistenceForTest(workflowStore), DecisionCards: cards, HumanTasks: cards, BundleSourceFact: mustPipelineTestBundleSourceFact(card.BundleHash),
				})
				payload, err := canonicaljson.Bytes(map[string]any{"card_id": card.CardID})
				if err != nil {
					t.Fatal(err)
				}
				parent := eventtest.RuntimeControl(lifecycleEventID, lifecycle.eventType, "platform", "", payload, 0, runID, "", events.EventEnvelope{}, now.Add(time.Hour))
				hostile := cards.continuations[card.CardID]
				hostile.RequesterRoute.FlowInstance = "provider/foreign"
				cards.continuations[card.CardID] = hostile
				switch lifecycle.name {
				case "deferred":
					_, err = pc.handleDecisionCardDeferredEvent(ctx, parent)
				case "expired":
					_, err = pc.handleDecisionCardExpiredEvent(ctx, parent)
				}
				if err == nil {
					t.Fatal("human-task lifecycle accepted a foreign requester route")
				}
				if len(bus.directPublishes) != 0 || len(cards.completedTx) != 0 {
					t.Fatalf("foreign requester mutated lifecycle state: publishes=%d completions=%#v", len(bus.directPublishes), cards.completedTx)
				}
				hostile.RequesterRoute = source.Route()
				cards.continuations[card.CardID] = hostile
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
			instance := materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: uuid.NewString(), StorageRef: entityID, WorkflowName: "gate-test", WorkflowVersion: "1",
				CurrentState: "drafting", EnteredStageAt: now,
				Metadata: map[string]any{"entity_id": entityID, "run_id": runtimeRunID(ctx)},
			})
			if err := workflowStore.upsert(ctx, instance); err != nil {
				t.Fatal(err)
			}
			cards := &gateLifecycleCardStore{createErr: errors.New("planted card persistence failure")}
			bundle := gateLifecycleBundle()
			pc := newGateLifecyclePipelineCoordinator(&recordingPipelineBus{}, workflowStore.testDB(), PipelineCoordinatorOptions{
				Module: &pipelineFixtureWorkflowModule{source: semanticview.Wrap(bundle)}, Persistence: workflowPersistenceForTest(workflowStore),
				DecisionCards: cards, BundleSourceFact: mustPipelineTestBundleSourceFact(pipelineTestBundleHash),
			})

			err := pc.applyWorkflowGateIntents(ctx, testWorkflowInstanceRoute("gate-test"), entityID, "drafting", "awaiting_review", "draft.ready", time.Now().UTC())
			if err == nil || err.Error() != cards.createErr.Error() {
				t.Fatalf("applyWorkflowGateIntents error = %v, want planted card failure", err)
			}
			if len(cards.createTx) != 1 || !cards.createTx[0] {
				t.Fatalf("card create transaction evidence = %#v, want active transaction", cards.createTx)
			}
			loaded, ok, err := workflowStore.Load(ctx, testWorkflowInstanceRoute("gate-test"))
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
			if err := workflowStore.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: uuid.NewString(), StorageRef: entityID, WorkflowName: "gate-test", WorkflowVersion: "1",
				CurrentState: "drafting", EnteredStageAt: now, Metadata: map[string]any{"entity_id": entityID, "run_id": runtimeRunID(ctx)},
			})); err != nil {
				t.Fatal(err)
			}
			cards := &gateLifecycleCardStore{}
			pc := newGateLifecyclePipelineCoordinator(&recordingPipelineBus{}, workflowStore.testDB(), PipelineCoordinatorOptions{
				Module: &pipelineFixtureWorkflowModule{source: semanticview.Wrap(gateLifecycleBundle())}, Persistence: workflowPersistenceForTest(workflowStore),
				DecisionCards: cards, BundleSourceFact: mustPipelineTestBundleSourceFact(pipelineTestBundleHash),
			})
			if err := pc.applyWorkflowGateIntents(ctx, testWorkflowInstanceRoute("gate-test"), entityID, "drafting", "awaiting_review", "draft.ready", time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			if len(cards.created) != 1 || len(cards.createTx) != 1 || !cards.createTx[0] {
				t.Fatalf("created cards/transaction = %#v/%#v", cards.created, cards.createTx)
			}
			loaded, ok, err := workflowStore.Load(ctx, testWorkflowInstanceRoute("gate-test"))
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
			if err := workflowStore.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: "human-readable-instance", StorageRef: entityID, WorkflowName: "gate-test", WorkflowVersion: "1",
				CurrentState: "awaiting_review", EnteredStageAt: now, Metadata: map[string]any{"entity_id": entityID, "run_id": runID},
			})); err != nil {
				t.Fatal(err)
			}
			cards := &gateLifecycleCardStore{}
			bus := &recordingPipelineBus{}
			if _, err := workflowStore.testDB().ExecContext(ctx, `CREATE TABLE gate_outcome_atomic_probe (event_id TEXT PRIMARY KEY)`); err != nil {
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
				return nil
			}
			bundleHash := "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			pc := newGateLifecyclePipelineCoordinator(bus, workflowStore.testDB(), PipelineCoordinatorOptions{
				Module: &pipelineFixtureWorkflowModule{source: semanticview.Wrap(gateLifecycleBundle())}, Persistence: workflowPersistenceForTest(workflowStore),
				DecisionCards: cards, BundleSourceFact: mustPipelineTestBundleSourceFact(bundleHash),
			})
			if err := pc.applyWorkflowGateIntents(ctx, testWorkflowInstanceRoute("gate-test"), entityID, "", "awaiting_review", "state:awaiting_review", time.Now().UTC()); err != nil {
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
			parent := eventtest.RuntimeControl(decisionEventID, workflowGateDecisionEventType, "platform", "", json.RawMessage(`{"card_id":"`+card.CardID+`"}`), 0, runID, "", testWorkflowSourceEnvelope("gate-test", "gate-test", entityID), card.DecidedAt)
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
			if err := workflowStore.testDB().QueryRowContext(ctx, `SELECT COUNT(*) FROM gate_outcome_atomic_probe`).Scan(&persisted); err != nil || persisted != 0 {
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
			if err := workflowStore.testDB().QueryRowContext(ctx, `SELECT COUNT(*) FROM gate_outcome_atomic_probe`).Scan(&persisted); err != nil || persisted != 1 {
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
				if err := workflowStore.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{InstanceID: uuid.NewString(), StorageRef: entityID, WorkflowName: "gate-test", WorkflowVersion: "1", CurrentState: "awaiting_review", EnteredStageAt: now, Metadata: map[string]any{"entity_id": entityID, "run_id": runID}})); err != nil {
					t.Fatal(err)
				}
				cards := &gateLifecycleCardStore{}
				pc := newGateLifecyclePipelineCoordinator(&recordingPipelineBus{}, workflowStore.testDB(), PipelineCoordinatorOptions{Module: &pipelineFixtureWorkflowModule{source: semanticview.Wrap(gateLifecycleBundle())}, Persistence: workflowPersistenceForTest(workflowStore), DecisionCards: cards, BundleSourceFact: mustPipelineTestBundleSourceFact(pipelineTestBundleHash)})
				if err := pc.applyWorkflowGateIntents(ctx, testWorkflowInstanceRoute("gate-test"), entityID, "", "awaiting_review", "state:awaiting_review", time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
				card := cards.created[0]
				if err := workflowStore.CommitDecision(ctx, card, uuid.NewString(), now.Add(time.Minute)); err != nil {
					t.Fatal(err)
				}
				route := testWorkflowInstanceRoute("gate-test")
				transitionCtx := testWorkflowStateTransitionContext(ctx, route, entityID, sourceEvent)
				err := pc.persistWorkflowStateForTest(transitionCtx, route, entityID, "operating", sourceEvent)
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
			if err := workflowStore.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{InstanceID: uuid.NewString(), StorageRef: entityID, WorkflowName: "gate-test", WorkflowVersion: "1", CurrentState: "awaiting_review", EnteredStageAt: now, Metadata: map[string]any{"entity_id": entityID, "run_id": runID}})); err != nil {
				t.Fatal(err)
			}
			cards := &gateLifecycleCardStore{}
			pc := newGateLifecyclePipelineCoordinator(&recordingPipelineBus{}, workflowStore.testDB(), PipelineCoordinatorOptions{Module: &pipelineFixtureWorkflowModule{source: semanticview.Wrap(gateLifecycleBundle())}, Persistence: workflowPersistenceForTest(workflowStore), DecisionCards: cards, BundleSourceFact: mustPipelineTestBundleSourceFact(pipelineTestBundleHash)})
			if err := pc.applyWorkflowGateIntents(ctx, testWorkflowInstanceRoute("gate-test"), entityID, "", "awaiting_review", "state:awaiting_review", time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			decisionEventID := uuid.NewString()
			card := cards.created[0]
			if err := workflowStore.CommitDecision(ctx, card, decisionEventID, now.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			card.Status, card.Verdict, card.DecisionEventID, card.DecidedAt = decisioncard.StatusDecided, "approve", decisionEventID, now.Add(time.Minute)
			cards.created[0] = card
			pc.bundleSourceFact = mustPipelineTestBundleSourceFact("bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
			parent := eventtest.RuntimeControl(decisionEventID, workflowGateDecisionEventType, "platform", "", json.RawMessage(`{"card_id":"`+card.CardID+`"}`), 0, runID, "", testWorkflowSourceEnvelope("gate-test", "gate-test", entityID), card.DecidedAt)
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

func TestInitialStageLifecycleArmsStandingGateOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			workflowStore, ctx := tc.open(t)
			ctx = runtimeeffects.WithExecutionMode(ctx, executionmode.Live)
			runID := runtimeRunID(ctx)
			ensurePipelineTestRun(t, workflowStore, runID)
			entityID := uuid.NewString()
			if err := workflowStore.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{InstanceID: "standing-readable-id", StorageRef: entityID, WorkflowName: "gate-test", WorkflowVersion: "1", CurrentState: "awaiting_review", EnteredStageAt: time.Now().UTC(), Metadata: map[string]any{"entity_id": entityID, "run_id": runID, "activation": "standing"}})); err != nil {
				t.Fatal(err)
			}
			cards := &gateLifecycleCardStore{}
			pc := newGateLifecyclePipelineCoordinator(&recordingPipelineBus{}, workflowStore.testDB(), PipelineCoordinatorOptions{Module: &pipelineFixtureWorkflowModule{source: semanticview.Wrap(gateLifecycleBundle())}, Persistence: workflowPersistenceForTest(workflowStore), DecisionCards: cards, BundleSourceFact: mustPipelineTestBundleSourceFact(pipelineTestBundleHash)})
			if err := applyTestInitialEntryEffect(ctx, pc, testWorkflowInstanceRoute("gate-test"), entityID); err != nil {
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
			ctx = runtimeeffects.WithExecutionMode(ctx, executionmode.Mock)
			runID := runtimeRunID(ctx)
			ensurePipelineTestRun(t, workflowStore, runID)
			entityID := uuid.NewString()
			if err := workflowStore.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{InstanceID: "display-instance-id", StorageRef: entityID, WorkflowName: "gate-test", WorkflowVersion: "1", CurrentState: "awaiting_review", EnteredStageAt: time.Now().UTC(), Metadata: map[string]any{"entity_id": entityID, "run_id": runID}})); err != nil {
				t.Fatal(err)
			}
			cards := &gateLifecycleCardStore{}
			bus := &recordingPipelineBus{}
			pc := newGateLifecyclePipelineCoordinator(bus, workflowStore.testDB(), PipelineCoordinatorOptions{Module: &pipelineFixtureWorkflowModule{source: semanticview.Wrap(gateLifecycleBundle())}, Persistence: workflowPersistenceForTest(workflowStore), DecisionCards: cards, BundleSourceFact: mustPipelineTestBundleSourceFact(pipelineTestBundleHash)})
			if err := pc.applyWorkflowGateIntents(ctx, testWorkflowInstanceRoute("gate-test"), entityID, "", "awaiting_review", "state:awaiting_review", time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			if err := pc.MarkTerminated(ctx, testWorkflowInstanceRoute("gate-test"), identity.NormalizeEntityID(entityID), time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			if len(cards.supersededFor) != 1 || cards.supersededFor[0] != entityID {
				t.Fatalf("supersession entity identities = %#v, want canonical %s", cards.supersededFor, entityID)
			}
			cardAnchor := mustStageGateAnchor(t, cards.created[0])
			if len(bus.publishes) != 1 || bus.publishes[0].FlowInstance() != cardAnchor.Route.InstancePath || bus.publishes[0].EntityID() != entityID {
				t.Fatalf("terminated-flow supersession events = %#v, want card flow %q and entity %q", bus.publishes, cardAnchor.Route.InstancePath, entityID)
			}
			if cards.created[0].ExecutionMode != executionmode.Mock || bus.publishes[0].ExecutionMode() != executionmode.Mock {
				t.Fatalf("terminated-flow modes = card:%q event:%q, want mock", cards.created[0].ExecutionMode, bus.publishes[0].ExecutionMode())
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
			if err := workflowStore.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{InstanceID: "child/review-1", StorageRef: entityID, WorkflowName: "gate-test", WorkflowVersion: "1", CurrentState: "drafting", EnteredStageAt: time.Now().UTC(), Metadata: map[string]any{"entity_id": entityID, "run_id": runID}})); err != nil {
				t.Fatal(err)
			}
			cards := &gateLifecycleCardStore{}
			bus := &recordingPipelineBus{}
			pc := newGateLifecyclePipelineCoordinator(bus, workflowStore.testDB(), PipelineCoordinatorOptions{Module: &pipelineFixtureWorkflowModule{source: semanticview.Wrap(gateLifecycleBundle())}, Persistence: workflowPersistenceForTest(workflowStore), DecisionCards: cards, BundleSourceFact: mustPipelineTestBundleSourceFact(pipelineTestBundleHash)})
			route := testWorkflowInstanceRoute("gate-test")
			entryCtx := testPersistedWorkflowStateTransitionContext(t, workflowStore, ctx, route, entityID, "draft.ready")
			if err := pc.persistWorkflowStateForTest(entryCtx, route, entityID, "awaiting_review", "draft.ready"); err != nil {
				t.Fatal(err)
			}
			transitionCtx := testPersistedWorkflowStateTransitionContext(t, workflowStore, ctx, route, entityID, "review.expired")
			if err := pc.persistWorkflowStateForTest(transitionCtx, route, entityID, "operating", "review.expired"); err != nil {
				t.Fatal(err)
			}
			if len(bus.publishes) != 1 || len(cards.created) != 1 {
				t.Fatalf("ordinary-exit supersession events = %#v direct = %#v durable = %#v superseded = %#v cards = %#v", bus.publishes, bus.directPublishes, bus.outboxIntents, cards.supersededFor, cards.created)
			}
			cardAnchor := mustStageGateAnchor(t, cards.created[0])
			if got := bus.publishes[0]; got.RunID() != runID || got.EntityID() != entityID || got.FlowInstance() != cardAnchor.Route.InstancePath {
				t.Fatalf("ordinary-exit identity = run:%q entity:%q flow:%q, want %q/%q/%q", got.RunID(), got.EntityID(), got.FlowInstance(), runID, entityID, cardAnchor.Route.InstancePath)
			}
		})
	}
}

func ensurePipelineTestRun(t *testing.T, store *workflowInstanceStore, runID string) {
	t.Helper()
	if store.isSQLite() {
		runlifecyclefixture.RequireSQLite(t, testAuthorActivityContext(t, context.Background()), store.testDB(), runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID})
	} else {
		runlifecyclefixture.RequirePostgres(t, testAuthorActivityContext(t, context.Background()), store.testDB(), runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID})
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
	loaded, ok, err := store.Load(ctx, testWorkflowInstanceRoute("gate-test"))
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
