package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type gateLifecycleCardStore struct {
	decisioncard.Store
	createErr error
	created   []decisioncard.Card
	createTx  []bool
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

func TestWorkflowGateEntryUsesOneTransactionAndRollsBackOnCardFailure(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			workflowStore, ctx := tc.open(t)
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
				DecisionCards: cards, BundleFingerprint: "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
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
				DecisionCards: cards, BundleFingerprint: "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
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
			if activation.CardID != cards.created[0].CardID || activation.ActivationID != cards.created[0].StageActivationID || activation.Status != gateruntime.StatusOpen {
				t.Fatalf("activation/card mismatch: activation=%#v card=%#v", activation, cards.created[0])
			}
		})
	}
}

func gateLifecycleBundle() *runtimecontracts.WorkflowContractBundle {
	gates := []runtimecontracts.WorkflowGatePlan{{
		Stage: "awaiting_review", Decision: "launch_review", Outcomes: map[string]runtimecontracts.WorkflowGateOutcomePlan{
			"approve": {Verdict: "approve", AdvancesTo: "operating"},
		},
	}}
	return &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{},
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name: "gate-test", Version: "1", InitialStage: "drafting", Gates: gates,
		},
	}
}

func runtimeRunID(ctx context.Context) string {
	// The store test cases always stamp the run identity in context.
	return runtimecorrelation.RunIDFromContext(ctx)
}
