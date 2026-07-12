package store

import (
	"context"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestMaterializeRunForkDecisionCardsCreatesForkLocalPendingAuthority(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	sourceRunID, forkRunID, entityID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	now := time.Date(2026, 7, 12, 15, 0, 0, 0, time.UTC)
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $3), ($2::uuid, 'running', $3)`, sourceRunID, forkRunID, now); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	sourceActivation, err := gateruntime.New(sourceRunID, "launch/review", entityID, "launch", "awaiting_review", "launch_review", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "event-1", now)
	if err != nil {
		t.Fatal(err)
	}
	sourceCard, err := decisioncard.New(decisioncard.Card{
		CardID: sourceActivation.CardID, RunID: sourceRunID, FlowInstance: "launch/review", FlowID: "launch", EntityID: entityID,
		Stage: sourceActivation.Stage, StageActivationID: sourceActivation.ActivationID, DecisionID: sourceActivation.DecisionID,
		Snapshot:   decisioncard.Snapshot{Decision: sourceActivation.DecisionID, Context: map[string]any{"summary": "source snapshot"}, Outcomes: map[string]runtimecontracts.WorkflowGateOutcomePlan{"approve": {Verdict: "approve", AdvancesTo: "done"}}},
		BundleHash: sourceActivation.BundleHash, WorkflowVersion: "1", CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := (&PostgresStore{DB: db}).CreateDecisionCard(ctx, sourceCard); err != nil {
		t.Fatalf("create source card: %v", err)
	}
	forkActivation, err := gateruntime.New(forkRunID, "launch/review", entityID, "launch", "awaiting_review", "launch_review", sourceActivation.BundleHash, sourceActivation.StartedByEvent, sourceActivation.OpenedAt)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := materializeRunForkDecisionCards(ctx, tx, forkRunID, entityID, []runForkGateActivationBinding{{Source: sourceActivation, Fork: forkActivation}}, now.Add(time.Minute)); err != nil {
		_ = tx.Rollback()
		t.Fatalf("materialize fork cards: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	forkCard, err := (&PostgresStore{DB: db}).GetDecisionCard(ctx, forkActivation.CardID)
	if err != nil {
		t.Fatalf("load fork card: %v", err)
	}
	if forkCard.RunID != forkRunID || forkCard.CardID == sourceCard.CardID || forkCard.StageActivationID == sourceCard.StageActivationID || forkCard.Status != decisioncard.StatusPending {
		t.Fatalf("fork card retained source authority: source=%#v fork=%#v", sourceCard, forkCard)
	}
	if forkCard.CardContentHash != sourceCard.CardContentHash || forkCard.Snapshot.Context["summary"] != "source snapshot" || forkCard.Provenance["forked_from_card_id"] != sourceCard.CardID {
		t.Fatalf("fork card snapshot/provenance = %#v", forkCard)
	}
}
