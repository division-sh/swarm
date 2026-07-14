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
		CardID: sourceActivation.CardID, RunID: sourceRunID,
		Anchor:     newDecisionCardTestStageAnchor("launch/review", "launch", entityID, sourceActivation.Stage, sourceActivation.ActivationID),
		Snapshot:   freezeDecisionCardTestSnapshot(t, sourceActivation.DecisionID, map[string]any{"summary": "source snapshot"}, map[string]runtimecontracts.WorkflowGateOutcomePlan{"approve": {Verdict: "approve", AdvancesTo: "done"}}),
		BundleHash: sourceActivation.BundleHash, WorkflowVersion: "1", CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	cardStore := &PostgresStore{DB: db}
	if err := cardStore.CreateDecisionCard(ctx, sourceCard); err != nil {
		t.Fatalf("create source card: %v", err)
	}
	humanCard, humanContinuation := newHumanTaskDecisionCardTestFixture(t, sourceRunID, "source-human-task", now, 10, now.Add(24*time.Hour))
	if err := cardStore.CreateHumanTaskCard(ctx, humanCard, humanContinuation); err != nil {
		t.Fatalf("create source human-task card: %v", err)
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
	forkCard, err := cardStore.GetDecisionCard(ctx, forkActivation.CardID)
	if err != nil {
		t.Fatalf("load fork card: %v", err)
	}
	forkAnchor := mustDecisionCardTestStageAnchor(t, forkCard)
	sourceAnchor := mustDecisionCardTestStageAnchor(t, sourceCard)
	if forkCard.RunID != forkRunID || forkCard.CardID == sourceCard.CardID || forkAnchor.StageActivationID == sourceAnchor.StageActivationID || forkCard.Status != decisioncard.StatusPending {
		t.Fatalf("fork card retained source authority: source=%#v fork=%#v", sourceCard, forkCard)
	}
	summary, _ := forkCard.Snapshot.Context.Lookup("summary")
	forkedFrom, _ := forkCard.Provenance.Lookup("forked_from_card_id")
	if forkCard.CardContentHash != sourceCard.CardContentHash || summary.Interface() != "source snapshot" || forkedFrom.Interface() != sourceCard.CardID {
		t.Fatalf("fork card snapshot/provenance = %#v", forkCard)
	}
	forkCards, _, err := cardStore.ListDecisionCards(ctx, decisioncard.ListOptions{RunID: forkRunID, Limit: 10})
	if err != nil {
		t.Fatalf("list fork cards: %v", err)
	}
	if len(forkCards) != 1 || forkCards[0].CardID != forkCard.CardID || forkCards[0].Anchor.Kind() != decisioncard.AnchorKindStageGate {
		t.Fatalf("fork cards = %#v, want only materialized stage-gate authority", forkCards)
	}
	if sourceHuman, err := cardStore.GetDecisionCard(ctx, humanCard.CardID); err != nil || sourceHuman.RunID != sourceRunID || sourceHuman.Status != decisioncard.StatusPending {
		t.Fatalf("source human task changed during fork = %#v, %v", sourceHuman, err)
	}
}

func TestMaterializeRunForkDecisionCardsPreservesCommittedSemanticFields(t *testing.T) {
	const safeInteger = int64(9007199254740991)
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	sourceRunID, forkRunID, entityID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	now := time.Date(2026, 7, 13, 15, 0, 0, 0, time.UTC)
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $3), ($2::uuid, 'running', $3)`, sourceRunID, forkRunID, now); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	sourceActivation, err := gateruntime.New(sourceRunID, "launch/review", entityID, "launch", "awaiting_review", "launch_review", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "event-1", now)
	if err != nil {
		t.Fatal(err)
	}
	sourceCard, err := decisioncard.New(decisioncard.Card{
		CardID: sourceActivation.CardID, RunID: sourceRunID,
		Anchor: newDecisionCardTestStageAnchor("launch/review", "launch", entityID, sourceActivation.Stage, sourceActivation.ActivationID),
		Snapshot: freezeDecisionCardTestSnapshot(t, sourceActivation.DecisionID, map[string]any{"safe_integer": safeInteger}, map[string]runtimecontracts.WorkflowGateOutcomePlan{
			"approve": {Verdict: "approve", AdvancesTo: "done", Input: map[string]runtimecontracts.WorkflowGateInputField{"score": {Type: "integer", Required: true}}},
		}),
		BundleHash: sourceActivation.BundleHash, WorkflowVersion: "1", CreatedAt: now,
		Provenance: admitDecisionCardTestObject(t, map[string]any{"safe_integer": safeInteger}),
	})
	if err != nil {
		t.Fatal(err)
	}
	cardStore := &PostgresStore{DB: db}
	if err := cardStore.CreateDecisionCard(ctx, sourceCard); err != nil {
		t.Fatalf("create source card: %v", err)
	}
	decisionEventID := uuid.NewString()
	if _, err := cardStore.DecideDecisionCard(ctx, decisioncard.DecideRequest{
		CardID: sourceCard.CardID, Verdict: "approve", Fields: admitDecisionCardTestObject(t, map[string]any{"score": safeInteger}),
		ActorTokenID: "operator", ObservedContentHash: sourceCard.CardContentHash, DecisionEventID: decisionEventID, Now: now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("decide source card: %v", err)
	}
	if err := sourceActivation.CommitDecision(decisionEventID, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	forkActivation, err := gateruntime.New(forkRunID, "launch/review", entityID, "launch", "awaiting_review", "launch_review", sourceActivation.BundleHash, sourceActivation.StartedByEvent, sourceActivation.OpenedAt)
	if err != nil {
		t.Fatal(err)
	}
	if err := forkActivation.CommitDecision(decisionEventID, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := materializeRunForkDecisionCards(ctx, tx, forkRunID, entityID, []runForkGateActivationBinding{{Source: sourceActivation, Fork: forkActivation}}, now.Add(2*time.Minute)); err != nil {
		_ = tx.Rollback()
		t.Fatalf("materialize committed fork card: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	forkCard, err := cardStore.GetDecisionCard(ctx, forkActivation.CardID)
	if err != nil {
		t.Fatal(err)
	}
	field, _ := forkCard.Fields.Lookup("score")
	fieldNumber, ok := field.Number()
	contextNumber, _ := forkCard.Snapshot.Context.Lookup("safe_integer")
	contextValue, _ := contextNumber.Number()
	provenanceNumber, _ := forkCard.Provenance.Lookup("safe_integer")
	provenanceValue, _ := provenanceNumber.Number()
	if forkCard.Status != decisioncard.StatusDecided || forkCard.DecisionEventID != decisionEventID || !ok || fieldNumber != float64(safeInteger) || contextValue != float64(safeInteger) || provenanceValue != float64(safeInteger) {
		t.Fatalf("committed fork card lost semantic authority: %#v", forkCard)
	}
}
