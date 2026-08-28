package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type runForkGateSelectedStore interface {
	decisioncard.Store
	decisioncard.ProposedEffectStore
}

type runForkGateSelectedStoreProof struct {
	StageGatePending            bool
	StageGateForkLocal          bool
	StageGateProvenanceRetained bool
	ProposedEffectPending       bool
	ProposedEffectForkLocal     bool
	ProposedEffectReplyDetached bool
	ProposedEffectInputRetained bool
}

func TestMaterializeRunForkGateAuthoritiesSelectedStoreParity(t *testing.T) {
	proofs := make(map[string]runForkGateSelectedStoreProof, 2)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			var db *sql.DB
			var selected runForkGateSelectedStore
			if backend == "sqlite" {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				db, selected = store.backend.ConstructionHandle(), store
			} else {
				_, db, _ = testutil.StartPostgres(t)
				selected = admitTestPostgresStore(t, db)
			}
			sourceRunID := "00000000-0000-0000-0000-000000023610"
			forkRunID := "00000000-0000-0000-0000-000000023611"
			entityID := "00000000-0000-0000-0000-000000023612"
			now := time.Date(2026, 8, 26, 15, 0, 0, 0, time.UTC)
			requireRunningRunForTest(t, ctx, selected, sourceRunID, now)
			requireRunningRunForTest(t, ctx, selected, forkRunID, now)

			sourceActivation, err := gateruntime.New(sourceRunID, "launch/review", entityID, "launch", "awaiting_review", "launch_review", authorActivityTestBundleHash, testGateRoutes(t), "event-1", now)
			if err != nil {
				t.Fatal(err)
			}
			sourceCard, err := decisioncard.New(decisioncard.Card{
				CardID: sourceActivation.CardID, RunID: sourceRunID, ExecutionMode: "live",
				Anchor: newDecisionCardTestStageAnchor("launch/review", "launch", entityID, sourceActivation.Stage, sourceActivation.ActivationID),
				Snapshot: freezeDecisionCardTestSnapshot(t, sourceActivation.DecisionID, map[string]any{"summary": "source snapshot"}, map[string]runtimecontracts.WorkflowGateOutcomePlan{
					"approve": {Verdict: "approve", AdvancesTo: "done"},
				}),
				BundleHash: sourceActivation.BundleHash, WorkflowVersion: "1", CreatedAt: now,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := selected.CreateDecisionCard(ctx, sourceCard); err != nil {
				t.Fatalf("create source stage gate: %v", err)
			}
			sourceEffectCard, sourceEffect := newProposedEffectTestCard(t, sourceRunID, now, attemptgeneration.Generation{})
			if err := selected.CreateProposedEffectCard(ctx, sourceEffectCard, sourceEffect); err != nil {
				t.Fatalf("create source proposed effect: %v", err)
			}
			if _, err := db.ExecContext(ctx, `
				INSERT INTO entity_state (
					run_id, entity_id, flow_instance, entity_type, current_state,
					gates, fields, accumulator, entered_state_at, created_at, updated_at
				) VALUES ($1, $2, 'root', 'default', 'operating', '{}', '{}', '{}', $3, $3, $3)
			`, forkRunID, sourceEffect.EntityID, now); err != nil {
				t.Fatalf("create fork proposed-effect entity: %v", err)
			}
			forkActivation, err := gateruntime.New(forkRunID, "launch/review", entityID, "launch", "awaiting_review", "launch_review", sourceActivation.BundleHash, sourceActivation.RoutesJSON, sourceActivation.StartedByEvent, sourceActivation.OpenedAt)
			if err != nil {
				t.Fatal(err)
			}
			decisionProjection, err := projectRunForkEntityOwnership(sourceRunID, forkRunID, entityID, "launch/review")
			if err != nil {
				t.Fatal(err)
			}
			effectProjection, err := projectRunForkEntityOwnership(sourceRunID, forkRunID, sourceEffect.EntityID, "root")
			if err != nil {
				t.Fatal(err)
			}
			if err := materializeRunForkGateAuthoritiesForTest(ctx, selected, sourceRunID, forkRunID, decisionProjection, effectProjection, sourceActivation, forkActivation, runfork.RunForkPoint{EventID: uuid.NewString(), Timestamp: now.Add(time.Minute)}, now.Add(2*time.Minute)); err != nil {
				t.Fatalf("materialize fork-local gate authorities: %v", err)
			}

			forkStageCard, err := selected.GetDecisionCard(ctx, forkActivation.CardID)
			if err != nil {
				t.Fatalf("load fork stage gate: %v", err)
			}
			forkedFrom, _ := forkStageCard.Provenance.Lookup("forked_from_card_id")
			items, _, err := selected.ListDecisionCards(ctx, decisioncard.ListOptions{RunID: forkRunID, Limit: 10})
			if err != nil {
				t.Fatalf("list fork gate authorities: %v", err)
			}
			var forkEffectCard decisioncard.Card
			for _, item := range items {
				card, loadErr := selected.GetDecisionCard(ctx, item.CardID)
				if loadErr != nil {
					t.Fatalf("load fork gate authority %s: %v", item.CardID, loadErr)
				}
				if card.Anchor.Kind() == decisioncard.AnchorKindProposedEffect {
					forkEffectCard = card
				}
			}
			if forkEffectCard.CardID == "" {
				t.Fatalf("fork proposed-effect authority missing: %#v", items)
			}
			forkEffect, err := selected.LoadProposedEffectContinuation(ctx, forkEffectCard.CardID)
			if err != nil {
				t.Fatalf("load fork proposed-effect continuation: %v", err)
			}
			proofs[backend] = runForkGateSelectedStoreProof{
				StageGatePending:            forkStageCard.Status == decisioncard.StatusPending,
				StageGateForkLocal:          forkStageCard.RunID == forkRunID && forkStageCard.CardID != sourceCard.CardID,
				StageGateProvenanceRetained: forkedFrom.Interface() == sourceCard.CardID,
				ProposedEffectPending:       forkEffectCard.Status == decisioncard.StatusPending && forkEffect.State == decisioncard.ProposedEffectPending,
				ProposedEffectForkLocal:     forkEffect.RunID == forkRunID && forkEffect.CardID != sourceEffect.CardID && forkEffect.SourceRunID == forkRunID,
				ProposedEffectReplyDetached: sourceEffect.ReplyContextID != "" && forkEffect.ReplyContextID == "",
				ProposedEffectInputRetained: forkEffect.Input.Equal(sourceEffect.Input),
			}
		})
	}
	if !reflect.DeepEqual(proofs["sqlite"], proofs["postgres"]) {
		t.Fatalf("selected-store fork gate materialization differs:\nsqlite=%#v\npostgres=%#v", proofs["sqlite"], proofs["postgres"])
	}
	for backend, proof := range proofs {
		if !proof.StageGatePending || !proof.StageGateForkLocal || !proof.StageGateProvenanceRetained || !proof.ProposedEffectPending || !proof.ProposedEffectForkLocal || !proof.ProposedEffectReplyDetached || !proof.ProposedEffectInputRetained {
			t.Fatalf("%s fork gate proof incomplete: %#v", backend, proof)
		}
	}
}

func materializeRunForkGateAuthoritiesForTest(ctx context.Context, selected runForkGateSelectedStore, sourceRunID, forkRunID string, decisionProjection, effectProjection runForkEntityProjection, sourceActivation, forkActivation gateruntime.Activation, point runfork.RunForkPoint, now time.Time) error {
	operation := func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, decisionMaterializer func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error, effectMaterializer func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		if err := decisionMaterializer(txctx, tx, story); err != nil {
			return err
		}
		return effectMaterializer(txctx, tx, story)
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.runPrivateAuthorActivityMutation(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
			return operation(txctx, tx, story,
				func(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
					return store.runForkPostgresOwner.MaterializeRunForkDecisionCardsTx(ctx, tx, story, forkRunID, decisionProjection, []runForkGateActivationBinding{{Source: sourceActivation, Fork: forkActivation}}, now)
				},
				func(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
					return store.runForkPostgresOwner.MaterializeRunForkProposedEffectCardsTx(ctx, tx, story, sourceRunID, forkRunID, effectProjection, point, now)
				})
		})
	case *SQLiteRuntimeStore:
		return store.runPrivateAuthorActivityMutation(ctx, "test materialize SQLite run-fork gate authorities", func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
			return operation(txctx, tx, story,
				func(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
					return store.runForkSQLiteOwner.MaterializeRunForkDecisionCardsTx(ctx, tx, story, forkRunID, decisionProjection, []runForkGateActivationBinding{{Source: sourceActivation, Fork: forkActivation}}, now)
				},
				func(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
					return store.runForkSQLiteOwner.MaterializeRunForkProposedEffectCardsTx(ctx, tx, story, sourceRunID, forkRunID, effectProjection, point, now)
				})
		})
	default:
		return fmt.Errorf("unsupported selected-store gate test owner %T", selected)
	}
}

func TestMaterializeRunForkRootAuthoritiesExecuteWithForkIdentitySelectedStoreParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			var db *sql.DB
			var selected runForkGateSelectedStore
			if backend == "sqlite" {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				db, selected = store.backend.ConstructionHandle(), store
			} else {
				_, db, _ = testutil.StartPostgres(t)
				selected = admitTestPostgresStore(t, db)
			}

			sourceRunID := "00000000-0000-0000-0000-000000023620"
			forkRunID := "00000000-0000-0000-0000-000000023621"
			now := time.Date(2026, 8, 28, 9, 0, 0, 0, time.UTC)
			requireRunningRunForTest(t, ctx, selected, sourceRunID, now)
			requireRunningRunForTest(t, ctx, selected, forkRunID, now)

			sourceActivation, err := gateruntime.New(sourceRunID, sourceRunID, sourceRunID, "", "awaiting_review", "root_review", authorActivityTestBundleHash, testGateRoutes(t), "event-root", now)
			if err != nil {
				t.Fatal(err)
			}
			sourceAnchor, err := decisioncard.NewStageGateAnchor(decisioncard.StageGateAnchor{
				Route: runtimeflowidentity.RouteForInstancePath(sourceRunID), EntityID: sourceRunID,
				Source: eventtest.RootRoutingSource(sourceRunID), Stage: sourceActivation.Stage,
				StageActivationID: sourceActivation.ActivationID,
			})
			if err != nil {
				t.Fatal(err)
			}
			sourceCard, err := decisioncard.New(decisioncard.Card{
				CardID: sourceActivation.CardID, RunID: sourceRunID, ExecutionMode: "live", Anchor: sourceAnchor,
				Snapshot: freezeDecisionCardTestSnapshot(t, sourceActivation.DecisionID, map[string]any{"summary": "root source"}, map[string]runtimecontracts.WorkflowGateOutcomePlan{
					"approve": {Verdict: "approve", AdvancesTo: "done"},
				}),
				BundleHash: sourceActivation.BundleHash, WorkflowVersion: "1", CreatedAt: now,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := selected.CreateDecisionCard(ctx, sourceCard); err != nil {
				t.Fatalf("create source root stage gate: %v", err)
			}
			sourceEffectCard, sourceEffect := newRootProposedEffectTestCard(t, sourceRunID, now)
			if err := selected.CreateProposedEffectCard(ctx, sourceEffectCard, sourceEffect); err != nil {
				t.Fatalf("create source root proposed effect: %v", err)
			}
			if _, err := db.ExecContext(ctx, `
				INSERT INTO entity_state (
					run_id, entity_id, flow_instance, entity_type, current_state,
					gates, fields, accumulator, entered_state_at, created_at, updated_at
				) VALUES ($1, $2, $3, 'default', 'operating', '{}', '{}', '{}', $4, $4, $4)
			`, forkRunID, forkRunID, forkRunID, now); err != nil {
				t.Fatalf("create fork root entity: %v", err)
			}

			forkActivation, err := gateruntime.New(forkRunID, forkRunID, forkRunID, "", "awaiting_review", "root_review", sourceActivation.BundleHash, sourceActivation.RoutesJSON, sourceActivation.StartedByEvent, sourceActivation.OpenedAt)
			if err != nil {
				t.Fatal(err)
			}
			projection, err := projectRunForkEntityOwnership(sourceRunID, forkRunID, sourceRunID, sourceRunID)
			if err != nil {
				t.Fatal(err)
			}
			if err := materializeRunForkGateAuthoritiesForTest(ctx, selected, sourceRunID, forkRunID, projection, projection, sourceActivation, forkActivation, runfork.RunForkPoint{EventID: uuid.NewString(), Timestamp: now.Add(time.Minute)}, now.Add(2*time.Minute)); err != nil {
				t.Fatalf("materialize fork-local root authorities: %v", err)
			}

			forkStageCard, err := selected.GetDecisionCard(ctx, forkActivation.CardID)
			if err != nil {
				t.Fatalf("load fork root stage gate: %v", err)
			}
			forkStageAnchor := mustDecisionCardTestStageAnchor(t, forkStageCard)
			stageSourceRoute := forkStageAnchor.Source.Route()
			if forkStageAnchor.Route.InstancePath != forkRunID || forkStageAnchor.EntityID != forkRunID || stageSourceRoute.EntityID != forkRunID {
				t.Fatalf("fork root stage authority retained source identity: anchor=%#v source=%#v", forkStageAnchor, stageSourceRoute)
			}
			stageDecisionEventID := uuid.NewString()
			if _, err := selected.DecideDecisionCard(ctx, decisioncard.DecideRequest{
				CardID: forkStageCard.CardID, Verdict: "approve", Fields: admitDecisionCardTestObject(t, map[string]any{}),
				ActorTokenID: "operator", ObservedContentHash: forkStageCard.CardContentHash,
				DecisionEventID: stageDecisionEventID, Now: now.Add(3 * time.Minute),
			}); err != nil {
				t.Fatalf("execute fork root stage authority: %v", err)
			}

			items, _, err := selected.ListDecisionCards(ctx, decisioncard.ListOptions{RunID: forkRunID, Limit: 10})
			if err != nil {
				t.Fatalf("list fork root authorities: %v", err)
			}
			var forkEffectCard decisioncard.Card
			for _, item := range items {
				if item.Anchor.Kind() == decisioncard.AnchorKindProposedEffect {
					forkEffectCard, err = selected.GetDecisionCard(ctx, item.CardID)
					if err != nil {
						t.Fatalf("load fork root proposed effect: %v", err)
					}
				}
			}
			if forkEffectCard.CardID == "" {
				t.Fatalf("fork root proposed-effect authority missing: %#v", items)
			}
			forkEffectAnchor, err := forkEffectCard.Anchor.ProposedEffect()
			if err != nil {
				t.Fatal(err)
			}
			effectSourceRoute := forkEffectAnchor.Source.Route()
			if forkEffectAnchor.Scope.FlowInstance != forkRunID || forkEffectAnchor.Scope.EntityID != forkRunID || effectSourceRoute.EntityID != forkRunID {
				t.Fatalf("fork root proposed-effect authority retained source identity: anchor=%#v source=%#v", forkEffectAnchor, effectSourceRoute)
			}
			effectDecisionEventID := uuid.NewString()
			if _, err := selected.DecideDecisionCard(ctx, decisioncard.DecideRequest{
				CardID: forkEffectCard.CardID, Verdict: "approve", Fields: admitDecisionCardTestObject(t, map[string]any{}),
				ActorTokenID: "operator", ObservedContentHash: forkEffectCard.CardContentHash,
				DecisionEventID: effectDecisionEventID, Now: now.Add(4 * time.Minute),
			}); err != nil {
				t.Fatalf("decide fork root proposed effect: %v", err)
			}
			completed := completeProposedEffectRouteInTestMutation(t, ctx, selected, forkEffectCard.CardID, effectDecisionEventID, now.Add(5*time.Minute))
			if completed.RunID != forkRunID || completed.SourceRunID != forkRunID || completed.EntityID != forkRunID || completed.FlowInstance != forkRunID || completed.State != decisioncard.ProposedEffectRequestReleased {
				t.Fatalf("executed fork root proposed effect retained source authority: %#v", completed)
			}
		})
	}
}

func newRootProposedEffectTestCard(t *testing.T, runID string, now time.Time) (decisioncard.Card, decisioncard.ProposedEffectContinuation) {
	t.Helper()
	card, continuation := newProposedEffectTestCard(t, runID, now, attemptgeneration.Generation{})
	continuation.EntityID = runID
	continuation.FlowInstance = runID
	continuation.SourceRunID = runID
	continuation = continuation.Canonical()
	effect, err := continuation.EffectValue()
	if err != nil {
		t.Fatal(err)
	}
	continuation.EffectContentHash, err = canonicaljson.HashValue(effect)
	if err != nil {
		t.Fatal(err)
	}
	anchor, err := decisioncard.NewProposedEffectAnchor(decisioncard.ProposedEffectAnchor{
		RequestEventID: continuation.RequestEventID, ActivityID: continuation.ActivityID, Decision: "support_reply",
		Scope:  decisioncard.Scope{Kind: decisioncard.ScopeEntity, FlowInstance: runID, EntityID: runID},
		Source: eventtest.RootRoutingSource(runID),
	})
	if err != nil {
		t.Fatal(err)
	}
	card.Anchor = anchor
	card.EffectContentHash = continuation.EffectContentHash
	card, err = decisioncard.New(card)
	if err != nil {
		t.Fatal(err)
	}
	return card, continuation
}

func TestMaterializeRunForkDecisionCardsCreatesForkLocalPendingAuthority(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext()
	sourceRunID, forkRunID, entityID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	now := time.Date(2026, 7, 12, 15, 0, 0, 0, time.UTC)
	requireRunningPostgresRunForTest(t, ctx, db, sourceRunID, now)
	requireRunningPostgresRunForTest(t, ctx, db, forkRunID, now)
	sourceActivation, err := gateruntime.New(sourceRunID, "launch/review", entityID, "launch", "awaiting_review", "launch_review", authorActivityTestBundleHash, testGateRoutes(t), "event-1", now)
	if err != nil {
		t.Fatal(err)
	}
	sourceCard, err := decisioncard.New(decisioncard.Card{
		CardID: sourceActivation.CardID, RunID: sourceRunID,
		ExecutionMode: "live",
		Anchor:        newDecisionCardTestStageAnchor("launch/review", "launch", entityID, sourceActivation.Stage, sourceActivation.ActivationID),
		Snapshot:      freezeDecisionCardTestSnapshot(t, sourceActivation.DecisionID, map[string]any{"summary": "source snapshot"}, map[string]runtimecontracts.WorkflowGateOutcomePlan{"approve": {Verdict: "approve", AdvancesTo: "done"}}),
		BundleHash:    sourceActivation.BundleHash, WorkflowVersion: "1", CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	cardStore := admitTestPostgresStore(t, db)
	if err := cardStore.CreateDecisionCard(ctx, sourceCard); err != nil {
		t.Fatalf("create source card: %v", err)
	}
	humanCard, humanContinuation := newHumanTaskDecisionCardTestFixture(t, sourceRunID, "source-human-task", now, 10, now.Add(24*time.Hour))
	if err := cardStore.CreateHumanTaskCard(ctx, humanCard, humanContinuation); err != nil {
		t.Fatalf("create source human-task card: %v", err)
	}
	forkActivation, err := gateruntime.New(forkRunID, "launch/review", entityID, "launch", "awaiting_review", "launch_review", sourceActivation.BundleHash, sourceActivation.RoutesJSON, sourceActivation.StartedByEvent, sourceActivation.OpenedAt)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := projectRunForkEntityOwnership(sourceRunID, forkRunID, entityID, "launch/review")
	if err != nil {
		t.Fatal(err)
	}
	if err := cardStore.runPrivateAuthorActivityMutation(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		return cardStore.runForkPostgresOwner.MaterializeRunForkDecisionCardsTx(txctx, tx, story, forkRunID, projection, []runForkGateActivationBinding{{Source: sourceActivation, Fork: forkActivation}}, now.Add(time.Minute))
	}); err != nil {
		t.Fatalf("materialize fork cards: %v", err)
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
	ctx := testAuthorActivityContext()
	sourceRunID, forkRunID, entityID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	now := time.Date(2026, 7, 13, 15, 0, 0, 0, time.UTC)
	requireRunningPostgresRunForTest(t, ctx, db, sourceRunID, now)
	requireRunningPostgresRunForTest(t, ctx, db, forkRunID, now)
	sourceActivation, err := gateruntime.New(sourceRunID, "launch/review", entityID, "launch", "awaiting_review", "launch_review", authorActivityTestBundleHash, testGateRoutes(t), "event-1", now)
	if err != nil {
		t.Fatal(err)
	}
	sourceCard, err := decisioncard.New(decisioncard.Card{
		CardID: sourceActivation.CardID, RunID: sourceRunID,
		ExecutionMode: "live",
		Anchor:        newDecisionCardTestStageAnchor("launch/review", "launch", entityID, sourceActivation.Stage, sourceActivation.ActivationID),
		Snapshot: freezeDecisionCardTestSnapshot(t, sourceActivation.DecisionID, map[string]any{"safe_integer": safeInteger}, map[string]runtimecontracts.WorkflowGateOutcomePlan{
			"approve": {Verdict: "approve", AdvancesTo: "done", Input: map[string]runtimecontracts.WorkflowGateInputField{"score": {Type: "integer", Required: true}}},
		}),
		BundleHash: sourceActivation.BundleHash, WorkflowVersion: "1", CreatedAt: now,
		Provenance: admitDecisionCardTestObject(t, map[string]any{"safe_integer": safeInteger}),
	})
	if err != nil {
		t.Fatal(err)
	}
	cardStore := admitTestPostgresStore(t, db)
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
	forkActivation, err := gateruntime.New(forkRunID, "launch/review", entityID, "launch", "awaiting_review", "launch_review", sourceActivation.BundleHash, sourceActivation.RoutesJSON, sourceActivation.StartedByEvent, sourceActivation.OpenedAt)
	if err != nil {
		t.Fatal(err)
	}
	if err := forkActivation.CommitDecision(decisionEventID, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	projection, err := projectRunForkEntityOwnership(sourceRunID, forkRunID, entityID, "launch/review")
	if err != nil {
		t.Fatal(err)
	}
	if err := cardStore.runPrivateAuthorActivityMutation(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		return cardStore.runForkPostgresOwner.MaterializeRunForkDecisionCardsTx(txctx, tx, story, forkRunID, projection, []runForkGateActivationBinding{{Source: sourceActivation, Fork: forkActivation}}, now.Add(2*time.Minute))
	}); err != nil {
		t.Fatalf("materialize committed fork card: %v", err)
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

func TestMaterializeRunForkProposedEffectCreatesFreshPendingAuthority(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext()
	sourceRunID, forkRunID := uuid.NewString(), uuid.NewString()
	now := time.Date(2026, 7, 14, 18, 0, 0, 0, time.UTC)
	requireRunningPostgresRunForTest(t, ctx, db, sourceRunID, now)
	requireRunningPostgresRunForTest(t, ctx, db, forkRunID, now)
	cards := admitTestPostgresStore(t, db)
	sourceCard, sourceContinuation := newProposedEffectTestCard(t, sourceRunID, now, attemptgeneration.Generation{})
	if err := cards.CreateProposedEffectCard(ctx, sourceCard, sourceContinuation); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, current_state,
			gates, fields, accumulator, entered_state_at, created_at, updated_at
		) VALUES ($1::uuid, $2::uuid, 'root', 'default', 'operating', '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, $3, $3, $3)
	`, forkRunID, sourceContinuation.EntityID, now); err != nil {
		t.Fatal(err)
	}
	point := runfork.RunForkPoint{EventID: uuid.NewString(), Timestamp: now.Add(time.Minute)}
	projection, err := projectRunForkEntityOwnership(sourceRunID, forkRunID, sourceContinuation.EntityID, sourceContinuation.FlowInstance)
	if err != nil {
		t.Fatal(err)
	}
	if err := cards.runPrivateAuthorActivityMutation(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		return cards.runForkPostgresOwner.MaterializeRunForkProposedEffectCardsTx(txctx, tx, story, sourceRunID, forkRunID, projection, point, now.Add(2*time.Minute))
	}); err != nil {
		t.Fatal(err)
	}
	items, _, err := cards.ListDecisionCards(ctx, decisioncard.ListOptions{RunID: forkRunID, Limit: 10})
	if err != nil || len(items) != 1 {
		t.Fatalf("fork proposed cards = %#v, %v", items, err)
	}
	forkCard, err := cards.GetDecisionCard(ctx, items[0].CardID)
	if err != nil {
		t.Fatal(err)
	}
	forkContinuation, err := cards.LoadProposedEffectContinuation(ctx, forkCard.CardID)
	if err != nil {
		t.Fatal(err)
	}
	if sourceContinuation.ReplyContextID == "" || forkContinuation.ReplyContextID != "" {
		t.Fatalf("fork reply authority = source:%q fork:%q, want source-only", sourceContinuation.ReplyContextID, forkContinuation.ReplyContextID)
	}
	if forkCard.Status != decisioncard.StatusPending || forkCard.CardID == sourceCard.CardID || forkContinuation.RequestEventID == sourceContinuation.RequestEventID || forkContinuation.SourceRunID != forkRunID {
		t.Fatalf("fork authority retained source identity: source=%#v/%#v fork=%#v/%#v", sourceCard, sourceContinuation, forkCard, forkContinuation)
	}
	if forkContinuation.Input.Equal(sourceContinuation.Input) == false || forkContinuation.EffectContentHash == sourceContinuation.EffectContentHash {
		t.Fatalf("fork effect content = source:%#v fork:%#v", sourceContinuation, forkContinuation)
	}
	forkedFrom, ok := forkCard.Provenance.Lookup("forked_from_card_id")
	if value, stringOK := forkedFrom.String(); !ok || !stringOK || value != sourceCard.CardID {
		t.Fatalf("fork provenance = %#v", forkCard.Provenance)
	}
}

func TestPrepareRunForkApprovedProposedEffectRequiresUnambiguousTerminalEvidence(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     string
		wantErr    string
		wantCopied bool
	}{
		{name: "succeeded", status: "succeeded", wantCopied: true},
		{name: "uncertain", status: "uncertain", wantErr: "ambiguous dispatch evidence"},
		{name: "started", status: "started", wantErr: "recorded evidence is not terminal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			ctx := testAuthorActivityContext()
			sourceRunID, forkRunID := uuid.NewString(), uuid.NewString()
			now := time.Date(2026, 7, 14, 19, 0, 0, 0, time.UTC)
			requireRunningPostgresRunForTest(t, ctx, db, sourceRunID, now)
			requireRunningPostgresRunForTest(t, ctx, db, forkRunID, now)
			cards := admitTestPostgresStore(t, db)
			card, continuation := newProposedEffectTestCard(t, sourceRunID, now, attemptgeneration.Generation{})
			if err := cards.CreateProposedEffectCard(ctx, card, continuation); err != nil {
				t.Fatal(err)
			}
			decisionEventID := uuid.NewString()
			if _, err := cards.DecideDecisionCard(ctx, decisioncard.DecideRequest{
				CardID: card.CardID, Verdict: "approve", ActorTokenID: "operator",
				ObservedContentHash: card.CardContentHash, DecisionEventID: decisionEventID, Now: now.Add(time.Minute),
			}); err != nil {
				t.Fatal(err)
			}
			completeProposedEffectRouteInTestMutation(t, ctx, cards, card.CardID, decisionEventID, now.Add(2*time.Minute))
			if _, err := db.ExecContext(ctx, `
				INSERT INTO entity_state (
					run_id, entity_id, flow_instance, entity_type, current_state,
					gates, fields, accumulator, entered_state_at, created_at, updated_at
				) VALUES ($1::uuid, $2::uuid, 'root', 'default', 'operating', '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, $3, $3, $3)
			`, forkRunID, continuation.EntityID, now); err != nil {
				t.Fatal(err)
			}
			owner, err := activityidentity.ParseOwnerKey(continuation.NodeID)
			if err != nil {
				t.Fatal(err)
			}
			resultEventID := activityidentity.ResultEventID(activityidentity.Fact{
				RunID: sourceRunID, SourceEventID: continuation.SourceEventID, EntityID: continuation.EntityID,
				Owner: owner, ExecutionFlowID: continuation.FlowID,
				HandlerEventKey: continuation.HandlerEventKey,
				ActivityID:      continuation.ActivityID, Tool: continuation.Tool, Attempt: 1,
			}, continuation.SuccessEvent)
			var storedResultEventID any = resultEventID
			var resultEventType any = continuation.SuccessEvent
			var resultPayload any = `{"activity_id":"send_support_reply","result":{"ok":true}}`
			var failure any
			var completedAt any = now.Add(3 * time.Minute)
			switch tc.status {
			case "uncertain":
				resultEventType = continuation.FailureEvent
				resultPayload = `{"activity_id":"send_support_reply","failure":{"code":"provider_outcome_uncertain"}}`
				failure = `{"schema_version":"platform.failure/v1","class":"platform.outcome_uncertain","detail":{"code":"provider_outcome_uncertain"},"retryable":false,"deterministic":false,"message":"Provider outcome is uncertain.","remediation":"Inspect provider state.","component":"activity-runtime","operation":"execute"}`
			case "started":
				storedResultEventID, resultEventType, resultPayload, failure, completedAt = nil, nil, nil, nil, nil
			}
			if _, err := db.ExecContext(ctx, `
				INSERT INTO activity_attempts (
					request_event_id, run_id, execution_mode, source_event_id, entity_id, flow_instance, node_id, handler_event_key,
					activity_id, tool, effect_class, attempt, status, success_event, failure_event,
					result_event_id, result_event_type, result_payload, failure, input_hash, loop_generation, loop_stage,
					started_at, completed_at, updated_at
				) VALUES (
					$1::uuid, $2::uuid, 'live', $3::uuid, $4::uuid, 'root', $5, $6,
					$7, $8, 'non_idempotent_write', 1, $9, $10, $11,
					$12::uuid, $13, $14::jsonb, $15::jsonb, 'input-hash', '{}'::jsonb, '', $16, $17, $16
				)
			`, continuation.RequestEventID, sourceRunID, continuation.SourceEventID, continuation.EntityID,
				continuation.NodeID, continuation.HandlerEventKey, continuation.ActivityID, continuation.Tool, tc.status,
				continuation.SuccessEvent, continuation.FailureEvent, storedResultEventID, resultEventType,
				resultPayload, failure, now.Add(3*time.Minute), completedAt); err != nil {
				t.Fatal(err)
			}
			payload, err := json.Marshal(map[string]any{
				"activity_id": continuation.ActivityID, "tool": continuation.Tool, "input": continuation.Input.Interface(),
				"effect_class": string(continuation.EffectClass), "success_event": continuation.SuccessEvent,
				"failure_event": continuation.FailureEvent, "fork_policy": string(continuation.ForkPolicy),
				"entity_id": continuation.EntityID, "node_id": continuation.NodeID, "flow_id": continuation.FlowID,
				"handler_event_key": continuation.HandlerEventKey, "source_event_id": continuation.SourceEventID,
				"source_run_id": sourceRunID, "attempt": 1,
			})
			if err != nil {
				t.Fatal(err)
			}
			var prepared runfork.RunForkSelectedContractSourceEvent
			err = cards.runPrivateAuthorActivityMutation(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
				var inner error
				prepared, inner = prepareRunForkSelectedContractSourceEvent(txctx, tx, story, forkRunID, runfork.RunForkSelectedContractSourceEvent{
					SourceEventID: continuation.RequestEventID, EventName: runForkActivityRequestEvent,
					ExecutionMode: continuation.ExecutionMode,
					EntityID:      continuation.EntityID, FlowInstance: "root", Payload: payload,
				})
				return inner
			})
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("prepare error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var forkPayload runForkActivityRequestPayload
			if err := json.Unmarshal(prepared.Payload, &forkPayload); err != nil {
				t.Fatal(err)
			}
			forkOwner, err := activityidentity.ParseOwnerKey(forkPayload.NodeID)
			if err != nil {
				t.Fatal(err)
			}
			forkRequestID := activityidentity.RequestEventID(activityidentity.Fact{
				RunID: forkRunID, SourceEventID: forkPayload.SourceEventID, ParentEventID: forkPayload.ParentEventID,
				EntityID: forkPayload.EntityID, Owner: forkOwner, ExecutionFlowID: forkPayload.FlowID,
				HandlerEventKey: forkPayload.HandlerEventKey, ActivityID: forkPayload.ActivityID, Tool: forkPayload.Tool, Attempt: 1,
			})
			var copiedStatus string
			if err := db.QueryRowContext(ctx, `SELECT status FROM activity_attempts WHERE request_event_id = $1::uuid`, forkRequestID).Scan(&copiedStatus); err != nil {
				t.Fatal(err)
			}
			if !tc.wantCopied || copiedStatus != tc.status {
				t.Fatalf("copied status = %q", copiedStatus)
			}
		})
	}
}
