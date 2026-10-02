package runtimepersistence

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func Test2376ForkRejectedEffectHistoricalBoundaryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, afterPoint := range []bool{false, true} {
			name := "rejected_before_point"
			if afterPoint {
				name = "rejected_after_point"
			}
			t.Run(backend+"/"+name, func(t *testing.T) {
				var db *sql.DB
				var selected runForkGateSelectedStore
				if backend == "sqlite" {
					s := newBootstrappedSQLiteRuntimeStoreForTest(t)
					db, selected = s.backend.ConstructionHandle(), s
				} else {
					_, db, _ = testutil.StartPostgres(t)
					selected = admitTestPostgresStore(t, db)
				}
				sourceCtx := testAuthorActivityContext()
				targetBundle := runForkRootGateBundleForTest(t)
				target, err := runtimecontracts.BootBundleIdentity(targetBundle)
				if err != nil {
					t.Fatal(err)
				}
				targetCtx := testAuthorActivityContextForBundle(target.BundleHash)
				registerTestAuthorActivityCatalogForContext(t, selected, targetCtx)
				now := time.Date(2026, 10, 2, 22, 0, 0, 0, time.UTC)
				sourceRun, childRun := uuid.NewString(), uuid.NewString()
				requireRunningRunForTest(t, sourceCtx, selected, sourceRun, now)
				requireRunFixtureForTest(t, targetCtx, selected, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: childRun, BundleHash: target.BundleHash, Artifact: targetBundle.SourceArtifact, StartedAt: now})
				card, effect := newRootProposedEffectTestCard(t, sourceRun, now)
				if err := selected.CreateProposedEffectCard(sourceCtx, card, effect); err != nil {
					t.Fatal(err)
				}
				decisionAt := now.Add(30 * time.Second)
				if afterPoint {
					decisionAt = now.Add(90 * time.Second)
				}
				if _, err := DecisionCardDomainForTest(selected).ApplyDecisionForTest(sourceCtx, decisioncard.DecideRequest{CardID: card.CardID, Verdict: "reject", PrincipalID: "operator", ObservedContentHash: card.CardContentHash, DecisionEventID: uuid.NewString(), Now: decisionAt}); err != nil {
					t.Fatal(err)
				}
				card, err = selected.GetDecisionCard(sourceCtx, card.CardID)
				if err != nil || card.Status != decisioncard.StatusDecided || card.Verdict != "reject" {
					t.Fatalf("source rejection: %+v, %v", card, err)
				}
				effect, err = selected.LoadProposedEffectContinuation(sourceCtx, card.CardID)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := db.ExecContext(targetCtx, `INSERT INTO entity_state
				(run_id,entity_id,flow_instance,entity_type,current_state,gates,fields,accumulator,entered_state_at,created_at,updated_at)
				VALUES ($1,$1,$2,'default','operating','{}','{}','{}',$3,$3,$3)`, childRun, childRun, now); err != nil {
					t.Fatal(err)
				}
				projection, err := projectRunForkEntityOwnership(sourceRun, childRun, sourceRun, sourceRun)
				if err != nil {
					t.Fatal(err)
				}
				correspondence, err := loopruntime.NewForkCorrespondence(nil, childRun, projection.Fork.EntityID)
				if err != nil {
					t.Fatal(err)
				}
				point := runfork.RunForkPoint{EventID: uuid.NewString(), Timestamp: now.Add(time.Minute)}
				apply := func() error {
					return runSelectedFixtureMutation(targetCtx, selected, "materialize rejected effect history", func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
						switch s := selected.(type) {
						case *PostgresStore:
							return s.runForkPostgresOwner.MaterializeRunForkProposedEffectCardsTx(ctx, attempt, sourceRun, childRun, target, projection, point, correspondence, now.Add(2*time.Minute))
						case *SQLiteRuntimeStore:
							return s.runForkSQLiteOwner.MaterializeRunForkProposedEffectCardsTx(ctx, attempt, sourceRun, childRun, target, projection, point, correspondence, now.Add(2*time.Minute))
						default:
							t.Fatalf("unsupported selected owner %T", selected)
							return nil
						}
					})
				}
				if err := apply(); err != nil {
					t.Fatal(err)
				}
				beforeRepeat := snapshotForkHistoricalExecutionTables(t, db, backend == "postgres")
				if err := apply(); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(beforeRepeat, snapshotForkHistoricalExecutionTables(t, db, backend == "postgres")) {
					t.Fatal("repeat duplicated rejected-effect authority or evidence")
				}
				items, _, err := selected.ListDecisionCards(targetCtx, decisioncard.ListOptions{RunID: childRun, Limit: 10})
				if err != nil {
					t.Fatal(err)
				}
				if !afterPoint && len(items) != 0 {
					t.Fatal("historical rejection created child executable authority")
				}
				if afterPoint {
					if len(items) != 1 {
						t.Fatalf("pending-at-point child cards: %d", len(items))
					}
					child, err := selected.GetDecisionCard(targetCtx, items[0].CardID)
					if err != nil || child.Status != decisioncard.StatusPending || child.Verdict != "" || child.DecisionEventID != "" || child.BundleHash != target.BundleHash || child.WorkflowVersion != target.WorkflowVersion {
						t.Fatalf("child inherited later source rejection: %+v, %v", child, err)
					}
					pending, err := selected.LoadProposedEffectContinuation(targetCtx, child.CardID)
					if err != nil || pending.Validate(child) != nil || pending.RequestEventID == effect.RequestEventID || pending.ReplyContextID != "" || pending.State != decisioncard.ProposedEffectPending || pending.DecisionEventID != "" {
						t.Fatalf("child continuation inherited historical authority: %+v, %v", pending, err)
					}
				}
				originalCard, err := selected.GetDecisionCard(sourceCtx, card.CardID)
				if err != nil || !reflect.DeepEqual(originalCard, card) {
					t.Fatalf("source rejection changed: %v", err)
				}
				originalEffect, err := selected.LoadProposedEffectContinuation(sourceCtx, effect.CardID)
				if err != nil || !reflect.DeepEqual(originalEffect, effect) {
					t.Fatalf("source effect history changed: %v", err)
				}
			})
		}
	}
}
