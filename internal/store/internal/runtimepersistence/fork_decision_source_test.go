package runtimepersistence

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func Test2376ForkDecisionExecutionSourceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, status := range []gateruntime.Status{gateruntime.StatusOpen, gateruntime.StatusDecisionCommitted, gateruntime.StatusRouted, gateruntime.StatusSuperseded} {
			t.Run(backend+"/"+string(status), func(t *testing.T) {
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
				targetHash := targetBundle.SourceArtifact.BundleHash()
				targetCtx := testAuthorActivityContextForBundle(targetHash)
				registerTestAuthorActivityCatalogForContext(t, selected, targetCtx)
				now := time.Date(2026, 10, 2, 22, 0, 0, 0, time.UTC)
				sourceRun, childRun, entity := uuid.NewString(), uuid.NewString(), uuid.NewString()
				requireRunningRunForTest(t, sourceCtx, selected, sourceRun, now)
				requireRunFixtureForTest(t, targetCtx, selected, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: childRun, BundleHash: targetHash, Artifact: targetBundle.SourceArtifact, StartedAt: now})
				activation, err := gateruntime.New(sourceRun, "launch/review", entity, "launch", "awaiting_review", "launch_review", authorActivityTestBundleHash, testGateRoutes(t), "event-1", now)
				if err != nil {
					t.Fatal(err)
				}
				card, err := decisioncard.New(decisioncard.Card{
					CardID: activation.CardID, RunID: sourceRun, ExecutionMode: "live",
					Anchor:     newDecisionCardTestStageAnchor("launch/review", "launch", entity, activation.Stage, activation.ActivationID),
					Snapshot:   freezeDecisionCardTestSnapshot(t, activation.DecisionID, map[string]any{"summary": "frozen source"}, map[string]runtimecontracts.WorkflowGateOutcomePlan{"approve": {Verdict: "approve", AdvancesTo: "done"}}),
					BundleHash: activation.BundleHash, WorkflowVersion: "source-version", CreatedAt: now,
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := selected.CreateDecisionCard(sourceCtx, card); err != nil {
					t.Fatal(err)
				}
				switch status {
				case gateruntime.StatusDecisionCommitted, gateruntime.StatusRouted:
					eventID := uuid.NewString()
					if _, err := DecisionCardDomainForTest(selected).ApplyDecisionForTest(sourceCtx, decisioncard.DecideRequest{CardID: card.CardID, Verdict: "approve", PrincipalID: "operator", ObservedContentHash: card.CardContentHash, DecisionEventID: eventID, Now: now.Add(time.Second)}); err != nil {
						t.Fatal(err)
					}
					if err := activation.CommitDecision(eventID, now.Add(time.Second)); err != nil {
						t.Fatal(err)
					}
					if status == gateruntime.StatusRouted {
						if err := activation.Route(eventID, now.Add(2*time.Second)); err != nil {
							t.Fatal(err)
						}
					}
				case gateruntime.StatusSuperseded:
					if err := selected.SupersedeDecisionCardsForStage(sourceCtx, sourceRun, entity, activation.ActivationID, "stage_exited", now.Add(time.Second)); err != nil {
						t.Fatal(err)
					}
					if !activation.Supersede("stage_exited", now.Add(time.Second)) {
						t.Fatal("source activation did not supersede")
					}
				}
				card, err = selected.GetDecisionCard(sourceCtx, card.CardID)
				if err != nil {
					t.Fatal(err)
				}
				effectCard, effect := newProposedEffectTestCard(t, sourceRun, now, attemptgeneration.Generation{})
				if err := selected.CreateProposedEffectCard(sourceCtx, effectCard, effect); err != nil {
					t.Fatal(err)
				}
				humanCard, human := newHumanTaskDecisionCardTestFixture(t, sourceRun, "source-human-task", now, 10, now.Add(24*time.Hour))
				humanStore := selected.(interface {
					CreateHumanTaskCard(context.Context, decisioncard.Card, decisioncard.HumanTaskContinuation) error
				})
				if err := humanStore.CreateHumanTaskCard(sourceCtx, humanCard, human); err != nil {
					t.Fatal(err)
				}
				humanCard, err = selected.GetDecisionCard(sourceCtx, humanCard.CardID)
				if err != nil {
					t.Fatal(err)
				}
				effect, err = selected.LoadProposedEffectContinuation(sourceCtx, effect.CardID)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := db.ExecContext(targetCtx, `INSERT INTO entity_state
				(run_id,entity_id,flow_instance,entity_type,current_state,gates,fields,accumulator,entered_state_at,created_at,updated_at)
				VALUES ($1,$2,'root','default','operating','{}','{}','{}',$3,$3,$3)`, childRun, effect.EntityID, now); err != nil {
					t.Fatal(err)
				}
				buckets := map[string]map[string]any{}
				if err := gateruntime.Store(buckets, activation); err != nil {
					t.Fatal(err)
				}
				_, bindings, err := forkGateActivationState(runtimeengine.NewStateCarrier(nil, nil, buckets).PersistedStateBuckets(), childRun, "launch/review", entity, targetHash)
				if err != nil {
					t.Fatal(err)
				}
				if len(bindings) != 1 || bindings[0].Fork.Status != status || bindings[0].Fork.BundleHash != targetHash || !reflect.DeepEqual(bindings[0].Source, activation) || bindings[0].Fork.RoutesJSON != activation.RoutesJSON {
					t.Fatal("child activation changed frozen evidence or retained parent authority")
				}
				childActivation := bindings[0].Fork
				stageProjection, err := projectRunForkEntityOwnership(sourceRun, childRun, entity, "launch/review")
				if err != nil {
					t.Fatal(err)
				}
				effectProjection, err := projectRunForkEntityOwnership(sourceRun, childRun, effect.EntityID, effect.FlowInstance)
				if err != nil {
					t.Fatal(err)
				}
				target, err := runtimecontracts.BootBundleIdentity(targetBundle)
				if err != nil {
					t.Fatal(err)
				}
				point := runfork.RunForkPoint{EventID: uuid.NewString(), Timestamp: now.Add(time.Minute)}
				apply := func(identity runtimecontracts.BundleIdentity, projection runForkEntityProjection) error {
					return materializeRunForkGateAuthoritiesForTest(targetCtx, selected, sourceRun, childRun, identity, stageProjection, projection, activation, childActivation, point, now.Add(2*time.Minute))
				}
				beforeFailure := snapshotForkHistoricalExecutionTables(t, db, backend == "postgres")
				contradictory := target
				contradictory.BundleHash = activation.BundleHash
				if err := apply(contradictory, effectProjection); err == nil {
					t.Fatal("accepted source/target pin contradiction")
				}
				wrongEffect := effectProjection
				wrongEffect.Source.FlowInstance = "wrong/source"
				if err := apply(target, wrongEffect); err == nil {
					t.Fatal("accepted contradictory effect ownership after stage insertion")
				}
				if !reflect.DeepEqual(beforeFailure, snapshotForkHistoricalExecutionTables(t, db, backend == "postgres")) {
					t.Fatal("failed materialization left partial child authority or activity")
				}
				if err := apply(target, effectProjection); err != nil {
					t.Fatal(err)
				}
				beforeRepeat := snapshotForkHistoricalExecutionTables(t, db, backend == "postgres")
				if err := apply(target, effectProjection); err != nil {
					t.Fatalf("exact repeat failed: %v", err)
				}
				if !reflect.DeepEqual(beforeRepeat, snapshotForkHistoricalExecutionTables(t, db, backend == "postgres")) {
					t.Fatal("exact repeat duplicated effects or durable activity")
				}
				contradictory = target
				contradictory.WorkflowVersion = card.WorkflowVersion
				if err := apply(contradictory, effectProjection); err == nil {
					t.Fatal("conflicting repeat accepted a parent workflow version")
				}
				if !reflect.DeepEqual(beforeRepeat, snapshotForkHistoricalExecutionTables(t, db, backend == "postgres")) {
					t.Fatal("conflicting repeat changed child authority")
				}
				items, _, err := selected.ListDecisionCards(targetCtx, decisioncard.ListOptions{RunID: childRun, Limit: 10})
				if err != nil || len(items) != 2 {
					t.Fatalf("child decisions: %v %v", items, err)
				}
				for _, item := range items {
					child, err := selected.GetDecisionCard(targetCtx, item.CardID)
					if err != nil || child.BundleHash != targetHash || child.WorkflowVersion != targetBundle.WorkflowVersion() {
						t.Fatalf("child decision retained source authority: %+v %v", child, err)
					}
					if child.Anchor.Kind() == decisioncard.AnchorKindProposedEffect {
						continuation, err := selected.LoadProposedEffectContinuation(targetCtx, child.CardID)
						if err != nil || child.Status != decisioncard.StatusPending || continuation.BundleHash != targetHash || continuation.WorkflowVersion != targetBundle.WorkflowVersion() || continuation.Validate(child) != nil || !continuation.Input.Equal(effect.Input) || continuation.ReplyContextID != "" || continuation.RequestEventID == effect.RequestEventID || continuation.EffectContentHash == effect.EffectContentHash {
							t.Fatalf("child effect retained source authority: %+v %v", continuation, err)
						}
						wrong := child
						wrong.WorkflowVersion = card.WorkflowVersion
						if continuation.Validate(wrong) == nil {
							t.Fatal("accepted contradictory executable workflow version")
						}
					} else {
						before, err := card.Snapshot.SemanticValue()
						after, afterErr := child.Snapshot.SemanticValue()
						if err != nil || afterErr != nil || !before.Equal(after) || child.CardContentHash != card.CardContentHash || child.DecisionSchemaHash != card.DecisionSchemaHash {
							t.Fatal("frozen stage snapshot was rewritten")
						}
						if child.Status != card.Status || child.Verdict != card.Verdict || child.DecisionEventID != card.DecisionEventID || child.SupersededReason != card.SupersededReason || !child.Fields.Equal(card.Fields) {
							t.Fatal("child gate disposition lost frozen semantic evidence")
						}
					}
				}
				original, err := selected.GetDecisionCard(sourceCtx, card.CardID)
				if err != nil || !reflect.DeepEqual(original, card) {
					t.Fatalf("source card changed: %v", err)
				}
				originalEffect, err := selected.LoadProposedEffectContinuation(sourceCtx, effect.CardID)
				if err != nil || !reflect.DeepEqual(originalEffect, effect) {
					t.Fatalf("source effect changed: %v", err)
				}
				originalHuman, err := selected.GetDecisionCard(sourceCtx, humanCard.CardID)
				if err != nil || !reflect.DeepEqual(originalHuman, humanCard) {
					t.Fatalf("excluded source human task changed: %+v, %v", originalHuman, err)
				}
			})
		}
	}
}
