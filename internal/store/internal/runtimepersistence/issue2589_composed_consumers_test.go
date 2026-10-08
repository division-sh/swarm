package runtimepersistence

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/runstate"
	"github.com/google/uuid"
)

func TestIssue2589ComposedConsumersPreserveResultsBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			opened := backend.open(t)
			selected := opened.store
			ctx := testAuthorActivityContext()
			owner := selected.(snapshotOwnershipStore)
			cards := selected.(decisioncard.Store)
			at := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)

			t.Run("scenario_multiple_entities_and_diffs", func(t *testing.T) {
				requireDefaultSourceArtifactForTest(t, ctx, selected)
				runID := uuid.NewString()
				request := pipeline.ScenarioSetupRequest{RunID: runID, CreatedAt: at}
				for _, alias := range []string{"one", "two"} {
					request.Entities = append(request.Entities, pipeline.ScenarioSetupEntityRequest{
						Alias: alias, EntityID: uuid.NewString(), FlowInstance: "owner/" + alias,
						EntityType: "review_item", CurrentState: "active",
						Fields: map[string]any{"note": alias, "count": 7}, Gates: map[string]bool{"ready": true},
					})
				}
				result, err := owner.SetupScenarioEntities(ctx, request)
				if err != nil || !result.Acknowledged || len(result.Entities) != 2 {
					t.Fatalf("scenario result = %+v, err=%v", result, err)
				}
			})

			for _, operation := range []string{"ordinary_engine", "engine_card_creation", "engine_supersession", "standalone_card_first", "standalone_supersession_first", "fork_materialization"} {
				t.Run(operation, func(t *testing.T) {
					f := newSnapshotOwnershipFixture(t, eventRecordContractBackend{name: backend.name, open: func(*testing.T) authorActivityReceiptFixture { return opened }}, false, false)
					fact, ok := correlation.SourceArtifactFactFromContext(f.ctx)
					if !ok {
						t.Fatal("snapshot construction has no admitted source")
					}
					bundleHash := fact.BundleHash()
					state := f.state
					state.Transition = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
					state.ExpectedState, state.ExpectedRevision = "ready", 2
					state.CurrentState, state.UpdatedAt = "later", time.Now().UTC()
					command := pipeline.WorkflowEngineMutationCommand{State: state}
					card := newDecisionCardTestCard(t, f.runID, at)
					card.BundleHash = bundleHash
					card.Anchor = newDecisionCardTestStageAnchor("owner/one", "owner", f.entityID, "ready", uuid.NewString())
					var err error
					card, err = decisioncard.New(card)
					if err != nil {
						t.Fatal(err)
					}
					if operation == "engine_supersession" || operation == "standalone_supersession_first" {
						if err := cards.CreateDecisionCard(f.ctx, card); err != nil {
							t.Fatal(err)
						}
						anchor := mustDecisionCardTestStageAnchor(t, card)
						command.Lifecycle.GateCards = []pipeline.WorkflowGateCardMutation{{Kind: pipeline.WorkflowGateCardMutationSupersede, Card: card, EntityID: f.entityID, ActivationID: anchor.StageActivationID, Reason: "stage_exited", OccurredAt: state.UpdatedAt}}
					} else if operation == "engine_card_creation" {
						command.Lifecycle.GateCards = []pipeline.WorkflowGateCardMutation{{Kind: pipeline.WorkflowGateCardMutationCreate, Card: card, EntityID: f.entityID}}
					}
					switch operation {
					case "fork_materialization":
						fork, err := owner.MaterializeRunFork(f.ctx, runfork.RunForkMaterializeRequest{SourceRunID: f.runID, At: f.eventID})
						if err != nil || !fork.ExecutionReady || fork.MaterializedEntityCount != 1 {
							t.Fatalf("real fork materialization = %+v, %v", fork, err)
						}
					case "standalone_card_first":
						if err := cards.CreateDecisionCard(f.ctx, card); err != nil {
							t.Fatal(err)
						}
						// The consume-only card path is unlocked and cannot mint first admission.
					case "standalone_supersession_first":
						anchor := mustDecisionCardTestStageAnchor(t, card)
						if err := cards.SupersedeDecisionCardsForStage(f.ctx, f.runID, anchor.EntityID, anchor.StageActivationID, "stage_exited", state.UpdatedAt); err != nil {
							t.Fatal(err)
						}
					default:
						if _, err := owner.CommitWorkflowEngineMutation(f.ctx, command); err != nil {
							t.Fatal(err)
						}
						assertWorkflowTargetTransitionRows(t, backend.name, opened.db, f.runID, f.entityID, "owner/one", "owner", "later", 3, 1)
					}
					if operation != "ordinary_engine" && operation != "fork_materialization" {
						stored, err := cards.GetDecisionCard(f.ctx, card.CardID)
						wantStatus := decisioncard.StatusPending
						if operation == "engine_supersession" || operation == "standalone_supersession_first" {
							wantStatus = decisioncard.StatusSuperseded
						}
						if err != nil || stored.Status != wantStatus || stored.RunID != f.runID || stored.BundleHash != bundleHash {
							t.Fatalf("committed card = %+v, err=%v, want status=%s", stored, err, wantStatus)
						}
					}
				})
			}

			t.Run("fanout_two_real_publications", func(t *testing.T) {
				// The publication fixture installs its own exact process source set.
				opened := backend.open(t)
				selected := opened.store
				if sqlite, ok := selected.(*SQLiteRuntimeStore); ok {
					// Receipt fixtures freeze July's clock; issuance here uses real current time.
					sqlite.nowFn = time.Now
				}
				_, fanctx, _, _, fanowner, command := prepareP16PublicationGroup(t, selected.(selectedFanOutLifecycleOwner), opened.db, backend.name, 2)
				committed, err := fanowner.CommitFanOutChunk(fanctx, command)
				if err != nil || len(committed.Publications) != 2 || committed.Intent.Cursor != 2 {
					t.Fatalf("two-publication native fan-out = %+v, %v", committed, err)
				}
			})
		})
	}
}

func TestIssue2589NativeAdmissionFrameIsolationBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			opened := backend.open(t)
			ctx := testAuthorActivityContext()
			runs := []string{uuid.NewString(), uuid.NewString()}
			for _, runID := range runs {
				requireRunningRunForTest(t, ctx, opened.store, runID, time.Now().UTC())
			}
			active := func(ctx context.Context, tx *sql.Tx, runID string) error {
				if backend.name == "postgres" {
					return runstate.RequirePostgresActiveTx(ctx, tx, runID)
				}
				return runstate.RequireSQLiteActiveTx(ctx, tx, runID)
			}
			for _, control := range []string{"independent_first", "admitted_nested_different_runs"} {
				t.Run(control, func(t *testing.T) {
					operation := func() error {
						return runSelectedFixtureMutation(ctx, opened.store, "issue2589 native admission controls", func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
							return attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
								for _, runID := range runs {
									if _, cached, err := mutationprotocol.CachedActiveRunSource(txctx, tx, runID); err != nil || cached {
										t.Fatalf("fresh native frame reused admission: cached=%t err=%v", cached, err)
									}
								}
								if err := active(txctx, tx, runs[0]); err != nil {
									return err
								}
								for i := 0; i < 2; i++ {
									switch control {
									case "independent_first":
										return nil
									case "admitted_nested_different_runs":
										if err := attempt.WithSQL(txctx, func(nestedctx context.Context, nestedtx *sql.Tx) error {
											if err := active(nestedctx, nestedtx, runs[0]); err != nil {
												return err
											}
											if err := active(nestedctx, nestedtx, runs[1]); err != nil {
												return err
											}
											for _, runID := range runs {
												fact, cached, err := mutationprotocol.CachedActiveRunSource(nestedctx, nestedtx, runID)
												if err != nil || !cached || fact.BundleHash() != authorActivityTestBundleHash {
													t.Fatalf("nested exact admission for %s = %t %+v %v", runID, cached, fact, err)
												}
											}
											return nil
										}); err != nil {
											return err
										}
									}
								}
								return nil
							})
						})
					}
					if err := operation(); err != nil {
						t.Fatal(err)
					}
					switch control {
					case "independent_first":
						if err := operation(); err != nil {
							t.Fatal(err)
						}
						return
					case "admitted_nested_different_runs":
					}
				})
			}
		})
	}
}
