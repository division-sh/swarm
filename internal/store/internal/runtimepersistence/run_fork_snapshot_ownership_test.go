package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

type snapshotOwnershipStore interface {
	runForkSelectedLifecycleStore
	runtimepipeline.WorkflowEngineMutationOwner
	SetupScenarioEntities(context.Context, runtimepipeline.ScenarioSetupRequest) (runtimepipeline.ScenarioSetupResult, error)
}

type snapshotOwnershipFixture struct {
	store                    snapshotOwnershipStore
	db                       *sql.DB
	ctx                      context.Context
	runID, entityID, eventID string
	state                    runtimepipeline.WorkflowEngineStateRecord
}

func newSnapshotOwnershipFixture(t *testing.T, backend eventRecordContractBackend, eventContext, reverse bool) snapshotOwnershipFixture {
	t.Helper()
	opened := backend.open(t)
	f := snapshotOwnershipFixture{store: opened.store.(snapshotOwnershipStore), db: opened.db, ctx: testAuthorActivityContext(), runID: uuid.NewString(), entityID: uuid.NewString()}
	f.ctx = runtimecorrelation.WithRunID(f.ctx, f.runID)
	requireDefaultSourceArtifactForTest(t, f.ctx, opened.store)
	at := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	if _, err := f.store.SetupScenarioEntities(f.ctx, runtimepipeline.ScenarioSetupRequest{
		RunID: f.runID, CreatedAt: at,
		Entities: []runtimepipeline.ScenarioSetupEntityRequest{{
			Alias: "subject", EntityID: f.entityID, FlowInstance: "owner/one", EntityType: "review_item", CurrentState: "active",
			Fields: map[string]any{"entity_type": "authored-not-owner"},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	f.state = stateOnlyWorkflowEngineMutationRecord(t, f.runID, "owner", "owner/one", f.entityID, "active", 1, at)
	f.state.CurrentState = "ready"
	f.state.Slug, f.state.Name = "snapshot-slug", "Snapshot Name"
	f.state.Fields = json.RawMessage(`{"entity_type":"authored-not-owner","value":"at-R"}`)
	f.state.Gates = json.RawMessage(`{"review":true}`)
	f.state.Bookkeeping = json.RawMessage(`{"activation":"snapshot"}`)
	f.state.Accumulator = json.RawMessage(`{"total":7}`)
	if _, err := f.store.CommitWorkflowEngineMutation(f.ctx, runtimepipeline.WorkflowEngineMutationCommand{State: f.state}); err != nil {
		t.Fatal(err)
	}
	flows := []string{"causal/one", "causal/two"}
	if !eventContext {
		flows = []string{""}
	}
	if reverse {
		flows[0], flows[1] = flows[1], flows[0]
	}
	for _, flow := range flows {
		f.eventID = uuid.NewString()
		event := eventtest.ExistingRunRootIngress(f.eventID, "fork.metadata_context", "metadata-proof", "", json.RawMessage(`{"entity_type":"event-not-owner"}`), 0,
			f.runID, events.EventEnvelope{EntityID: f.entityID, FlowInstance: flow}, time.Now().UTC())
		if err := commitSemanticPipelineProcessedEventFixture(f.ctx, f.store, event); err != nil {
			t.Fatal(err)
		}
	}
	return f
}

func (f snapshotOwnershipFixture) plan(t *testing.T) runfork.RunForkPlan {
	t.Helper()
	plan, err := f.store.PlanRunFork(f.ctx, runfork.RunForkPlanRequest{SourceRunID: f.runID, At: f.eventID})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func (f snapshotOwnershipFixture) advance(t *testing.T) {
	t.Helper()
	state := f.state
	state.Transition = runtimepipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
	state.ExpectedRevision, state.ExpectedState = 2, "ready"
	state.CurrentState, state.Name, state.Slug = "later", "Later Name", "later-slug"
	state.Fields, state.Gates = json.RawMessage(`{"entity_type":"later-authored","value":"after-R"}`), json.RawMessage(`{"review":false}`)
	state.Bookkeeping, state.Accumulator = json.RawMessage(`{"activation":"later"}`), json.RawMessage(`{"total":99}`)
	state.UpdatedAt, state.EnteredStageAt = time.Now().UTC(), time.Now().UTC()
	if _, err := f.store.CommitWorkflowEngineMutation(f.ctx, runtimepipeline.WorkflowEngineMutationCommand{State: state}); err != nil {
		t.Fatal(err)
	}
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "fork.after_metadata", "metadata-proof", "", json.RawMessage(`{}`), 0, f.runID, events.EventEnvelope{}, time.Now().UTC())
	if err := commitSemanticPipelineProcessedEventFixture(f.ctx, f.store, event); err != nil {
		t.Fatal(err)
	}
}

func TestRunForkSnapshotOwnershipMetadataBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			for _, order := range []string{"ordinary_materialization", "forward", "reverse"} {
				t.Run(order, func(t *testing.T) {
					f := newSnapshotOwnershipFixture(t, backend, order != "ordinary_materialization", order == "reverse")
					plan := f.plan(t)
					if len(plan.Entities) != 1 {
						t.Fatalf("plan = %#v", plan)
					}
					entity := plan.Entities[0]
					wantMetadata := runfork.RunForkMaterializedEntitySnapshotMetadata{
						Owner: runfork.RunForkMaterializedEntitySnapshotMetadataOwner, Source: runfork.RunForkMaterializedEntitySnapshotMetadataSourceEntityState,
						FlowInstance: "owner/one", EntityType: "review_item", Slug: "snapshot-slug", Name: "Snapshot Name",
					}
					if entity.MaterializationMetadata == nil || *entity.MaterializationMetadata != wantMetadata {
						t.Fatalf("metadata = %#v, want %#v", entity.MaterializationMetadata, wantMetadata)
					}
					if entity.Fields["entity_type"] != "authored-not-owner" {
						t.Fatalf("authored type field lost: %#v", entity.Fields)
					}
					f.advance(t)
					if later := f.plan(t); !reflect.DeepEqual(later.Entities, plan.Entities) || later.ForkPoint != plan.ForkPoint {
						t.Fatalf("fixed revision changed: before=%#v after=%#v", plan, later)
					}
					if order != "ordinary_materialization" {
						if plan.ExecutionReady || len(plan.UnsupportedBlockers) != 1 || plan.UnsupportedBlockers[0].Code != runfork.RunForkBlockerFlowRouteHistoryUnproven {
							t.Fatalf("event context must retain ordinary route-history refusal: %#v", plan.UnsupportedBlockers)
						}
						return
					}
					req := runfork.RunForkMaterializeRequest{SourceRunID: f.runID, At: f.eventID}
					materialized, err := f.store.MaterializeRunFork(f.ctx, req)
					if err != nil {
						t.Fatal(err)
					}
					assertSnapshotOwnershipEntity(t, f, materialized.ForkRunID, entity, 1)
					before := snapshotOwnershipCounts(t, f, materialized.ForkRunID)
					replayed, err := f.store.MaterializeRunFork(f.ctx, req)
					if err != nil || replayed.ForkRunID != materialized.ForkRunID {
						t.Fatalf("exact replay = %#v, %v", replayed, err)
					}
					assertSnapshotOwnershipEntity(t, f, materialized.ForkRunID, entity, 1)
					if after := snapshotOwnershipCounts(t, f, materialized.ForkRunID); after != before {
						t.Fatalf("exact replay wrote facts: before=%v after=%v", before, after)
					}
					if _, err := f.store.ActivateRunFork(f.ctx, runfork.RunForkActivateRequest{ForkRunID: materialized.ForkRunID, ConfirmSourceFreeze: true}); err == nil {
						t.Fatal("state-only activation accepted source advancement")
					} else if _, fact, ok := runForkReplayResumeBlockerFromError(err); !ok || fact != runfork.RunForkReplayResumeFactSourceAdvanced {
						t.Fatalf("activation refused for wrong reason: %v", err)
					}
					if after := snapshotOwnershipCounts(t, f, materialized.ForkRunID); after != before {
						t.Fatalf("refused activation wrote facts: before=%v after=%v", before, after)
					}
					var sourceState, sourceName string
					if err := f.db.QueryRowContext(f.ctx, `SELECT current_state, name FROM entity_state WHERE run_id=$1 AND entity_id=$2`, f.runID, f.entityID).Scan(&sourceState, &sourceName); err != nil {
						t.Fatal(err)
					}
					if sourceState != "later" || sourceName != "Later Name" {
						t.Fatalf("fork changed source: %q %q", sourceState, sourceName)
					}
				})
			}
		})
	}
}

func assertSnapshotOwnershipEntity(t *testing.T, f snapshotOwnershipFixture, forkRunID string, want runfork.RunForkEntityState, wantRevision int64) {
	t.Helper()
	got := readSnapshotOwnershipEntity(t, f, forkRunID)
	meta := want.MaterializationMetadata
	if got.Flow != meta.FlowInstance || got.EntityType != meta.EntityType || got.Slug != meta.Slug || got.Name != meta.Name || got.State != want.CurrentState || got.Revision != wantRevision || want.EnteredStateAt == nil || !got.Entered.Equal(*want.EnteredStateAt) {
		t.Fatalf("fork row differs: got=%#v want=%#v revision=%d", got, want, wantRevision)
	}
	for i, bucket := range []map[string]any{want.Fields, want.Gates, want.Bookkeeping, want.Accumulator} {
		if !reflect.DeepEqual(got.Buckets[i], bucket) {
			t.Fatalf("fork bucket = %#v, want %#v", got.Buckets[i], bucket)
		}
	}
}

type snapshotOwnershipEntityRow struct {
	Flow, EntityType, Slug, Name, State string
	Revision                            int64
	Entered                             time.Time
	Buckets                             [4]map[string]any
}

func readSnapshotOwnershipEntity(t *testing.T, f snapshotOwnershipFixture, runID string) snapshotOwnershipEntityRow {
	t.Helper()
	var flow, entityType, slug, name, state string
	var fields, gates, bookkeeping, accumulator []byte
	var revision int64
	var rawEntered any
	if err := f.db.QueryRowContext(f.ctx, `SELECT flow_instance, entity_type, slug, name, current_state, fields, gates, bookkeeping, accumulator, revision, entered_state_at FROM entity_state WHERE run_id=$1 AND entity_id=$2`, runID, f.entityID).Scan(&flow, &entityType, &slug, &name, &state, &fields, &gates, &bookkeeping, &accumulator, &revision, &rawEntered); err != nil {
		t.Fatal(err)
	}
	entered, present, err := sqliteTimeValue(rawEntered)
	if err != nil || !present {
		t.Fatalf("decode committed entered_state_at: present=%t err=%v raw=%#v", present, err, rawEntered)
	}
	row := snapshotOwnershipEntityRow{Flow: flow, EntityType: entityType, Slug: slug, Name: name, State: state, Revision: revision, Entered: entered}
	for i, raw := range [][]byte{fields, gates, bookkeeping, accumulator} {
		if err := json.Unmarshal(raw, &row.Buckets[i]); err != nil {
			t.Fatal(err)
		}
	}
	return row
}

func snapshotOwnershipCounts(t *testing.T, f snapshotOwnershipFixture, runID string) [5]int {
	t.Helper()
	var counts [5]int
	for i, table := range []string{"entity_state", "entity_mutations", "run_fork_fact_revisions", "events", "flow_instances"} {
		if err := f.db.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM "+table+" WHERE run_id=$1", runID).Scan(&counts[i]); err != nil {
			t.Fatal(err)
		}
	}
	return counts
}

func TestRunForkSnapshotOwnershipInvalidMetadataBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			for _, phase := range []string{"initial", "exact_replay"} {
				t.Run(phase, func(t *testing.T) {
					for _, invalid := range []string{"missing", "blank_flow", "blank_type", "duplicate_identical", "duplicate_flow", "duplicate_type"} {
						t.Run(invalid, func(t *testing.T) {
							f := newSnapshotOwnershipFixture(t, backend, false, false)
							plan := f.plan(t)
							var forkRunID string
							var forkBefore [5]int
							var forkEntityBefore snapshotOwnershipEntityRow
							if phase == "exact_replay" {
								materialized, err := f.store.MaterializeRunFork(f.ctx, runfork.RunForkMaterializeRequest{SourceRunID: f.runID, At: f.eventID})
								if err != nil {
									t.Fatal(err)
								}
								forkRunID = materialized.ForkRunID
								forkBefore = snapshotOwnershipCounts(t, f, forkRunID)
								assertSnapshotOwnershipEntity(t, f, forkRunID, plan.Entities[0], 1)
								forkEntityBefore = readSnapshotOwnershipEntity(t, f, forkRunID)
							}
							f.advance(t)
							sourceEntityBefore := readSnapshotOwnershipEntity(t, f, f.runID)
							latest, err := f.store.PlanRunFork(f.ctx, runfork.RunForkPlanRequest{SourceRunID: f.runID})
							if err != nil || len(latest.Entities) != 1 {
								t.Fatalf("latest source plan: %#v, %v", latest, err)
							}
							var raw []byte
							var revision int64
							if err := f.db.QueryRowContext(f.ctx, `SELECT revision, fact FROM run_fork_fact_revisions WHERE run_id=$1 AND family='entity_metadata' AND fact_key=$2 AND revision <= $3 ORDER BY revision DESC LIMIT 1`, f.runID, f.entityID, plan.ForkPoint.Revision).Scan(&revision, &raw); err != nil {
								t.Fatal(err)
							}
							var fact map[string]any
							if err := json.Unmarshal(raw, &fact); err != nil {
								t.Fatal(err)
							}
							if strings.HasSuffix(invalid, "flow") {
								fact["flow_instance"] = ""
							}
							if strings.HasSuffix(invalid, "type") {
								fact["entity_type"] = ""
							}
							if invalid == "duplicate_flow" {
								fact["flow_instance"] = "foreign/one"
							}
							if invalid == "duplicate_type" {
								fact["entity_type"] = "foreign_type"
							}
							encoded, err := json.Marshal(fact)
							if err != nil {
								t.Fatal(err)
							}
							// Corrupt only historical evidence beneath a later valid live projection.
							if strings.HasPrefix(invalid, "duplicate") {
								key := uuid.NewString()
								_, err = f.db.ExecContext(f.ctx, `INSERT INTO run_fork_fact_revisions (run_id,revision,family,fact_key,fact,present) VALUES ($1,$2,'entity_metadata',$3,$4,TRUE)`, f.runID, revision, key, string(encoded))
								if err == nil {
									_, err = f.db.ExecContext(f.ctx, `INSERT INTO run_fork_fact_revisions (run_id,revision,family,fact_key,fact,present) VALUES ($1,$2,'entity_metadata',$3,'{}',FALSE)`, f.runID, latest.ForkPoint.Revision, key)
								}
							} else if invalid == "missing" {
								_, err = f.db.ExecContext(f.ctx, `UPDATE run_fork_fact_revisions SET fact='{}', present=FALSE WHERE run_id=$1 AND revision <= $2 AND family='entity_metadata' AND fact_key=$3`, f.runID, plan.ForkPoint.Revision, f.entityID)
							} else {
								_, err = f.db.ExecContext(f.ctx, `UPDATE run_fork_fact_revisions SET fact=$1 WHERE run_id=$2 AND revision=$3 AND family='entity_metadata' AND fact_key=$4`, string(encoded), f.runID, revision, f.entityID)
							}
							if err != nil {
								t.Fatal(err)
							}
							tx, err := f.db.BeginTx(f.ctx, nil)
							if err != nil {
								t.Fatal(err)
							}
							if backend.name == "postgres" {
								err = runforkrevision.ValidateCompletePostgres(f.ctx, tx, f.runID)
							} else {
								err = runforkrevision.ValidateCompleteSQLite(f.ctx, tx, f.runID)
							}
							_ = tx.Rollback()
							if err != nil {
								t.Fatalf("latest projection must remain valid despite historical corruption: %v", err)
							}
							before := snapshotOwnershipCounts(t, f, f.runID)
							got, planErr := f.store.PlanRunFork(f.ctx, runfork.RunForkPlanRequest{SourceRunID: f.runID, At: f.eventID})
							if planErr == nil && !runForkTestHasPlanBlocker(got, runfork.RunForkBlockerEntitySnapshotMetadataUnproven) {
								t.Fatalf("invalid historical metadata admitted: %#v", got)
							}
							if _, err := f.store.MaterializeRunFork(f.ctx, runfork.RunForkMaterializeRequest{SourceRunID: f.runID, At: f.eventID}); err == nil {
								t.Fatal("invalid historical metadata materialized")
							}
							if after := snapshotOwnershipCounts(t, f, f.runID); after != before {
								t.Fatalf("invalid admission changed source: before=%v after=%v", before, after)
							}
							if after := readSnapshotOwnershipEntity(t, f, f.runID); !reflect.DeepEqual(after, sourceEntityBefore) {
								t.Fatalf("invalid admission changed committed source snapshot: before=%#v after=%#v", sourceEntityBefore, after)
							}
							var forks int
							if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM runs WHERE forked_from_run_id=$1`, f.runID).Scan(&forks); err != nil {
								t.Fatal(err)
							}
							wantForks := 0
							if phase == "exact_replay" {
								wantForks = 1
								if after := readSnapshotOwnershipEntity(t, f, forkRunID); !reflect.DeepEqual(after, forkEntityBefore) {
									t.Fatalf("invalid replay changed committed child snapshot: before=%#v after=%#v", forkEntityBefore, after)
								}
								if after := snapshotOwnershipCounts(t, f, forkRunID); after != forkBefore {
									t.Fatalf("invalid replay changed child: before=%v after=%v", forkBefore, after)
								}
							}
							if forks != wantForks {
								t.Fatalf("invalid metadata left %d forks, want %d", forks, wantForks)
							}
						})
					}
				})
			}
		})
	}
}
