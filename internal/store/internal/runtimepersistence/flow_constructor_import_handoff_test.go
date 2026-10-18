package runtimepersistence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

func TestFlowConstructorScenarioImportCannotAcquireExecutionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, empty := range []bool{false, true} {
				t.Run(fmt.Sprintf("empty=%t", empty), func(t *testing.T) {
					authoredFields := map[string]any{"marker": "recorded"}
					entitySchema := "record:\n  marker: text\n"
					if empty {
						authoredFields = map[string]any{}
						entitySchema = "record:\n  marker: text?\n"
					}
					f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, map[string]string{
						"schema.yaml":   "name: imported-history\nstages:\n  pending: {}\n",
						"events.yaml":   "checkpoint:\n",
						"entities.yaml": entitySchema,
					}, nil)
					owner := f.store.(interface {
						SetupScenarioEntities(context.Context, pipeline.ScenarioSetupRequest) (pipeline.ScenarioSetupResult, error)
						PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
						MaterializeRunFork(context.Context, runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)
						InspectRunMutationDrift(context.Context, string) (mutationlog.DriftReport, error)
						LoadOperatorEntity(context.Context, string, string) (operatorread.OperatorEntityFull, error)
						ListOperatorEntities(context.Context, operatorread.OperatorEntityListOptions) (operatorread.OperatorEntityListResult, error)
						pipeline.WorkflowEngineMutationOwner
					})
					runID, entityID := correlation.RunIDFromContext(f.ctx), uuid.NewString()
					at := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
					if _, err := owner.SetupScenarioEntities(f.ctx, pipeline.ScenarioSetupRequest{
						RunID: runID, CreatedAt: at,
						Entities: []pipeline.ScenarioSetupEntityRequest{{Alias: "import", EntityID: entityID, FlowInstance: ".", EntityType: "record", Fields: maps.Clone(authoredFields)}},
					}); err != nil {
						t.Fatal(err)
					}
					marker := eventtest.ExistingRunRootIngress(uuid.NewString(), "checkpoint", "test", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
					if err := commitSemanticPipelineProcessedEventFixture(f.ctx, f.store, marker); err != nil {
						t.Fatal(err)
					}
					assertImport := func(inspectedRun string) {
						t.Helper()
						entity, err := owner.LoadOperatorEntity(f.ctx, entityID, inspectedRun)
						if err != nil || entity.Entity.EntityID != entityID || entity.Entity.RunID != inspectedRun ||
							!reflect.DeepEqual(entity.Fields, authoredFields) || entity.Entity.StageDefined != nil || entity.Entity.CurrentState != nil ||
							entity.Entity.Revision != nil || entity.Entity.EnteredStateAt != nil || entity.Entity.LifecycleCreatedAt != nil || entity.Entity.LifecycleUpdatedAt != nil ||
							!entity.Entity.CreatedAt.Equal(at) || !entity.Entity.UpdatedAt.Equal(at) ||
							entity.Gates != nil || entity.Bookkeeping != nil || entity.Accumulated != nil || entity.Loops != nil {
							t.Fatalf("import readback changed data membership or invented lifecycle: run=%s entity=%+v err=%v", inspectedRun, entity, err)
						}
						inventory, err := owner.ListOperatorEntities(f.ctx, operatorread.OperatorEntityListOptions{RunID: inspectedRun})
						if err != nil || len(inventory.Entities) != 1 || !reflect.DeepEqual(inventory.Entities[0], entity.Entity) {
							t.Fatalf("import inventory lost exact data membership or clocks: run=%s inventory=%+v err=%v", inspectedRun, inventory, err)
						}
						report, err := owner.InspectRunMutationDrift(f.ctx, inspectedRun)
						if err != nil || report.RunID != inspectedRun || report.EntitiesChecked != 1 || len(report.Rows) != 0 {
							t.Fatalf("import oracle lost a real member or disagreed with history: run=%s report=%+v err=%v", inspectedRun, report, err)
						}
					}
					assertImport(runID)
					history, err := owner.PlanRunFork(f.ctx, runfork.RunForkPlanRequest{SourceRunID: runID, At: marker.ID()})
					if err != nil || len(history.Entities) != 1 || history.Entities[0].EntityID != entityID || history.Entities[0].CurrentState != nil || history.Entities[0].EnteredStateAt != nil || history.Entities[0].Gates != nil || history.Entities[0].Bookkeeping != nil || history.Entities[0].Accumulator != nil ||
						!reflect.DeepEqual(history.Entities[0].Fields, authoredFields) || history.Entities[0].MaterializationMetadata == nil ||
						history.Entities[0].MaterializationMetadata.Source != runfork.RunForkMaterializedEntitySnapshotMetadataSourceEntityState {
						t.Fatalf("scenario import lost its historical meaning: %+v err=%v", history.Entities, err)
					}
					metadata := history.Entities[0].MaterializationMetadata
					if metadata.DataCreatedAt == nil || metadata.DataUpdatedAt == nil || !metadata.DataCreatedAt.Equal(at) || !metadata.DataUpdatedAt.Equal(at) ||
						!metadata.CreatedAt.IsZero() || !metadata.UpdatedAt.IsZero() || !metadata.EnteredStateAt.IsZero() || metadata.HistoricalConstruction != nil {
						t.Fatalf("import historical metadata changed data clocks or invented lifecycle: %+v", metadata)
					}
					storeTestWorkOwner(t)
					work, _ := storeTestWorkFixtures.Load(t)
					capability, err := agentfixture.ProcessCapability(t, f.ctx, f.store)
					if err != nil {
						t.Fatal(err)
					}
					fixture := work.(*storeTestWorkFixture)
					fixture.capabilitiesMu.Lock()
					if fixture.capabilities == nil {
						fixture.capabilities = make(map[any]startupownership.ProcessCapability)
					}
					fixture.capabilities[f.store] = capability
					fixture.capabilitiesMu.Unlock()
					before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
					record := stateOnlyWorkflowEngineMutationRecord(t, runID, ".", ".", entityID, "pending", 1, at)
					record.Mode = f.bundle.FlowTree.ByID["."].Schema.EffectiveMode()
					record.CurrentState = "pending"
					record.EntityType = "record"
					record.Fields, err = json.Marshal(authoredFields)
					if err != nil {
						t.Fatal(err)
					}
					record.Transition = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
					if _, err := owner.CommitWorkflowEngineMutation(f.ctx, pipeline.WorkflowEngineMutationCommand{State: record}); err == nil || err.Error() != "workflow engine state route is missing: ." {
						t.Fatalf("ordinary mutation granted construction to an import: %v", err)
					}
					prepared, err := prepareSelectedStoreForkForTest(t, f.ctx, f.store, runID, marker.ID(), runfork.RunForkContractSelection{Mode: "selected_contracts"})
					if prepared != nil || err == nil || !strings.Contains(err.Error(), runfork.RunForkMaterializedEntitySnapshotMetadataOwner) {
						t.Fatalf("selected preparation granted construction to an import: prepared=%v err=%v", prepared, err)
					}
					if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
						t.Fatal("refused imported execution changed source or fork state")
					}
					request := runfork.RunForkMaterializeRequest{SourceRunID: runID, At: marker.ID()}
					fork, err := owner.MaterializeRunFork(f.ctx, request)
					if err != nil {
						t.Fatal(err)
					}
					assertImport(runID)
					assertImport(fork.ForkRunID)
					var headers, fields int
					if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM flow_instances WHERE run_id=$1`, fork.ForkRunID).Scan(&headers); err != nil {
						t.Fatal(err)
					}
					if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM entity_state WHERE run_id=$1 AND entity_id=$2`, fork.ForkRunID, entityID).Scan(&fields); err != nil || headers != 0 || fields != 1 {
						t.Fatalf("materialize-only import fabricated construction or lost state: headers=%d fields=%d err=%v", headers, fields, err)
					}
					before = snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
					if replay, err := owner.MaterializeRunFork(f.ctx, request); err != nil || replay.ForkRunID != fork.ForkRunID || !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
						t.Fatalf("exact imported history replay changed state: %+v err=%v", replay, err)
					}
					for _, fault := range []struct {
						name, column string
						value        any
						retired      bool
					}{
						{"fields", "fields", `{"marker":"changed"}`, false},
						{"stage", "current_state", "changed", true},
						{"entry_clock", "entered_state_at", at.Add(time.Second), true},
						{"creation_clock", "created_at", at.Add(-time.Hour), false},
						{"update_clock", "updated_at", at.Add(time.Hour), false},
						{"revision", "revision", int64(2), true},
						{"gates", "gates", `{"foreign":true}`, true},
						{"bookkeeping", "bookkeeping", `{"foreign":true}`, true},
						{"accumulator", "accumulator", `{"foreign":true}`, true},
						{"slug", "slug", "changed", false},
						{"name", "name", "changed", false},
					} {
						kind := "reuse_rejects"
						if fault.retired {
							kind = "retired_column_rejects"
						}
						t.Run(kind+"/"+fault.name, func(t *testing.T) {
							if fault.retired {
								before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
								_, err := f.db.ExecContext(f.ctx, "UPDATE entity_state SET "+fault.column+"=$1 WHERE run_id=$2 AND entity_id=$3", fault.value, fork.ForkRunID, entityID)
								if backend == "postgres" {
									var native *pq.Error
									if !errors.As(err, &native) || native.Code != "42703" || !strings.Contains(native.Message, `"`+fault.column+`"`) {
										t.Fatalf("retired column %s did not fail for undefined-column admission: %v", fault.column, err)
									}
								} else if err == nil || !strings.Contains(err.Error(), "no such column: "+fault.column) {
									t.Fatalf("retired column %s did not fail for undefined-column admission: %v", fault.column, err)
								}
								if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
									t.Fatal("retired-column refusal changed imported state")
								}
								return
							}
							var original any
							if err := f.db.QueryRowContext(f.ctx, "SELECT "+fault.column+" FROM entity_state WHERE run_id=$1 AND entity_id=$2", fork.ForkRunID, entityID).Scan(&original); err != nil {
								t.Fatal(err)
							}
							if _, err := f.db.ExecContext(f.ctx, "UPDATE entity_state SET "+fault.column+"=$1 WHERE run_id=$2 AND entity_id=$3", fault.value, fork.ForkRunID, entityID); err != nil {
								t.Fatal(err)
							}
							before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
							if _, err := owner.MaterializeRunFork(f.ctx, request); err == nil {
								t.Errorf("exact replay accepted changed imported %s", fault.name)
							}
							if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
								t.Error("imported history refusal changed or repaired state")
							}
							if _, err := f.db.ExecContext(f.ctx, "UPDATE entity_state SET "+fault.column+"=$1 WHERE run_id=$2 AND entity_id=$3", original, fork.ForkRunID, entityID); err != nil {
								t.Fatal(err)
							}
						})
					}
					t.Run("reuse_rejects/constructed_header", func(t *testing.T) {
						original := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
						seedWorkflowHeaderProjectionFixture(t, f.ctx, f.db, fork.ForkRunID, entityID, ".", ".", "record", "pending", "{}", at)
						before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
						if _, err := owner.MaterializeRunFork(f.ctx, request); err == nil || err.Error() != fmt.Sprintf("fork materialization %s has unexpected entity header/%s", fork.ForkRunID, entityID) {
							t.Fatalf("import acquired construction or lost exact header refusal: %v", err)
						}
						if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
							t.Fatal("unexpected-header refusal changed or repaired imported state")
						}
						if _, err := f.db.ExecContext(f.ctx, `DELETE FROM flow_instances WHERE run_id=$1 AND entity_id=$2`, fork.ForkRunID, entityID); err != nil {
							t.Fatal(err)
						}
						if !reflect.DeepEqual(original, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
							t.Fatal("header fault cleanup did not preserve physical imported state")
						}
					})
				})
			}
		})
	}
}
