package runtimepersistence

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

func TestFlowConstructorScenarioImportCannotAcquireExecutionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, map[string]string{
				"schema.yaml":   "name: imported-history\nstages:\n  pending: {initial: true}\n",
				"events.yaml":   "checkpoint:\n",
				"entities.yaml": "record:\n  marker: text\n",
			}, nil)
			owner := f.store.(interface {
				SetupScenarioEntities(context.Context, pipeline.ScenarioSetupRequest) (pipeline.ScenarioSetupResult, error)
				PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
				MaterializeRunFork(context.Context, runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)
				pipeline.WorkflowEngineMutationOwner
			})
			runID, entityID := correlation.RunIDFromContext(f.ctx), uuid.NewString()
			at := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
			if _, err := owner.SetupScenarioEntities(f.ctx, pipeline.ScenarioSetupRequest{
				RunID: runID, CreatedAt: at,
				Entities: []pipeline.ScenarioSetupEntityRequest{{Alias: "import", EntityID: entityID, FlowInstance: ".", EntityType: "record", CurrentState: "pending", Fields: map[string]any{"marker": "recorded"}}},
			}); err != nil {
				t.Fatal(err)
			}
			marker := eventtest.ExistingRunRootIngress(uuid.NewString(), "checkpoint", "test", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
			if err := commitSemanticPipelineProcessedEventFixture(f.ctx, f.store, marker); err != nil {
				t.Fatal(err)
			}
			history, err := owner.PlanRunFork(f.ctx, runfork.RunForkPlanRequest{SourceRunID: runID, At: marker.ID()})
			if err != nil || len(history.Entities) != 1 || history.Entities[0].EntityID != entityID || history.Entities[0].CurrentState != "pending" ||
				history.Entities[0].Fields["marker"] != "recorded" || history.Entities[0].MaterializationMetadata == nil ||
				history.Entities[0].MaterializationMetadata.Source != runfork.RunForkMaterializedEntitySnapshotMetadataSourceEntityState {
				t.Fatalf("scenario import lost its historical meaning: %+v err=%v", history.Entities, err)
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
			var headers, fields int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM flow_instances WHERE run_id=$1`, fork.ForkRunID).Scan(&headers); err != nil {
				t.Fatal(err)
			}
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM entity_state WHERE run_id=$1 AND entity_id=$2 AND current_state='pending'`, fork.ForkRunID, entityID).Scan(&fields); err != nil || headers != 0 || fields != 1 {
				t.Fatalf("materialize-only import fabricated construction or lost state: headers=%d fields=%d err=%v", headers, fields, err)
			}
			before = snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
			if replay, err := owner.MaterializeRunFork(f.ctx, request); err != nil || replay.ForkRunID != fork.ForkRunID || !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
				t.Fatalf("exact imported history replay changed state: %+v err=%v", replay, err)
			}
			for _, fault := range []struct {
				name, column string
				value        any
			}{
				{"fields", "fields", `{"marker":"changed"}`},
				{"stage", "current_state", "changed"},
				{"entry_clock", "entered_state_at", at.Add(time.Second)},
				{"creation_clock", "created_at", at.Add(-time.Hour)},
				{"update_clock", "updated_at", at.Add(time.Hour)},
				{"revision", "revision", int64(2)},
				{"gates", "gates", `{"foreign":true}`},
				{"bookkeeping", "bookkeeping", `{"foreign":true}`},
				{"accumulator", "accumulator", `{"foreign":true}`},
				{"slug", "slug", "changed"},
				{"name", "name", "changed"},
			} {
				t.Run("reuse_rejects/"+fault.name, func(t *testing.T) {
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
		})
	}
}
