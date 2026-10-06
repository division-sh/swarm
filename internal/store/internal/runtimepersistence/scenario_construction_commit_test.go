package runtimepersistence

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestScenarioConstructionNativeCommitBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, commit := range []bool{false, true} {
			cut := "rollback_before_ack"
			if commit {
				cut = "commit_before_lost_ack"
			}
			t.Run(backend+"/"+cut, func(t *testing.T) {
				selected, db, connector := newP16RaceStore(t, backend)
				f := newReceiverConfigActivationFixtureForStore(t, selected.(agentFixtureFlowStore), false, map[string]string{
					"schema.yaml":        "name: scenario-construction\nstages:\n  pending: {}\n",
					"entities.yaml":      "record:\n  marker: text\n",
					"nested/schema.yaml": "name: nested\n",
				}, nil, ownStoreTestAgentManager, nil)
				runID := uuid.NewString()
				ctx := correlation.WithRunID(f.ctx, runID)
				at := time.Now().UTC().Truncate(time.Microsecond)
				seed := pipeline.ScenarioSetupEntityRequest{
					Alias: "root", EntityID: runID, FlowInstance: runID, EntityType: "record",
					CurrentState: "pending", Fields: map[string]any{"marker": "imported"}, Gates: map[string]bool{},
				}
				source := semanticview.Wrap(f.bundle)
				plan, err := f.manager.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{
					ContractBundle: source, OccurredAt: at, ScenarioSeed: &seed,
					Instance: flowidentity.Stored(source, semanticview.RootExecutionFlowID(source), runID, runID, "", ""),
				})
				if err != nil {
					t.Fatal(err)
				}
				if len(plan.Children) != 1 {
					t.Fatalf("expected the complete eligible tree, got %d children", len(plan.Children))
				}
				command := runtimebus.ScenarioSetupCommand{Setup: pipeline.ScenarioSetupRequest{
					RunID: runID, CreatedAt: at, Entities: []pipeline.ScenarioSetupEntityRequest{seed},
				}, Activations: []runtimebus.FlowInstanceActivationCommand{{Plan: plan}}}
				for _, construction := range plan.ConstructionPlans() {
					command.Activations[0].RouteTopology = append(command.Activations[0].RouteTopology, runtimebus.FlowInstanceRouteRecordSet{
						Identity: flowidentity.RunScopedFlowInstance{RunID: runID, Route: construction.Identity.Route()},
					})
				}
				owner := selected.(runtimebus.ScenarioSetupCommitOwner)
				unchanged := snapshotForkHistoricalExecutionTables(t, db, backend == "postgres")
				for _, mismatch := range []string{"fields", "stage", "gates", "run"} {
					hostile := command
					hostile.Setup.Entities = append([]pipeline.ScenarioSetupEntityRequest(nil), command.Setup.Entities...)
					switch mismatch {
					case "fields":
						hostile.Setup.Entities[0].Fields = map[string]any{"marker": "different"}
					case "stage":
						hostile.Setup.Entities[0].CurrentState = "foreign"
					case "gates":
						hostile.Setup.Entities[0].Gates = map[string]bool{"foreign": true}
					case "run":
						hostile.Setup.RunID = uuid.NewString()
					}
					if result, err := owner.CommitScenarioSetup(ctx, hostile); err == nil || result.Acknowledged || !reflect.DeepEqual(unchanged, snapshotForkHistoricalExecutionTables(t, db, backend == "postgres")) {
						t.Fatalf("%s mismatch mutated or admitted setup: %+v err=%v", mismatch, result, err)
					}
				}
				fault := errors.New("injected scenario physical COMMIT acknowledgment loss")
				var commits atomic.Int32
				connector.arm(func(tx driver.Tx) error {
					commits.Add(1)
					if commit {
						return errors.Join(fault, tx.Commit())
					}
					return errors.Join(fault, tx.Rollback())
				})
				result, err := owner.CommitScenarioSetup(ctx, command)
				if !errors.Is(err, fault) || result.Acknowledged || len(result.Activations) != 0 || commits.Load() != 1 {
					t.Fatalf("uncertain setup returned authority: %+v err=%v commits=%d", result, err, commits.Load())
				}
				want := 0
				if commit {
					want = 1
				}
				for _, check := range []struct {
					table string
					rows  int
				}{{"runs", want}, {"flow_instances", 2 * want}, {"entity_state", want}, {"flow_instance_runtime_readiness", 2 * want}} {
					var rows int
					if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+check.table+" WHERE run_id=$1", runID).Scan(&rows); err != nil || rows != check.rows {
						t.Fatalf("atomic %s: rows=%d want=%d err=%v", check.table, rows, check.rows, err)
					}
				}
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				if result, err := owner.CommitScenarioSetup(cancelled, command); !errors.Is(err, context.Canceled) || result.Acknowledged {
					t.Fatalf("cancelled setup returned authority: %+v err=%v", result, err)
				}
				result, err = owner.CommitScenarioSetup(ctx, command)
				if err != nil || !result.Acknowledged || len(result.Activations) != 1 || result.Activations[0].Created == commit || commits.Load() != 1 {
					t.Fatalf("exact setup reconciliation: %+v err=%v commits=%d", result, err, commits.Load())
				}
				before := snapshotForkHistoricalExecutionTables(t, db, backend == "postgres")
				if replay, err := owner.CommitScenarioSetup(ctx, command); err != nil || !replay.Acknowledged || replay.Activations[0].Created || !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, db, backend == "postgres")) {
					t.Fatalf("setup replay repeated construction: %+v err=%v", replay, err)
				}
				var fields, stage string
				if err := db.QueryRowContext(ctx, `SELECT CAST(fields AS TEXT), current_state FROM entity_state WHERE run_id=$1 AND entity_id=$1`, runID).Scan(&fields, &stage); err != nil || stage != "pending" {
					t.Fatalf("setup changed imported meaning: fields=%s stage=%s err=%v", fields, stage, err)
				}
				var decoded map[string]any
				if err := json.Unmarshal([]byte(fields), &decoded); err != nil || !reflect.DeepEqual(decoded, seed.Fields) {
					t.Fatalf("setup changed imported fields: %#v err=%v", decoded, err)
				}
				var occurrences int
				if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1`, runID).Scan(&occurrences); err != nil || occurrences != 0 {
					t.Fatalf("setup fabricated a creating event: %d err=%v", occurrences, err)
				}
			})
		}
	}
}

func TestScenarioConstructionFieldlessStateBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, map[string]string{
				"schema.yaml": "name: fieldless-scenario\nstages:\n  pending: {}\n",
			}, nil)
			runID := uuid.NewString()
			ctx := correlation.WithRunID(f.ctx, runID)
			at := time.Now().UTC().Truncate(time.Microsecond)
			seed := pipeline.ScenarioSetupEntityRequest{Alias: "root", EntityID: runID, FlowInstance: runID,
				EntityType: "default", CurrentState: "pending", Fields: map[string]any{}, Gates: map[string]bool{}}
			source := semanticview.Wrap(f.bundle)
			plan, err := f.manager.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{
				ContractBundle: source, OccurredAt: at, ScenarioSeed: &seed,
				Instance: flowidentity.Stored(source, semanticview.RootExecutionFlowID(source), runID, runID, "", ""),
			})
			if err != nil {
				t.Fatal(err)
			}
			command := runtimebus.ScenarioSetupCommand{Setup: pipeline.ScenarioSetupRequest{RunID: runID, CreatedAt: at, Entities: []pipeline.ScenarioSetupEntityRequest{seed}},
				Activations: []runtimebus.FlowInstanceActivationCommand{{Plan: plan, RouteTopology: []runtimebus.FlowInstanceRouteRecordSet{{Identity: flowidentity.RunScopedFlowInstance{RunID: runID, Route: plan.Identity.Route()}}}}}}
			result, err := f.store.(runtimebus.ScenarioSetupCommitOwner).CommitScenarioSetup(ctx, command)
			if err != nil || !result.Acknowledged || len(result.Activations) != 1 || !result.Activations[0].Created {
				t.Fatalf("fieldless scenario construction: %+v err=%v", result, err)
			}
			var headers, fields int
			if err := f.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM flow_instances WHERE run_id=$1 AND entity_id=$1 AND current_state='pending'`, runID).Scan(&headers); err != nil {
				t.Fatal(err)
			}
			if err := f.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_state WHERE run_id=$1`, runID).Scan(&fields); err != nil || headers != 1 || fields != 0 {
				t.Fatalf("fieldless setup invented a field companion: headers=%d fields=%d err=%v", headers, fields, err)
			}
		})
	}
}
