package runtimepersistence

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestFlowConstructorImmutableReplayConflictBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, variant := range []string{"attempt_limit", "policy", "nullable", "instance_id", "flow_path", "fields", "bookkeeping", "accumulator", "initial_stage", "entity_contract", "occurrence", "workflow_version"} {
			t.Run(backend+"/"+variant, func(t *testing.T) {
				f, plan := newWorkflowTargetConstructionFixture(t, backend)
				plan.Instance.Config = map[string]any{
					"attempt_limit": 3, "policy": map[string]any{"weights": []any{1, 2, 3}},
					"nullable": nil, "instance_id": "business-key", "flow_path": "business/path",
				}
				plan.Instance.StateBuckets = map[string]any{"totals": map[string]any{"accepted": 1}}
				plan.Instance.Bookkeeping = map[string]any{"accepted": 1}
				plan.OccurredAt = plan.OccurredAt.Add(987654321 * time.Nanosecond)
				plan.Instance.CreatedAt, plan.Instance.EnteredStageAt = plan.OccurredAt, plan.OccurredAt
				committer := agentFixtureFlowActivationCommitter{store: f.store}
				first, err := committer.CommitFlowInstanceActivation(f.ctx, plan)
				if err != nil || !first.Acknowledged || !first.Created {
					t.Fatalf("canonical construction: %+v %v", first, err)
				}
				if variant == "attempt_limit" {
					if _, err := f.db.ExecContext(f.ctx, `UPDATE workflow_instance_initial_materializations SET projection_version=1 WHERE run_id=$1 AND instance_path=$2`, plan.Readiness.RunID, plan.Identity.InstancePath); err == nil {
						t.Fatal("fresh selected-store schema admitted retired projection version 1")
					}
				}
				record, err := plan.PersistenceRecord()
				if err != nil {
					t.Fatal(err)
				}
				stored, found, err := f.workflows.Load(f.ctx, record.Identity)
				wantClock := plan.OccurredAt.UTC().Truncate(time.Microsecond)
				if err != nil || !found || !stored.CreatedAt.Equal(wantClock) || !stored.EnteredStageAt.Equal(wantClock) {
					t.Fatalf("constructor lost exact persisted clock: %+v found=%t err=%v", stored, found, err)
				}
				wantConfig, err := canonicaljson.MarshalPreservingNumberKinds(plan.Instance.Config)
				if err != nil {
					t.Fatal(err)
				}
				gotConfig, err := canonicaljson.MarshalPreservingNumberKinds(stored.Config)
				if err != nil || string(gotConfig) != string(wantConfig) {
					t.Fatalf("constructor changed business config: got=%s want=%s err=%v", gotConfig, wantConfig, err)
				}
				before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
				exact, err := committer.CommitFlowInstanceActivation(f.ctx, plan)
				if err != nil || !exact.Acknowledged || exact.Created || !reflect.DeepEqual(exact.Lifecycle, pipeline.CommittedWorkflowLifecycleMutation{}) {
					t.Fatalf("exact replay repeated construction: %+v %v", exact, err)
				}
				conflict, err := plan.Normalized()
				if err != nil {
					t.Fatal(err)
				}
				switch variant {
				case "attempt_limit", "policy", "nullable", "instance_id", "flow_path":
					conflict.Instance.Config[variant] = "changed"
				case "fields":
					conflict.Instance.Fields["account_id"] = "changed"
				case "bookkeeping":
					conflict.Instance.Bookkeeping["accepted"] = 2
				case "accumulator":
					conflict.Instance.StateBuckets = map[string]any{"totals": map[string]any{"accepted": 2}}
				case "initial_stage":
					conflict.Instance.CurrentState = "done"
				case "entity_contract":
					conflict.Instance.EntityType = "foreign"
				case "occurrence":
					conflict.OccurredAt = conflict.OccurredAt.Add(time.Hour)
					conflict.Instance.CreatedAt, conflict.Instance.EnteredStageAt = conflict.OccurredAt, conflict.OccurredAt
				case "workflow_version":
					conflict.Instance.WorkflowVersion = "2.0.0"
				}
				if err := conflict.Validate(); err != nil {
					t.Fatalf("conflict fixture failed before immutable replay comparison: %v", err)
				}
				rejected, err := committer.CommitFlowInstanceActivation(f.ctx, conflict)
				failure, typed := failures.As(err)
				if err == nil || rejected.Acknowledged || rejected.Created || !typed || failure.Failure.Class != failures.ClassConflictingDuplicate {
					t.Fatalf("changed immutable %s acquired construction authority: %+v err=%v failure=%+v", variant, rejected, err, failure)
				}
				if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
					t.Fatal("exact or conflicting replay changed persisted evidence")
				}
			})
		}
	}
}

func TestFlowConstructorReplayAndRefusalBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, variant := range []string{"progress", "missing_header", "missing_fields", "missing_receipt", "missing_readiness", "foreign_fields", "foreign_header", "wrong_receipt_clock"} {
			t.Run(backend+"/"+variant, func(t *testing.T) {
				f, plan := newWorkflowTargetConstructionFixture(t, backend)
				committer := agentFixtureFlowActivationCommitter{store: f.store}
				first, err := committer.CommitFlowInstanceActivation(f.ctx, plan)
				if err != nil || !first.Acknowledged || !first.Created {
					t.Fatalf("canonical constructor: %+v %v", first, err)
				}
				record, err := plan.PersistenceRecord()
				if err != nil {
					t.Fatal(err)
				}
				if variant == "progress" {
					state := record.State
					state.ExpectedState, state.ExpectedRevision = state.CurrentState, 1
					state.Transition = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
					state.CurrentState = "done"
					state.Fields = json.RawMessage(`{"account_id":"preserved","handled":true}`)
					state.Gates = json.RawMessage(`{"approved":true}`)
					state.Bookkeeping = json.RawMessage(`{"completed":1}`)
					state.Accumulator = json.RawMessage(`{"totals":{"accepted":4}}`)
					state.EnteredStageAt, state.UpdatedAt = state.CreatedAt.Add(time.Minute), state.CreatedAt.Add(time.Minute)
					state = workflowTargetMutationEntry(t, f, state, "finish.requested")
					if _, err := f.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(f.ctx, pipeline.WorkflowEngineMutationCommand{State: state}); err != nil {
						t.Fatalf("legitimate workflow progress: %v", err)
					}
				} else {
					query := ""
					args := []any{record.Identity.RunID, record.Identity.Route.InstancePath}
					switch variant {
					case "missing_header":
						query = `DELETE FROM flow_instances WHERE run_id=$1 AND instance_path=$2`
					case "missing_fields":
						query = `DELETE FROM entity_state WHERE run_id=$1 AND flow_instance=$2`
					case "missing_receipt":
						query = `DELETE FROM workflow_instance_initial_materializations WHERE run_id=$1 AND instance_path=$2`
					case "missing_readiness":
						query = `DELETE FROM flow_instance_runtime_readiness WHERE run_id=$1 AND instance_path=$2`
					case "foreign_fields":
						query = `UPDATE entity_state SET entity_type='foreign' WHERE run_id=$1 AND flow_instance=$2`
					case "foreign_header":
						query = `UPDATE flow_instances SET flow_template='foreign' WHERE run_id=$1 AND instance_path=$2`
					case "wrong_receipt_clock":
						query = `UPDATE workflow_instance_initial_materializations SET occurred_at=$3 WHERE run_id=$1 AND instance_path=$2`
						args = append(args, record.CreatedAt.Add(time.Hour))
					}
					if _, err := f.db.ExecContext(f.ctx, query, args...); err != nil {
						t.Fatal(err)
					}
				}
				before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
				replayed, err := committer.CommitFlowInstanceActivation(f.ctx, plan)
				if variant == "progress" {
					if err != nil || !replayed.Acknowledged || replayed.Created || !reflect.DeepEqual(replayed.Lifecycle, pipeline.CommittedWorkflowLifecycleMutation{}) {
						t.Fatalf("replay repeated construction or lifecycle: %+v %v", replayed, err)
					}
					after := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
					for _, table := range []string{"entity_state", "flow_instances", "workflow_instance_initial_materializations", "flow_instance_runtime_readiness", "entity_mutations"} {
						if !reflect.DeepEqual(before[table], after[table]) {
							t.Fatalf("creation replay changed progressed %s", table)
						}
					}
					stored, found, err := f.workflows.Load(f.ctx, record.Identity)
					if err != nil || !found || stored.CurrentState != "done" || stored.Revision != 2 || stored.Fields["handled"] != true || !stored.Gates["approved"] {
						t.Fatalf("creation replay rewrote workflow progress: %+v found=%t err=%v", stored, found, err)
					}
					return
				}
				if err == nil || replayed.Acknowledged || replayed.Created {
					t.Fatalf("corrupt construction acquired replay authority: %+v %v", replayed, err)
				}
				if variant == "missing_fields" || variant == "missing_receipt" {
					failure, typed := failures.As(err)
					if !typed || failure.Failure.Class != failures.ClassConflictingDuplicate {
						t.Fatalf("incomplete replay lost its conflicting-duplicate disposition: %v", err)
					}
				}
				if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
					t.Fatal("refused construction replay repaired or changed persisted evidence")
				}
			})
		}
	}
}
