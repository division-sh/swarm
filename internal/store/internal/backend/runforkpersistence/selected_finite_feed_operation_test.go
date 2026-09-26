package runforkpersistence

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestSelectedFiniteFeedRecoveryRequiresExactPermanentOperationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := forkOperationTestDatabase(t, backend)
			ctx := context.Background()
			point := runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 5}
			childID, bindingID := uuid.NewString(), uuid.NewString()
			request := runfork.ForkOperationRequest{
				OperationID: uuid.NewString(), Actor: "bearer:operator", IdempotencyKey: "recovery",
				TransportHash: "sha256:recovery", SourceRunID: uuid.NewString(),
				TargetBundleHash: "sha256:bundle", ContractSelection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts},
				ResolvedPoint: &point,
			}
			binding := runfork.RunForkSelectedContractBinding{
				BindingID: bindingID, ForkRunID: childID, SourceRunID: request.SourceRunID,
				ForkPoint: point, ContractSelection: request.ContractSelection,
			}
			check := func(b runfork.RunForkSelectedContractBinding, bundle string, want bool) {
				t.Helper()
				withForkOperationTx(t, db, func(tx *sql.Tx) {
					record, err := selectedForkOperationForRecoveryTx(ctx, tx, b, bundle, backend == "postgres")
					if want {
						if err != nil || record.Request.ResolvedPoint == nil || *record.Request.ResolvedPoint != point || record.ForkRunID != childID {
							t.Fatalf("exact recovery operation: record=%+v err=%v", record, err)
						}
					} else if err == nil {
						t.Fatalf("contradictory recovery operation accepted: %+v", record)
					}
				})
			}
			check(binding, request.TargetBundleHash, false)
			withForkOperationTx(t, db, func(tx *sql.Tx) {
				if _, _, err := bindForkOperationTx(ctx, tx, request, childID, bindingID, backend == "postgres"); err != nil {
					t.Fatal(err)
				}
			})
			check(binding, request.TargetBundleHash, true)
			changed := binding
			changed.BindingID = uuid.NewString()
			check(changed, request.TargetBundleHash, false)
			changed = binding
			changed.SourceRunID = uuid.NewString()
			check(changed, request.TargetBundleHash, false)
			changed = binding
			changed.ForkPoint.Revision++
			check(changed, request.TargetBundleHash, false)
			check(binding, "sha256:other", false)
		})
	}
}

func TestSelectedFiniteFeedRecoveryPinsMatchDurableFeedInventoryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := forkOperationTestDatabase(t, backend)
			for _, ddl := range []string{
				`CREATE TABLE runs (run_id TEXT PRIMARY KEY,status TEXT NOT NULL)`,
				`CREATE TABLE resource_version_pins (run_id TEXT,flow_path TEXT,event_name TEXT,schema_digest TEXT,version_id TEXT,selection TEXT)`,
				`CREATE TABLE fan_out_intents (run_id TEXT,origin_kind TEXT,source_resource_flow_path TEXT,source_resource_event_name TEXT,source_resource_version_id TEXT,deployment_schema_digest TEXT,bundle_hash TEXT)`,
			} {
				if _, err := db.Exec(ddl); err != nil {
					t.Fatal(err)
				}
			}
			ctx, runID := context.Background(), uuid.NewString()
			bundle := "bundle-v2:sha256:" + strings.Repeat("c", 64)
			schema := "resource-schema-v1:sha256:" + strings.Repeat("a", 64)
			version := "resource-version-v1:sha256:" + strings.Repeat("b", 64)
			if _, err := db.Exec(`INSERT INTO runs VALUES ($1,'paused')`, runID); err != nil {
				t.Fatal(err)
			}
			read := func(want bool) {
				t.Helper()
				withForkOperationTx(t, db, func(tx *sql.Tx) {
					pins, err := selectedFiniteFeedRecoveryPinsTx(ctx, tx, runID, bundle)
					if want && (err != nil || len(pins) != 1 || pins[0].RunID != runID || pins[0].RunState != "paused") {
						t.Fatalf("exact selected pins=%+v err=%v", pins, err)
					}
					if !want && err == nil {
						t.Fatalf("contradictory selected pins accepted: %+v", pins)
					}
				})
			}
			read(false)
			if _, err := db.Exec(`INSERT INTO fan_out_intents VALUES ($1,'deployment','.','root.ready',$2,$3,$4)`, runID, version, schema, bundle); err != nil {
				t.Fatal(err)
			}
			read(false)
			if _, err := db.Exec(`INSERT INTO resource_version_pins VALUES ($1,'.','root.ready',$2,$3,'fork_override')`, runID, schema, version); err != nil {
				t.Fatal(err)
			}
			read(true)
			if _, err := db.Exec(`UPDATE fan_out_intents SET deployment_schema_digest=$2 WHERE run_id=$1`, runID, "resource-schema-v1:sha256:"+strings.Repeat("d", 64)); err != nil {
				t.Fatal(err)
			}
			read(false)
			if _, err := db.Exec(`UPDATE fan_out_intents SET deployment_schema_digest=$2 WHERE run_id=$1`, runID, schema); err != nil {
				t.Fatal(err)
			}
			read(true)
			if _, err := db.Exec(`UPDATE resource_version_pins SET version_id='wrong' WHERE run_id=$1`, runID); err != nil {
				t.Fatal(err)
			}
			read(false)
		})
	}
}

func TestInterruptedSelectedFiniteFeedSettlesPermanentOperationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, uncertain := range []bool{false, true} {
			t.Run(backend+map[bool]string{false: "/failed", true: "/uncertain"}[uncertain], func(t *testing.T) {
				db := forkOperationTestDatabase(t, backend)
				ctx := context.Background()
				point := runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 5}
				request := runfork.ForkOperationRequest{
					OperationID: uuid.NewString(), Actor: "bearer:operator", IdempotencyKey: uuid.NewString(),
					TransportHash: "sha256:interrupted", SourceRunID: uuid.NewString(), ResolvedPoint: &point,
					TargetBundleHash: "sha256:bundle", ContractSelection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts},
				}
				binding := runfork.RunForkSelectedContractBinding{
					BindingID: uuid.NewString(), ForkRunID: uuid.NewString(), SourceRunID: request.SourceRunID,
					ForkPoint: point, ContractSelection: request.ContractSelection,
				}
				failure, ok := failures.EnvelopeFromError(failures.New(failures.ClassLifecycleConflict,
					"selected_recovery_prelaunch_abandoned", "selected-fork", "startup_recovery", nil))
				if !ok {
					t.Fatal("construct typed recovery failure")
				}
				withForkOperationTx(t, db, func(tx *sql.Tx) {
					if _, _, err := bindForkOperationTx(ctx, tx, request, binding.ForkRunID, binding.BindingID, backend == "postgres"); err != nil {
						t.Fatal(err)
					}
					if err := settleInterruptedSelectedFiniteFeedOperationTx(ctx, tx, binding, request.TargetBundleHash, failure, uncertain, time.Now().UTC(), backend == "postgres"); err != nil {
						t.Fatal(err)
					}
				})
				withForkOperationTx(t, db, func(tx *sql.Tx) {
					record, found, err := loadForkOperationByIDTx(ctx, tx, request.OperationID, backend == "postgres")
					if err != nil || !found {
						t.Fatalf("read terminal operation: found=%t err=%v", found, err)
					}
					want := runfork.ForkOperationFailed
					if uncertain {
						want = runfork.ForkOperationUncertain
					}
					if record.Status != want || record.Failure == nil || record.Failure.Code != "selected_recovery_prelaunch_abandoned" || record.Result != nil {
						t.Fatalf("interrupted operation: %+v, want %s with typed failure", record, want)
					}
					if err := settleInterruptedSelectedFiniteFeedOperationTx(ctx, tx, binding, request.TargetBundleHash, failure, uncertain, time.Now().UTC(), backend == "postgres"); err == nil {
						t.Fatal("terminal operation was settled a second time")
					}
				})
			})
		}
	}
}

func TestSelectedFiniteFeedRecoveryRequiresCheckpointAndOnlySettledExternalEffectsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := forkOperationTestDatabase(t, backend)
			for _, ddl := range []string{
				`CREATE TABLE fan_out_intents (run_id TEXT NOT NULL,origin_kind TEXT NOT NULL)`,
				`CREATE TABLE run_fork_selected_contract_runtime_executions (execution_id TEXT PRIMARY KEY,fork_run_id TEXT NOT NULL)`,
				`CREATE TABLE runtime_external_effect_operations (operation_id TEXT PRIMARY KEY,selected_execution_id TEXT,state TEXT)`,
				`CREATE TABLE runtime_external_effect_attempts (attempt_id TEXT PRIMARY KEY,operation_id TEXT,state TEXT)`,
			} {
				if _, err := db.Exec(ddl); err != nil {
					t.Fatal(err)
				}
			}
			ctx := context.Background()
			point := runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 5}
			childID, bindingID, predecessorID := uuid.NewString(), uuid.NewString(), uuid.NewString()
			request := runfork.ForkOperationRequest{
				OperationID: uuid.NewString(), Actor: "bearer:operator", IdempotencyKey: "resume",
				TransportHash: "sha256:resume", SourceRunID: uuid.NewString(),
				TargetBundleHash: "sha256:bundle", ContractSelection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts},
				ResolvedPoint: &point,
			}
			binding := runfork.RunForkSelectedContractBinding{
				BindingID: bindingID, ForkRunID: childID, SourceRunID: request.SourceRunID,
				ForkPoint: point, ContractSelection: request.ContractSelection,
			}
			withForkOperationTx(t, db, func(tx *sql.Tx) {
				if _, _, err := bindForkOperationTx(ctx, tx, request, childID, bindingID, backend == "postgres"); err != nil {
					t.Fatal(err)
				}
			})
			read := func(want bool) {
				t.Helper()
				withForkOperationTx(t, db, func(tx *sql.Tx) {
					operation, err := selectedFiniteFeedRecoveryOperationTx(ctx, tx, binding, request.TargetBundleHash, backend == "postgres")
					if err != nil || (operation != nil) != want {
						t.Fatalf("resumable finite feed=%t want=%t err=%v", operation != nil, want, err)
					}
				})
			}
			read(false)
			if _, err := db.Exec(`INSERT INTO fan_out_intents VALUES ($1,'handler')`, childID); err != nil {
				t.Fatal(err)
			}
			read(false)
			if _, err := db.Exec(`INSERT INTO fan_out_intents VALUES ($1,'deployment')`, childID); err != nil {
				t.Fatal(err)
			}
			read(true)
			if _, err := db.Exec(`INSERT INTO run_fork_selected_contract_runtime_executions VALUES ($1,$2)`, predecessorID, childID); err != nil {
				t.Fatal(err)
			}
			operationID, attemptID := uuid.NewString(), uuid.NewString()
			if _, err := db.Exec(`INSERT INTO runtime_external_effect_operations VALUES ($1,$2,'authorized')`, operationID, predecessorID); err != nil {
				t.Fatal(err)
			}
			read(false)
			if _, err := db.Exec(`UPDATE runtime_external_effect_operations SET state='settled' WHERE operation_id=$1`, operationID); err != nil {
				t.Fatal(err)
			}
			read(false) // A settled operation without its attempt is incomplete.
			if _, err := db.Exec(`INSERT INTO runtime_external_effect_attempts VALUES ($1,$2,'authorized')`, attemptID, operationID); err != nil {
				t.Fatal(err)
			}
			read(false)
			for _, state := range []string{"launched", "response_observed", "outcome_uncertain", "terminal_failure"} {
				if _, err := db.Exec(`UPDATE runtime_external_effect_attempts SET state=$1 WHERE attempt_id=$2`, state, attemptID); err != nil {
					t.Fatal(err)
				}
				read(false)
			}
			if _, err := db.Exec(`UPDATE runtime_external_effect_attempts SET state='settled' WHERE attempt_id=$1`, attemptID); err != nil {
				t.Fatal(err)
			}
			read(true)
			otherOperation, otherAttempt := uuid.NewString(), uuid.NewString()
			if _, err := db.Exec(`INSERT INTO runtime_external_effect_operations VALUES ($1,$2,'settled')`, otherOperation, predecessorID); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO runtime_external_effect_attempts VALUES ($1,$2,'settled')`, otherAttempt, otherOperation); err != nil {
				t.Fatal(err)
			}
			read(true)
			if _, err := db.Exec(`UPDATE runtime_external_effect_operations SET state='outcome_uncertain' WHERE operation_id=$1`, otherOperation); err != nil {
				t.Fatal(err)
			}
			read(false) // A second operation cannot inherit the first one's settlement.
			if _, err := db.Exec(`UPDATE runtime_external_effect_operations SET state='settled' WHERE operation_id=$1`, otherOperation); err != nil {
				t.Fatal(err)
			}
			read(true)
			foreignExecution, foreignOperation := uuid.NewString(), uuid.NewString()
			if _, err := db.Exec(`INSERT INTO run_fork_selected_contract_runtime_executions VALUES ($1,$2)`, foreignExecution, uuid.NewString()); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO runtime_external_effect_operations VALUES ($1,$2,'authorized')`, foreignOperation, foreignExecution); err != nil {
				t.Fatal(err)
			}
			read(true) // An unrelated selected execution is not this child's effect.
			if _, err := db.Exec(`INSERT INTO runtime_external_effect_attempts VALUES ($1,$2,'authorized')`, uuid.NewString(), operationID); err != nil {
				t.Fatal(err)
			}
			read(false) // One settled attempt cannot mask another unfinished one.
		})
	}
}
