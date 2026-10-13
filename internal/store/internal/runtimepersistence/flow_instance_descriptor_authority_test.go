package runtimepersistence_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type dynamicFlowSourceProjectionStore interface {
	nativeReadinessFixtureStore
	storetest.RunFixtureStore
	sourceartifactfixture.Writer
	runtimerunlifecycle.OperationOwner
	InspectDynamicFlowRuntimeReadinessForSource(context.Context, runtimecorrelation.SourceArtifactFact) (runtimepipeline.DynamicFlowRuntimeReadinessProjection, error)
	InspectDynamicFlowRuntimeReadinessForRun(context.Context, string, runtimecorrelation.SourceArtifactFact) ([]runtimepipeline.DynamicFlowRuntimeReadiness, error)
	LoadDynamicFlowRuntimeReadiness(context.Context, string, runtimeflowidentity.Route) (runtimepipeline.DynamicFlowRuntimeReadiness, bool, error)
	ReconcileDynamicFlowRuntimeReadinessPlans(context.Context, []runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation, time.Time) ([]runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliationResult, error)
}

func TestDynamicFlowRuntimeReadinessForSourceScopesInSQLBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) (dynamicFlowSourceProjectionStore, *sql.DB, bool)
	}{
		{"postgres", func(t *testing.T) (dynamicFlowSourceProjectionStore, *sql.DB, bool) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			return storetest.AdmitPostgresRuntimeStore(t, db), db, false
		}},
		{"sqlite", func(t *testing.T) (dynamicFlowSourceProjectionStore, *sql.DB, bool) {
			store := storetest.StartSQLiteRuntimeStore(t)
			return store, storetest.Database(store), true
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, db, sqlite := tc.open(t)
			sourceA := mustExternalStoreTestSourceArtifactFact()
			hashA := sourceA.BundleHash()
			sourceBArtifact := sourceartifactfixture.New("agents.yaml", []byte("agents:\n  source_b: {}\n"))
			sourceB := sourceartifactfixture.FactFor(sourceBArtifact)
			hashB := sourceB.BundleHash()
			runA, runB := uuid.NewString(), uuid.NewString()
			ctx := testAuthorActivityContext()
			storetest.RequireRun(t, ctx, store, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runA, BundleHash: hashA})
			storetest.RequireRun(t, ctx, store, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runB, Artifact: sourceBArtifact})
			seedExactFlowInstanceDescriptorOwner(t, store, runA, uuid.NewString(), "account/a", hashA)
			seedExactFlowInstanceDescriptorOwner(t, store, runB, uuid.NewString(), "account/b", hashB)
			setReadinessCoordinate(t, db, sqlite, runA, "account/a", "phase", time.Now().UTC())
			projection, err := store.InspectDynamicFlowRuntimeReadinessForSource(ctx, sourceA)
			if err != nil {
				t.Fatal(err)
			}
			if len(projection.CurrentCompleted) != 1 || projection.CurrentCompleted[0].InstancePath != "account/a" || len(projection.CurrentPending) != 1 || projection.CurrentPending[0].InstancePath != runA {
				t.Fatalf("source A projection = %#v", projection)
			}
			deleteQuery := `DELETE FROM flow_instance_runtime_readiness WHERE run_id = ? AND instance_path = ?`
			deleteArgs := []any{runB, "account/b"}
			if !sqlite {
				deleteQuery = `DELETE FROM flow_instance_runtime_readiness WHERE run_id = $1::uuid AND instance_path = $2`
			}
			if _, err := db.ExecContext(ctx, deleteQuery, deleteArgs...); err != nil {
				t.Fatal(err)
			}
			var corruption *runtimepipeline.FlowInstanceConstructionCorruption
			if _, err := store.InspectDynamicFlowRuntimeReadinessForSource(ctx, sourceB); !errors.As(err, &corruption) || corruption.RunID != runB || corruption.InstancePath != "account/b" {
				t.Fatalf("missing source-owned attachment evidence error = %v", err)
			}
		})
	}
}

func TestDynamicFlowRuntimeReadinessProjectionClassifiesSourceTransitionsBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) (dynamicFlowSourceProjectionStore, *sql.DB, bool)
	}{
		{"postgres", func(t *testing.T) (dynamicFlowSourceProjectionStore, *sql.DB, bool) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			return storetest.AdmitPostgresRuntimeStore(t, db), db, false
		}},
		{"sqlite", func(t *testing.T) (dynamicFlowSourceProjectionStore, *sql.DB, bool) {
			selected := storetest.StartSQLiteRuntimeStore(t)
			return selected, storetest.Database(selected), true
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected, db, sqlite := tc.open(t)
			current := mustExternalStoreTestSourceArtifactFact()
			currentHash := current.BundleHash()
			predecessorArtifact := sourceartifactfixture.New("agents.yaml", []byte("agents:\n  predecessor: {}\n"))
			predecessor := sourceartifactfixture.FactFor(predecessorArtifact)
			predecessorHash := predecessor.BundleHash()
			foreignArtifact := sourceartifactfixture.New("agents.yaml", []byte("agents:\n  foreign: {}\n"))
			foreign := sourceartifactfixture.FactFor(foreignArtifact)
			foreignHash := foreign.BundleHash()
			ctx := testAuthorActivityContext()
			type seeded struct {
				runID, path, planHash string
				complete              bool
			}
			rows := []seeded{
				{uuid.NewString(), "account/current-complete", currentHash, true},
				{uuid.NewString(), "account/current-pending", currentHash, false},
				{uuid.NewString(), "account/transition-complete", predecessorHash, true},
				{uuid.NewString(), "account/transition-pending", predecessorHash, false},
			}
			for _, row := range rows {
				if row.planHash == predecessorHash {
					requireReadinessRun(t, ctx, selected, row.runID, predecessorHash, predecessorArtifact)
				} else {
					requireReadinessRun(t, ctx, selected, row.runID, currentHash)
				}
				seedExactFlowInstanceDescriptorOwner(t, selected, row.runID, uuid.NewString(), row.path, row.planHash)
				if row.planHash == predecessorHash {
					setReadinessRunSource(t, selected, ctx, row.runID, current, sourceartifactfixture.Artifact())
				}
				if row.complete {
					setReadinessCoordinate(t, db, sqlite, row.runID, row.path, "phase", time.Now().UTC())
				}
			}
			foreignRun := uuid.NewString()
			requireReadinessRun(t, ctx, selected, foreignRun, foreignHash, foreignArtifact)
			seedExactFlowInstanceDescriptorOwner(t, selected, foreignRun, uuid.NewString(), "account/foreign-malformed", foreignHash)
			setReadinessPlanRaw(t, db, sqlite, foreignRun, "account/foreign-malformed", `{}`)

			projection, err := selected.InspectDynamicFlowRuntimeReadinessForSource(ctx, current)
			if err != nil {
				t.Fatalf("inspect current source: %v", err)
			}
			if len(projection.CurrentCompleted) != 1 || len(projection.CurrentPending) != 3 || len(projection.SourceTransitionRequired) != 4 {
				t.Fatalf("projection classes = %#v", projection)
			}
			transitionPending := 0
			for _, item := range projection.SourceTransitionRequired {
				if item.Pending() {
					transitionPending++
				}
				if !item.OwningRunSource.Matches(current) {
					t.Fatalf("transition owning source = %#v", item.OwningRunSource)
				}
			}
			if transitionPending != 3 {
				t.Fatalf("pending transition count = %d, want one receiver and both actual parents", transitionPending)
			}
			exactRun, err := selected.InspectDynamicFlowRuntimeReadinessForRun(ctx, rows[2].runID, current)
			if err != nil || len(exactRun) != 2 {
				t.Fatalf("exact run projection = %#v err=%v", exactRun, err)
			}
			if exactRun[0].InstancePath == exactRun[1].InstancePath {
				t.Fatalf("exact run projection duplicated an owner: %#v", exactRun)
			}
			for _, item := range exactRun {
				if item.InstancePath != rows[2].path && item.InstancePath != rows[2].runID {
					t.Fatalf("exact run projection included a foreign owner: %#v", item)
				}
			}
			wrongSource, err := selected.InspectDynamicFlowRuntimeReadinessForRun(ctx, rows[2].runID, foreign)
			if err != nil || len(wrongSource) != 0 {
				t.Fatalf("wrong-source run projection = %#v err=%v", wrongSource, err)
			}
			if _, err := selected.InspectDynamicFlowRuntimeReadinessForSource(ctx, foreign); err == nil {
				t.Fatal("foreign malformed row was not decoded by its owning source")
			}
		})
	}
}

func TestDynamicFlowRuntimeReadinessObservedStateGuardBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) (dynamicFlowSourceProjectionStore, *sql.DB, bool)
	}{
		{"postgres", func(t *testing.T) (dynamicFlowSourceProjectionStore, *sql.DB, bool) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			return storetest.AdmitPostgresRuntimeStore(t, db), db, false
		}},
		{"sqlite", func(t *testing.T) (dynamicFlowSourceProjectionStore, *sql.DB, bool) {
			selected := storetest.StartSQLiteRuntimeStore(t)
			return selected, storetest.Database(selected), true
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected, db, sqlite := tc.open(t)
			source := mustExternalStoreTestSourceArtifactFact()
			bundleHash := source.BundleHash()
			otherArtifact := sourceartifactfixture.New("agents.yaml", []byte("agents:\n  other: {}\n"))
			otherSource := sourceartifactfixture.FactFor(otherArtifact)
			for _, race := range []string{"plan", "source", "topology", "creation", "run_status", "instance_status", "terminated_at"} {
				t.Run(race, func(t *testing.T) {
					runID := uuid.NewString()
					path := "account/guard-" + race + "-" + uuid.NewString()
					ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)
					requireReadinessRun(t, ctx, selected, runID, bundleHash)
					seedExactFlowInstanceDescriptorOwner(t, selected, runID, uuid.NewString(), path, bundleHash)
					observed, found, err := selected.LoadDynamicFlowRuntimeReadiness(ctx, runID, runtimeflowidentity.RouteForInstancePath(path))
					if err != nil || !found {
						t.Fatalf("load observation: found=%v err=%v", found, err)
					}
					desired := observed.Plan
					desired.WorkflowVersion = "2.0.0"
					callCtx := ctx
					switch race {
					case "plan":
						changed := observed.Plan
						changed.WorkflowVersion = "concurrent"
						raw, marshalErr := json.Marshal(changed)
						if marshalErr != nil {
							t.Fatal(marshalErr)
						}
						setReadinessPlanRaw(t, db, sqlite, runID, path, string(raw))
						query := `UPDATE flow_instance_runtime_readiness SET plan_hash = ? WHERE run_id = ? AND instance_path = ?`
						if !sqlite {
							query = `UPDATE flow_instance_runtime_readiness SET plan_hash = $1 WHERE run_id = $2::uuid AND instance_path = $3`
						}
						if _, err := db.Exec(query, readinessPlanFixtureHash(t, string(raw)), runID, path); err != nil {
							t.Fatal(err)
						}
					case "source":
						setReadinessRunSource(t, selected, callCtx, runID, otherSource, otherArtifact)
						desired.BundleHash = otherSource.BundleHash()
						callCtx = runtimecorrelation.WithSourceArtifactFact(callCtx, otherSource)
					case "topology":
						setReadinessCoordinate(t, db, sqlite, runID, path, "phase", time.Now().UTC())
					case "creation":
						setReadinessCoordinate(t, db, sqlite, runID, path, "creation_event_emitted_at", time.Now().UTC())
					case "run_status":
						setReadinessRunStatus(t, selected, callCtx, runID, runtimerunlifecycle.StatePaused)
					case "instance_status":
						setReadinessInstanceStatus(t, db, sqlite, runID, path, "draining")
					case "terminated_at":
						setReadinessCoordinate(t, db, sqlite, runID, path, "instance_terminated_at", time.Now().UTC())
					}
					results, err := selected.ReconcileDynamicFlowRuntimeReadinessPlans(callCtx, []runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation{{Observed: observed, Expected: desired}}, time.Now().UTC())
					if len(results) != 0 || !runtimepipeline.IsDynamicFlowRuntimeReadinessObservationConflict(err) {
						t.Fatalf("stale observation results=%v err=%v", results, err)
					}
					stored, found, loadErr := selected.LoadDynamicFlowRuntimeReadiness(callCtx, runID, runtimeflowidentity.RouteForInstancePath(path))
					if loadErr != nil || !found {
						t.Fatalf("load guarded row: found=%v err=%v", found, loadErr)
					}
					if stored.Plan.WorkflowVersion == desired.WorkflowVersion {
						t.Fatalf("stale observation overwrote desired plan: %#v", stored.Plan)
					}
				})
			}
		})
	}
}

func TestDynamicFlowRuntimeReadinessRejectsABAObservationBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) (dynamicFlowSourceProjectionStore, *sql.DB, bool)
	}{
		{"postgres", func(t *testing.T) (dynamicFlowSourceProjectionStore, *sql.DB, bool) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			return storetest.AdmitPostgresRuntimeStore(t, db), db, false
		}},
		{"sqlite", func(t *testing.T) (dynamicFlowSourceProjectionStore, *sql.DB, bool) {
			selected := storetest.StartSQLiteRuntimeStore(t)
			return selected, storetest.Database(selected), true
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected, _, _ := tc.open(t)
			source := mustExternalStoreTestSourceArtifactFact()
			runID := uuid.NewString()
			path := "account/aba-" + uuid.NewString()
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)
			requireReadinessRun(t, ctx, selected, runID, source.BundleHash())
			seedExactFlowInstanceDescriptorOwner(t, selected, runID, uuid.NewString(), path, source.BundleHash())
			load := func() runtimepipeline.DynamicFlowRuntimeReadiness {
				t.Helper()
				item, found, err := selected.LoadDynamicFlowRuntimeReadiness(ctx, runID, runtimeflowidentity.RouteForInstancePath(path))
				if err != nil || !found {
					t.Fatalf("load readiness: found=%v err=%v", found, err)
				}
				return item
			}
			original := load()
			if original.AttemptOrdinal != 1 {
				t.Fatalf("initial revision = %d, want 1", original.AttemptOrdinal)
			}
			advance := func(observed runtimepipeline.DynamicFlowRuntimeReadiness, version string) {
				t.Helper()
				expected := observed.Plan
				expected.WorkflowVersion = version
				result, err := selected.ReconcileDynamicFlowRuntimeReadinessPlans(ctx, []runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation{{Observed: observed, Expected: expected}}, time.Now().UTC())
				if err != nil || len(result) != 1 || !result[0].Changed || result[0].AttemptOrdinal != observed.AttemptOrdinal+1 {
					t.Fatalf("advance to %s: result=%#v err=%v", version, result, err)
				}
			}
			advance(original, "2.0.0")
			advance(load(), original.Plan.WorkflowVersion)
			staleDesired := original.Plan
			staleDesired.WorkflowVersion = "3.0.0"
			result, err := selected.ReconcileDynamicFlowRuntimeReadinessPlans(ctx, []runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation{{Observed: original, Expected: staleDesired}}, time.Now().UTC())
			if len(result) != 0 || !runtimepipeline.IsDynamicFlowRuntimeReadinessObservationConflict(err) {
				t.Fatalf("stale A observation after A-B-A: result=%#v err=%v", result, err)
			}
			current := load()
			if current.AttemptOrdinal != 3 || current.Plan.WorkflowVersion != original.Plan.WorkflowVersion {
				t.Fatalf("stale observation changed current row: revision=%d plan=%#v", current.AttemptOrdinal, current.Plan)
			}
		})
	}
}

func TestDynamicFlowRuntimeReadinessPlanBatchIsAtomicBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) (dynamicFlowSourceProjectionStore, *sql.DB, bool)
	}{
		{"postgres", func(t *testing.T) (dynamicFlowSourceProjectionStore, *sql.DB, bool) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			return storetest.AdmitPostgresRuntimeStore(t, db), db, false
		}},
		{"sqlite", func(t *testing.T) (dynamicFlowSourceProjectionStore, *sql.DB, bool) {
			selected := storetest.StartSQLiteRuntimeStore(t)
			return selected, storetest.Database(selected), true
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected, db, sqlite := tc.open(t)
			source := mustExternalStoreTestSourceArtifactFact()
			bundleHash := source.BundleHash()
			seedBatch := func(t *testing.T, label string) (context.Context, []runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation) {
				t.Helper()
				runID := uuid.NewString()
				ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)
				requireReadinessRun(t, ctx, selected, runID, bundleHash)
				requests := make([]runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation, 0, 2)
				for index := range 2 {
					path := fmt.Sprintf("account/%s-%d-%s", label, index, uuid.NewString())
					seedExactFlowInstanceDescriptorOwner(t, selected, runID, uuid.NewString(), path, bundleHash)
					observed, found, err := selected.LoadDynamicFlowRuntimeReadiness(ctx, runID, runtimeflowidentity.RouteForInstancePath(path))
					if err != nil || !found {
						t.Fatalf("load batch observation %s: found=%v err=%v", path, found, err)
					}
					expected := observed.Plan
					expected.WorkflowVersion = "2.0.0"
					requests = append(requests, runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation{Observed: observed, Expected: expected})
				}
				return ctx, requests
			}
			assertVersion := func(t *testing.T, ctx context.Context, requests []runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation, version string) {
				t.Helper()
				for _, request := range requests {
					stored, found, err := selected.LoadDynamicFlowRuntimeReadiness(ctx, request.Expected.RunID, request.Expected.Identity.Route())
					if err != nil || !found || stored.Plan.WorkflowVersion != version {
						t.Fatalf("stored batch row %s version=%q found=%v err=%v", request.Expected.Identity.InstancePath, stored.Plan.WorkflowVersion, found, err)
					}
				}
			}

			t.Run("success", func(t *testing.T) {
				ctx, requests := seedBatch(t, "success")
				results, err := selected.ReconcileDynamicFlowRuntimeReadinessPlans(ctx, requests, time.Now().UTC())
				if err != nil || len(results) != 2 || !results[0].Changed || !results[1].Changed {
					t.Fatalf("batch success results=%#v err=%v", results, err)
				}
				assertVersion(t, ctx, requests, "2.0.0")
			})

			t.Run("last_row_validation_failure_changes_none", func(t *testing.T) {
				ctx, requests := seedBatch(t, "validation")
				requests[1].Expected.WorkflowVersion = ""
				results, err := selected.ReconcileDynamicFlowRuntimeReadinessPlans(ctx, requests, time.Now().UTC())
				if err == nil || len(results) != 0 {
					t.Fatalf("batch validation results=%#v err=%v", results, err)
				}
				assertVersion(t, ctx, requests, "1.0.0")
			})

			t.Run("last_row_conflict_changes_none", func(t *testing.T) {
				ctx, requests := seedBatch(t, "conflict")
				last := requests[1]
				setReadinessCoordinate(t, db, sqlite, last.Expected.RunID, last.Expected.Identity.InstancePath, "phase", time.Now().UTC())
				results, err := selected.ReconcileDynamicFlowRuntimeReadinessPlans(ctx, requests, time.Now().UTC())
				if len(results) != 0 || !runtimepipeline.IsDynamicFlowRuntimeReadinessObservationConflict(err) {
					t.Fatalf("batch conflict results=%#v err=%v", results, err)
				}
				assertVersion(t, ctx, requests, "1.0.0")
			})

			t.Run("later_update_failure_rolls_back", func(t *testing.T) {
				ctx, requests := seedBatch(t, "rollback")
				lastPath := requests[1].Expected.Identity.InstancePath
				triggerName := "fail_readiness_batch_" + strings.ReplaceAll(uuid.NewString(), "-", "")
				if sqlite {
					query := fmt.Sprintf(`CREATE TRIGGER %s BEFORE UPDATE OF plan ON flow_instance_runtime_readiness WHEN NEW.instance_path = '%s' BEGIN SELECT RAISE(ABORT, 'injected readiness batch failure'); END`, triggerName, strings.ReplaceAll(lastPath, "'", "''"))
					if _, err := db.Exec(query); err != nil {
						t.Fatal(err)
					}
				} else {
					functionName := triggerName + "_fn"
					functionSQL := fmt.Sprintf(`CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.instance_path = '%s' THEN RAISE EXCEPTION 'injected readiness batch failure'; END IF; RETURN NEW; END $$`, functionName, strings.ReplaceAll(lastPath, "'", "''"))
					if _, err := db.Exec(functionSQL); err != nil {
						t.Fatal(err)
					}
					triggerSQL := fmt.Sprintf(`CREATE TRIGGER %s BEFORE UPDATE OF plan ON flow_instance_runtime_readiness FOR EACH ROW EXECUTE FUNCTION %s()`, triggerName, functionName)
					if _, err := db.Exec(triggerSQL); err != nil {
						t.Fatal(err)
					}
				}
				results, err := selected.ReconcileDynamicFlowRuntimeReadinessPlans(ctx, requests, time.Now().UTC())
				if err == nil || len(results) != 0 {
					t.Fatalf("injected batch failure results=%#v err=%v", results, err)
				}
				assertVersion(t, ctx, requests, "1.0.0")
			})
		})
	}
}

type nativeReadinessFixtureStore interface {
	CommitFlowInstanceActivation(context.Context, bus.FlowInstanceActivationCommand) (runtimepipeline.CommittedFlowInstanceActivation, error)
	LoadWorkflowInstance(context.Context, runtimeflowidentity.RunScopedFlowInstance) (runtimepipeline.WorkflowInstance, bool, error)
	LoadFlowConstructionPublication(context.Context, runtimeflowidentity.RunScopedFlowInstance, string) (runtimepipeline.FlowConstructionPublicationEvidence, error)
}

// These are prepared attachment-component fixtures, not public constructor eligibility proofs.
func seedExactFlowInstanceDescriptorOwner(t *testing.T, selected nativeReadinessFixtureStore, runID, entityID, instancePath, bundleHash string) {
	t.Helper()
	fact, err := runtimecorrelation.NewSourceArtifactFact(bundleHash)
	if err != nil {
		t.Fatal(err)
	}
	ctx := runtimecorrelation.WithSourceArtifactFact(runtimecorrelation.WithRunID(testAuthorActivityContext(), runID), fact)
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope("11111111-1111-1111-1111-111111111111", bundleHash))
	rootOwner := runtimeflowidentity.RunScopedFlowInstance{RunID: runID, Route: runtimeflowidentity.StoredRoute(".", runID, runID)}
	root, found, err := selected.LoadWorkflowInstance(ctx, rootOwner)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC().Truncate(time.Microsecond)
	commit := func(instance runtimepipeline.WorkflowInstance) {
		t.Helper()
		command, err := flowactivationfixture.Command(ctx, instance, runtimepipeline.WorkflowLifecycleMutationPlan{}, at)
		if err != nil {
			t.Fatal(err)
		}
		result, err := selected.CommitFlowInstanceActivation(ctx, command)
		if err != nil || !result.Acknowledged || !result.Created {
			t.Fatalf("construct attachment component: acknowledged=%t created=%t err=%v", result.Acknowledged, result.Created, err)
		}
	}
	if !found {
		root = runtimepipeline.WorkflowInstance{
			InstanceID: runID, StorageRef: runID, EntityID: runID, WorkflowName: ".", WorkflowVersion: "1.0.0",
			Mode: "static", CurrentState: "active", CreatedAt: at, EnteredStageAt: at,
		}
		commit(root)
	}
	if _, err := selected.LoadFlowConstructionPublication(ctx, rootOwner, root.EntityID); err != nil {
		t.Fatalf("reuse actual captured parent: %v", err)
	}
	instanceID := strings.TrimPrefix(instancePath, "account/")
	if instanceID == "" || instanceID == instancePath || strings.Contains(instanceID, "/") {
		t.Fatal("attachment fixture requires its exact account child")
	}
	commit(runtimepipeline.WorkflowInstance{
		InstanceID: instanceID, StorageRef: instancePath, EntityID: entityID, EntityType: "account",
		WorkflowName: "account", WorkflowVersion: "1.0.0", Mode: "template", InstanceKey: instanceID,
		ParentFlowID: ".", ParentFlowInstance: runID, ParentEntityID: root.EntityID,
		StageDefined: true, CurrentState: "active", Fields: map[string]any{}, CreatedAt: at, EnteredStageAt: at,
	})
}

func requireReadinessRun(t *testing.T, ctx context.Context, selected storetest.RunFixtureStore, runID, bundleHash string, artifacts ...*sourceartifact.AdmittedSourceArtifact) {
	t.Helper()
	fixture := storetest.RunFixture{
		Origin:     storetest.ScenarioSetupOrigin(),
		RunID:      runID,
		BundleHash: bundleHash,
	}
	if len(artifacts) > 0 {
		fixture.Artifact = artifacts[0]
	}
	storetest.RequireRun(t, ctx, selected, fixture)
}

func setReadinessPlanRaw(t *testing.T, db *sql.DB, sqlite bool, runID, instancePath, plan string) {
	t.Helper()
	query := `UPDATE flow_instance_runtime_readiness SET plan = ? WHERE run_id = ? AND instance_path = ?`
	if !sqlite {
		query = `UPDATE flow_instance_runtime_readiness SET plan = $1::jsonb WHERE run_id = $2::uuid AND instance_path = $3`
	}
	if _, err := db.Exec(query, plan, runID, instancePath); err != nil {
		t.Fatalf("set readiness plan for %s: %v", instancePath, err)
	}
}

func setReadinessRunSource(t *testing.T, owner interface {
	runtimerunlifecycle.OperationOwner
	sourceartifactfixture.Writer
}, ctx context.Context, runID string, source runtimecorrelation.SourceArtifactFact, artifact *sourceartifact.AdmittedSourceArtifact) {
	t.Helper()
	sourceartifactfixture.RequireArtifact(t, ctx, owner, artifact)
	if _, err := owner.ReviseRunSource(ctx, runtimerunlifecycle.SourceRevisionRequest{RunID: runID, Source: source}); err != nil {
		t.Fatalf("set readiness run source for %s: %v", runID, err)
	}
}

func setReadinessRunStatus(t *testing.T, owner runtimerunlifecycle.OperationOwner, ctx context.Context, runID string, state runtimerunlifecycle.State) {
	t.Helper()
	if _, err := owner.TransitionActiveRun(ctx, runtimerunlifecycle.ActiveTransitionRequest{RunID: runID, State: state}); err != nil {
		t.Fatalf("set readiness run status for %s: %v", runID, err)
	}
}

func setReadinessCoordinate(t *testing.T, db *sql.DB, sqlite bool, runID, instancePath, coordinate string, value time.Time) {
	t.Helper()
	var query string
	switch coordinate {
	case "phase":
		// Reader-conflict fixtures represent historical completed attachment,
		// never an executable generation or a planned attempt skipping steps.
		grantID := uuid.NewString()
		query = `UPDATE flow_instance_runtime_readiness SET phase = 'ready', activation_attempt_state='retired', activation_attempt_grant_id=?, updated_at = ? WHERE run_id = ? AND instance_path = ?`
		if !sqlite {
			query = `UPDATE flow_instance_runtime_readiness SET phase = 'ready', activation_attempt_state='retired', activation_attempt_grant_id=$1::uuid, updated_at = $2 WHERE run_id = $3::uuid AND instance_path = $4`
		}
		if _, err := db.Exec(query, grantID, value, runID, instancePath); err != nil {
			t.Fatalf("set %s for %s: %v", coordinate, instancePath, err)
		}
		return
	case "creation_event_emitted_at":
		query = `UPDATE flow_instance_runtime_readiness SET creation_event_emitted_at = ? WHERE run_id = ? AND instance_path = ?`
		if !sqlite {
			query = `UPDATE flow_instance_runtime_readiness SET creation_event_emitted_at = $1 WHERE run_id = $2::uuid AND instance_path = $3`
		}
	case "instance_terminated_at":
		query = `UPDATE flow_instances SET terminated_at = ? WHERE run_id = ? AND instance_path = ?`
		if !sqlite {
			query = `UPDATE flow_instances SET terminated_at = $1 WHERE run_id = $2::uuid AND instance_path = $3`
		}
		if _, err := db.Exec(query, value, runID, instancePath); err != nil {
			t.Fatalf("set %s for %s: %v", coordinate, instancePath, err)
		}
		return
	default:
		t.Fatalf("unsupported readiness coordinate %q", coordinate)
	}
	if _, err := db.Exec(query, value, runID, instancePath); err != nil {
		t.Fatalf("set %s for %s: %v", coordinate, instancePath, err)
	}
}

func setReadinessInstanceStatus(t *testing.T, db *sql.DB, sqlite bool, runID, instancePath, status string) {
	t.Helper()
	query := `UPDATE flow_instances SET status = ? WHERE run_id = ? AND instance_path = ?`
	if !sqlite {
		query = `UPDATE flow_instances SET status = $1 WHERE run_id = $2::uuid AND instance_path = $3`
	}
	if _, err := db.Exec(query, status, runID, instancePath); err != nil {
		t.Fatalf("set readiness instance status for %s: %v", instancePath, err)
	}
}
