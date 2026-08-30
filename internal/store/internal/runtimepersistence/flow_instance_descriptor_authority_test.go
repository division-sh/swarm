package runtimepersistence_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/notifyallchildren"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type flowInstanceDescriptorAuthorityStore interface {
	externalStoreTestDurableEventBusStore
	runtimebus.FlowInstanceRouteSetPersistence
	runtimebus.FlowInstanceRouteRecordReader
	runtimebus.ActiveFlowInstanceDescriptorLister
}

type dynamicFlowSourceProjectionStore interface {
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
			fixtureA := runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runA, BundleHash: hashA}
			fixtureB := runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runB, Artifact: sourceBArtifact}
			if sqlite {
				runlifecyclefixture.RequireSQLite(t, ctx, db, fixtureA)
				runlifecyclefixture.RequireSQLite(t, ctx, db, fixtureB)
			} else {
				runlifecyclefixture.RequirePostgres(t, ctx, db, fixtureA)
				runlifecyclefixture.RequirePostgres(t, ctx, db, fixtureB)
			}
			seedExactFlowInstanceDescriptorOwner(t, db, sqlite, runA, uuid.NewString(), "account/a", hashA)
			seedExactFlowInstanceDescriptorOwner(t, db, sqlite, runB, uuid.NewString(), "account/b", hashB)
			seedStaticFlowInstanceRouteWithoutReadiness(t, db, sqlite, runA, "standing/a")
			query := `UPDATE flow_instance_runtime_readiness SET topology_ready_at = ? WHERE run_id = ? AND instance_id = ?`
			args := []any{time.Now().UTC(), runA, "account/a"}
			if !sqlite {
				query = `UPDATE flow_instance_runtime_readiness SET topology_ready_at = $1 WHERE run_id = $2::uuid AND instance_id = $3`
			}
			if _, err := db.ExecContext(ctx, query, args...); err != nil {
				t.Fatal(err)
			}
			projection, err := store.InspectDynamicFlowRuntimeReadinessForSource(ctx, sourceA)
			if err != nil {
				t.Fatal(err)
			}
			if len(projection.CurrentCompleted) != 1 || projection.CurrentCompleted[0].InstancePath != "account/a" || len(projection.CurrentPending) != 0 {
				t.Fatalf("source A projection = %#v", projection)
			}
			deleteQuery := `DELETE FROM flow_instance_runtime_readiness WHERE run_id = ? AND instance_id = ?`
			routeQuery := `INSERT INTO routing_rules (event_pattern, subscriber_type, subscriber_id, flow_instance, source_flow, is_materialized, status, created_at) VALUES (?, 'node', 'receiver', ?, 'account', TRUE, 'active', ?)`
			deleteArgs := []any{runB, "account/b"}
			routeArgs := []any{"account/b/event", "account/b", time.Now().UTC()}
			if !sqlite {
				deleteQuery = `DELETE FROM flow_instance_runtime_readiness WHERE run_id = $1::uuid AND instance_id = $2`
				routeQuery = `INSERT INTO routing_rules (event_pattern, subscriber_type, subscriber_id, flow_instance, source_flow, is_materialized, status, created_at) VALUES ($1, 'node', 'receiver', $2, 'account', TRUE, 'active', $3)`
			}
			if _, err := db.ExecContext(ctx, deleteQuery, deleteArgs...); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, routeQuery, routeArgs...); err != nil {
				t.Fatal(err)
			}
			if _, err := store.InspectDynamicFlowRuntimeReadinessForSource(ctx, sourceB); err == nil || !strings.Contains(err.Error(), "no dynamic runtime readiness owner") {
				t.Fatalf("invalid source-owned route error = %v", err)
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
				requireReadinessRun(t, ctx, db, sqlite, row.runID, currentHash)
				seedExactFlowInstanceDescriptorOwner(t, db, sqlite, row.runID, uuid.NewString(), row.path, row.planHash)
				if row.complete {
					setReadinessCoordinate(t, db, sqlite, row.runID, row.path, "topology_ready_at", time.Now().UTC())
				}
			}
			foreignRun := uuid.NewString()
			requireReadinessRun(t, ctx, db, sqlite, foreignRun, foreignHash, foreignArtifact)
			seedExactFlowInstanceDescriptorOwner(t, db, sqlite, foreignRun, uuid.NewString(), "account/foreign-malformed", foreignHash)
			setReadinessPlanRaw(t, db, sqlite, foreignRun, "account/foreign-malformed", `{}`)

			projection, err := selected.InspectDynamicFlowRuntimeReadinessForSource(ctx, current)
			if err != nil {
				t.Fatalf("inspect current source: %v", err)
			}
			if len(projection.CurrentCompleted) != 1 || len(projection.CurrentPending) != 1 || len(projection.SourceTransitionRequired) != 2 {
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
			if transitionPending != 1 {
				t.Fatalf("pending transition count = %d, want 1", transitionPending)
			}
			exactRun, err := selected.InspectDynamicFlowRuntimeReadinessForRun(ctx, rows[2].runID, current)
			if err != nil || len(exactRun) != 1 || exactRun[0].InstancePath != rows[2].path {
				t.Fatalf("exact run projection = %#v err=%v", exactRun, err)
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
					requireReadinessRun(t, ctx, db, sqlite, runID, bundleHash)
					seedExactFlowInstanceDescriptorOwner(t, db, sqlite, runID, uuid.NewString(), path, bundleHash)
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
					case "source":
						setReadinessRunSource(t, selected, callCtx, runID, otherSource, otherArtifact)
						desired.BundleHash = otherSource.BundleHash()
						callCtx = runtimecorrelation.WithSourceArtifactFact(callCtx, otherSource)
					case "topology":
						setReadinessCoordinate(t, db, sqlite, runID, path, "topology_ready_at", time.Now().UTC())
					case "creation":
						setReadinessCoordinate(t, db, sqlite, runID, path, "creation_event_emitted_at", time.Now().UTC())
					case "run_status":
						setReadinessRunStatus(t, selected, callCtx, runID, runtimerunlifecycle.StatePaused)
					case "instance_status":
						setReadinessInstanceStatus(t, db, sqlite, path, "draining")
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
				requireReadinessRun(t, ctx, db, sqlite, runID, bundleHash)
				requests := make([]runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation, 0, 2)
				for index := range 2 {
					path := fmt.Sprintf("account/%s-%d-%s", label, index, uuid.NewString())
					seedExactFlowInstanceDescriptorOwner(t, db, sqlite, runID, uuid.NewString(), path, bundleHash)
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
				setReadinessCoordinate(t, db, sqlite, last.Expected.RunID, last.Expected.Identity.InstancePath, "topology_ready_at", time.Now().UTC())
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
					query := fmt.Sprintf(`CREATE TRIGGER %s BEFORE UPDATE OF plan ON flow_instance_runtime_readiness WHEN NEW.instance_id = '%s' BEGIN SELECT RAISE(ABORT, 'injected readiness batch failure'); END`, triggerName, strings.ReplaceAll(lastPath, "'", "''"))
					if _, err := db.Exec(query); err != nil {
						t.Fatal(err)
					}
				} else {
					functionName := triggerName + "_fn"
					functionSQL := fmt.Sprintf(`CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.instance_id = '%s' THEN RAISE EXCEPTION 'injected readiness batch failure'; END IF; RETURN NEW; END $$`, functionName, strings.ReplaceAll(lastPath, "'", "''"))
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

func seedStaticFlowInstanceRouteWithoutReadiness(t *testing.T, db *sql.DB, sqlite bool, runID, instancePath string) {
	t.Helper()
	now := time.Now().UTC()
	entityID := uuid.NewString()
	if sqlite {
		if _, err := db.Exec(`INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at) VALUES (?, 'standing', 'static', '{}', 'active', ?)`, instancePath, now); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO entity_state (entity_id, run_id, flow_instance, entity_type, current_state, fields, created_at, updated_at) VALUES (?, ?, ?, 'standing', 'active', '{}', ?, ?)`, entityID, runID, instancePath, now, now); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO routing_rules (event_pattern, subscriber_type, subscriber_id, flow_instance, source_flow, is_materialized, status, created_at) VALUES (?, 'node', 'receiver', ?, 'standing', TRUE, 'active', ?)`, instancePath+"/event", instancePath, now); err != nil {
			t.Fatal(err)
		}
		return
	}
	if _, err := db.Exec(`INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at) VALUES ($1, 'standing', 'static', '{}'::jsonb, 'active', $2)`, instancePath, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO entity_state (entity_id, run_id, flow_instance, entity_type, current_state, fields, created_at, updated_at) VALUES ($1::uuid, $2::uuid, $3, 'standing', 'active', '{}'::jsonb, $4, $4)`, entityID, runID, instancePath, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO routing_rules (event_pattern, subscriber_type, subscriber_id, flow_instance, source_flow, is_materialized, status, created_at) VALUES ($1, 'node', 'receiver', $2, 'standing', TRUE, 'active', $3)`, instancePath+"/event", instancePath, now); err != nil {
		t.Fatal(err)
	}
}

func TestActiveFlowInstanceDescriptorAuthorityScopesCensusToExactRunBothStores(t *testing.T) {
	type backendCase struct {
		name  string
		setup func(*testing.T) (flowInstanceDescriptorAuthorityStore, *sql.DB, bool)
	}
	backends := []backendCase{
		{
			name: "postgres",
			setup: func(t *testing.T) (flowInstanceDescriptorAuthorityStore, *sql.DB, bool) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				return storetest.AdmitPostgresRuntimeStore(t, db), db, false
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) (flowInstanceDescriptorAuthorityStore, *sql.DB, bool) {
				selected := storetest.StartSQLiteRuntimeStore(t)
				return selected, storetest.Database(selected), true
			},
		},
	}

	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			selected, db, sqlite := backend.setup(t)
			runA, runB := uuid.NewString(), uuid.NewString()
			entityA, entityB := uuid.NewString(), uuid.NewString()
			source := mustExternalStoreTestSourceArtifactFact()
			bundleHash := source.BundleHash()
			ctxA := runtimecorrelation.WithRunID(testAuthorActivityContext(), runA)
			ctxB := runtimecorrelation.WithRunID(testAuthorActivityContext(), runB)
			if sqlite {
				runlifecyclefixture.RequireSQLite(t, ctxA, db, runlifecyclefixture.Fixture{
					Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runA,
					BundleHash: bundleHash,
				})
				runlifecyclefixture.RequireSQLite(t, ctxB, db, runlifecyclefixture.Fixture{
					Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runB,
					BundleHash: bundleHash,
				})
			} else {
				runlifecyclefixture.RequirePostgres(t, ctxA, db, runlifecyclefixture.Fixture{
					Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runA,
					BundleHash: bundleHash,
				})
				runlifecyclefixture.RequirePostgres(t, ctxB, db, runlifecyclefixture.Fixture{
					Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runB,
					BundleHash: bundleHash,
				})
			}

			seedExactFlowInstanceDescriptorOwner(t, db, sqlite, runA, entityA, "account/a", bundleHash)
			seedExactFlowInstanceDescriptorOwner(t, db, sqlite, runB, entityB, "account/b", bundleHash)

			for _, check := range []struct {
				ctx  context.Context
				path string
			}{
				{ctx: ctxA, path: "account/a"},
				{ctx: ctxB, path: "account/b"},
			} {
				descriptors, err := selected.ListActiveFlowInstanceDescriptors(check.ctx)
				if err != nil {
					t.Fatalf("ListActiveFlowInstanceDescriptors(%s): %v", check.path, err)
				}
				if len(descriptors) != 1 || descriptors[0].FlowInstance != check.path {
					t.Fatalf("descriptors for %s = %#v, want exact run-owned descriptor", check.path, descriptors)
				}
			}
		})
	}
}

func seedExactFlowInstanceDescriptorOwner(
	t *testing.T,
	db *sql.DB,
	sqlite bool,
	runID, entityID, instancePath, bundleHash string,
) {
	t.Helper()
	readiness := exactFlowInstanceDescriptorReadinessJSON(
		t, runID, bundleHash, "1.0.0", "account", instancePath, entityID,
	)
	if sqlite {
		if _, err := db.Exec(`
			INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
			VALUES (?, 'account', 'template', '{}', 'active', CURRENT_TIMESTAMP)
		`, instancePath); err != nil {
			t.Fatalf("seed sqlite flow instance %s: %v", instancePath, err)
		}
		if _, err := db.Exec(`
			INSERT INTO entity_state (entity_id, run_id, flow_instance, entity_type, current_state, fields, created_at, updated_at)
			VALUES (?, ?, ?, 'account', 'active', '{}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		`, entityID, runID, instancePath); err != nil {
			t.Fatalf("seed sqlite entity state %s: %v", instancePath, err)
		}
		if _, err := db.Exec(`
			INSERT INTO flow_instance_runtime_readiness (run_id, instance_id, plan, created_at, updated_at)
			VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		`, runID, instancePath, readiness); err != nil {
			t.Fatalf("seed sqlite readiness %s: %v", instancePath, err)
		}
		return
	}
	if _, err := db.Exec(`
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES ($1, 'account', 'template', '{}'::jsonb, 'active', NOW())
	`, instancePath); err != nil {
		t.Fatalf("seed postgres flow instance %s: %v", instancePath, err)
	}
	if _, err := db.Exec(`
		INSERT INTO entity_state (entity_id, run_id, flow_instance, entity_type, current_state, fields, created_at, updated_at)
		VALUES ($1::uuid, $2::uuid, $3, 'account', 'active', '{}'::jsonb, NOW(), NOW())
	`, entityID, runID, instancePath); err != nil {
		t.Fatalf("seed postgres entity state %s: %v", instancePath, err)
	}
	if _, err := db.Exec(`
		INSERT INTO flow_instance_runtime_readiness (run_id, instance_id, plan, created_at, updated_at)
		VALUES ($1::uuid, $2, $3::jsonb, NOW(), NOW())
	`, runID, instancePath, readiness); err != nil {
		t.Fatalf("seed postgres readiness %s: %v", instancePath, err)
	}
}

func requireReadinessRun(t *testing.T, ctx context.Context, db *sql.DB, sqlite bool, runID, bundleHash string, artifacts ...*sourceartifact.AdmittedSourceArtifact) {
	t.Helper()
	fixture := runlifecyclefixture.Fixture{
		Origin:     runlifecyclefixture.ScenarioSetupOrigin(),
		RunID:      runID,
		BundleHash: bundleHash,
	}
	if len(artifacts) > 0 {
		fixture.Artifact = artifacts[0]
	}
	if sqlite {
		runlifecyclefixture.RequireSQLite(t, ctx, db, fixture)
		return
	}
	runlifecyclefixture.RequirePostgres(t, ctx, db, fixture)
}

func setReadinessPlanRaw(t *testing.T, db *sql.DB, sqlite bool, runID, instancePath, plan string) {
	t.Helper()
	query := `UPDATE flow_instance_runtime_readiness SET plan = ? WHERE run_id = ? AND instance_id = ?`
	if !sqlite {
		query = `UPDATE flow_instance_runtime_readiness SET plan = $1::jsonb WHERE run_id = $2::uuid AND instance_id = $3`
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
	case "topology_ready_at":
		query = `UPDATE flow_instance_runtime_readiness SET topology_ready_at = ? WHERE run_id = ? AND instance_id = ?`
		if !sqlite {
			query = `UPDATE flow_instance_runtime_readiness SET topology_ready_at = $1 WHERE run_id = $2::uuid AND instance_id = $3`
		}
	case "creation_event_emitted_at":
		query = `UPDATE flow_instance_runtime_readiness SET creation_event_emitted_at = ? WHERE run_id = ? AND instance_id = ?`
		if !sqlite {
			query = `UPDATE flow_instance_runtime_readiness SET creation_event_emitted_at = $1 WHERE run_id = $2::uuid AND instance_id = $3`
		}
	case "instance_terminated_at":
		query = `UPDATE flow_instances SET terminated_at = ? WHERE instance_id = ?`
		if !sqlite {
			query = `UPDATE flow_instances SET terminated_at = $1 WHERE instance_id = $2`
		}
		if _, err := db.Exec(query, value, instancePath); err != nil {
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

func setReadinessInstanceStatus(t *testing.T, db *sql.DB, sqlite bool, instancePath, status string) {
	t.Helper()
	query := `UPDATE flow_instances SET status = ? WHERE instance_id = ?`
	if !sqlite {
		query = `UPDATE flow_instances SET status = $1 WHERE instance_id = $2`
	}
	if _, err := db.Exec(query, status, instancePath); err != nil {
		t.Fatalf("set readiness instance status for %s: %v", instancePath, err)
	}
}

func TestActiveFlowInstanceDescriptorAuthorityPreservesRoutesOnInvalidProvenanceBothStores(t *testing.T) {
	type backendCase struct {
		name  string
		setup func(*testing.T) (flowInstanceDescriptorAuthorityStore, *sql.DB, bool)
	}
	backends := []backendCase{
		{
			name: "postgres",
			setup: func(t *testing.T) (flowInstanceDescriptorAuthorityStore, *sql.DB, bool) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				return storetest.AdmitPostgresRuntimeStore(t, db), db, false
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) (flowInstanceDescriptorAuthorityStore, *sql.DB, bool) {
				selected := storetest.StartSQLiteRuntimeStore(t)
				return selected, storetest.Database(selected), true
			},
		},
	}
	cases := []struct {
		name                string
		readiness           string
		readinessOnWrongRun bool
		foreignBundle       bool
		wantListError       string
		wantError           string
	}{
		{name: "no readiness row", wantListError: "missing exact readiness plan"},
		{name: "wrong-run readiness row", readiness: "exact", readinessOnWrongRun: true, wantListError: "missing exact readiness plan"},
		{name: "incomplete readiness plan", readiness: `{}`, wantListError: "validate"},
		{name: "stale readiness version", readiness: `{"version":3}`, wantListError: "unsupported version 3"},
		{name: "malformed readiness plan", readiness: `{"workflow_version":{"unexpected":true}}`, wantListError: "decode"},
		{name: "exact readiness owner", readiness: "exact"},
		{name: "foreign run source", readiness: "exact", foreignBundle: true, wantListError: "readiness source does not match persisted run"},
	}

	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					selected, db, sqlite := backend.setup(t)
					runID := uuid.NewString()
					ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)
					bundle := notifyallchildren.LoadBundle(t, notifyallchildren.Options{})
					source := semanticview.Wrap(bundle)
					ctx = runtimecorrelation.WithSourceArtifactFact(ctx, sourceartifactfixture.FactFor(bundle.SourceArtifact))
					scope, ok := runtimeauthoractivity.ScopeFromContext(ctx)
					if !ok {
						t.Fatal("test author-activity scope is unavailable")
					}
					ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(scope.RuntimeInstanceID, bundle.SourceArtifact.BundleHash()))
					storetest.RequireBundleDataCatalog(t, ctx, selected, bundle)
					planBundleHash := bundle.SourceArtifact.BundleHash()
					bundleHash := planBundleHash
					if tc.foreignBundle {
						foreign := sourceartifactfixture.New("agents.yaml", []byte("agents:\n  foreign: {}\n"))
						sourceartifactfixture.RequireArtifact(t, ctx, selected, foreign)
						bundleHash = foreign.BundleHash()
					}
					wrongRunID := uuid.NewString()
					seedFlowInstanceDescriptorAuthorityCase(
						t,
						ctx,
						db,
						sqlite,
						runID,
						wrongRunID,
						bundleHash,
						planBundleHash,
						source.WorkflowVersion(),
						tc.readiness,
						tc.readinessOnWrongRun,
					)

					descriptors, err := selected.ListActiveFlowInstanceDescriptors(ctx)
					if tc.wantListError != "" {
						if err == nil || !strings.Contains(err.Error(), tc.wantListError) {
							t.Fatalf("ListActiveFlowInstanceDescriptors error = %v, want %q", err, tc.wantListError)
						}
						return
					}
					if err != nil {
						t.Fatalf("ListActiveFlowInstanceDescriptors: %v", err)
					}
					var testedDescriptor *runtimebus.ActiveFlowInstanceDescriptor
					for idx := range descriptors {
						if descriptors[idx].FlowInstance == "account/existing" {
							testedDescriptor = &descriptors[idx]
							break
						}
					}
					if testedDescriptor == nil {
						t.Fatalf("active descriptors = %#v, want account/existing", descriptors)
					}
					if !testedDescriptor.HasSemanticSource() {
						t.Fatalf("descriptor semantic source = %#v, want exact source", *testedDescriptor)
					}

					identity := runtimeflowidentity.DeriveRoute(notifyallchildren.ChildFlowID, "current")
					prior := runtimebus.FlowInstanceRouteRecord{
						Identity:       identity,
						EventPattern:   identity.InstancePath + "/prior.event",
						SubscriberType: "agent",
						SubscriberID:   "prior-agent",
						SourceFlow:     notifyallchildren.ChildFlowID,
					}
					if err := selected.ReplaceFlowInstanceRouteRecords(ctx, identity, []runtimebus.FlowInstanceRouteRecord{prior}); err != nil {
						t.Fatalf("seed prior exact route set: %v", err)
					}
					before, err := selected.ListFlowInstanceRouteRecords(ctx, identity)
					if err != nil {
						t.Fatalf("read prior exact route set: %v", err)
					}

					eventBus, err := newStoreTestEventBus(t, selected, runtimebus.EventBusOptions{
						ContractBundle:     source,
						SourceArtifactFact: sourceartifactfixture.FactFor(bundle.SourceArtifact),
					})
					if err != nil {
						t.Fatalf("NewEventBusWithOptions: %v", err)
					}
					for _, resolution := range []struct {
						name       string
						localEvent string
					}{
						{name: "select", localEvent: "account.notify.requested"},
						{name: "select-or-create", localEvent: "account.registered"},
					} {
						raw, err := json.Marshal(map[string]any{
							"account_id": "existing",
							"command":    "inspect",
						})
						if err != nil {
							t.Fatalf("marshal %s event: %v", resolution.name, err)
						}
						evt := eventtest.RunCreatingRootIngress(
							uuid.NewString(),
							events.EventType(source.ResolveFlowEventReference(notifyallchildren.OwnerFlowID, resolution.localEvent)),
							notifyallchildren.OwnerFlowID,
							"",
							raw,
							0,
							runID,
							"",
							events.EventEnvelope{},
							time.Now().UTC(),
						)
						_, pinErr := eventBus.CheckPublishRecipientPlan(ctx, evt)
						if tc.wantError != "" {
							if pinErr == nil || !strings.Contains(pinErr.Error(), tc.wantError) {
								t.Fatalf("%s source admission error = %v, want %q", resolution.name, pinErr, tc.wantError)
							}
						} else if pinErr != nil {
							t.Fatalf("%s exact source admission: %v", resolution.name, pinErr)
						}
						afterPin, readErr := selected.ListFlowInstanceRouteRecords(ctx, identity)
						if readErr != nil {
							t.Fatalf("read exact route set after %s pin: %v", resolution.name, readErr)
						}
						if !reflect.DeepEqual(afterPin, before) {
							t.Fatalf("%s pin mutated route state: before=%#v after=%#v", resolution.name, before, afterPin)
						}
					}
					err = eventBus.StageFlowInstanceRouteContext(ctx, runtimebus.FlowInstanceRouteMaterializationRequest{
						Identity: identity,
						ActivationVariables: map[string]string{
							"account_id": "current",
						},
					})
					if tc.wantError != "" {
						if err == nil || !strings.Contains(err.Error(), tc.wantError) {
							t.Fatalf("StageFlowInstanceRouteContext error = %v, want %q", err, tc.wantError)
						}
						after, readErr := selected.ListFlowInstanceRouteRecords(ctx, identity)
						if readErr != nil {
							t.Fatalf("read exact route set after rejection: %v", readErr)
						}
						if !reflect.DeepEqual(after, before) {
							t.Fatalf("exact route set changed across provenance rejection: before=%#v after=%#v", before, after)
						}
						return
					}
					if err != nil {
						t.Fatalf("StageFlowInstanceRouteContext exact owner: %v", err)
					}
					after, err := selected.ListFlowInstanceRouteRecords(ctx, identity)
					if err != nil {
						t.Fatalf("read exact route set after replacement: %v", err)
					}
					if len(after) == 0 || reflect.DeepEqual(after, before) {
						t.Fatalf("exact readiness owner did not replace prior route set: before=%#v after=%#v", before, after)
					}
				})
			}
		})
	}
}

func seedFlowInstanceDescriptorAuthorityCase(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	sqlite bool,
	runID string,
	wrongRunID string,
	bundleHash string,
	planBundleHash string,
	workflowVersion string,
	readiness string,
	readinessOnWrongRun bool,
) {
	t.Helper()
	const instancePath = "account/existing"
	const stagedInstancePath = "account/current"
	const config = `{"workflow_version":"1.0.0"}`
	entityID := uuid.NewString()
	stagedEntityID := uuid.NewString()
	stagedReadiness := exactFlowInstanceDescriptorReadinessJSON(t, runID, planBundleHash, workflowVersion, notifyallchildren.ChildFlowID, stagedInstancePath, stagedEntityID)
	if readiness == "exact" {
		readiness = exactFlowInstanceDescriptorReadinessJSON(t, runID, planBundleHash, workflowVersion, notifyallchildren.ChildFlowID, instancePath, entityID)
	}
	if sqlite {
		now := time.Now().UTC()
		runlifecyclefixture.RequireSQLite(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(),
			RunID: runID, StartedAt: now, BundleHash: bundleHash,
		})
		runlifecyclefixture.RequireSQLite(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(),
			RunID: wrongRunID, StartedAt: now, BundleHash: bundleHash,
		})
		if _, err := db.ExecContext(ctx, `
			INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
			VALUES
				(?, ?, 'template', ?, 'active', ?),
				(?, ?, 'template', ?, 'active', ?)
		`, instancePath, notifyallchildren.ChildFlowID, config, now, stagedInstancePath, notifyallchildren.ChildFlowID, config, now); err != nil {
			t.Fatalf("seed sqlite flow instance: %v", err)
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO entity_state (entity_id, run_id, flow_instance, entity_type, current_state, fields, created_at, updated_at)
			VALUES
				(?, ?, ?, 'account', 'active', '{"account_id":"existing"}', ?, ?),
				(?, ?, ?, 'account', 'active', '{"account_id":"current"}', ?, ?)
		`, entityID, runID, instancePath, now, now, stagedEntityID, runID, stagedInstancePath, now, now); err != nil {
			t.Fatalf("seed sqlite entity state: %v", err)
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO flow_instance_runtime_readiness (run_id, instance_id, plan, created_at, updated_at)
				VALUES (?, ?, ?, ?, ?)
			`, runID, stagedInstancePath, stagedReadiness, now, now); err != nil {
			t.Fatalf("seed sqlite staged-owner readiness: %v", err)
		}
		if readiness != "" {
			readinessRunID := runID
			if readinessOnWrongRun {
				readinessRunID = wrongRunID
			}
			if _, err := db.ExecContext(ctx, `
				INSERT INTO flow_instance_runtime_readiness (run_id, instance_id, plan, created_at, updated_at)
				VALUES (?, ?, ?, ?, ?)
			`, readinessRunID, instancePath, readiness, now, now); err != nil {
				t.Fatalf("seed sqlite readiness: %v", err)
			}
		}
		return
	}

	runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(),
		RunID: runID, BundleHash: bundleHash,
	})
	runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(),
		RunID: wrongRunID, BundleHash: bundleHash,
	})
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES
			($1, $2, 'template', $3::jsonb, 'active', now()),
			($4, $2, 'template', $3::jsonb, 'active', now())
	`, instancePath, notifyallchildren.ChildFlowID, config, stagedInstancePath); err != nil {
		t.Fatalf("seed postgres flow instance: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (entity_id, run_id, flow_instance, entity_type, current_state, fields, created_at, updated_at)
		VALUES
			($1::uuid, $2::uuid, $3, 'account', 'active', '{"account_id":"existing"}'::jsonb, now(), now()),
			($4::uuid, $2::uuid, $5, 'account', 'active', '{"account_id":"current"}'::jsonb, now(), now())
	`, entityID, runID, instancePath, stagedEntityID, stagedInstancePath); err != nil {
		t.Fatalf("seed postgres entity state: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instance_runtime_readiness (run_id, instance_id, plan, created_at, updated_at)
			VALUES ($1::uuid, $2, $3::jsonb, now(), now())
		`, runID, stagedInstancePath, stagedReadiness); err != nil {
		t.Fatalf("seed postgres staged-owner readiness: %v", err)
	}
	if readiness != "" {
		readinessRunID := runID
		if readinessOnWrongRun {
			readinessRunID = wrongRunID
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO flow_instance_runtime_readiness (run_id, instance_id, plan, created_at, updated_at)
			VALUES ($1::uuid, $2, $3::jsonb, now(), now())
		`, readinessRunID, instancePath, readiness); err != nil {
			t.Fatalf("seed postgres readiness: %v", err)
		}
	}
}

func exactFlowInstanceDescriptorReadinessJSON(
	t *testing.T,
	runID string,
	bundleHash string,
	workflowVersion string,
	templateID string,
	instancePath string,
	entityID string,
) string {
	t.Helper()
	plan, err := (runtimepipeline.DynamicFlowRuntimeReadinessPlan{
		Identity: runtimeflowidentity.Instance{
			TemplateID: templateID, ScopeKey: templateID,
			InstanceID: runtimeflowidentity.LogicalInstanceID(instancePath), InstancePath: instancePath,
			EntityID: entityID, HasStoredPath: true,
		},
		RunID: runID, BundleHash: bundleHash, WorkflowVersion: workflowVersion, ExecutionMode: "live",
	}).Normalized()
	if err != nil {
		t.Fatalf("normalize exact descriptor readiness: %v", err)
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal exact descriptor readiness: %v", err)
	}
	return string(raw)
}
