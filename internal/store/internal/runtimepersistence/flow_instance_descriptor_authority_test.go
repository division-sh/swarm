package runtimepersistence_test

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
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/notifyallchildren"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

type flowInstanceDescriptorAuthorityStore interface {
	externalStoreTestDurableEventBusStore
	runtimebus.FlowInstanceRouteSetPersistence
	runtimebus.FlowInstanceRouteRecordReader
	runtimebus.ActiveFlowInstanceDescriptorLister
}

type dynamicFlowSourceProjectionStore interface {
	runtimerunlifecycle.OperationOwner
	InspectDynamicFlowRuntimeReadinessForSource(context.Context, runtimecorrelation.BundleSourceFact) (runtimepipeline.DynamicFlowRuntimeReadinessProjection, error)
	InspectDynamicFlowRuntimeReadinessForRun(context.Context, string, runtimecorrelation.BundleSourceFact) ([]runtimepipeline.DynamicFlowRuntimeReadiness, error)
	LoadDynamicFlowRuntimeReadiness(context.Context, string, runtimeflowidentity.Route) (runtimepipeline.DynamicFlowRuntimeReadiness, bool, error)
	ReconcileDynamicFlowRuntimeReadinessPlan(context.Context, runtimepipeline.DynamicFlowRuntimeReadiness, runtimepipeline.DynamicFlowRuntimeReadinessPlan, time.Time) (bool, error)
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
			sourceA := mustExternalStoreTestBundleSourceFact()
			hashA, kindA := sourceA.StorageValues()
			hashB := "bundle-v1:sha256:" + strings.Repeat("b", 64)
			sourceB, err := runtimecorrelation.NewEphemeralBundleSourceFact(hashB)
			if err != nil {
				t.Fatal(err)
			}
			_, kindB := sourceB.StorageValues()
			runA, runB := uuid.NewString(), uuid.NewString()
			ctx := testAuthorActivityContext()
			fixtureA := runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runA, BundleHash: hashA, BundleSource: kindA}
			fixtureB := runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runB, BundleHash: hashB, BundleSource: kindB}
			if sqlite {
				runlifecyclefixture.RequireSQLite(t, ctx, db, fixtureA)
				runlifecyclefixture.RequireSQLite(t, ctx, db, fixtureB)
			} else {
				runlifecyclefixture.RequirePostgres(t, ctx, db, fixtureA)
				runlifecyclefixture.RequirePostgres(t, ctx, db, fixtureB)
			}
			seedExactFlowInstanceDescriptorOwner(t, db, sqlite, runA, uuid.NewString(), "account/a", hashA, kindA)
			seedExactFlowInstanceDescriptorOwner(t, db, sqlite, runB, uuid.NewString(), "account/b", hashB, kindB)
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
			current := mustExternalStoreTestBundleSourceFact()
			currentHash, currentKind := current.StorageValues()
			predecessor, err := runtimecorrelation.NewPersistedBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("c", 64))
			if err != nil {
				t.Fatal(err)
			}
			predecessorHash, predecessorKind := predecessor.StorageValues()
			foreign, err := runtimecorrelation.NewEphemeralBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("d", 64))
			if err != nil {
				t.Fatal(err)
			}
			foreignHash, foreignKind := foreign.StorageValues()
			ctx := testAuthorActivityContext()
			type seeded struct {
				runID, path, planHash, planKind string
				complete                        bool
			}
			rows := []seeded{
				{uuid.NewString(), "account/current-complete", currentHash, currentKind, true},
				{uuid.NewString(), "account/current-pending", currentHash, currentKind, false},
				{uuid.NewString(), "account/transition-complete", predecessorHash, predecessorKind, true},
				{uuid.NewString(), "account/transition-pending", predecessorHash, predecessorKind, false},
			}
			for _, row := range rows {
				requireReadinessRun(t, ctx, db, sqlite, row.runID, currentHash, currentKind)
				seedExactFlowInstanceDescriptorOwner(t, db, sqlite, row.runID, uuid.NewString(), row.path, row.planHash, row.planKind)
				if row.complete {
					setReadinessCoordinate(t, db, sqlite, row.runID, row.path, "topology_ready_at", time.Now().UTC())
				}
			}
			foreignRun := uuid.NewString()
			requireReadinessRun(t, ctx, db, sqlite, foreignRun, foreignHash, foreignKind)
			seedExactFlowInstanceDescriptorOwner(t, db, sqlite, foreignRun, uuid.NewString(), "account/foreign-malformed", foreignHash, foreignKind)
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
			source := mustExternalStoreTestBundleSourceFact()
			bundleHash, bundleSource := source.StorageValues()
			otherSource, err := runtimecorrelation.NewEphemeralBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("e", 64))
			if err != nil {
				t.Fatal(err)
			}
			for _, race := range []string{"plan", "source", "topology", "creation", "run_status", "instance_status", "terminated_at"} {
				t.Run(race, func(t *testing.T) {
					runID := uuid.NewString()
					path := "account/guard-" + race + "-" + uuid.NewString()
					ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)
					requireReadinessRun(t, ctx, db, sqlite, runID, bundleHash, bundleSource)
					seedExactFlowInstanceDescriptorOwner(t, db, sqlite, runID, uuid.NewString(), path, bundleHash, bundleSource)
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
						otherHash, otherKind := otherSource.StorageValues()
						setReadinessRunSource(t, selected, callCtx, runID, otherSource)
						desired.BundleHash, desired.BundleSource = otherHash, otherKind
						callCtx = runtimecorrelation.WithBundleSourceFact(callCtx, otherSource)
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
					changed, err := selected.ReconcileDynamicFlowRuntimeReadinessPlan(callCtx, observed, desired, time.Now().UTC())
					if changed || !runtimepipeline.IsDynamicFlowRuntimeReadinessObservationConflict(err) {
						t.Fatalf("stale observation changed=%v err=%v", changed, err)
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
			source := mustExternalStoreTestBundleSourceFact()
			bundleHash, bundleSource := source.StorageValues()
			ctxA := runtimecorrelation.WithRunID(testAuthorActivityContext(), runA)
			ctxB := runtimecorrelation.WithRunID(testAuthorActivityContext(), runB)
			if sqlite {
				runlifecyclefixture.RequireSQLite(t, ctxA, db, runlifecyclefixture.Fixture{
					Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runA,
					BundleHash: bundleHash, BundleSource: bundleSource,
				})
				runlifecyclefixture.RequireSQLite(t, ctxB, db, runlifecyclefixture.Fixture{
					Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runB,
					BundleHash: bundleHash, BundleSource: bundleSource,
				})
			} else {
				runlifecyclefixture.RequirePostgres(t, ctxA, db, runlifecyclefixture.Fixture{
					Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runA,
					BundleHash: bundleHash, BundleSource: bundleSource,
				})
				runlifecyclefixture.RequirePostgres(t, ctxB, db, runlifecyclefixture.Fixture{
					Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runB,
					BundleHash: bundleHash, BundleSource: bundleSource,
				})
			}

			seedExactFlowInstanceDescriptorOwner(t, db, sqlite, runA, entityA, "account/a", bundleHash, bundleSource)
			seedExactFlowInstanceDescriptorOwner(t, db, sqlite, runB, entityB, "account/b", bundleHash, bundleSource)

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
	runID, entityID, instancePath, bundleHash, bundleSource string,
) {
	t.Helper()
	readiness := exactFlowInstanceDescriptorReadinessJSON(
		t, runID, bundleHash, bundleSource, "account", instancePath, entityID,
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

func requireReadinessRun(t *testing.T, ctx context.Context, db *sql.DB, sqlite bool, runID, bundleHash, bundleSource string) {
	t.Helper()
	fixture := runlifecyclefixture.Fixture{
		Origin:       runlifecyclefixture.ScenarioSetupOrigin(),
		RunID:        runID,
		BundleHash:   bundleHash,
		BundleSource: bundleSource,
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

func setReadinessRunSource(t *testing.T, owner runtimerunlifecycle.OperationOwner, ctx context.Context, runID string, source runtimecorrelation.BundleSourceFact) {
	t.Helper()
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
					source := notifyallchildren.LoadSource(t, notifyallchildren.Options{})
					planBundleHash, bundleSource := mustExternalStoreTestBundleSourceFact().StorageValues()
					bundleHash := planBundleHash
					if tc.foreignBundle {
						bundleHash = "bundle-v1:sha256:" + strings.Repeat("f", 64)
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
						bundleSource,
						planBundleHash,
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

					eventBus, err := newStoreTestEventBus(t, selected, runtimebus.EventBusOptions{ContractBundle: source})
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
	bundleSource string,
	planBundleHash string,
	readiness string,
	readinessOnWrongRun bool,
) {
	t.Helper()
	const instancePath = "account/existing"
	const stagedInstancePath = "account/current"
	const config = `{"workflow_version":"1.0.0"}`
	entityID := uuid.NewString()
	stagedEntityID := uuid.NewString()
	stagedReadiness := exactFlowInstanceDescriptorReadinessJSON(t, runID, planBundleHash, bundleSource, notifyallchildren.ChildFlowID, stagedInstancePath, stagedEntityID)
	if readiness == "exact" {
		readiness = exactFlowInstanceDescriptorReadinessJSON(t, runID, planBundleHash, bundleSource, notifyallchildren.ChildFlowID, instancePath, entityID)
	}
	if sqlite {
		now := time.Now().UTC()
		runlifecyclefixture.RequireSQLite(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(),
			RunID: runID, StartedAt: now, BundleHash: bundleHash, BundleSource: bundleSource,
		})
		runlifecyclefixture.RequireSQLite(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(),
			RunID: wrongRunID, StartedAt: now, BundleHash: bundleHash, BundleSource: bundleSource,
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
		RunID: runID, BundleHash: bundleHash, BundleSource: bundleSource,
	})
	runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(),
		RunID: wrongRunID, BundleHash: bundleHash, BundleSource: bundleSource,
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
	bundleSource string,
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
		RunID: runID, BundleHash: bundleHash, BundleSource: bundleSource, WorkflowVersion: "1.0.0", ExecutionMode: "live",
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
