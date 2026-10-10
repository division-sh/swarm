package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"
	"strings"
	"testing"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/testutil"
	runtimepipelinefixture "github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture"
	"github.com/google/uuid"
)

type flowRouteTestExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

type flowRouteTopologyTestStore interface {
	ReplaceFlowInstanceRouteTopology(context.Context, []runtimebus.FlowInstanceRouteRecordSet) (runtimebus.FlowInstanceRouteTopologyResult, error)
	ListFlowInstanceRouteRecords(context.Context, runtimeflowidentity.RunScopedFlowInstance) ([]runtimebus.FlowInstanceRouteRecord, error)
}

const flowRouteTestRunID = "12121212-1212-4212-8212-121212121212"

func flowRouteTestIdentity(route runtimeflowidentity.Route) runtimeflowidentity.RunScopedFlowInstance {
	return runtimeflowidentity.RunScopedFlowInstance{RunID: flowRouteTestRunID, Route: route}
}

func seedFlowRouteTestRun(t *testing.T, ctx context.Context, exec flowRouteTestExecutor, postgres bool) context.Context {
	t.Helper()
	runID := flowRouteTestRunID
	if postgres {
		switch selected := exec.(type) {
		case *sql.DB:
			requireRunningPostgresRunForTest(t, ctx, selected, runID, time.Now().UTC())
		default:
			t.Fatalf("seed flow route run requires PostgreSQL lifecycle owner, got %T", exec)
		}
	} else {
		now := time.Now().UTC()
		switch selected := exec.(type) {
		case *sql.DB:
			requireRunningSQLiteRunForTest(t, ctx, selected, runID, now)
		default:
			t.Fatalf("seed flow route run requires SQLite lifecycle owner, got %T", exec)
		}
	}
	return runtimecorrelation.WithRunID(ctx, runID)
}

func seedFlowRouteTestEntities(t *testing.T, ctx context.Context, exec flowRouteTestExecutor, postgres bool, flowInstances ...string) {
	t.Helper()
	for _, flowInstance := range flowInstances {
		if postgres {
			if _, err := exec.ExecContext(ctx, `INSERT INTO entity_state (entity_id, run_id, flow_instance, entity_type, current_state, fields, created_at, updated_at) VALUES ($1::uuid, $2::uuid, $3, 'flow-route-test', 'active', '{}'::jsonb, now(), now())`, runtimeflowidentity.EntityID(flowInstance), flowRouteTestRunID, flowInstance); err != nil {
				t.Fatalf("seed flow route entity %s: %v", flowInstance, err)
			}
			continue
		}
		now := time.Now().UTC()
		if _, err := exec.ExecContext(ctx, `INSERT INTO entity_state (entity_id, run_id, flow_instance, entity_type, current_state, fields, created_at, updated_at) VALUES (?, ?, ?, 'flow-route-test', 'active', '{}', ?, ?)`, runtimeflowidentity.EntityID(flowInstance), flowRouteTestRunID, flowInstance, now, now); err != nil {
			t.Fatalf("seed sqlite flow route entity %s: %v", flowInstance, err)
		}
	}
}

func seedFlowRouteHeaderFixture(t *testing.T, ctx context.Context, exec flowRouteTestExecutor, runID, path, flow, mode, status, entityID, entityType string) {
	t.Helper()
	at := time.Now().UTC()
	seedWorkflowHeaderProjectionFixture(t, ctx, exec, runID, entityID, path, flow, entityType, "active", "{}", at)
	payload, err := runtimepipeline.WorkflowInstanceHeaderPayloadForRoute(runtimeflowidentity.RouteForInstancePath(path), "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	config, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var terminatedAt any
	if status == "terminated" {
		terminatedAt = at
	}
	if _, err := exec.ExecContext(ctx, `UPDATE flow_instances SET mode=$1, status=$2, terminated_at=$3, config=$4 WHERE run_id=$5 AND instance_path=$6`, mode, status, terminatedAt, string(config), runID, path); err != nil {
		t.Fatalf("seed flow-route header controls: %v", err)
	}
}

func ensureFlowInstanceRouteTables(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS flow_instances (
			run_id UUID NOT NULL,
			instance_path TEXT NOT NULL,
			flow_template TEXT NOT NULL DEFAULT '',
			mode TEXT NOT NULL DEFAULT 'template',
			config JSONB NOT NULL DEFAULT '{}'::jsonb,
			status TEXT NOT NULL DEFAULT 'active',
			terminated_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (run_id, instance_path)
		)
	`); err != nil {
		t.Fatalf("ensure flow_instances table: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS entity_state (
			entity_id UUID PRIMARY KEY,
			run_id UUID NOT NULL,
			flow_instance TEXT NOT NULL,
			entity_type TEXT NOT NULL DEFAULT '',
			current_state TEXT NOT NULL DEFAULT '',
			fields JSONB NOT NULL DEFAULT '{}'::jsonb,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`); err != nil {
		t.Fatalf("ensure entity_state table: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS routing_rules (
			rule_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			event_pattern TEXT NOT NULL,
			subscriber_type TEXT NOT NULL,
			subscriber_id TEXT NOT NULL,
			flow_instance TEXT,
			source_flow TEXT,
			is_wildcard BOOLEAN NOT NULL DEFAULT FALSE,
			is_materialized BOOLEAN NOT NULL DEFAULT FALSE,
			materialized_from UUID,
			status TEXT NOT NULL DEFAULT 'active',
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`); err != nil {
		t.Fatalf("ensure routing_rules table: %v", err)
	}
}

func TestPostgresStoreReplaceFlowInstanceRouteTopologyIsAtomic(t *testing.T) {
	ctx := testAuthorActivityContext()
	_, db, _ := testutil.StartPostgres(t)
	selected := admitTestPostgresStore(t, db)
	ensureFlowInstanceRouteTables(t, ctx, db)
	testFlowInstanceRouteTopologyAtomicity(t, ctx, db, selected, true)
}

func TestSQLiteRuntimeStoreReplaceFlowInstanceRouteTopologyIsAtomic(t *testing.T) {
	ctx := testAuthorActivityContext()
	selected := newBootstrappedSQLiteRuntimeStoreForTest(t)
	testFlowInstanceRouteTopologyAtomicity(t, ctx, selected.backend.ConstructionHandle(), selected, false)
}

func TestFlowInstanceRouteTopologyReplacementPreservesOlderObserverSources(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, observerFirst := range []bool{false, true} {
			name := backend + "/producers_first"
			if observerFirst {
				name = backend + "/observer_first"
			}
			t.Run(name, func(t *testing.T) {
				ctx := testAuthorActivityContext()
				var db *sql.DB
				var selected flowRouteTopologyTestStore
				postgres := backend == "postgres"
				if postgres {
					_, db, _ = testutil.StartPostgres(t)
					selected = admitTestPostgresStore(t, db)
					ensureFlowInstanceRouteTables(t, ctx, db)
				} else {
					store := newBootstrappedSQLiteRuntimeStoreForTest(t)
					selected = store
					db = store.backend.ConstructionHandle()
				}
				ctx = seedFlowRouteTestRun(t, ctx, db, postgres)
				observer := flowRouteTestIdentity(runtimeflowidentity.DeriveRoute("review", "observer"))
				entityID := runtimeflowidentity.EntityID(observer.Route.InstancePath)
				if postgres {
					if _, err := db.ExecContext(ctx, `INSERT INTO flow_instances (run_id, instance_path, entity_id, entity_type, flow_template, mode, config, status, stage_defined, current_state, entered_state_at, created_at, updated_at, gates, bookkeeping, accumulator, revision) VALUES ($1::uuid, $2, $3::uuid, 'flow-route-test', 'review', 'template', '{}'::jsonb, 'active', TRUE, 'active', NOW(), NOW(), NOW(), '{}', '{}', '{}', 1)`, flowRouteTestRunID, observer.Route.InstancePath, entityID); err != nil {
						t.Fatal(err)
					}
				} else if _, err := db.ExecContext(ctx, `INSERT INTO flow_instances (run_id, instance_path, entity_id, entity_type, flow_template, mode, config, status, stage_defined, current_state, entered_state_at, created_at, updated_at, gates, bookkeeping, accumulator, revision) VALUES ($1, $2, $3, 'flow-route-test', 'review', 'template', '{}', 'active', TRUE, 'active', $4, $4, $4, '{}', '{}', '{}', 1)`, flowRouteTestRunID, observer.Route.InstancePath, entityID, time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
				if _, err := db.ExecContext(ctx, `INSERT INTO entity_state (entity_id, run_id, flow_instance, entity_type, current_state, fields, created_at, updated_at) VALUES ($1, $2, $3, 'flow-route-test', 'active', '{}', $4, $4)`, entityID, flowRouteTestRunID, observer.Route.InstancePath, time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
				patterns := []string{"producer/old-a/work.ready", "producer/old-b/work.ready", "other/old/other.ready"}
				replace := func(patterns []string) {
					t.Helper()
					routes := make([]runtimebus.FlowInstanceRouteRecord, 0, len(patterns))
					for _, pattern := range patterns {
						routes = append(routes, runtimebus.FlowInstanceRouteRecord{
							Identity: observer, EventPattern: pattern, SubscriberType: "node", SubscriberID: "observer-node", SourceFlow: "review",
						})
					}
					result, err := selected.ReplaceFlowInstanceRouteTopology(ctx, []runtimebus.FlowInstanceRouteRecordSet{{Identity: observer, Routes: routes}})
					if err != nil || !result.Acknowledged {
						t.Fatalf("replace complete observer set: result=%#v err=%v", result, err)
					}
				}
				if observerFirst {
					replace(nil)
				}
				replace(patterns)
				replace(append(patterns, "producer/new/work.ready"))
				got, err := selected.ListFlowInstanceRouteRecords(ctx, observer)
				if err != nil {
					t.Fatal(err)
				}
				gotPatterns := make([]string, 0, len(got))
				for _, route := range got {
					gotPatterns = append(gotPatterns, route.EventPattern)
				}
				sort.Strings(gotPatterns)
				want := []string{"other/old/other.ready", "producer/new/work.ready", "producer/old-a/work.ready", "producer/old-b/work.ready"}
				if strings.Join(gotPatterns, ",") != strings.Join(want, ",") {
					t.Fatalf("observer replacement lost valid routes: got=%#v want=%#v", gotPatterns, want)
				}
			})
		}
	}
}

func testFlowInstanceRouteTopologyAtomicity(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	selected flowRouteTopologyTestStore,
	postgres bool,
) {
	t.Helper()
	identities := []runtimeflowidentity.RunScopedFlowInstance{
		flowRouteTestIdentity(runtimeflowidentity.DeriveRoute("review", "topology-a")),
		flowRouteTestIdentity(runtimeflowidentity.DeriveRoute("review", "topology-b")),
	}
	ctx = seedFlowRouteTestRun(t, ctx, db, postgres)
	for _, identity := range identities {
		seedFlowRouteHeaderFixture(t, ctx, db, flowRouteTestRunID, identity.Route.InstancePath, "review", "template", "active", runtimeflowidentity.EntityID(identity.Route.InstancePath), "flow-route-test")
	}
	seedFlowRouteTestEntities(t, ctx, db, postgres, identities[0].Route.InstancePath, identities[1].Route.InstancePath)

	initial := flowRouteTopologySets(identities, "initial")
	replacement := flowRouteTopologySets(identities, "replacement")
	if committed, err := selected.ReplaceFlowInstanceRouteTopology(ctx, initial); err != nil || !committed.Acknowledged {
		t.Fatalf("seed route topology: committed=%+v err=%v", committed, err)
	}
	if committed, err := selected.ReplaceFlowInstanceRouteTopology(ctx, initial); err != nil || !committed.Acknowledged {
		t.Fatalf("exact no-op topology must still acknowledge durable comparison: committed=%+v err=%v", committed, err)
	}
	assertFlowRouteTopologySubscribers(t, ctx, selected, identities, "initial")
	// Admission is reusable only for the exact run within this transaction.
	// A later missing run must still fail and roll back earlier route owners.
	foreign := flowRouteTopologySets(identities[:1], "foreign")
	foreign[0].Identity.RunID = uuid.NewString()
	foreign[0].Routes[0].Identity = foreign[0].Identity
	mixed := append(append([]runtimebus.FlowInstanceRouteRecordSet(nil), replacement...), foreign...)
	if committed, err := selected.ReplaceFlowInstanceRouteTopology(ctx, mixed); err == nil || committed.Acknowledged {
		t.Fatal("route topology borrowed another run's active admission")
	}
	assertFlowRouteTopologySubscribers(t, ctx, selected, identities, "initial")

	removeFailure := installFlowRouteTopologySecondOwnerFailure(t, ctx, db, postgres)
	if committed, err := selected.ReplaceFlowInstanceRouteTopology(ctx, replacement); err == nil || committed.Acknowledged {
		t.Fatal("replace route topology with second-owner failure unexpectedly succeeded")
	}
	removeFailure()
	assertFlowRouteTopologySubscribers(t, ctx, selected, identities, "initial")

	if committed, err := selected.ReplaceFlowInstanceRouteTopology(ctx, replacement); err != nil || !committed.Acknowledged {
		t.Fatalf("commit route topology replacement: committed=%+v err=%v", committed, err)
	}
	assertFlowRouteTopologySubscribers(t, ctx, selected, identities, "replacement")
}

func installFlowRouteTopologySecondOwnerFailure(t *testing.T, ctx context.Context, db *sql.DB, postgres bool) func() {
	t.Helper()
	if postgres {
		if _, err := db.ExecContext(ctx, `
			CREATE OR REPLACE FUNCTION reject_flow_route_topology_second_owner()
			RETURNS trigger AS $$
			BEGIN
				IF NEW.subscriber_id = 'replacement-b' THEN
					RAISE EXCEPTION 'reject second topology owner';
				END IF;
				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql;
			CREATE TRIGGER reject_flow_route_topology_second_owner
			BEFORE INSERT OR UPDATE ON routing_rules
			FOR EACH ROW EXECUTE FUNCTION reject_flow_route_topology_second_owner();
		`); err != nil {
			t.Fatalf("install postgres route topology failure: %v", err)
		}
		return func() {
			t.Helper()
			if _, err := db.ExecContext(ctx, `
				DROP TRIGGER reject_flow_route_topology_second_owner ON routing_rules;
				DROP FUNCTION reject_flow_route_topology_second_owner();
			`); err != nil {
				t.Fatalf("remove postgres route topology failure: %v", err)
			}
		}
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TRIGGER reject_flow_route_topology_second_owner
		BEFORE INSERT ON routing_rules
		WHEN NEW.subscriber_id = 'replacement-b'
		BEGIN
			SELECT RAISE(ABORT, 'reject second topology owner');
		END
	`); err != nil {
		t.Fatalf("install sqlite route topology failure: %v", err)
	}
	return func() {
		t.Helper()
		if _, err := db.ExecContext(ctx, `DROP TRIGGER reject_flow_route_topology_second_owner`); err != nil {
			t.Fatalf("remove sqlite route topology failure: %v", err)
		}
	}
}

func flowRouteTopologySets(identities []runtimeflowidentity.RunScopedFlowInstance, subscriberPrefix string) []runtimebus.FlowInstanceRouteRecordSet {
	sets := make([]runtimebus.FlowInstanceRouteRecordSet, 0, len(identities))
	for index, identity := range identities {
		sets = append(sets, runtimebus.FlowInstanceRouteRecordSet{
			Identity: identity,
			Routes: []runtimebus.FlowInstanceRouteRecord{{
				Identity:       identity,
				EventPattern:   identity.Route.InstancePath + "/task.started",
				SubscriberType: "node",
				SubscriberID:   subscriberPrefix + "-" + string(rune('a'+index)),
				SourceFlow:     identity.Route.ScopeKey,
			}},
		})
	}
	return sets
}

func assertFlowRouteTopologySubscribers(
	t *testing.T,
	ctx context.Context,
	selected flowRouteTopologyTestStore,
	identities []runtimeflowidentity.RunScopedFlowInstance,
	wantPrefix string,
) {
	t.Helper()
	for index, identity := range identities {
		routes, err := selected.ListFlowInstanceRouteRecords(ctx, identity)
		if err != nil {
			t.Fatalf("list route topology owner %s: %v", identity.Route.InstancePath, err)
		}
		want := wantPrefix + "-" + string(rune('a'+index))
		if len(routes) != 1 || routes[0].SubscriberID != want {
			t.Fatalf("route topology owner %s = %#v, want subscriber %q", identity.Route.InstancePath, routes, want)
		}
	}
}

func TestPostgresStoreListActiveFlowInstanceDescriptorsFiltersToActiveTemplates(t *testing.T) {
	const runID = "11111111-1111-4111-8111-111111111111"
	const entityID = "22222222-2222-4222-8222-222222222222"
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ensureFlowInstanceRouteTables(t, ctx, db)
	const foreignRunID = "44444444-4444-4444-8444-444444444444"
	requireRunFixtureForTest(t, ctx, pg, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})
	requireRunFixtureForTest(t, ctx, pg, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: foreignRunID})

	seedFlowRouteHeaderFixture(t, ctx, db, runID, "component-scaffold/active", "component-scaffold", "template", "active", entityID, "component")
	seedFlowRouteHeaderFixture(t, ctx, db, runID, "component-scaffold/terminated", "component-scaffold", "template", "terminated", runtimeflowidentity.EntityID("component-scaffold/terminated"), "component")
	seedFlowRouteHeaderFixture(t, ctx, db, runID, "service-owner", "service-owner", "static", "active", runtimeflowidentity.EntityID("service-owner"), "component")
	seedFlowRouteHeaderFixture(t, ctx, db, foreignRunID, "component-scaffold/active", "component-scaffold", "template", "active", "33333333-3333-4333-8333-333333333333", "component")
	readinessOwner, err := (runtimepipeline.DynamicFlowRuntimeReadinessPlan{
		Identity: runtimeflowidentity.Instance{
			TemplateID: "component-scaffold", ScopeKey: "component-scaffold", InstanceID: "active",
			InstancePath: "component-scaffold/active", EntityID: entityID, HasStoredPath: true,
		},
		RunID: runID, BundleHash: authorActivityTestBundleHash,
		WorkflowVersion: "1.0.0", ExecutionMode: "live",
	}).Normalized()
	if err != nil {
		t.Fatalf("normalize readiness plan: %v", err)
	}
	readinessPlan, err := json.Marshal(readinessOwner)
	if err != nil {
		t.Fatalf("marshal readiness plan: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instance_runtime_readiness (run_id, instance_path, plan, plan_hash, created_at, updated_at)
		VALUES ($1::uuid, 'component-scaffold/active', $2::jsonb, $3, NOW(), NOW())
	`, runID, readinessPlan, readinessPlanFixtureHash(t, string(readinessPlan))); err != nil {
		t.Fatalf("seed flow-instance readiness: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (entity_id, run_id, flow_instance, entity_type, current_state, fields, created_at, updated_at)
		VALUES
			($2::uuid, $1::uuid, 'component-scaffold/active', 'component', 'ready', '{"vertical_id":"v-active","weight":1.1234567}'::jsonb, NOW(), NOW()),
			('33333333-3333-4333-8333-333333333333', $3::uuid, 'component-scaffold/active', 'component', 'ready', '{"vertical_id":"wrong-run"}'::jsonb, NOW() + INTERVAL '1 minute', NOW() + INTERVAL '1 minute')
	`, runID, entityID, foreignRunID); err != nil {
		t.Fatalf("seed entity_state: %v", err)
	}

	descriptors, err := pg.ListActiveFlowInstanceDescriptors(ctx, runID)
	if err != nil {
		t.Fatalf("ListActiveFlowInstanceDescriptors: %v", err)
	}
	if len(descriptors) != 1 {
		t.Fatalf("descriptors = %#v, want exactly active template descriptor", descriptors)
	}
	got := descriptors[0]
	if got.FlowInstance != "component-scaffold/active" {
		t.Fatalf("FlowInstance = %q, want component-scaffold/active", got.FlowInstance)
	}
	if got.InstanceID != "active" {
		t.Fatalf("InstanceID = %q, want active", got.InstanceID)
	}
	if got.EntityID != entityID {
		t.Fatalf("EntityID = %q, want exact readiness entity id", got.EntityID)
	}
	if got.FlowTemplate != "component-scaffold" {
		t.Fatalf("FlowTemplate = %q, want component-scaffold", got.FlowTemplate)
	}
	if got.BundleHash != authorActivityTestBundleHash ||
		got.WorkflowVersion != "1.0.0" {
		t.Fatalf("semantic source = %#v, want exact run bundle and workflow version", got)
	}
	if got.AddressFields["entity.vertical_id"] != "v-active" {
		t.Fatalf("AddressFields[entity.vertical_id] = %q, want v-active", got.AddressFields["entity.vertical_id"])
	}
	if got.AddressFields["entity.weight"] != "1.1234567" {
		t.Fatalf("AddressFields[entity.weight] = %q, want 1.1234567", got.AddressFields["entity.weight"])
	}
}

func TestPostgresStoreListActiveFlowInstanceDescriptorsRejectsUnscopedCensus(t *testing.T) {
	ctx := testAuthorActivityContext()
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ensureFlowInstanceRouteTables(t, ctx, db)

	if descriptors, err := pg.ListActiveFlowInstanceDescriptors(ctx, ""); err == nil || !strings.Contains(err.Error(), "exact run_id") {
		t.Fatalf("unscoped descriptor census: descriptors=%#v err=%v, want exact run scope rejection", descriptors, err)
	}
}

func TestPostgresStoreListActiveFlowInstanceDescriptorsDoesNotReadAmbientTransaction(t *testing.T) {
	const runID = "11111111-1111-4111-8111-111111111111"
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ensureFlowInstanceRouteTables(t, ctx, db)
	requireDefaultSourceArtifactForTest(t, ctx, pg)
	requireRunFixtureForTest(t, ctx, pg, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	seedFlowRouteHeaderFixture(t, ctx, tx, runID, "component-scaffold/uncommitted", "component-scaffold", "template", "active", runtimeflowidentity.EntityID("component-scaffold/uncommitted"), "component")
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO flow_instance_runtime_readiness (run_id, instance_path, plan, plan_hash, created_at, updated_at)
		VALUES ($1::uuid, 'component-scaffold/uncommitted', '{"workflow_version":"1.0.0"}'::jsonb, $2, NOW(), NOW())
	`, runID, readinessPlanFixtureHash(t, `{"workflow_version":"1.0.0"}`)); err != nil {
		t.Fatalf("seed readiness in tx: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO entity_state (entity_id, run_id, flow_instance, entity_type, current_state, fields, created_at, updated_at)
		VALUES ('22222222-2222-4222-8222-222222222222'::uuid, $1::uuid, 'component-scaffold/uncommitted', 'component', 'ready', '{}'::jsonb, NOW(), NOW())
	`, runID); err != nil {
		t.Fatalf("seed entity state in tx: %v", err)
	}

	descriptors, err := pg.ListActiveFlowInstanceDescriptors(runtimepipelinefixture.WithSQLTx(ctx, tx), runID)
	if err != nil {
		t.Fatalf("ListActiveFlowInstanceDescriptors: %v", err)
	}
	if len(descriptors) != 0 {
		t.Fatalf("descriptors = %#v, want no ambient uncommitted rows", descriptors)
	}
}
