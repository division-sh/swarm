package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
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
	ReplaceFlowInstanceRouteTopology(context.Context, []runtimebus.FlowInstanceRouteRecordSet) error
	ListFlowInstanceRouteRecords(context.Context, runtimeflowidentity.Route) ([]runtimebus.FlowInstanceRouteRecord, error)
}

func seedFlowRouteTestAuthority(t *testing.T, ctx context.Context, exec flowRouteTestExecutor, postgres bool, flowInstances ...string) context.Context {
	t.Helper()
	runID := uuid.NewString()
	if postgres {
		switch selected := exec.(type) {
		case *sql.DB:
			requireRunningPostgresRunForTest(t, ctx, selected, runID, time.Now().UTC())
		case *sql.Tx:
			requirePostgresRunFixtureInRawTxForTest(t, ctx, selected, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})
		default:
			t.Fatalf("seed flow route run requires PostgreSQL lifecycle owner, got %T", exec)
		}
		for _, flowInstance := range flowInstances {
			if _, err := exec.ExecContext(ctx, `INSERT INTO entity_state (entity_id, run_id, flow_instance, entity_type, current_state, fields, created_at, updated_at) VALUES ($1::uuid, $2::uuid, $3, 'flow-route-test', 'active', '{}'::jsonb, now(), now())`, uuid.NewString(), runID, flowInstance); err != nil {
				t.Fatalf("seed flow route entity %s: %v", flowInstance, err)
			}
		}
	} else {
		now := time.Now().UTC()
		switch selected := exec.(type) {
		case *sql.DB:
			requireRunningSQLiteRunForTest(t, ctx, selected, runID, now)
		case *sql.Tx:
			requireSQLiteRunFixtureInRawTxForTest(t, ctx, selected, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, StartedAt: now})
		default:
			t.Fatalf("seed flow route run requires SQLite lifecycle owner, got %T", exec)
		}
		for _, flowInstance := range flowInstances {
			if _, err := exec.ExecContext(ctx, `INSERT INTO entity_state (entity_id, run_id, flow_instance, entity_type, current_state, fields, created_at, updated_at) VALUES (?, ?, ?, 'flow-route-test', 'active', '{}', ?, ?)`, uuid.NewString(), runID, flowInstance, now, now); err != nil {
				t.Fatalf("seed sqlite flow route entity %s: %v", flowInstance, err)
			}
		}
	}
	return runtimecorrelation.WithRunID(ctx, runID)
}

func ensureFlowInstanceRouteTables(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS flow_instances (
			instance_id TEXT PRIMARY KEY,
			flow_template TEXT NOT NULL DEFAULT '',
			mode TEXT NOT NULL DEFAULT 'template',
			config JSONB NOT NULL DEFAULT '{}'::jsonb,
			status TEXT NOT NULL DEFAULT 'active',
			terminated_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
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

func TestPostgresStoreFlowInstanceRoutes(t *testing.T) {
	ctx := testAuthorActivityContext()
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ensureFlowInstanceRouteTables(t, ctx, db)

	route := runtimebus.FlowInstanceRouteRecord{
		Identity:       runtimeflowidentity.DeriveRoute("review", "inst-1"),
		EventPattern:   "review/inst-1/task.started",
		SubscriberType: "node",
		SubscriberID:   "reviewer-inst-1",
		SourceFlow:     "review",
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES ($1, 'review', 'template', '{}'::jsonb, 'active', NOW())
	`, route.Identity.InstancePath); err != nil {
		t.Fatalf("seed flow_instances: %v", err)
	}
	ctx = seedFlowRouteTestAuthority(t, ctx, db, true, route.Identity.InstancePath)
	if err := pg.UpsertFlowInstanceRoute(ctx, route); err != nil {
		t.Fatalf("UpsertFlowInstanceRoute: %v", err)
	}
	routes, err := pg.ListFlowInstanceRoutes(ctx)
	if err != nil {
		t.Fatalf("ListFlowInstanceRoutes: %v", err)
	}
	want := route.Identity
	if len(routes) != 1 || routes[0] != want {
		t.Fatalf("listed routes = %#v, want %#v", routes, want)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE flow_instances
		SET status = 'terminated', terminated_at = $2
		WHERE instance_id = $1
	`, route.Identity.InstancePath, time.Now().UTC()); err != nil {
		t.Fatalf("terminate flow_instance: %v", err)
	}
	if err := pg.DeleteFlowInstanceRoute(ctx, route.Identity); err != nil {
		t.Fatalf("DeleteFlowInstanceRoute: %v", err)
	}
	routes, err = pg.ListFlowInstanceRoutes(ctx)
	if err != nil {
		t.Fatalf("ListFlowInstanceRoutes after delete: %v", err)
	}
	if len(routes) != 0 {
		t.Fatalf("listed routes after delete = %#v, want none", routes)
	}
}

func TestPostgresStoreUpsertFlowInstanceRouteOwnsNamedTransaction(t *testing.T) {
	ctx := testAuthorActivityContext()
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ensureFlowInstanceRouteTables(t, ctx, db)

	route := runtimebus.FlowInstanceRouteRecord{
		Identity:       runtimeflowidentity.DeriveRoute("review", "inst-1"),
		EventPattern:   "review/inst-1/task.started",
		SubscriberType: "node",
		SubscriberID:   "reviewer-inst-1",
		SourceFlow:     "review",
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES ($1, 'review', 'template', '{}'::jsonb, 'active', NOW())
	`, route.Identity.InstancePath); err != nil {
		t.Fatalf("seed flow_instances: %v", err)
	}
	ctx = seedFlowRouteTestAuthority(t, ctx, db, true, route.Identity.InstancePath)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	txctx := runtimepipelinefixture.WithSQLTx(ctx, tx)
	if err := pg.UpsertFlowInstanceRoute(txctx, route); err != nil {
		t.Fatalf("UpsertFlowInstanceRoute in tx: %v", err)
	}
	var inTx int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM routing_rules
		WHERE flow_instance = $1
		  AND is_materialized = true
		  AND status = 'active'
	`, route.Identity.InstancePath).Scan(&inTx); err != nil {
		t.Fatalf("count routing_rules in tx: %v", err)
	}
	if inTx != 1 {
		t.Fatalf("routing_rules in tx = %d, want 1", inTx)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	var leaked int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM routing_rules
		WHERE flow_instance = $1
		  AND is_materialized = true
	`, route.Identity.InstancePath).Scan(&leaked); err != nil {
		t.Fatalf("count routing_rules after rollback: %v", err)
	}
	if leaked != 1 {
		t.Fatalf("named-operation routing rules after unrelated rollback = %d, want 1", leaked)
	}
}

func TestPostgresStoreReplaceFlowInstanceRouteRecordsIsExactAndTransactional(t *testing.T) {
	ctx := testAuthorActivityContext()
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ensureFlowInstanceRouteTables(t, ctx, db)
	identity := runtimeflowidentity.DeriveRoute("review", "inst-exact")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES ($1, 'review', 'template', '{}'::jsonb, 'active', NOW())
	`, identity.InstancePath); err != nil {
		t.Fatalf("seed flow instance: %v", err)
	}
	ctx = seedFlowRouteTestAuthority(t, ctx, db, true, identity.InstancePath)
	first := []runtimebus.FlowInstanceRouteRecord{
		{
			Identity: identity, EventPattern: "review/inst-exact/task.started",
			SubscriberType: "agent", SubscriberID: "reviewer", SourceFlow: "review",
		},
		{
			Identity: identity, EventPattern: "producer/source-1/task.done",
			SubscriberType: "node", SubscriberID: "observer", SourceFlow: "review",
		},
	}
	if err := pg.ReplaceFlowInstanceRouteRecords(ctx, identity, first); err != nil {
		t.Fatalf("ReplaceFlowInstanceRouteRecords first: %v", err)
	}
	if got, err := pg.ListFlowInstanceRouteRecords(ctx, identity); err != nil || len(got) != 2 {
		t.Fatalf("first exact route set: routes=%#v err=%v", got, err)
	}
	if err := pg.ReplaceFlowInstanceRouteRecords(ctx, identity, first[:1]); err != nil {
		t.Fatalf("ReplaceFlowInstanceRouteRecords second: %v", err)
	}
	got, err := pg.ListFlowInstanceRouteRecords(ctx, identity)
	if err != nil || len(got) != 1 || got[0].SubscriberID != "reviewer" {
		t.Fatalf("second exact route set: routes=%#v err=%v", got, err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	txctx := runtimepipelinefixture.WithSQLTx(ctx, tx)
	if err := pg.ReplaceFlowInstanceRouteRecords(txctx, identity, nil); err != nil {
		_ = tx.Rollback()
		t.Fatalf("ReplaceFlowInstanceRouteRecords rollback mutation: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	got, err = pg.ListFlowInstanceRouteRecords(ctx, identity)
	if err != nil || len(got) != 0 {
		t.Fatalf("named-operation route set after unrelated rollback: routes=%#v err=%v", got, err)
	}
}

func TestSQLiteRuntimeStoreUpsertFlowInstanceRouteOwnsNamedTransaction(t *testing.T) {
	ctx := context.Background()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	route := runtimebus.FlowInstanceRouteRecord{
		Identity:       runtimeflowidentity.DeriveRoute("review", "inst-1"),
		EventPattern:   "review/inst-1/task.started",
		SubscriberType: "node",
		SubscriberID:   "reviewer-inst-1",
		SourceFlow:     "review",
	}
	if _, err := store.backend.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES (?, 'review', 'template', '{}', 'active', ?)
	`, route.Identity.InstancePath, time.Now().UTC()); err != nil {
		t.Fatalf("seed sqlite flow_instances: %v", err)
	}
	ctx = seedFlowRouteTestAuthority(t, ctx, store.backend.ConstructionHandle(), false, route.Identity.InstancePath)
	tx, err := store.backend.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	txctx := runtimepipelinefixture.WithSQLTx(ctx, tx)
	if err := store.UpsertFlowInstanceRoute(txctx, route); err != nil {
		t.Fatalf("UpsertFlowInstanceRoute in sqlite tx: %v", err)
	}
	var inTx int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM routing_rules
		WHERE flow_instance = ? AND is_materialized = TRUE AND status = 'active'
	`, route.Identity.InstancePath).Scan(&inTx); err != nil {
		t.Fatalf("count sqlite routing_rules in tx: %v", err)
	}
	if inTx != 1 {
		t.Fatalf("sqlite routing_rules in tx = %d, want 1", inTx)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	var leaked int
	if err := store.backend.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM routing_rules
		WHERE flow_instance = ? AND is_materialized = TRUE
	`, route.Identity.InstancePath).Scan(&leaked); err != nil {
		t.Fatalf("count sqlite routing_rules after rollback: %v", err)
	}
	if leaked != 1 {
		t.Fatalf("sqlite named-operation routing rules after unrelated rollback = %d, want 1", leaked)
	}
}

func TestSQLiteRuntimeStoreReplaceFlowInstanceRouteRecordsOwnsNamedTransaction(t *testing.T) {
	ctx := context.Background()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	identity := runtimeflowidentity.DeriveRoute("review", "inst-exact")
	if _, err := store.backend.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES (?, 'review', 'template', '{}', 'active', ?)
	`, identity.InstancePath, time.Now().UTC()); err != nil {
		t.Fatalf("seed flow instance: %v", err)
	}
	ctx = seedFlowRouteTestAuthority(t, ctx, store.backend.ConstructionHandle(), false, identity.InstancePath)
	first := []runtimebus.FlowInstanceRouteRecord{
		{
			Identity: identity, EventPattern: "review/inst-exact/task.started",
			SubscriberType: "agent", SubscriberID: "reviewer", SourceFlow: "review",
		},
		{
			Identity: identity, EventPattern: "producer/source-1/task.done",
			SubscriberType: "node", SubscriberID: "observer", SourceFlow: "review",
		},
	}
	if err := store.ReplaceFlowInstanceRouteRecords(ctx, identity, first); err != nil {
		t.Fatalf("ReplaceFlowInstanceRouteRecords first: %v", err)
	}
	if got, err := store.ListFlowInstanceRouteRecords(ctx, identity); err != nil || len(got) != 2 {
		t.Fatalf("first exact route set: routes=%#v err=%v", got, err)
	}
	if err := store.ReplaceFlowInstanceRouteRecords(ctx, identity, first[:1]); err != nil {
		t.Fatalf("ReplaceFlowInstanceRouteRecords second: %v", err)
	}
	got, err := store.ListFlowInstanceRouteRecords(ctx, identity)
	if err != nil || len(got) != 1 || got[0].SubscriberID != "reviewer" {
		t.Fatalf("second exact route set: routes=%#v err=%v", got, err)
	}

	tx, err := store.backend.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	txctx := runtimepipelinefixture.WithSQLTx(ctx, tx)
	if err := store.ReplaceFlowInstanceRouteRecords(txctx, identity, nil); err != nil {
		_ = tx.Rollback()
		t.Fatalf("ReplaceFlowInstanceRouteRecords rollback mutation: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	got, err = store.ListFlowInstanceRouteRecords(ctx, identity)
	if err != nil || len(got) != 0 {
		t.Fatalf("named-operation route set after unrelated rollback: routes=%#v err=%v", got, err)
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

func testFlowInstanceRouteTopologyAtomicity(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	selected flowRouteTopologyTestStore,
	postgres bool,
) {
	t.Helper()
	identities := []runtimeflowidentity.Route{
		runtimeflowidentity.DeriveRoute("review", "topology-a"),
		runtimeflowidentity.DeriveRoute("review", "topology-b"),
	}
	for _, identity := range identities {
		if postgres {
			if _, err := db.ExecContext(ctx, `
				INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
				VALUES ($1, 'review', 'template', '{}'::jsonb, 'active', NOW())
			`, identity.InstancePath); err != nil {
				t.Fatalf("seed postgres flow instance %s: %v", identity.InstancePath, err)
			}
			continue
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
			VALUES (?, 'review', 'template', '{}', 'active', ?)
		`, identity.InstancePath, time.Now().UTC()); err != nil {
			t.Fatalf("seed sqlite flow instance %s: %v", identity.InstancePath, err)
		}
	}
	ctx = seedFlowRouteTestAuthority(t, ctx, db, postgres, identities[0].InstancePath, identities[1].InstancePath)

	initial := flowRouteTopologySets(identities, "initial")
	replacement := flowRouteTopologySets(identities, "replacement")
	if err := selected.ReplaceFlowInstanceRouteTopology(ctx, initial); err != nil {
		t.Fatalf("seed route topology: %v", err)
	}

	removeFailure := installFlowRouteTopologySecondOwnerFailure(t, ctx, db, postgres)
	if err := selected.ReplaceFlowInstanceRouteTopology(ctx, replacement); err == nil {
		t.Fatal("replace route topology with second-owner failure unexpectedly succeeded")
	}
	removeFailure()
	assertFlowRouteTopologySubscribers(t, ctx, selected, identities, "initial")

	if err := selected.ReplaceFlowInstanceRouteTopology(ctx, replacement); err != nil {
		t.Fatalf("commit route topology replacement: %v", err)
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

func flowRouteTopologySets(identities []runtimeflowidentity.Route, subscriberPrefix string) []runtimebus.FlowInstanceRouteRecordSet {
	sets := make([]runtimebus.FlowInstanceRouteRecordSet, 0, len(identities))
	for index, identity := range identities {
		sets = append(sets, runtimebus.FlowInstanceRouteRecordSet{
			Identity: identity,
			Routes: []runtimebus.FlowInstanceRouteRecord{{
				Identity:       identity,
				EventPattern:   identity.InstancePath + "/task.started",
				SubscriberType: "node",
				SubscriberID:   subscriberPrefix + "-" + string(rune('a'+index)),
				SourceFlow:     identity.ScopeKey,
			}},
		})
	}
	return sets
}

func assertFlowRouteTopologySubscribers(
	t *testing.T,
	ctx context.Context,
	selected flowRouteTopologyTestStore,
	identities []runtimeflowidentity.Route,
	wantPrefix string,
) {
	t.Helper()
	for index, identity := range identities {
		routes, err := selected.ListFlowInstanceRouteRecords(ctx, identity)
		if err != nil {
			t.Fatalf("list route topology owner %s: %v", identity.InstancePath, err)
		}
		want := wantPrefix + "-" + string(rune('a'+index))
		if len(routes) != 1 || routes[0].SubscriberID != want {
			t.Fatalf("route topology owner %s = %#v, want subscriber %q", identity.InstancePath, routes, want)
		}
	}
}

func TestPostgresStoreDeleteFlowInstanceRouteOwnsNamedTransaction(t *testing.T) {
	ctx := testAuthorActivityContext()
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ensureFlowInstanceRouteTables(t, ctx, db)

	route := runtimebus.FlowInstanceRouteRecord{
		Identity:       runtimeflowidentity.DeriveRoute("review", "inst-1"),
		EventPattern:   "review/inst-1/task.started",
		SubscriberType: "node",
		SubscriberID:   "reviewer-inst-1",
		SourceFlow:     "review",
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, terminated_at, created_at)
		VALUES ($1, 'review', 'template', '{}'::jsonb, 'terminated', NOW(), NOW())
	`, route.Identity.InstancePath); err != nil {
		t.Fatalf("seed flow_instances: %v", err)
	}
	ctx = seedFlowRouteTestAuthority(t, ctx, db, true, route.Identity.InstancePath)
	if err := pg.UpsertFlowInstanceRoute(ctx, route); err != nil {
		t.Fatalf("UpsertFlowInstanceRoute: %v", err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	txctx := runtimepipelinefixture.WithSQLTx(ctx, tx)
	if err := pg.DeleteFlowInstanceRoute(txctx, route.Identity); err != nil {
		t.Fatalf("DeleteFlowInstanceRoute in tx: %v", err)
	}
	var inTxStatus string
	if err := tx.QueryRowContext(ctx, `
		SELECT status
		FROM routing_rules
		WHERE flow_instance = $1
	`, route.Identity.InstancePath).Scan(&inTxStatus); err != nil {
		t.Fatalf("query routing_rules in tx: %v", err)
	}
	if strings.TrimSpace(inTxStatus) != "inactive" {
		t.Fatalf("routing_rules status in tx = %q, want inactive", inTxStatus)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	var status string
	if err := db.QueryRowContext(ctx, `
		SELECT status
		FROM routing_rules
		WHERE flow_instance = $1
	`, route.Identity.InstancePath).Scan(&status); err != nil {
		t.Fatalf("query routing_rules after rollback: %v", err)
	}
	if strings.TrimSpace(status) != "inactive" {
		t.Fatalf("named-operation routing_rules status after unrelated rollback = %q, want inactive", status)
	}
}

func TestPostgresStoreRollbackFlowInstanceRouteOwnsNamedTransaction(t *testing.T) {
	ctx := testAuthorActivityContext()
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ensureFlowInstanceRouteTables(t, ctx, db)

	route := runtimebus.FlowInstanceRouteRecord{
		Identity:       runtimeflowidentity.DeriveRoute("review", "inst-1"),
		EventPattern:   "review/inst-1/task.started",
		SubscriberType: "node",
		SubscriberID:   "reviewer-inst-1",
		SourceFlow:     "review",
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES ($1, 'review', 'template', '{}'::jsonb, 'active', NOW())
	`, route.Identity.InstancePath); err != nil {
		t.Fatalf("seed flow_instances: %v", err)
	}
	ctx = seedFlowRouteTestAuthority(t, ctx, db, true, route.Identity.InstancePath)
	if err := pg.UpsertFlowInstanceRoute(ctx, route); err != nil {
		t.Fatalf("UpsertFlowInstanceRoute: %v", err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	txctx := runtimepipelinefixture.WithSQLTx(ctx, tx)
	if err := pg.RollbackFlowInstanceRoute(txctx, route.Identity); err != nil {
		t.Fatalf("RollbackFlowInstanceRoute in tx: %v", err)
	}
	var inTxStatus string
	if err := tx.QueryRowContext(ctx, `
		SELECT status
		FROM routing_rules
		WHERE flow_instance = $1
	`, route.Identity.InstancePath).Scan(&inTxStatus); err != nil {
		t.Fatalf("query routing_rules in tx: %v", err)
	}
	if strings.TrimSpace(inTxStatus) != "inactive" {
		t.Fatalf("routing_rules status in tx = %q, want inactive", inTxStatus)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	var status string
	if err := db.QueryRowContext(ctx, `
		SELECT status
		FROM routing_rules
		WHERE flow_instance = $1
	`, route.Identity.InstancePath).Scan(&status); err != nil {
		t.Fatalf("query routing_rules after rollback: %v", err)
	}
	if strings.TrimSpace(status) != "inactive" {
		t.Fatalf("named-operation routing_rules status after unrelated rollback = %q, want inactive", status)
	}
}

func TestPostgresStoreFlowInstanceRoutes_NestedTemplateScope(t *testing.T) {
	ctx := testAuthorActivityContext()
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ensureFlowInstanceRouteTables(t, ctx, db)

	route := runtimebus.FlowInstanceRouteRecord{
		Identity:       runtimeflowidentity.DeriveRoute("child/grandchild", "inst-1"),
		EventPattern:   "child/grandchild/inst-1/micro.started",
		SubscriberType: "node",
		SubscriberID:   "worker-inst-1",
		SourceFlow:     "child/grandchild",
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES ($1, 'grandchild', 'template', '{}'::jsonb, 'active', NOW())
	`, route.Identity.InstancePath); err != nil {
		t.Fatalf("seed flow_instances: %v", err)
	}
	ctx = seedFlowRouteTestAuthority(t, ctx, db, true, route.Identity.InstancePath)
	if err := pg.UpsertFlowInstanceRoute(ctx, route); err != nil {
		t.Fatalf("UpsertFlowInstanceRoute: %v", err)
	}
	routes, err := pg.ListFlowInstanceRoutes(ctx)
	if err != nil {
		t.Fatalf("ListFlowInstanceRoutes: %v", err)
	}
	want := route.Identity
	if len(routes) != 1 || routes[0] != want {
		t.Fatalf("listed routes = %#v, want %#v", routes, want)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE flow_instances
		SET status = 'terminated', terminated_at = $2
		WHERE instance_id = $1
	`, route.Identity.InstancePath, time.Now().UTC()); err != nil {
		t.Fatalf("terminate flow_instance: %v", err)
	}
	if err := pg.DeleteFlowInstanceRoute(ctx, route.Identity); err != nil {
		t.Fatalf("DeleteFlowInstanceRoute: %v", err)
	}
	routes, err = pg.ListFlowInstanceRoutes(ctx)
	if err != nil {
		t.Fatalf("ListFlowInstanceRoutes after delete: %v", err)
	}
	if len(routes) != 0 {
		t.Fatalf("listed routes after delete = %#v, want none", routes)
	}
}

func TestPostgresStoreFlowInstanceRoutes_CanonicalizesInstancePathOnlyIdentity(t *testing.T) {
	ctx := testAuthorActivityContext()
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ensureFlowInstanceRouteTables(t, ctx, db)

	instancePath := "child/grandchild/inst-1"
	route := runtimebus.FlowInstanceRouteRecord{
		Identity: runtimeflowidentity.Route{
			InstancePath: instancePath,
		},
		EventPattern:   instancePath + "/micro.started",
		SubscriberType: "node",
		SubscriberID:   "worker-inst-1",
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES ($1, 'grandchild', 'template', '{}'::jsonb, 'active', NOW())
	`, instancePath); err != nil {
		t.Fatalf("seed flow_instances: %v", err)
	}
	ctx = seedFlowRouteTestAuthority(t, ctx, db, true, instancePath)
	if err := pg.UpsertFlowInstanceRoute(ctx, route); err != nil {
		t.Fatalf("UpsertFlowInstanceRoute: %v", err)
	}

	routes, err := pg.ListFlowInstanceRoutes(ctx)
	if err != nil {
		t.Fatalf("ListFlowInstanceRoutes: %v", err)
	}
	want := runtimeflowidentity.StoredRoute("", "", instancePath)
	if len(routes) != 1 || routes[0] != want {
		t.Fatalf("listed routes = %#v, want %#v", routes, want)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE flow_instances
		SET status = 'terminated', terminated_at = $2
		WHERE instance_id = $1
	`, instancePath, time.Now().UTC()); err != nil {
		t.Fatalf("terminate flow_instance: %v", err)
	}

	if err := pg.DeleteFlowInstanceRoute(ctx, runtimeflowidentity.Route{InstancePath: instancePath}); err != nil {
		t.Fatalf("DeleteFlowInstanceRoute: %v", err)
	}
	routes, err = pg.ListFlowInstanceRoutes(ctx)
	if err != nil {
		t.Fatalf("ListFlowInstanceRoutes after delete: %v", err)
	}
	if len(routes) != 0 {
		t.Fatalf("listed routes after delete = %#v, want none", routes)
	}
}

func TestPostgresStoreFlowInstanceRouteDeletionRequiresCanonicalTermination(t *testing.T) {
	ctx := testAuthorActivityContext()
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ensureFlowInstanceRouteTables(t, ctx, db)

	const instancePath = "review/inst-1"
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES ($1, 'review', 'template', '{}'::jsonb, 'active', NOW())
	`, instancePath); err != nil {
		t.Fatalf("seed flow_instances: %v", err)
	}
	ctx = seedFlowRouteTestAuthority(t, ctx, db, true, instancePath)
	route := runtimebus.FlowInstanceRouteRecord{
		Identity:       runtimeflowidentity.StoredRoute("", "", instancePath),
		EventPattern:   instancePath + "/task.started",
		SubscriberType: "agent",
		SubscriberID:   "reviewer-inst-1",
		SourceFlow:     "review",
	}
	if err := pg.UpsertFlowInstanceRoute(ctx, route); err != nil {
		t.Fatalf("UpsertFlowInstanceRoute: %v", err)
	}

	err := pg.DeleteFlowInstanceRoute(ctx, route.Identity)
	if err == nil || !strings.Contains(err.Error(), "requires terminal flow_instances status") {
		t.Fatalf("DeleteFlowInstanceRoute err = %v, want terminal-status denial", err)
	}

	if _, err := db.ExecContext(ctx, `
		UPDATE flow_instances
		SET status = 'terminated', terminated_at = $2
		WHERE instance_id = $1
	`, instancePath, time.Now().UTC()); err != nil {
		t.Fatalf("terminate flow_instance: %v", err)
	}
	if err := pg.DeleteFlowInstanceRoute(ctx, route.Identity); err != nil {
		t.Fatalf("DeleteFlowInstanceRoute after termination: %v", err)
	}

	var status string
	if err := db.QueryRowContext(ctx, `
		SELECT status
		FROM routing_rules
		WHERE flow_instance = $1
	`, instancePath).Scan(&status); err != nil {
		t.Fatalf("query routing_rules: %v", err)
	}
	if strings.TrimSpace(status) != "inactive" {
		t.Fatalf("routing_rules.status = %q, want inactive", status)
	}
}

func TestPostgresStoreListFlowInstanceRoutesFiltersTerminatedInstances(t *testing.T) {
	ctx := testAuthorActivityContext()
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ensureFlowInstanceRouteTables(t, ctx, db)

	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES
			('review/inst-active', 'review', 'template', '{}'::jsonb, 'active', NOW()),
			('review/inst-terminated', 'review', 'template', '{}'::jsonb, 'terminated', NOW())
	`); err != nil {
		t.Fatalf("seed flow_instances: %v", err)
	}
	ctx = seedFlowRouteTestAuthority(t, ctx, db, true, "review/inst-active", "review/inst-terminated")
	for _, route := range []runtimebus.FlowInstanceRouteRecord{
		{
			Identity:       runtimeflowidentity.StoredRoute("", "", "review/inst-active"),
			EventPattern:   "review/inst-active/task.started",
			SubscriberType: "agent",
			SubscriberID:   "reviewer-active",
			SourceFlow:     "review",
		},
		{
			Identity:       runtimeflowidentity.StoredRoute("", "", "review/inst-terminated"),
			EventPattern:   "review/inst-terminated/task.started",
			SubscriberType: "agent",
			SubscriberID:   "reviewer-terminated",
			SourceFlow:     "review",
		},
	} {
		if err := pg.UpsertFlowInstanceRoute(ctx, route); err != nil {
			t.Fatalf("UpsertFlowInstanceRoute(%s): %v", route.Identity.InstancePath, err)
		}
	}

	routes, err := pg.ListFlowInstanceRoutes(ctx)
	if err != nil {
		t.Fatalf("ListFlowInstanceRoutes: %v", err)
	}
	if len(routes) != 1 || routes[0].InstancePath != "review/inst-active" {
		t.Fatalf("listed routes = %#v, want only active flow-instance route", routes)
	}
}

func TestPostgresStoreListActiveFlowInstanceDescriptorsFiltersToActiveTemplates(t *testing.T) {
	const runID = "11111111-1111-4111-8111-111111111111"
	const entityID = "22222222-2222-4222-8222-222222222222"
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ensureFlowInstanceRouteTables(t, ctx, db)

	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES
			('component-scaffold/active', 'component-scaffold', 'template', '{}'::jsonb, 'active', NOW()),
			('component-scaffold/terminated', 'component-scaffold', 'template', '{}'::jsonb, 'terminated', NOW()),
			('service-owner', 'service-owner', 'static', '{}'::jsonb, 'active', NOW())
	`); err != nil {
		t.Fatalf("seed flow_instances: %v", err)
	}
	requireRunFixtureForTest(t, ctx, pg, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})
	requireRunFixtureForTest(t, ctx, pg, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(),
		RunID: "44444444-4444-4444-8444-444444444444",
	})
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
		INSERT INTO flow_instance_runtime_readiness (run_id, instance_id, plan, created_at, updated_at)
		VALUES ($1::uuid, 'component-scaffold/active', $2::jsonb, NOW(), NOW())
	`, runID, readinessPlan); err != nil {
		t.Fatalf("seed flow-instance readiness: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (entity_id, run_id, flow_instance, entity_type, current_state, fields, created_at, updated_at)
		VALUES
			($2::uuid, $1::uuid, 'component-scaffold/active', 'component', 'ready', '{"vertical_id":"v-active","weight":1.1234567}'::jsonb, NOW(), NOW()),
			('33333333-3333-4333-8333-333333333333', '44444444-4444-4444-8444-444444444444'::uuid, 'component-scaffold/active', 'component', 'ready', '{"vertical_id":"wrong-run"}'::jsonb, NOW() + INTERVAL '1 minute', NOW() + INTERVAL '1 minute')
	`, runID, entityID); err != nil {
		t.Fatalf("seed entity_state: %v", err)
	}

	descriptors, err := pg.ListActiveFlowInstanceDescriptors(ctx)
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

	if descriptors, err := pg.ListActiveFlowInstanceDescriptors(ctx); err == nil || !strings.Contains(err.Error(), "run_id is required") {
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

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	requirePostgresRunFixtureInRawTxForTest(t, ctx, tx, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES ('component-scaffold/uncommitted', 'component-scaffold', 'template', '{}'::jsonb, 'active', NOW())
	`); err != nil {
		t.Fatalf("seed flow_instances in tx: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO flow_instance_runtime_readiness (run_id, instance_id, plan, created_at, updated_at)
		VALUES ($1::uuid, 'component-scaffold/uncommitted', '{"workflow_version":"1.0.0"}'::jsonb, NOW(), NOW())
	`, runID); err != nil {
		t.Fatalf("seed readiness in tx: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO entity_state (entity_id, run_id, flow_instance, entity_type, current_state, fields, created_at, updated_at)
		VALUES ('22222222-2222-4222-8222-222222222222'::uuid, $1::uuid, 'component-scaffold/uncommitted', 'component', 'ready', '{}'::jsonb, NOW(), NOW())
	`, runID); err != nil {
		t.Fatalf("seed entity state in tx: %v", err)
	}

	descriptors, err := pg.ListActiveFlowInstanceDescriptors(runtimepipelinefixture.WithSQLTx(ctx, tx))
	if err != nil {
		t.Fatalf("ListActiveFlowInstanceDescriptors: %v", err)
	}
	if len(descriptors) != 0 {
		t.Fatalf("descriptors = %#v, want no ambient uncommitted rows", descriptors)
	}
}
