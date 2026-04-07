package store

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	runtimebus "swarm/internal/runtime/bus"
	runtimeflowidentity "swarm/internal/runtime/core/flowidentity"
	"swarm/internal/testutil"
)

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
	ctx := context.Background()
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
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

func TestPostgresStoreFlowInstanceRoutes_NestedTemplateScope(t *testing.T) {
	ctx := context.Background()
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
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
	ctx := context.Background()
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
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
	ctx := context.Background()
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ensureFlowInstanceRouteTables(t, ctx, db)

	const instancePath = "review/inst-1"
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES ($1, 'review', 'template', '{}'::jsonb, 'active', NOW())
	`, instancePath); err != nil {
		t.Fatalf("seed flow_instances: %v", err)
	}
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
	ctx := context.Background()
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ensureFlowInstanceRouteTables(t, ctx, db)

	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES
			('review/inst-active', 'review', 'template', '{}'::jsonb, 'active', NOW()),
			('review/inst-terminated', 'review', 'template', '{}'::jsonb, 'terminated', NOW())
	`); err != nil {
		t.Fatalf("seed flow_instances: %v", err)
	}
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
