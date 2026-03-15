package store

import (
	"context"
	"testing"

	runtimebus "empireai/internal/runtime/bus"
	runtimecontracts "empireai/internal/runtime/contracts"
	"empireai/internal/testutil"
)

func TestPostgresStoreFlowInstanceRoutes(t *testing.T) {
	ctx := context.Background()
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS routing_rules`); err != nil {
		t.Fatalf("drop legacy routing_rules: %v", err)
	}

	var spec runtimecontracts.PlatformSpecDocument
	spec.PlatformTables.Tables = map[string]struct {
		Description string `yaml:"description"`
		DDL         string `yaml:"ddl"`
	}{
		"routing_rules": {
			DDL: "CREATE TABLE routing_rules (\n    rule_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),\n    event_pattern TEXT NOT NULL,\n    subscriber_type TEXT NOT NULL,\n    subscriber_id TEXT NOT NULL,\n    flow_instance TEXT,\n    source_flow TEXT,\n    is_wildcard BOOLEAN NOT NULL DEFAULT FALSE,\n    is_materialized BOOLEAN NOT NULL DEFAULT FALSE,\n    materialized_from UUID,\n    status TEXT NOT NULL DEFAULT 'active',\n    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\n);",
		},
	}
	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs: %v", err)
	}
	if err := pg.EnsureSchemaTables(ctx, plans); err != nil {
		t.Fatalf("EnsureSchemaTables: %v", err)
	}

	route := runtimebus.FlowInstanceRouteRecord{
		TemplateID:     "review",
		InstanceID:     "inst-1",
		InstancePath:   "review/inst-1",
		EventPattern:   "review/inst-1/task.started",
		SubscriberType: "node",
		SubscriberID:   "reviewer-inst-1",
		SourceFlow:     "review",
	}
	if err := pg.UpsertFlowInstanceRoute(ctx, route); err != nil {
		t.Fatalf("UpsertFlowInstanceRoute: %v", err)
	}
	routes, err := pg.ListFlowInstanceRoutes(ctx)
	if err != nil {
		t.Fatalf("ListFlowInstanceRoutes: %v", err)
	}
	want := runtimebus.FlowInstanceRouteRecord{
		TemplateID:   route.TemplateID,
		InstanceID:   route.InstanceID,
		InstancePath: route.InstancePath,
	}
	if len(routes) != 1 || routes[0] != want {
		t.Fatalf("listed routes = %#v, want %#v", routes, want)
	}
	if err := pg.DeleteFlowInstanceRoute(ctx, route.TemplateID, route.InstanceID); err != nil {
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
