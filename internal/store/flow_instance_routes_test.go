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

	var spec runtimecontracts.PlatformSpecDocument
	spec.WorkflowState.DDL = "CREATE TABLE workflow_instances (\n    instance_id UUID PRIMARY KEY,\n    workflow_name TEXT NOT NULL,\n    transition_history JSONB NOT NULL DEFAULT '[]'\n);\nCREATE TABLE flow_instance_routes (\n    template_id TEXT NOT NULL,\n    instance_id TEXT NOT NULL,\n    instance_path TEXT NOT NULL,\n    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),\n    PRIMARY KEY (template_id, instance_id)\n);\n"
	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs: %v", err)
	}
	if err := pg.EnsureSchemaTables(ctx, plans); err != nil {
		t.Fatalf("EnsureSchemaTables: %v", err)
	}

	route := runtimebus.FlowInstanceRouteRecord{
		TemplateID:   "review",
		InstanceID:   "inst-1",
		InstancePath: "review/inst-1",
	}
	if err := pg.UpsertFlowInstanceRoute(ctx, route); err != nil {
		t.Fatalf("UpsertFlowInstanceRoute: %v", err)
	}
	routes, err := pg.ListFlowInstanceRoutes(ctx)
	if err != nil {
		t.Fatalf("ListFlowInstanceRoutes: %v", err)
	}
	if len(routes) != 1 || routes[0] != route {
		t.Fatalf("listed routes = %#v, want %#v", routes, route)
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
