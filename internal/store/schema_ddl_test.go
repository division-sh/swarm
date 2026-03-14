package store

import (
	"strings"
	"testing"

	runtimecontracts "empireai/internal/runtime/contracts"
)

func TestSchemaFieldTypeToDDL(t *testing.T) {
	cases := []struct {
		schemaType string
		wantDDL    string
	}{
		{schemaType: "text", wantDDL: "TEXT"},
		{schemaType: "string", wantDDL: "TEXT"},
		{schemaType: "integer", wantDDL: "BIGINT"},
		{schemaType: "numeric(12,2)", wantDDL: "NUMERIC(12,2)"},
		{schemaType: "boolean", wantDDL: "BOOLEAN"},
		{schemaType: "jsonb", wantDDL: "JSONB"},
		{schemaType: "timestamp", wantDDL: "TIMESTAMPTZ"},
		{schemaType: "uuid", wantDDL: "UUID"},
	}
	for _, tc := range cases {
		t.Run(tc.schemaType, func(t *testing.T) {
			got, err := SchemaFieldTypeToDDL(tc.schemaType)
			if err != nil {
				t.Fatalf("SchemaFieldTypeToDDL(%q): %v", tc.schemaType, err)
			}
			if got != tc.wantDDL {
				t.Fatalf("SchemaFieldTypeToDDL(%q) = %q, want %q", tc.schemaType, got, tc.wantDDL)
			}
		})
	}
}

func TestSchemaFieldTypeToDDLError(t *testing.T) {
	if _, err := SchemaFieldTypeToDDL("object"); err == nil || !strings.Contains(err.Error(), `unknown schema type "object"`) {
		t.Fatalf("expected unknown schema type error, got %v", err)
	}
}

func TestGeneratePlatformTableDDLs(t *testing.T) {
	var spec runtimecontracts.PlatformSpecDocument
	spec.WorkflowState.DDL = "CREATE TABLE workflow_instances (\n    instance_id UUID PRIMARY KEY,\n    workflow_name TEXT NOT NULL,\n    transition_history JSONB NOT NULL DEFAULT '[]'\n);\nCREATE INDEX idx_wi_workflow ON workflow_instances(workflow_name);\nCREATE TABLE flow_instance_routes (\n    template_id TEXT NOT NULL,\n    instance_id TEXT NOT NULL,\n    instance_path TEXT NOT NULL,\n    PRIMARY KEY (template_id, instance_id)\n);\n"

	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs: %v", err)
	}
	if len(plans) != 1 {
		t.Fatalf("expected 1 platform DDL plan, got %d", len(plans))
	}
	if plans[0].TableName != "workflow_instances" {
		t.Fatalf("unexpected platform table %q", plans[0].TableName)
	}
	if plans[0].ColumnCount != 3 {
		t.Fatalf("unexpected platform column count %d", plans[0].ColumnCount)
	}
	if got := plans[0].Statements[0]; !strings.Contains(got, "CREATE TABLE IF NOT EXISTS workflow_instances") {
		t.Fatalf("expected idempotent create table, got %q", got)
	}
	if got := plans[0].Statements[1]; !strings.Contains(got, "CREATE INDEX IF NOT EXISTS idx_wi_workflow") {
		t.Fatalf("expected idempotent create index, got %q", got)
	}
	if got := plans[0].Statements[2]; !strings.Contains(got, "CREATE TABLE IF NOT EXISTS flow_instance_routes") {
		t.Fatalf("expected flow_instance_routes table ddl, got %q", got)
	}
}

func TestGenerateEntityTableDDLs(t *testing.T) {
	schema := runtimecontracts.EntitySchema{
		Groups: []runtimecontracts.EntitySchemaGroup{{
			Name: "items",
			Fields: []runtimecontracts.EntitySchemaField{
				{Name: "status", Type: "string", Indexed: true},
				{Name: "score", Type: "numeric(5,2)", Nullable: true},
			},
		}},
	}

	plans, err := GenerateEntityTableDDLs(schema)
	if err != nil {
		t.Fatalf("GenerateEntityTableDDLs: %v", err)
	}
	if len(plans) != 1 {
		t.Fatalf("expected 1 entity DDL plan, got %d", len(plans))
	}
	if plans[0].TableName != "items" {
		t.Fatalf("unexpected entity table %q", plans[0].TableName)
	}
	if plans[0].ColumnCount != 5 {
		t.Fatalf("unexpected entity column count %d", plans[0].ColumnCount)
	}
	createStmt := plans[0].Statements[0]
	if !strings.Contains(createStmt, `"entity_id" UUID PRIMARY KEY`) {
		t.Fatalf("expected entity_id primary key, got %q", createStmt)
	}
	if !strings.Contains(createStmt, `"status" TEXT NOT NULL`) {
		t.Fatalf("expected status column, got %q", createStmt)
	}
	if !strings.Contains(createStmt, `"score" NUMERIC(5,2)`) {
		t.Fatalf("expected score column, got %q", createStmt)
	}
	if len(plans[0].Statements) != 2 || !strings.Contains(plans[0].Statements[1], `CREATE INDEX IF NOT EXISTS "idx_items_status"`) {
		t.Fatalf("expected indexed status column, got %#v", plans[0].Statements)
	}
}

func TestGenerateNodeStateTableDDLs(t *testing.T) {
	nodes := map[string]runtimecontracts.SystemNodeContract{
		"processing-node": {
			StateTable: "processing_node_state",
			StateSchema: runtimecontracts.NodeStateSchema{
				Fields: []runtimecontracts.NodeStateField{
					{Name: "attempts", Type: "integer"},
					{Name: "last_error", Type: "text"},
				},
			},
		},
	}

	plans, err := GenerateNodeStateTableDDLs(nodes)
	if err != nil {
		t.Fatalf("GenerateNodeStateTableDDLs: %v", err)
	}
	if len(plans) != 1 {
		t.Fatalf("expected 1 state DDL plan, got %d", len(plans))
	}
	createStmt := plans[0].Statements[0]
	if !strings.Contains(createStmt, `"entity_id" UUID NOT NULL`) {
		t.Fatalf("expected entity_id column, got %q", createStmt)
	}
	if !strings.Contains(createStmt, `"node_id" TEXT NOT NULL`) {
		t.Fatalf("expected node_id column, got %q", createStmt)
	}
	if !strings.Contains(createStmt, `"attempts" BIGINT`) || !strings.Contains(createStmt, `"last_error" TEXT`) {
		t.Fatalf("expected state_schema fields, got %q", createStmt)
	}
	if !strings.Contains(createStmt, `PRIMARY KEY ("entity_id", "node_id")`) {
		t.Fatalf("expected composite primary key, got %q", createStmt)
	}
}
