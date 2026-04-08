package store

import (
	"errors"
	"strings"
	"testing"

	runtimecontracts "swarm/internal/runtime/contracts"
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
		{schemaType: "timestamptz (set on state change)", wantDDL: "TIMESTAMPTZ"},
		{schemaType: "uuid", wantDDL: "UUID"},
		{schemaType: "uuid (null for non-derived)", wantDDL: "UUID"},
		{schemaType: "integer default 0", wantDDL: "BIGINT"},
		{schemaType: "numeric(5,2) (computed by node weighted_average)", wantDDL: "NUMERIC(5,2)"},
		{schemaType: "text[]", wantDDL: "TEXT[]"},
		{schemaType: "text[] (scanner types dispatched)", wantDDL: "TEXT[]"},
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
	if _, err := SchemaFieldTypeToDDL("object"); err == nil || !errors.Is(err, ErrUnknownSchemaType) {
		t.Fatalf("expected unknown schema type error, got %v", err)
	}
}

func TestGeneratePlatformTableDDLs(t *testing.T) {
	var spec runtimecontracts.PlatformSpecDocument
	spec.PlatformTables.Tables = map[string]struct {
		Description string `yaml:"description"`
		DDL         string `yaml:"ddl"`
	}{
		"events": {
			DDL: "CREATE TABLE events (\n    event_id UUID PRIMARY KEY,\n    entity_id UUID NOT NULL,\n    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),\n    INDEX idx_events_entity (entity_id, created_at)\n);",
		},
		"flow_instances": {
			DDL: "CREATE TABLE flow_instances (\n    instance_id TEXT PRIMARY KEY,\n    flow_template TEXT NOT NULL\n);",
		},
	}

	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs: %v", err)
	}
	if len(plans) != 2 {
		t.Fatalf("expected 2 platform DDL plans, got %d", len(plans))
	}
	if plans[0].TableName != "events" {
		t.Fatalf("unexpected first platform table %q", plans[0].TableName)
	}
	if plans[0].ColumnCount != 3 {
		t.Fatalf("unexpected platform column count %d", plans[0].ColumnCount)
	}
	if got := plans[0].Statements[0]; !strings.Contains(got, "CREATE TABLE IF NOT EXISTS events") {
		t.Fatalf("expected idempotent create table, got %q", got)
	}
	if strings.Contains(plans[0].Statements[0], "INDEX idx_events_entity") {
		t.Fatalf("expected inline index to be extracted from table ddl, got %q", plans[0].Statements[0])
	}
	if got := plans[0].Statements[1]; !strings.Contains(got, `CREATE INDEX IF NOT EXISTS "idx_events_entity" ON "events"(entity_id, created_at)`) {
		t.Fatalf("expected idempotent create index, got %q", got)
	}
	if plans[1].TableName != "flow_instances" {
		t.Fatalf("unexpected second platform table %q", plans[1].TableName)
	}
}

func TestGeneratePlatformTableDDLs_ExtractsInlineUniquePartialIndex(t *testing.T) {
	var spec runtimecontracts.PlatformSpecDocument
	spec.PlatformTables.Tables = map[string]struct {
		Description string `yaml:"description"`
		DDL         string `yaml:"ddl"`
	}{
		"agent_sessions": {
			DDL: "CREATE TABLE agent_sessions (\n    session_id UUID PRIMARY KEY,\n    agent_id TEXT NOT NULL,\n    scope_key TEXT NOT NULL,\n    status TEXT NOT NULL,\n    UNIQUE INDEX agent_sessions_nonterminated_unique (agent_id, scope_key) WHERE status <> 'terminated'\n);",
		},
	}

	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs: %v", err)
	}
	if len(plans) != 1 || len(plans[0].Statements) != 2 {
		t.Fatalf("plans = %#v", plans)
	}
	if got := plans[0].Statements[1]; !strings.Contains(got, `CREATE UNIQUE INDEX IF NOT EXISTS "agent_sessions_nonterminated_unique" ON "agent_sessions"(agent_id, scope_key) WHERE status <> 'terminated'`) {
		t.Fatalf("unexpected unique partial index statement: %q", got)
	}
}

func TestGeneratePlatformTableDDLs_OrdersRunsBeforeEvents(t *testing.T) {
	var spec runtimecontracts.PlatformSpecDocument
	spec.PlatformTables.Tables = map[string]struct {
		Description string `yaml:"description"`
		DDL         string `yaml:"ddl"`
	}{
		"events": {
			DDL: "CREATE TABLE events (\n    event_id UUID PRIMARY KEY,\n    run_id UUID REFERENCES runs(run_id)\n);",
		},
		"runs": {
			DDL: "CREATE TABLE runs (\n    run_id UUID PRIMARY KEY\n);",
		},
		"entity_mutations": {
			DDL: "CREATE TABLE entity_mutations (\n    mutation_id UUID PRIMARY KEY,\n    run_id UUID REFERENCES runs(run_id),\n    caused_by_event UUID REFERENCES events(event_id)\n);",
		},
	}
	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs: %v", err)
	}
	if len(plans) != 3 {
		t.Fatalf("expected 3 platform DDL plans, got %d", len(plans))
	}
	if plans[0].TableName != "runs" {
		t.Fatalf("first table = %q, want runs", plans[0].TableName)
	}
	if plans[1].TableName != "events" {
		t.Fatalf("second table = %q, want events", plans[1].TableName)
	}
	if plans[2].TableName != "entity_mutations" {
		t.Fatalf("third table = %q, want entity_mutations", plans[2].TableName)
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

func TestGenerateEntityTableDDLs_IgnoresManagedTimestampDuplicates(t *testing.T) {
	schema := runtimecontracts.EntitySchema{
		Groups: []runtimecontracts.EntitySchemaGroup{{
			Name: "metadata",
			Fields: []runtimecontracts.EntitySchemaField{
				{Name: "created_at", Type: "timestamptz"},
				{Name: "updated_at", Type: "timestamptz"},
				{Name: "human_notes", Type: "text"},
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
	createStmt := plans[0].Statements[0]
	if strings.Count(createStmt, `"created_at" TIMESTAMPTZ NOT NULL DEFAULT NOW()`) != 1 {
		t.Fatalf("expected single managed created_at column, got %q", createStmt)
	}
	if strings.Count(createStmt, `"updated_at" TIMESTAMPTZ NOT NULL DEFAULT NOW()`) != 1 {
		t.Fatalf("expected single managed updated_at column, got %q", createStmt)
	}
	if !strings.Contains(createStmt, `"human_notes" TEXT NOT NULL`) {
		t.Fatalf("expected human_notes column, got %q", createStmt)
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
