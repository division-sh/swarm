package store

import (
	"errors"
	"os"
	"path/filepath"
	stdruntime "runtime"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
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
		{schemaType: "float", wantDDL: "DOUBLE PRECISION"},
		{schemaType: "numeric", wantDDL: "NUMERIC"},
		{schemaType: "numeric(12,2)", wantDDL: "NUMERIC(12,2)"},
		{schemaType: "boolean", wantDDL: "BOOLEAN"},
		{schemaType: "jsonb", wantDDL: "JSONB"},
		{schemaType: "timestamp", wantDDL: "TIMESTAMPTZ"},
		{schemaType: "uuid", wantDDL: "UUID"},
		{schemaType: "text[]", wantDDL: "TEXT[]"},
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
	if _, err := SchemaFieldTypeToDDL("numeric (12,2)"); err == nil || !errors.Is(err, ErrUnknownSchemaType) {
		t.Fatalf("expected spaced numeric type to fail fast, got %v", err)
	}
}

func TestNodeStateFieldTypeToDDL_AllowsNamedTypesAsJSONB(t *testing.T) {
	cases := map[string]string{
		"DimensionScore":   "JSONB",
		"[DimensionScore]": "JSONB",
		"DimensionScore[]": "JSONB",
		"text[]":           "TEXT[]",
	}
	for schemaType, wantDDL := range cases {
		t.Run(schemaType, func(t *testing.T) {
			got, err := NodeStateFieldTypeToDDL(schemaType)
			if err != nil {
				t.Fatalf("NodeStateFieldTypeToDDL(%q): %v", schemaType, err)
			}
			if got != wantDDL {
				t.Fatalf("NodeStateFieldTypeToDDL(%q) = %q, want %q", schemaType, got, wantDDL)
			}
		})
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

func TestPlatformSpecEntityStateUsesRunScopedIdentity(t *testing.T) {
	_, file, _, _ := stdruntime.Caller(0)
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	raw, err := os.ReadFile(runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	var spec runtimecontracts.PlatformSpecDocument
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		t.Fatalf("unmarshal platform spec: %v", err)
	}
	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs: %v", err)
	}
	var entityState SchemaTableDDL
	for _, plan := range plans {
		if plan.TableName == "entity_state" {
			entityState = plan
			break
		}
	}
	if entityState.TableName == "" {
		t.Fatal("entity_state ddl plan missing")
	}
	joined := strings.Join(entityState.Statements, "\n")
	for _, want := range []string{
		"run_id            UUID NOT NULL REFERENCES runs(run_id)",
		"entity_id         UUID NOT NULL",
		"PRIMARY KEY (run_id, entity_id)",
		`CREATE INDEX IF NOT EXISTS "idx_entity_flow" ON "entity_state"(run_id, flow_instance, current_state)`,
		`CREATE INDEX IF NOT EXISTS "idx_entity_cross_run" ON "entity_state"(entity_id)`,
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("entity_state ddl missing %q:\n%s", want, joined)
		}
	}
	if strings.Contains(joined, "entity_id         UUID PRIMARY KEY") {
		t.Fatalf("entity_state ddl kept global entity_id primary key:\n%s", joined)
	}
}

func TestPlatformSpecOwnsMultiBundleSchemaFoundation(t *testing.T) {
	_, file, _, _ := stdruntime.Caller(0)
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	raw, err := os.ReadFile(runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	var spec runtimecontracts.PlatformSpecDocument
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		t.Fatalf("unmarshal platform spec: %v", err)
	}
	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs: %v", err)
	}
	var runs SchemaTableDDL
	var bundles SchemaTableDDL
	for _, plan := range plans {
		if plan.TableName == "runs" {
			runs = plan
		}
		if plan.TableName == "bundles" {
			bundles = plan
		}
	}
	if bundles.TableName == "" {
		t.Fatal("bundles ddl plan missing")
	}
	bundleDDL := strings.Join(bundles.Statements, "\n")
	for _, want := range []string{
		"bundle_hash      TEXT PRIMARY KEY",
		"content_yaml     TEXT NOT NULL",
		"parsed_json      JSONB NOT NULL",
		"data_blob        BYTEA",
		"metadata         JSONB NOT NULL",
		"ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()",
		`CREATE INDEX IF NOT EXISTS "idx_bundles_ingested_at" ON "bundles"(ingested_at)`,
	} {
		if !strings.Contains(bundleDDL, want) {
			t.Fatalf("bundles ddl missing %q:\n%s", want, bundleDDL)
		}
	}
	if runs.TableName == "" {
		t.Fatal("runs ddl plan missing")
	}
	joined := strings.Join(runs.Statements, "\n")
	for _, want := range []string{
		"bundle_hash        TEXT CHECK",
		"bundle_source      TEXT NOT NULL DEFAULT 'legacy'",
		"bundle_fingerprint TEXT",
		`CREATE INDEX IF NOT EXISTS "idx_runs_bundle_hash" ON "runs"(bundle_hash) WHERE bundle_hash IS NOT NULL`,
		`CREATE INDEX IF NOT EXISTS "idx_runs_bundle_source_status" ON "runs"(bundle_source, status, started_at)`,
		`CREATE INDEX IF NOT EXISTS "idx_runs_bundle_delete_planning" ON "runs"(bundle_hash, status) WHERE bundle_hash IS NOT NULL`,
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("runs ddl missing %q:\n%s", want, joined)
		}
	}
	if strings.Contains(joined, "bundle_hash TEXT REFERENCES") || strings.Contains(joined, "FOREIGN KEY (bundle_hash)") {
		t.Fatalf("runs ddl must not create a bundle_hash foreign key:\n%s", joined)
	}
}

func TestPlatformSpecEventReceiptsUsesTypedSubscriberIdentity(t *testing.T) {
	_, file, _, _ := stdruntime.Caller(0)
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	raw, err := os.ReadFile(runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	var spec runtimecontracts.PlatformSpecDocument
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		t.Fatalf("unmarshal platform spec: %v", err)
	}
	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs: %v", err)
	}
	var receipts SchemaTableDDL
	for _, plan := range plans {
		if plan.TableName == "event_receipts" {
			receipts = plan
			break
		}
	}
	if receipts.TableName == "" {
		t.Fatal("event_receipts ddl plan missing")
	}
	joined := strings.Join(receipts.Statements, "\n")
	if !strings.Contains(joined, "UNIQUE (event_id, subscriber_type, subscriber_id)") {
		t.Fatalf("event_receipts ddl missing typed subscriber identity:\n%s", joined)
	}
	if strings.Contains(joined, "UNIQUE (event_id, subscriber_id)") {
		t.Fatalf("event_receipts ddl kept untyped subscriber identity:\n%s", joined)
	}
}

func TestGeneratePlatformTableDDLs_StripsDeprecatedEntitySubjectDDL(t *testing.T) {
	var spec runtimecontracts.PlatformSpecDocument
	spec.PlatformTables.Tables = map[string]struct {
		Description string `yaml:"description"`
		DDL         string `yaml:"ddl"`
	}{
		"entity_state": {
			DDL: "CREATE TABLE entity_state (\n    entity_id UUID PRIMARY KEY,\n    subject_id UUID,\n    flow_instance TEXT NOT NULL,\n    INDEX idx_entity_subject (subject_id) WHERE subject_id IS NOT NULL,\n    INDEX idx_entity_state_flow (flow_instance)\n);",
		},
	}

	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs: %v", err)
	}
	if len(plans) != 1 {
		t.Fatalf("expected 1 platform DDL plan, got %d", len(plans))
	}
	for _, statement := range plans[0].Statements {
		if strings.Contains(statement, "subject_id") {
			t.Fatalf("deprecated subject_id DDL survived: %q", statement)
		}
		if strings.Contains(statement, "idx_entity_subject") {
			t.Fatalf("deprecated idx_entity_subject DDL survived: %q", statement)
		}
	}
	if plans[0].ColumnCount != 2 {
		t.Fatalf("column count = %d, want 2", plans[0].ColumnCount)
	}
	if len(plans[0].Statements) != 2 {
		t.Fatalf("statements = %#v, want create table plus surviving flow index", plans[0].Statements)
	}
	if got := plans[0].Statements[1]; !strings.Contains(got, `CREATE INDEX IF NOT EXISTS "idx_entity_state_flow" ON "entity_state"(flow_instance)`) {
		t.Fatalf("expected non-subject inline index to survive, got %q", got)
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
					{Name: "score", Type: "float"},
					{Name: "dimensions_received", Type: "[DimensionScore]"},
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
	if !strings.Contains(createStmt, `"attempts" BIGINT`) || !strings.Contains(createStmt, `"last_error" TEXT`) || !strings.Contains(createStmt, `"score" DOUBLE PRECISION`) || !strings.Contains(createStmt, `"dimensions_received" JSONB`) {
		t.Fatalf("expected state_schema fields, got %q", createStmt)
	}
	if !strings.Contains(createStmt, `PRIMARY KEY ("entity_id", "node_id")`) {
		t.Fatalf("expected composite primary key, got %q", createStmt)
	}
}

func TestGenerateNodeStateTableDDLs_RejectsPseudoTypes(t *testing.T) {
	nodes := map[string]runtimecontracts.SystemNodeContract{
		"processing-node": {
			StateTable: "processing_node_state",
			StateSchema: runtimecontracts.NodeStateSchema{
				Fields: []runtimecontracts.NodeStateField{
					{Name: "received_items", Type: "dimension score receipts keyed by dimension name"},
				},
			},
		},
	}

	_, err := GenerateNodeStateTableDDLs(nodes)
	if err == nil || !strings.Contains(err.Error(), "not canonical") {
		t.Fatalf("expected pseudo-type error, got %v", err)
	}
}
