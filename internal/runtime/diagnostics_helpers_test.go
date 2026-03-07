package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"empireai/internal/events"
	runtimebus "empireai/internal/runtime/bus"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestDiagnosticsHelpers_MarshalNullableAndSanitize(t *testing.T) {
	if got := string(marshalJSONOrEmpty(nil)); got != "{}" {
		t.Fatalf("marshalJSONOrEmpty(nil)=%q", got)
	}
	if got := string(marshalJSONOrEmpty(map[string]any{"k": "v"})); got == "{}" {
		t.Fatalf("marshalJSONOrEmpty(map) should not be empty object: %q", got)
	}
	if got := string(marshalJSONOrEmpty(map[string]any{"bad": make(chan int)})); got != "{}" {
		t.Fatalf("marshalJSONOrEmpty(error)=%q", got)
	}

	if got := marshalJSONOrNil(nil); got != nil {
		t.Fatalf("marshalJSONOrNil(nil) expected nil, got %q", string(got))
	}
	if got := string(marshalJSONOrNil(map[string]any{"k": 1})); got == "" {
		t.Fatalf("marshalJSONOrNil(map) expected bytes")
	}
	if got := marshalJSONOrNil(map[string]any{"bad": make(chan int)}); got != nil {
		t.Fatalf("marshalJSONOrNil(error) expected nil, got %q", string(got))
	}

	if v := maybeJSONString(nil); v != nil {
		t.Fatalf("maybeJSONString(nil) expected nil, got %#v", v)
	}
	if v := maybeJSONString([]byte("abc")); v != "abc" {
		t.Fatalf("maybeJSONString bytes mismatch: %#v", v)
	}

	if v := nullableUUID(""); v != nil {
		t.Fatalf("nullableUUID(empty) expected nil, got %#v", v)
	}
	if v := nullableUUID("not-a-uuid"); v != nil {
		t.Fatalf("nullableUUID(invalid) expected nil, got %#v", v)
	}
	validID := uuid.NewString()
	if v := nullableUUID(validID); v != validID {
		t.Fatalf("nullableUUID(valid) expected %q, got %#v", validID, v)
	}

	out := sanitizeStringSlice([]string{" a ", "", "a", "b", " b "})
	if len(out) != 2 || out[0] != "a" || out[1] != "b" {
		t.Fatalf("sanitizeStringSlice unexpected: %#v", out)
	}
}

func TestDiagnosticsHelpers_MissingTableAndDBTxNilPaths(t *testing.T) {
	if isMissingDiagnosticsTable(nil) {
		t.Fatal("nil error should not be missing-table")
	}
	if !isMissingDiagnosticsTable(errors.New(`relation "runtime_log" does not exist`)) {
		t.Fatal("runtime_log missing should be detected")
	}
	if !isMissingDiagnosticsTable(errors.New(`relation "pipeline_transitions" does not exist`)) {
		t.Fatal("pipeline_transitions missing should be detected")
	}
	if isMissingDiagnosticsTable(errors.New("permission denied")) {
		t.Fatal("non-missing-table error should not match")
	}

	ctx := context.Background()
	if got := withSQLTxContext(ctx, nil); got != ctx {
		t.Fatal("withSQLTxContext should return original ctx when tx=nil")
	}
	if tx, ok := sqlTxFromContext(nil); ok || tx != nil {
		t.Fatalf("sqlTxFromContext(nil) expected nil,false got %#v,%v", tx, ok)
	}
	if tx, ok := sqlTxFromContext(context.Background()); ok || tx != nil {
		t.Fatalf("sqlTxFromContext(empty ctx) expected nil,false got %#v,%v", tx, ok)
	}
}

func TestRuntimeLogger_IntegrationAndMissingTableTolerance(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS runtime_log (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			level TEXT NOT NULL,
			component TEXT NOT NULL,
			action TEXT NOT NULL,
			event_id UUID,
			event_type TEXT,
			agent_id TEXT,
			vertical_id UUID,
			campaign_id UUID,
			scan_id UUID,
			session_id UUID,
			detail JSONB NOT NULL DEFAULT '{}'::jsonb,
			error TEXT,
			duration_us INT,
			created_at TIMESTAMPTZ DEFAULT now()
		)
	`); err != nil {
		t.Fatalf("create runtime_log table: %v", err)
	}

	logger := NewRuntimeLogger(db)
	bus := NewEventBus(runtimebus.InMemoryEventStore{})
	bus.SetRuntimeLogger(logger)
	if err := bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     mustJSON(map[string]any{"directive_text": "SaaS in Chile"}),
		CreatedAt:   time.Now(),
	}); err != nil {
		t.Fatalf("publish with runtime logger: %v", err)
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM runtime_log`).Scan(&count); err != nil {
		t.Fatalf("count runtime_log: %v", err)
	}
	if count == 0 {
		t.Fatal("expected runtime_log rows to be written")
	}

	logger.Log(ctx, RuntimeLogEntry{
		Level:      "info",
		Component:  "test",
		Action:     "manual",
		EventID:    "not-a-uuid",
		VerticalID: "not-a-uuid",
		Detail:     map[string]any{"ok": true},
	})

	if _, err := db.ExecContext(ctx, `DROP TABLE runtime_log`); err != nil {
		t.Fatalf("drop runtime_log: %v", err)
	}

	logger.Log(ctx, RuntimeLogEntry{
		Level:     "warn",
		Component: "test",
		Action:    "missing-table-path",
		Detail:    map[string]any{"expected": "no panic"},
	})
}
