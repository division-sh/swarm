package entitystore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

func TestSQLiteEntityDiffContributesExactFactsAndLifecycleStory(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE entity_mutations (
		mutation_id TEXT, run_id TEXT, entity_id TEXT, domain TEXT, path TEXT,
		old_value TEXT, new_value TEXT, caused_by_event TEXT, writer_type TEXT,
		writer_id TEXT, handler_step TEXT, created_at TEXT
	)`); err != nil {
		t.Fatal(err)
	}
	runID, entityID := uuid.NewString(), uuid.NewString()
	createdAt := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	before := runtimemutationlog.EntityStateProjection{}
	after := runtimemutationlog.EntityStateProjection{CurrentState: "active", Fields: map[string]any{"score": json.Number("7")}}
	writer := runtimemutationlog.Writer{Type: "agent", ID: "entity-writer", HandlerStep: "create_entity"}
	var facts []string
	var drafts []runtimeauthoractivity.Draft
	insert := func(tx *sql.Tx, old, next runtimemutationlog.EntityStateProjection) error {
		return insertSQLiteEntityStateDiff(context.Background(), tx, runID, entityID, old, next, writer, createdAt,
			func(gotRunID string, family privaterunforkrevision.Family, key string) error {
				if gotRunID != runID || family != privaterunforkrevision.FamilyEntityMutations {
					t.Fatalf("wrong exact fact: %q %q %q", gotRunID, family, key)
				}
				facts = append(facts, key)
				return nil
			}, func(_ context.Context, draft runtimeauthoractivity.Draft) error {
				drafts = append(drafts, draft)
				return nil
			})
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := insert(tx, before, after); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if len(facts) != 2 || len(drafts) != 1 || drafts[0].RunID != runID || drafts[0].EntityID != entityID {
		t.Fatalf("unexpected contributions: facts=%v drafts=%v", facts, drafts)
	}
	if err := insert(tx, after, after); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if len(facts) != 2 || len(drafts) != 1 {
		t.Fatalf("no-op emitted contributions: facts=%v drafts=%v", facts, drafts)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(`SELECT mutation_id, run_id, entity_id, created_at FROM entity_mutations ORDER BY mutation_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	seen := map[string]bool{}
	for rows.Next() {
		var id, gotRunID, gotEntityID, at string
		if err := rows.Scan(&id, &gotRunID, &gotEntityID, &at); err != nil {
			t.Fatal(err)
		}
		if gotRunID != runID || gotEntityID != entityID || at == "" {
			t.Fatalf("wrong persisted mutation: %q %q %q %q", id, gotRunID, gotEntityID, at)
		}
		seen[id] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for _, fact := range facts {
		if !seen[fact] {
			t.Fatalf("exact fact %q missing from durable mutations", fact)
		}
	}
	if len(seen) != len(facts) {
		t.Fatalf("durable mutations=%d, exact facts=%d", len(seen), len(facts))
	}

	rollbackTx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := insert(rollbackTx, before, after); err != nil {
		_ = rollbackTx.Rollback()
		t.Fatal(err)
	}
	if err := rollbackTx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM entity_mutations`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("rolled-back mutation persisted: count=%d", count)
	}
}

func TestEntityJSONArgPreservesRawBytes(t *testing.T) {
	for _, input := range []any{json.RawMessage(`{"score":7}`), []byte(`{"score":7}`)} {
		got, err := toolJSONSQLArg(input)
		if err != nil || got != `{"score":7}` {
			t.Fatalf("JSON argument for %T = %v, %v", input, got, err)
		}
	}
}

func TestEntityWritesRetainSchemaGuardBeforeMutation(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	backend, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	refused := errors.New("schema is not current")
	owner, err := NewSQLite(backend, func() error { return refused }, nil)
	if err != nil {
		t.Fatal(err)
	}
	runID, entityID := uuid.NewString(), uuid.NewString()
	create := runtimetools.EntityCreateRecord{RunID: runID, EntityID: entityID, FlowInstance: "root", EntityType: "task", CurrentState: "active"}
	if _, err := owner.CreateEntity(context.Background(), create); !errors.Is(err, refused) {
		t.Fatalf("create bypassed schema guard: %v", err)
	}
	update := runtimetools.EntityFieldUpdate{RunID: runID, EntityID: entityID, FieldPath: "score", ValueJSON: json.RawMessage(`7`)}
	if _, err := owner.SaveEntityField(context.Background(), update); !errors.Is(err, refused) {
		t.Fatalf("field update bypassed schema guard: %v", err)
	}
	owner.schemaGuard = nil
	if _, err := owner.CreateEntity(context.Background(), create); err == nil || !strings.Contains(err.Error(), "entity sqlite owner is required") {
		t.Fatalf("missing schema guard was not refused: %v", err)
	}
	if _, err := owner.SaveEntityField(context.Background(), update); err == nil || !strings.Contains(err.Error(), "entity sqlite owner is required") {
		t.Fatalf("missing field schema guard was not refused: %v", err)
	}
}
