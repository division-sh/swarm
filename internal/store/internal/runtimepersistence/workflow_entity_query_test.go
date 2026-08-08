package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/entityquery"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestWorkflowEntityQueryProjectionOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			owner, db, ctx, runID := openWorkflowEntityQueryBackend(t, backend)
			seedWorkflowEntityQueryRow(t, backend, db, runID, "child/one", map[string]any{"request_id": "req-1", "score": 2})
			seedWorkflowEntityQueryRow(t, backend, db, runID, "child/two", map[string]any{"request_id": "req-2", "score": 5})
			seedWorkflowEntityQueryRow(t, backend, db, runID, "sibling/outside", map[string]any{"request_id": "req-1", "score": 9})

			source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
			contract := entityruntime.Contract{
				FlowID:     "child",
				EntityType: "child_entity",
				Entity: runtimecontracts.EntityContract{Fields: map[string]runtimecontracts.EntityFieldDecl{
					"request_id": {Type: "text"},
					"score":      {Type: "integer"},
				}},
			}
			count, err := owner.CountWorkflowEntities(ctx, entityquery.Request{
				RunID: runID, Source: source, Contract: contract,
				Predicate: entityquery.Predicate{Field: "request_id", Op: "==", Value: "req-1"},
			})
			if err != nil {
				t.Fatalf("count workflow entities: %v", err)
			}
			if count != 1 {
				t.Fatalf("count = %d, want one in-scope entity", count)
			}

			count, err = owner.CountWorkflowEntities(ctx, entityquery.Request{
				RunID: runID, Source: source, Contract: contract,
				Predicate: entityquery.Predicate{Field: "score", Op: ">", Value: float64(2)},
			})
			if err != nil {
				t.Fatalf("count numeric workflow entities: %v", err)
			}
			if count != 1 {
				t.Fatalf("numeric count = %d, want one in-scope entity", count)
			}
		})
	}
}

type workflowEntityQueryOwner interface {
	CountWorkflowEntities(context.Context, entityquery.Request) (int, error)
}

func openWorkflowEntityQueryBackend(t *testing.T, backend string) (workflowEntityQueryOwner, *sql.DB, context.Context, string) {
	t.Helper()
	runID := uuid.NewString()
	ctx := context.Background()
	switch backend {
	case "sqlite":
		selected := newBootstrappedSQLiteRuntimeStoreForTest(t)
		requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, StartedAt: time.Now().UTC()})
		return selected, selected.backend.ConstructionHandle(), ctx, runID
	case "postgres":
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		selected := admitTestPostgresStore(t, db)
		requireRunningRunForTest(t, ctx, selected, runID, time.Now().UTC())
		return selected, db, ctx, runID
	default:
		t.Fatalf("unsupported backend %q", backend)
		return nil, nil, nil, ""
	}
}

func seedWorkflowEntityQueryRow(t *testing.T, backend string, db *sql.DB, runID, flowInstance string, fields map[string]any) {
	t.Helper()
	entityID := uuid.NewString()
	raw, err := json.Marshal(fields)
	if err != nil {
		t.Fatalf("marshal workflow entity fields: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	var query string
	var args []any
	switch backend {
	case "sqlite":
		query = `INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, current_state, gates, fields, accumulator, revision, entered_state_at, created_at, updated_at) VALUES (?, ?, ?, 'child_entity', 'ready', '{}', ?, '{}', 1, ?, ?, ?)`
		args = []any{runID, entityID, flowInstance, string(raw), now, now, now}
	case "postgres":
		query = `INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, current_state, gates, fields, accumulator, revision, entered_state_at, created_at, updated_at) VALUES ($1::uuid, $2::uuid, $3, 'child_entity', 'ready', '{}'::jsonb, $4::jsonb, '{}'::jsonb, 1, $5, $5, $5)`
		args = []any{runID, entityID, flowInstance, string(raw), now}
	default:
		t.Fatalf("unsupported backend %q", backend)
	}
	if _, err := db.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatalf("seed workflow entity query row: %v", err)
	}
}
