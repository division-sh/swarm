package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"sort"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/entityquery"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
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

func TestWorkflowEntityCollectionIncludesStateOnlyRowsOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, db, ctx, runID := openStateOnlyAcquisitionStore(t, backend)
			reader, ok := selected.(interface {
				QueryWorkflowEntityCollection(context.Context, runtimepipeline.WorkflowEntityCollectionOwner) ([]runtimepipeline.WorkflowEntityStatePersistenceRecord, error)
			})
			if !ok {
				t.Fatalf("%s selected store has no workflow entity collection operation", backend)
			}
			source := stateOnlyAcquisitionSourceWithMode(t, "child", runtimecontracts.FlowModeTemplate)
			owner, err := runtimepipeline.AdmitWorkflowEntityCollectionOwner(source, "child", "review_item", runID)
			if err != nil {
				t.Fatalf("admit workflow entity collection owner: %v", err)
			}

			seedWorkflowEntityQueryRowAs(t, backend, db, runID, "child/state-only", "review_item", map[string]any{"account_id": "state-only"})
			seedWorkflowEntityQueryRowAs(t, backend, db, runID, "child/materialized", "review_item", map[string]any{"account_id": "materialized"})
			seedStateOnlyAcquisitionLifecycle(t, backend, db, "child/materialized", "active")
			seedWorkflowEntityQueryRowAs(t, backend, db, runID, "child/terminated", "review_item", map[string]any{"account_id": "terminated"})
			seedStateOnlyAcquisitionLifecycle(t, backend, db, "child/terminated", "terminated")
			seedWorkflowEntityQueryRowAs(t, backend, db, runID, "sibling/outside", "review_item", map[string]any{"account_id": "wrong-flow"})
			seedWorkflowEntityQueryRowAs(t, backend, db, runID, "child/wrong-type", "other_item", map[string]any{"account_id": "wrong-type"})

			wrongRunID := uuid.NewString()
			requireRunningRunForTest(t, context.Background(), selected, wrongRunID, time.Now().UTC())
			seedWorkflowEntityQueryRowAs(t, backend, db, wrongRunID, "child/wrong-run", "review_item", map[string]any{"account_id": "wrong-run"})

			records, err := reader.QueryWorkflowEntityCollection(ctx, owner)
			if err != nil {
				t.Fatalf("query workflow entity collection: %v", err)
			}
			got := make([]string, 0, len(records))
			for _, record := range records {
				var fields map[string]any
				if err := json.Unmarshal(record.Fields, &fields); err != nil {
					t.Fatalf("decode returned fields: %v", err)
				}
				got = append(got, fields["account_id"].(string))
			}
			sort.Strings(got)
			if !reflect.DeepEqual(got, []string{"materialized", "state-only"}) {
				t.Fatalf("entity collection = %#v, want state-only and materialized rows only", got)
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
	seedWorkflowEntityQueryRowAs(t, backend, db, runID, flowInstance, "child_entity", fields)
}

func seedWorkflowEntityQueryRowAs(t *testing.T, backend string, db *sql.DB, runID, flowInstance, entityType string, fields map[string]any) {
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
		query = `INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, current_state, gates, fields, accumulator, revision, entered_state_at, created_at, updated_at) VALUES (?, ?, ?, ?, 'ready', '{}', ?, '{}', 1, ?, ?, ?)`
		args = []any{runID, entityID, flowInstance, entityType, string(raw), now, now, now}
	case "postgres":
		query = `INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, current_state, gates, fields, accumulator, revision, entered_state_at, created_at, updated_at) VALUES ($1::uuid, $2::uuid, $3, $4, 'ready', '{}'::jsonb, $5::jsonb, '{}'::jsonb, 1, $6, $6, $6)`
		args = []any{runID, entityID, flowInstance, entityType, string(raw), now}
	default:
		t.Fatalf("unsupported backend %q", backend)
	}
	if _, err := db.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatalf("seed workflow entity query row: %v", err)
	}
}
