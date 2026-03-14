package tools_test

import (
	"context"
	"strings"
	"testing"

	runtimecontracts "empireai/internal/runtime/contracts"
	models "empireai/internal/runtime/core/actors"
	"empireai/internal/runtime/semanticview"
	runtimetools "empireai/internal/runtime/tools"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestEntityTools_HappyPath(t *testing.T) {
	ctx, exec := newEntityToolTestExecutor(t)
	entityID := uuid.NewString()

	out, err := exec.Execute(ctx, "create_entity", map[string]any{
		"entity_type": "accounts",
		"entity_id":   entityID,
		"fields": map[string]any{
			"status":   "open",
			"score":    42.5,
			"priority": 3,
			"active":   true,
			"metadata": map[string]any{"region": "us"},
		},
	})
	if err != nil {
		t.Fatalf("create_entity: %v", err)
	}
	created, ok := out.(map[string]any)
	if !ok || strings.TrimSpace(asString(created["entity_id"])) != entityID {
		t.Fatalf("unexpected create_entity output: %#v", out)
	}

	got, err := exec.Execute(ctx, "get_entity", map[string]any{
		"entity_type": "accounts",
		"entity_id":   entityID,
	})
	if err != nil {
		t.Fatalf("get_entity: %v", err)
	}
	entity, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected entity map, got %#v", got)
	}
	if strings.TrimSpace(asString(entity["status"])) != "open" {
		t.Fatalf("expected status=open, got %#v", entity)
	}

	if _, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_type": "accounts",
		"entity_id":   entityID,
		"field":       "status",
		"value":       "closed",
	}); err != nil {
		t.Fatalf("save_entity_field: %v", err)
	}

	searchOut, err := exec.Execute(ctx, "search_entities", map[string]any{
		"entity_type": "accounts",
		"filter":      "status = 'closed' AND score > 40",
		"select":      []string{"entity_id", "status", "score"},
		"limit":       10,
	})
	if err != nil {
		t.Fatalf("search_entities: %v", err)
	}
	results, ok := searchOut.([]map[string]any)
	if !ok {
		t.Fatalf("expected search result slice, got %#v", searchOut)
	}
	if len(results) != 1 || strings.TrimSpace(asString(results[0]["entity_id"])) != entityID {
		t.Fatalf("unexpected search results: %#v", results)
	}

	metricOut, err := exec.Execute(ctx, "query_metrics", map[string]any{
		"entity_type": "accounts",
		"metric":      "count",
		"group_by":    "status",
	})
	if err != nil {
		t.Fatalf("query_metrics: %v", err)
	}
	metricResult, ok := metricOut.(map[string]any)
	if !ok {
		t.Fatalf("expected metric result map, got %#v", metricOut)
	}
	grouped, ok := metricResult["result"].(map[string]any)
	if !ok || grouped["closed"] == nil {
		t.Fatalf("expected grouped metrics, got %#v", metricOut)
	}
}

func TestEntityTools_InvalidEntityType(t *testing.T) {
	ctx, exec := newEntityToolTestExecutor(t)
	_, err := exec.Execute(ctx, "get_entity", map[string]any{
		"entity_type": "missing",
		"entity_id":   uuid.NewString(),
	})
	if err == nil || !strings.Contains(err.Error(), `unknown entity_type "missing"`) {
		t.Fatalf("expected invalid entity_type error, got %v", err)
	}
}

func TestEntityTools_InvalidField(t *testing.T) {
	ctx, exec := newEntityToolTestExecutor(t)
	_, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_type": "accounts",
		"entity_id":   uuid.NewString(),
		"field":       "unknown_field",
		"value":       "x",
	})
	if err == nil || !strings.Contains(err.Error(), "does not define field unknown_field") {
		t.Fatalf("expected invalid field error, got %v", err)
	}
}

func TestEntityTools_GetEntityNotFound(t *testing.T) {
	ctx, exec := newEntityToolTestExecutor(t)
	_, err := exec.Execute(ctx, "get_entity", map[string]any{
		"entity_type": "accounts",
		"entity_id":   uuid.NewString(),
	})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected not found error, got %v", err)
	}
}

func TestEntityTools_CreateEntityDuplicate(t *testing.T) {
	ctx, exec := newEntityToolTestExecutor(t)
	entityID := uuid.NewString()
	input := map[string]any{
		"entity_type": "accounts",
		"entity_id":   entityID,
		"fields": map[string]any{
			"status": "open",
			"score":  10.0,
			"active": true,
		},
	}
	if _, err := exec.Execute(ctx, "create_entity", input); err != nil {
		t.Fatalf("create_entity first: %v", err)
	}
	if _, err := exec.Execute(ctx, "create_entity", input); err == nil {
		t.Fatal("expected duplicate create_entity error")
	}
}

func newEntityToolTestExecutor(t *testing.T) (context.Context, *runtimetools.Executor) {
	t.Helper()
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	schema := runtimecontracts.EntitySchema{
		Groups: []runtimecontracts.EntitySchemaGroup{{
			Name: "accounts",
			Fields: []runtimecontracts.EntitySchemaField{
				{Name: "status", Type: "string", Indexed: true},
				{Name: "score", Type: "numeric(10,2)"},
				{Name: "priority", Type: "integer", Nullable: true},
				{Name: "active", Type: "boolean"},
				{Name: "metadata", Type: "jsonb", Nullable: true},
			},
		}},
	}
	plans, err := store.GenerateEntityTableDDLs(schema)
	if err != nil {
		t.Fatalf("GenerateEntityTableDDLs: %v", err)
	}
	if err := pg.EnsureSchemaTables(context.Background(), plans); err != nil {
		t.Fatalf("EnsureSchemaTables: %v", err)
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			EntitySchema: schema,
		},
	}
	exec := runtimetools.NewExecutorWithOptions(nil, nil, runtimetools.ExecutorOptions{
		SQLDB:          db,
		WorkflowSource: semanticview.Wrap(bundle),
	})
	actor := models.AgentConfig{ID: "tester", Role: "operator"}
	return runtimetools.WithActor(context.Background(), actor), exec
}

func asString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	default:
		return ""
	}
}
