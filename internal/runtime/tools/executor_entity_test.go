package tools_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	runtimecontracts "swarm/internal/runtime/contracts"
	models "swarm/internal/runtime/core/actors"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
	runtimetools "swarm/internal/runtime/tools"
	"swarm/internal/testutil"
)

func TestEntityTools_HappyPath(t *testing.T) {
	ctx, exec := newEntityToolTestExecutor(t)
	entityID := uuid.NewString()

	out, err := exec.Execute(ctx, "create_entity", map[string]any{
		"entity_id":     entityID,
		"flow_instance": "review/inst-1",
		"entity_type":   "accounts",
		"name":          "Acme",
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
	if got := strings.TrimSpace(asString(created["subject_id"])); got != entityID {
		t.Fatalf("create_entity subject_id = %q, want %q", got, entityID)
	}
	if got := strings.TrimSpace(asString(created["current_state"])); got != "queued" {
		t.Fatalf("create_entity current_state = %q, want queued", got)
	}

	got, err := exec.Execute(ctx, "get_entity", map[string]any{
		"entity_id": entityID,
	})
	if err != nil {
		t.Fatalf("get_entity: %v", err)
	}
	entity, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected entity map, got %#v", got)
	}
	if strings.TrimSpace(asString(entity["flow_instance"])) != "review/inst-1" {
		t.Fatalf("flow_instance = %#v, want review/inst-1", entity["flow_instance"])
	}
	if got := strings.TrimSpace(asString(entity["subject_id"])); got != entityID {
		t.Fatalf("subject_id = %q, want %q", got, entityID)
	}
	fields, ok := entity["fields"].(map[string]any)
	if !ok || strings.TrimSpace(asString(fields["status"])) != "open" {
		t.Fatalf("expected fields.status=open, got %#v", entity)
	}
	metadata, ok := fields["metadata"].(map[string]any)
	if !ok || strings.TrimSpace(asString(metadata["region"])) != "us" {
		t.Fatalf("expected fields.metadata.region=us, got %#v", fields["metadata"])
	}

	saveOut, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_id": entityID,
		"field":     "status",
		"value":     "closed",
	})
	if err != nil {
		t.Fatalf("save_entity_field: %v", err)
	}
	saved, ok := saveOut.(map[string]any)
	if !ok || saved["revision"] == nil {
		t.Fatalf("unexpected save_entity_field output: %#v", saveOut)
	}

	searchOut, err := exec.Execute(ctx, "search_entities", map[string]any{
		"flow_instance": "review/inst-1",
		"current_state": "queued",
		"filter": map[string]any{
			"status": "closed",
		},
		"limit":  10,
		"offset": 0,
	})
	if err != nil {
		t.Fatalf("search_entities: %v", err)
	}
	searchResult, ok := searchOut.(map[string]any)
	if !ok {
		t.Fatalf("expected search result map, got %#v", searchOut)
	}
	results, ok := searchResult["results"].([]map[string]any)
	if !ok {
		t.Fatalf("expected search results slice, got %#v", searchResult["results"])
	}
	if len(results) != 1 || strings.TrimSpace(asString(results[0]["entity_id"])) != entityID {
		t.Fatalf("unexpected search results: %#v", results)
	}
	if total, ok := searchResult["total"].(int); !ok || total != 1 {
		t.Fatalf("search total = %#v, want 1", searchResult["total"])
	}

	queryOut, err := exec.Execute(ctx, "query_entities", map[string]any{
		"entity_type": "accounts",
		"filter":      `status == "closed"`,
		"select":      []string{"current_state", "status"},
		"limit":       10,
	})
	if err != nil {
		t.Fatalf("query_entities: %v", err)
	}
	queryResult, ok := queryOut.(map[string]any)
	if !ok {
		t.Fatalf("expected query_entities result map, got %#v", queryOut)
	}
	queryResults, ok := queryResult["results"].([]map[string]any)
	if !ok {
		t.Fatalf("expected query_entities results slice, got %#v", queryResult["results"])
	}
	if len(queryResults) != 1 || strings.TrimSpace(asString(queryResults[0]["status"])) != "closed" {
		t.Fatalf("unexpected query_entities results: %#v", queryResults)
	}

	groupedOut, err := exec.Execute(ctx, "query_entities", map[string]any{
		"filter":   `status == "closed"`,
		"group_by": "status",
		"limit":    10,
	})
	if err != nil {
		t.Fatalf("query_entities grouped: %v", err)
	}
	groupedResult, ok := groupedOut.(map[string]any)
	if !ok {
		t.Fatalf("expected grouped query result map, got %#v", groupedOut)
	}
	groupedRows, ok := groupedResult["results"].([]map[string]any)
	if !ok || len(groupedRows) != 1 || strings.TrimSpace(asString(groupedRows[0]["group_key"])) != "closed" {
		t.Fatalf("unexpected grouped query results: %#v", groupedResult["results"])
	}

	metricOut, err := exec.Execute(ctx, "query_metrics", map[string]any{
		"metric":   "count",
		"group_by": "status",
	})
	if err != nil {
		t.Fatalf("query_metrics: %v", err)
	}
	metricResult, ok := metricOut.(map[string]any)
	if !ok {
		t.Fatalf("expected metric result map, got %#v", metricOut)
	}
	groups, ok := metricResult["groups"].([]map[string]any)
	if !ok || len(groups) != 1 || strings.TrimSpace(asString(groups[0]["group_key"])) != "closed" {
		t.Fatalf("expected grouped metrics, got %#v", metricOut)
	}
}

func TestEntityTools_SaveEntityField_JSONBRoundTripsPlainTextWithoutBase64(t *testing.T) {
	ctx, exec := newEntityToolTestExecutor(t)
	entityID := uuid.NewString()
	if _, err := exec.Execute(ctx, "create_entity", map[string]any{
		"entity_id":     entityID,
		"flow_instance": "review/inst-1",
		"fields": map[string]any{
			"status": "open",
			"score":  10.0,
			"active": true,
		},
	}); err != nil {
		t.Fatalf("create_entity: %v", err)
	}

	const brief = "BUSINESS BRIEF - sample plain text"
	if _, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_id": entityID,
		"field":     "metadata",
		"value":     brief,
	}); err != nil {
		t.Fatalf("save_entity_field metadata: %v", err)
	}

	got, err := exec.Execute(ctx, "get_entity", map[string]any{"entity_id": entityID})
	if err != nil {
		t.Fatalf("get_entity: %v", err)
	}
	entity, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected entity map, got %#v", got)
	}
	fields, ok := entity["fields"].(map[string]any)
	if !ok {
		t.Fatalf("expected fields map, got %#v", entity["fields"])
	}
	if got := strings.TrimSpace(asString(fields["metadata"])); got != brief {
		t.Fatalf("metadata = %q, want %q", got, brief)
	}
	if strings.HasPrefix(strings.TrimSpace(asString(fields["metadata"])), "Ik") {
		t.Fatalf("metadata appears base64-encoded: %q", fields["metadata"])
	}
}

func TestEntityTools_GetEntityReturnsStoredCurrentState(t *testing.T) {
	ctx, exec, db := newEntityToolTestHarness(t)
	entityID := uuid.NewString()
	if _, err := exec.Execute(ctx, "create_entity", map[string]any{
		"entity_id":     entityID,
		"flow_instance": "review/inst-1",
		"fields": map[string]any{
			"status": "open",
			"score":  10.0,
			"active": true,
		},
	}); err != nil {
		t.Fatalf("create_entity: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE entity_state
		SET current_state = 'marginal_review'
		WHERE entity_id = $1::uuid
	`, entityID); err != nil {
		t.Fatalf("seed current_state: %v", err)
	}

	out, err := exec.Execute(ctx, "get_entity", map[string]any{"entity_id": entityID})
	if err != nil {
		t.Fatalf("get_entity: %v", err)
	}
	entity, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("expected entity map, got %#v", out)
	}
	if got := strings.TrimSpace(asString(entity["current_state"])); got != "marginal_review" {
		t.Fatalf("current_state = %q, want marginal_review", got)
	}
}

func TestEntityTools_SearchAndQueryUseStoredCurrentState(t *testing.T) {
	ctx, exec, db := newEntityToolTestHarness(t)
	entityID := uuid.NewString()
	if _, err := exec.Execute(ctx, "create_entity", map[string]any{
		"entity_id":     entityID,
		"flow_instance": "review/inst-1",
		"entity_type":   "accounts",
		"fields": map[string]any{
			"status": "open",
			"score":  10.0,
			"active": true,
		},
	}); err != nil {
		t.Fatalf("create_entity: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE entity_state
		SET current_state = 'marginal_review'
		WHERE entity_id = $1::uuid
	`, entityID); err != nil {
		t.Fatalf("seed current_state: %v", err)
	}

	searchOut, err := exec.Execute(ctx, "search_entities", map[string]any{
		"current_state": "marginal_review",
		"limit":         10,
		"offset":        0,
	})
	if err != nil {
		t.Fatalf("search_entities: %v", err)
	}
	searchResult, ok := searchOut.(map[string]any)
	if !ok {
		t.Fatalf("expected search result map, got %#v", searchOut)
	}
	results, ok := searchResult["results"].([]map[string]any)
	if !ok || len(results) != 1 {
		t.Fatalf("unexpected search results: %#v", searchResult["results"])
	}
	if got := strings.TrimSpace(asString(results[0]["current_state"])); got != "marginal_review" {
		t.Fatalf("search result current_state = %q, want marginal_review", got)
	}

	queryOut, err := exec.Execute(ctx, "query_entities", map[string]any{
		"filter": `current_state == "marginal_review"`,
		"select": []string{"current_state"},
		"limit":  10,
	})
	if err != nil {
		t.Fatalf("query_entities: %v", err)
	}
	queryResult, ok := queryOut.(map[string]any)
	if !ok {
		t.Fatalf("expected query result map, got %#v", queryOut)
	}
	queryResults, ok := queryResult["results"].([]map[string]any)
	if !ok || len(queryResults) != 1 {
		t.Fatalf("unexpected query results: %#v", queryResult["results"])
	}
}

func TestEntityTools_GetSubjectStatusAggregatesFlowLocalEntities(t *testing.T) {
	ctx, exec := newEntityToolTestExecutor(t)
	subjectID := uuid.NewString()
	scoringID := uuid.NewString()
	validationID := uuid.NewString()
	if _, err := exec.Execute(ctx, "create_entity", map[string]any{
		"entity_id":     scoringID,
		"subject_id":    subjectID,
		"flow_instance": "scoring",
		"initial_state": "marginal_review",
		"fields": map[string]any{
			"status": "open",
		},
	}); err != nil {
		t.Fatalf("create_entity scoring: %v", err)
	}
	if _, err := exec.Execute(ctx, "create_entity", map[string]any{
		"entity_id":     validationID,
		"subject_id":    subjectID,
		"flow_instance": "validation",
		"initial_state": "researching",
		"fields": map[string]any{
			"status": "open",
		},
	}); err != nil {
		t.Fatalf("create_entity validation: %v", err)
	}

	out, err := exec.Execute(ctx, "get_subject_status", map[string]any{"subject_id": subjectID})
	if err != nil {
		t.Fatalf("get_subject_status: %v", err)
	}
	status, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("expected status map, got %#v", out)
	}
	if got := strings.TrimSpace(asString(status["subject_id"])); got != subjectID {
		t.Fatalf("subject_id = %q, want %q", got, subjectID)
	}
	entities, ok := status["entities"].([]map[string]any)
	if !ok || len(entities) != 2 {
		t.Fatalf("entities = %#v, want 2 rows", status["entities"])
	}
	if got := strings.TrimSpace(asString(status["latest_flow"])); got == "" {
		t.Fatalf("latest_flow empty in %#v", status)
	}
	if got := strings.TrimSpace(asString(status["latest_state"])); got == "" {
		t.Fatalf("latest_state empty in %#v", status)
	}
	if allTerminal, ok := status["all_terminal"].(bool); !ok || allTerminal {
		t.Fatalf("all_terminal = %#v, want false", status["all_terminal"])
	}
}

func TestEntityTools_InvalidField(t *testing.T) {
	ctx, exec := newEntityToolTestExecutor(t)
	_, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_id": uuid.NewString(),
		"field":     "unknown_field",
		"value":     "x",
	})
	if err == nil || !errors.Is(err, runtimetools.ErrUnknownEntityField) {
		t.Fatalf("expected invalid field error, got %v", err)
	}
}

func TestEntityTools_GetEntityNotFound(t *testing.T) {
	ctx, exec := newEntityToolTestExecutor(t)
	_, err := exec.Execute(ctx, "get_entity", map[string]any{
		"entity_id": uuid.NewString(),
	})
	re, ok := runtimetools.AsRuntimeError(err)
	if err == nil || !ok || re.Code != "not_found" {
		t.Fatalf("expected not found error, got %v", err)
	}
}

func TestEntityTools_CreateEntityDuplicate(t *testing.T) {
	ctx, exec := newEntityToolTestExecutor(t)
	entityID := uuid.NewString()
	input := map[string]any{
		"entity_id":     entityID,
		"flow_instance": "review/inst-1",
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

func TestEntityTools_ConstrainedAllowedToolsStillPermitOnlyUniversalEntityTools(t *testing.T) {
	ctx, exec := newEntityToolTestExecutorWithActor(t, models.AgentConfig{
		ID:   "tester",
		Role: "operator",
		Config: mustJSONRaw(t, map[string]any{
			"tools": []string{"emit_something"},
		}),
	})
	entityID := uuid.NewString()
	if _, err := exec.Execute(ctx, "create_entity", map[string]any{
		"entity_id":     entityID,
		"flow_instance": "review/inst-1",
		"fields": map[string]any{
			"status": "open",
			"score":  11.0,
			"active": true,
		},
	}); err == nil {
		t.Fatalf("expected create_entity to be denied when not listed in tools")
	}
	if _, err := exec.Execute(ctx, "query_entities", map[string]any{}); err != nil {
		t.Fatalf("query_entities with constrained tools: %v", err)
	}
}

func TestEntityTools_NoSchemaAcceptsArbitraryFieldNames(t *testing.T) {
	ctx, exec := newEntityToolTestExecutorWithBundle(t, models.AgentConfig{
		ID:   "tester",
		Role: "operator",
		Config: mustJSONRaw(t, map[string]any{
			"tools": []string{"create_entity", "save_entity_field", "get_entity"},
		}),
	}, &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			InitialStage: "queued",
		},
	})
	entityID := uuid.NewString()
	if _, err := exec.Execute(ctx, "create_entity", map[string]any{
		"entity_id":     entityID,
		"flow_instance": "review/inst-1",
		"fields": map[string]any{
			"custom_flag": "x",
		},
	}); err != nil {
		t.Fatalf("create_entity without schema: %v", err)
	}
	if _, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_id": entityID,
		"field":     "another_custom_field",
		"value":     7,
	}); err != nil {
		t.Fatalf("save_entity_field without schema: %v", err)
	}
}

func TestEntityTools_SaveEntityFieldRejectsCrossFlowWrite(t *testing.T) {
	bundle := loadEntityToolFixtureBundle(t, "tests/tier11-flow-composition/test-required-agents-child")
	ctx, exec, _ := newEntityToolTestHarnessWithBundle(t, models.AgentConfig{
		ID:   "analyzer",
		Role: "analyzer",
		Config: mustJSONRaw(t, map[string]any{
			"tools": []string{"create_entity", "save_entity_field", "get_entity"},
		}),
	}, bundle)
	entityID := uuid.NewString()
	if _, err := exec.Execute(ctx, "create_entity", map[string]any{
		"entity_id":     entityID,
		"flow_instance": "other-flow/inst-1",
		"fields": map[string]any{
			"status": "open",
		},
	}); err != nil {
		t.Fatalf("create_entity: %v", err)
	}

	_, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_id": entityID,
		"field":     "status",
		"value":     "closed",
	})
	re, ok := runtimetools.AsRuntimeError(err)
	if err == nil || !ok || re.Code != "cross_flow_write_forbidden" {
		t.Fatalf("expected cross_flow_write_forbidden, got %v", err)
	}
}

func TestEntityTools_SaveEntityFieldAllowsSameFlowWrite(t *testing.T) {
	bundle := loadEntityToolFixtureBundle(t, "tests/tier11-flow-composition/test-required-agents-child")
	ctx, exec, _ := newEntityToolTestHarnessWithBundle(t, models.AgentConfig{
		ID:   "analyzer",
		Role: "analyzer",
		Config: mustJSONRaw(t, map[string]any{
			"tools": []string{"create_entity", "save_entity_field", "get_entity"},
		}),
	}, bundle)
	entityID := uuid.NewString()
	if _, err := exec.Execute(ctx, "create_entity", map[string]any{
		"entity_id":     entityID,
		"flow_instance": "analyzer-flow/inst-1",
		"fields": map[string]any{
			"status": "open",
		},
	}); err != nil {
		t.Fatalf("create_entity: %v", err)
	}

	if _, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_id": entityID,
		"field":     "status",
		"value":     "closed",
	}); err != nil {
		t.Fatalf("save_entity_field same flow: %v", err)
	}
}

func newEntityToolTestExecutor(t *testing.T) (context.Context, *runtimetools.Executor) {
	t.Helper()
	ctx, exec, _ := newEntityToolTestHarnessWithActor(t, models.AgentConfig{
		ID:   "tester",
		Role: "operator",
		Config: mustJSONRaw(t, map[string]any{
			"tools": []string{"create_entity", "get_entity", "get_subject_status", "save_entity_field", "search_entities", "query_entities", "query_metrics"},
		}),
	})
	return ctx, exec
}

func newEntityToolTestExecutorWithActor(t *testing.T, actor models.AgentConfig) (context.Context, *runtimetools.Executor) {
	t.Helper()
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			InitialStage: "queued",
			EntitySchema: runtimecontracts.EntitySchema{
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
			},
		},
	}
	ctx, exec, _ := newEntityToolTestHarnessWithBundle(t, actor, bundle)
	return ctx, exec
}

func newEntityToolTestExecutorWithBundle(t *testing.T, actor models.AgentConfig, bundle *runtimecontracts.WorkflowContractBundle) (context.Context, *runtimetools.Executor) {
	t.Helper()
	ctx, exec, _ := newEntityToolTestHarnessWithBundle(t, actor, bundle)
	return ctx, exec
}

func newEntityToolTestHarness(t *testing.T) (context.Context, *runtimetools.Executor, *sql.DB) {
	t.Helper()
	return newEntityToolTestHarnessWithActor(t, models.AgentConfig{
		ID:   "tester",
		Role: "operator",
		Config: mustJSONRaw(t, map[string]any{
			"tools": []string{"create_entity", "get_entity", "get_subject_status", "save_entity_field", "search_entities", "query_entities", "query_metrics"},
		}),
	})
}

func newEntityToolTestHarnessWithActor(t *testing.T, actor models.AgentConfig) (context.Context, *runtimetools.Executor, *sql.DB) {
	t.Helper()
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			InitialStage: "queued",
			EntitySchema: runtimecontracts.EntitySchema{
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
			},
		},
	}
	return newEntityToolTestHarnessWithBundle(t, actor, bundle)
}

func newEntityToolTestHarnessWithBundle(t *testing.T, actor models.AgentConfig, bundle *runtimecontracts.WorkflowContractBundle) (context.Context, *runtimetools.Executor, *sql.DB) {
	t.Helper()
	_, db, _ := testutil.StartPostgres(t)
	exec := runtimetools.NewExecutorWithOptions(nil, nil, runtimetools.ExecutorOptions{
		SQLDB:          db,
		WorkflowSource: semanticview.Wrap(bundle),
	})
	return runtimetools.WithActor(context.Background(), actor), exec, db
}

func asString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	default:
		return ""
	}
}

func mustJSONRaw(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return raw
}

func loadEntityToolFixtureBundle(t *testing.T, fixtureRoot string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, filepath.Join(repoRoot, fixtureRoot), platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides(%s): %v", fixtureRoot, err)
	}
	return bundle
}
