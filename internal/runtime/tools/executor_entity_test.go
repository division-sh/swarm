package tools_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

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
	entityID := mustCreateEntityID(t, ctx, exec, map[string]any{
		"flow_instance": "review/inst-1",
		"name":          "Acme",
		"fields": map[string]any{
			"status":   "open",
			"score":    42.5,
			"priority": 3,
			"active":   true,
			"metadata": map[string]any{"region": "us"},
		},
	})
	out, err := exec.Execute(ctx, "get_entity", map[string]any{"entity_id": entityID})
	if err != nil {
		t.Fatalf("get_entity after create_entity: %v", err)
	}
	created, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("unexpected get_entity output: %#v", out)
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
		"filter": `status == "closed"`,
		"select": []string{"current_state", "status"},
		"limit":  10,
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
	entityID := mustCreateEntityID(t, ctx, exec, map[string]any{
		"flow_instance": "review/inst-1",
		"fields": map[string]any{
			"status": "open",
			"score":  10.0,
			"active": true,
		},
	})

	const brief = "BUSINESS BRIEF - sample plain text"
	if _, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_id": entityID,
		"field":     "notes",
		"value":     brief,
	}); err != nil {
		t.Fatalf("save_entity_field notes: %v", err)
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
	if got := strings.TrimSpace(asString(fields["notes"])); got != brief {
		t.Fatalf("notes = %q, want %q", got, brief)
	}
	if strings.HasPrefix(strings.TrimSpace(asString(fields["notes"])), "Ik") {
		t.Fatalf("notes appears base64-encoded: %q", fields["notes"])
	}
}

func TestEntityTools_CreateEntityAcceptsAnnotatedJSONBFields(t *testing.T) {
	ctx, exec := newAnnotatedEntityToolExecutor(t)
	entityID := mustCreateEntityID(t, ctx, exec, map[string]any{
		"flow_instance": "validation/inst-1",
		"fields": map[string]any{
			"business_brief": map[string]any{"summary": "validated"},
			"validation_kit": map[string]any{"checklist": []any{"ux", "brand"}},
		},
	})

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
	brief, ok := fields["business_brief"].(map[string]any)
	if !ok || strings.TrimSpace(asString(brief["summary"])) != "validated" {
		t.Fatalf("business_brief = %#v, want persisted annotated jsonb object", fields["business_brief"])
	}
	kit, ok := fields["validation_kit"].(map[string]any)
	if !ok {
		t.Fatalf("validation_kit = %#v, want persisted annotated jsonb object", fields["validation_kit"])
	}
	checklist, ok := kit["checklist"].([]any)
	if !ok || len(checklist) != 2 {
		t.Fatalf("validation_kit.checklist = %#v, want persisted array", kit["checklist"])
	}
}

func TestEntityTools_SaveEntityFieldAcceptsAnnotatedJSONBFields(t *testing.T) {
	ctx, exec := newAnnotatedEntityToolExecutor(t)
	entityID := mustCreateEntityID(t, ctx, exec, map[string]any{"flow_instance": "validation/inst-1"})
	if _, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_id": entityID,
		"field":     "mvp_spec",
		"value":     map[string]any{"headline": "launch fast", "approved": true},
	}); err != nil {
		t.Fatalf("save_entity_field annotated jsonb: %v", err)
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
	spec, ok := fields["mvp_spec"].(map[string]any)
	if !ok || strings.TrimSpace(asString(spec["headline"])) != "launch fast" {
		t.Fatalf("mvp_spec = %#v, want persisted annotated jsonb object", fields["mvp_spec"])
	}
}

func TestEntityTools_SearchEntitiesAcceptsAnnotatedJSONBFilter(t *testing.T) {
	ctx, exec := newAnnotatedEntityToolExecutor(t)
	brief := map[string]any{"summary": "validated"}
	entityID := mustCreateEntityID(t, ctx, exec, map[string]any{
		"flow_instance": "validation/inst-1",
		"fields": map[string]any{
			"business_brief": brief,
		},
	})

	out, err := exec.Execute(ctx, "search_entities", map[string]any{
		"flow_instance": "validation/inst-1",
		"filter": map[string]any{
			"business_brief.summary": "validated",
		},
	})
	if err != nil {
		t.Fatalf("search_entities annotated jsonb filter: %v", err)
	}
	result, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("expected search result map, got %#v", out)
	}
	results, ok := result["results"].([]map[string]any)
	if !ok {
		t.Fatalf("expected search results slice, got %#v", result["results"])
	}
	if len(results) != 1 || strings.TrimSpace(asString(results[0]["entity_id"])) != entityID {
		t.Fatalf("unexpected search results: %#v", results)
	}
}

func TestEntityTools_SaveEntityFieldAllowsDeclaredNestedWritePath(t *testing.T) {
	ctx, exec := newAnnotatedEntityToolExecutor(t)
	entityID := mustCreateEntityID(t, ctx, exec, map[string]any{
		"flow_instance": "validation/inst-1",
		"fields": map[string]any{
			"mvp_spec": map[string]any{
				"headline": "old",
				"approved": false,
			},
		},
	})

	out, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_id": entityID,
		"field":     "mvp_spec.headline",
		"value":     "launch fast",
	})
	if err != nil {
		t.Fatalf("save_entity_field nested path: %v", err)
	}
	saved, ok := out.(map[string]any)
	if !ok || strings.TrimSpace(asString(saved["field"])) != "mvp_spec.headline" {
		t.Fatalf("unexpected save_entity_field output: %#v", out)
	}

	got, err := exec.Execute(ctx, "get_entity", map[string]any{"entity_id": entityID})
	if err != nil {
		t.Fatalf("get_entity after nested save: %v", err)
	}
	entity, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected entity map, got %#v", got)
	}
	fields, ok := entity["fields"].(map[string]any)
	if !ok {
		t.Fatalf("expected fields map, got %#v", entity["fields"])
	}
	mvpSpec, ok := fields["mvp_spec"].(map[string]any)
	if !ok {
		t.Fatalf("expected mvp_spec object, got %#v", fields["mvp_spec"])
	}
	if got := strings.TrimSpace(asString(mvpSpec["headline"])); got != "launch fast" {
		t.Fatalf("mvp_spec.headline = %q, want launch fast", got)
	}
	if approved, ok := mvpSpec["approved"].(bool); !ok || approved {
		t.Fatalf("mvp_spec.approved = %#v, want false", mvpSpec["approved"])
	}
}

func TestEntityTools_SaveEntityFieldAllowsNestedListWritePath(t *testing.T) {
	ctx, exec := newAnnotatedEntityToolExecutor(t)
	entityID := mustCreateEntityID(t, ctx, exec, map[string]any{
		"flow_instance": "validation/inst-1",
		"fields": map[string]any{
			"validation_kit": map[string]any{
				"checklist": []any{"ux"},
			},
		},
	})

	if _, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_id": entityID,
		"field":     "validation_kit.checklist",
		"value":     []any{"ux", "brand"},
	}); err != nil {
		t.Fatalf("save_entity_field nested list path: %v", err)
	}

	got, err := exec.Execute(ctx, "get_entity", map[string]any{"entity_id": entityID})
	if err != nil {
		t.Fatalf("get_entity after nested list save: %v", err)
	}
	entity, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected entity map, got %#v", got)
	}
	fields, ok := entity["fields"].(map[string]any)
	if !ok {
		t.Fatalf("expected fields map, got %#v", entity["fields"])
	}
	validationKit, ok := fields["validation_kit"].(map[string]any)
	if !ok {
		t.Fatalf("expected validation_kit object, got %#v", fields["validation_kit"])
	}
	checklist, ok := validationKit["checklist"].([]any)
	if !ok || len(checklist) != 2 {
		t.Fatalf("validation_kit.checklist = %#v, want two items", validationKit["checklist"])
	}
}

func TestEntityTools_SaveEntityFieldRejectsInvalidDottedPathsBeforePersistence(t *testing.T) {
	ctx, exec, db := newEntityToolTestHarnessWithActor(t, models.AgentConfig{
		ID:    "tester",
		Role:  "operator",
		Tools: []string{"create_entity", "get_entity", "save_entity_field"},
	})
	entityID := mustCreateEntityID(t, ctx, exec, map[string]any{
		"flow_instance": "review/inst-1",
		"fields": map[string]any{
			"status":   "open",
			"metadata": map[string]any{"region": "us"},
		},
	})

	for _, tc := range []struct {
		name  string
		field string
	}{
		{name: "undeclared nested path", field: "metadata.regoin"},
		{name: "envelope field", field: "entity_id"},
		{name: "list index path", field: "validation_kit.checklist[0]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := exec.Execute(ctx, "save_entity_field", map[string]any{
				"entity_id": entityID,
				"field":     tc.field,
				"value":     "x",
			})
			re, ok := runtimetools.AsRuntimeError(err)
			if err == nil || !ok || re.Code != "invalid_tool_input" {
				t.Fatalf("expected invalid_tool_input, got %v", err)
			}

			var mutationCount int
			if err := db.QueryRowContext(ctx, `
				SELECT COUNT(*)
				FROM entity_mutations
				WHERE entity_id = $1::uuid AND field = $2
			`, entityID, tc.field).Scan(&mutationCount); err != nil {
				t.Fatalf("count entity mutations: %v", err)
			}
			if mutationCount != 0 {
				t.Fatalf("mutation_count = %d, want 0 for %s", mutationCount, tc.field)
			}

			got, err := exec.Execute(ctx, "get_entity", map[string]any{"entity_id": entityID})
			if err != nil {
				t.Fatalf("get_entity after invalid nested save: %v", err)
			}
			entity, ok := got.(map[string]any)
			if !ok {
				t.Fatalf("expected entity map, got %#v", got)
			}
			fields, ok := entity["fields"].(map[string]any)
			if !ok {
				t.Fatalf("expected fields map, got %#v", entity["fields"])
			}
			metadata, ok := fields["metadata"].(map[string]any)
			if !ok || strings.TrimSpace(asString(metadata["region"])) != "us" {
				t.Fatalf("fields.metadata.region = %#v, want us", fields["metadata"])
			}
		})
	}
}

func TestEntityTools_SaveEntityFieldRejectsImmutableFieldUpdateAfterCreate(t *testing.T) {
	actor := models.AgentConfig{
		ID:    "tester",
		Role:  "operator",
		Tools: []string{"create_entity", "save_entity_field", "get_entity"},
	}
	bundle := loadWave1EntityToolBundle(t, actor, "review", "accounts", "", `
accounts:
  code:
    type: text
    immutable: true
  status: text
`)
	ctx, exec, _ := newEntityToolTestHarnessWithBundle(t, actor, bundle)
	entityID := mustCreateEntityID(t, ctx, exec, map[string]any{
		"flow_instance": "review/inst-1",
		"fields": map[string]any{
			"code":   "acct-001",
			"status": "open",
		},
	})

	if _, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_id": entityID,
		"field":     "code",
		"value":     "acct-002",
	}); err == nil || !strings.Contains(err.Error(), "immutable field code cannot be changed after create") {
		t.Fatalf("save_entity_field immutable error = %v, want immutable rejection", err)
	}

	if _, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_id": entityID,
		"field":     "code",
		"value":     "acct-001",
	}); err != nil {
		t.Fatalf("save_entity_field immutable idempotent write: %v", err)
	}
}

func TestEntityTools_ReadsIgnoreLegacyUndeclaredStoredFields(t *testing.T) {
	ctx, exec, db := newEntityToolTestHarnessWithActor(t, models.AgentConfig{
		ID:    "tester",
		Role:  "operator",
		Tools: []string{"create_entity", "get_entity", "query_entities"},
	})
	entityID := mustCreateEntityID(t, ctx, exec, map[string]any{
		"flow_instance": "review/inst-1",
		"fields": map[string]any{
			"status": "open",
			"score":  10.0,
		},
	})
	if _, err := db.Exec(`
		UPDATE entity_state
		SET fields = jsonb_set(COALESCE(fields, '{}'::jsonb), '{legacy_flag}', 'true'::jsonb, true)
		WHERE entity_id = $1::uuid
	`, entityID); err != nil {
		t.Fatalf("inject legacy field: %v", err)
	}

	got, err := exec.Execute(ctx, "get_entity", map[string]any{"entity_id": entityID})
	if err != nil {
		t.Fatalf("get_entity with legacy stored field: %v", err)
	}
	entity, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected entity map, got %#v", got)
	}
	fields, ok := entity["fields"].(map[string]any)
	if !ok {
		t.Fatalf("expected fields map, got %#v", entity["fields"])
	}
	if _, exists := fields["legacy_flag"]; exists {
		t.Fatalf("legacy stored field leaked into materialized entity: %#v", fields)
	}

	queryOut, err := exec.Execute(ctx, "query_entities", map[string]any{
		"filter": `status == "open"`,
		"select": []string{"status"},
		"limit":  10,
	})
	if err != nil {
		t.Fatalf("query_entities with legacy stored field: %v", err)
	}
	queryResult, ok := queryOut.(map[string]any)
	if !ok {
		t.Fatalf("expected query result map, got %#v", queryOut)
	}
	queryRows, ok := queryResult["results"].([]map[string]any)
	if !ok || len(queryRows) != 1 {
		t.Fatalf("unexpected query results: %#v", queryResult["results"])
	}
}

func TestEntityTools_SaveEntityField_LogsMutationRow(t *testing.T) {
	ctx, exec, db := newEntityToolTestHarness(t)
	entityID := mustCreateEntityID(t, ctx, exec, map[string]any{
		"flow_instance": "review/inst-1",
		"fields": map[string]any{
			"status": "open",
		},
	})
	if _, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_id": entityID,
		"field":     "status",
		"value":     "closed",
	}); err != nil {
		t.Fatalf("save_entity_field: %v", err)
	}

	var (
		field      string
		oldValue   string
		newValue   string
		writerType string
		step       string
	)
	if err := db.QueryRowContext(ctx, `
		SELECT
			COALESCE(field, ''),
			COALESCE(old_value::text, ''),
			COALESCE(new_value::text, ''),
			COALESCE(writer_type, ''),
			COALESCE(handler_step, '')
		FROM entity_mutations
		WHERE entity_id = $1::uuid AND field = 'status'
		ORDER BY created_at DESC
		LIMIT 1
	`, entityID).Scan(&field, &oldValue, &newValue, &writerType, &step); err != nil {
		t.Fatalf("load entity mutation: %v", err)
	}
	if field != "status" {
		t.Fatalf("mutation field = %q, want status", field)
	}
	if oldValue != `"open"` {
		t.Fatalf("mutation old_value = %s, want \"open\"", oldValue)
	}
	if newValue != `"closed"` {
		t.Fatalf("mutation new_value = %s, want \"closed\"", newValue)
	}
	if writerType != "agent" {
		t.Fatalf("mutation writer_type = %q, want agent", writerType)
	}
	if step != "save_entity_field" {
		t.Fatalf("mutation handler_step = %q, want save_entity_field", step)
	}
}

func TestEntityTools_SaveEntityField_LogsNestedMutationRow(t *testing.T) {
	ctx, exec, db := newEntityToolTestHarnessWithActor(t, models.AgentConfig{
		ID:    "tester",
		Role:  "operator",
		Tools: []string{"create_entity", "get_entity", "save_entity_field"},
	})
	entityID := mustCreateEntityID(t, ctx, exec, map[string]any{
		"flow_instance": "review/inst-1",
		"fields": map[string]any{
			"metadata": map[string]any{"region": "us"},
		},
	})
	if _, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_id": entityID,
		"field":     "metadata.region",
		"value":     "ca",
	}); err != nil {
		t.Fatalf("save_entity_field nested mutation: %v", err)
	}

	var (
		field      string
		oldValue   string
		newValue   string
		writerType string
		step       string
	)
	if err := db.QueryRowContext(ctx, `
		SELECT
			COALESCE(field, ''),
			COALESCE(old_value::text, ''),
			COALESCE(new_value::text, ''),
			COALESCE(writer_type, ''),
			COALESCE(handler_step, '')
		FROM entity_mutations
		WHERE entity_id = $1::uuid AND field = 'metadata.region'
		ORDER BY created_at DESC
		LIMIT 1
	`, entityID).Scan(&field, &oldValue, &newValue, &writerType, &step); err != nil {
		t.Fatalf("load nested entity mutation: %v", err)
	}
	if field != "metadata.region" {
		t.Fatalf("mutation field = %q, want metadata.region", field)
	}
	if oldValue != `"us"` {
		t.Fatalf("mutation old_value = %s, want \"us\"", oldValue)
	}
	if newValue != `"ca"` {
		t.Fatalf("mutation new_value = %s, want \"ca\"", newValue)
	}
	if writerType != "agent" {
		t.Fatalf("mutation writer_type = %q, want agent", writerType)
	}
	if step != "save_entity_field" {
		t.Fatalf("mutation handler_step = %q, want save_entity_field", step)
	}
}

func TestEntityTools_CreateEntity_LogsInitialMutationRows(t *testing.T) {
	ctx, exec, db := newEntityToolTestHarness(t)
	entityID := mustCreateEntityID(t, ctx, exec, map[string]any{
		"flow_instance": "review/inst-1",
		"fields": map[string]any{
			"status": "open",
			"score":  10.0,
		},
	})

	rows, err := db.QueryContext(ctx, `
		SELECT field, COALESCE(writer_type, ''), COALESCE(writer_id, ''), COALESCE(handler_step, '')
		FROM entity_mutations
		WHERE entity_id = $1::uuid
		ORDER BY created_at ASC, mutation_id ASC
	`, entityID)
	if err != nil {
		t.Fatalf("query entity mutations: %v", err)
	}
	defer rows.Close()

	fields := map[string][3]string{}
	for rows.Next() {
		var (
			field       string
			writerType  string
			writerID    string
			handlerStep string
		)
		if err := rows.Scan(&field, &writerType, &writerID, &handlerStep); err != nil {
			t.Fatalf("scan entity mutation: %v", err)
		}
		fields[strings.TrimSpace(field)] = [3]string{writerType, writerID, handlerStep}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read entity mutations: %v", err)
	}

	for _, want := range []string{"current_state", "score", "status"} {
		meta, ok := fields[want]
		if !ok {
			t.Fatalf("missing mutation field %q in %#v", want, fields)
		}
		if meta[0] != "platform" || meta[1] != "create_entity" || meta[2] != "create_entity" {
			t.Fatalf("mutation metadata for %q = %#v, want platform/create_entity/create_entity", want, meta)
		}
	}
}

func TestEntityTools_GetEntityReturnsStoredCurrentState(t *testing.T) {
	ctx, exec, db := newEntityToolTestHarness(t)
	entityID := mustCreateEntityID(t, ctx, exec, map[string]any{
		"flow_instance": "review/inst-1",
		"fields": map[string]any{
			"status": "open",
			"score":  10.0,
			"active": true,
		},
	})
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
	if _, exists := entity["flow_states"]; exists {
		t.Fatalf("unexpected legacy flow_states in entity payload: %#v", entity)
	}
}

func TestEntityTools_SearchAndQueryUseStoredCurrentState(t *testing.T) {
	ctx, exec, db := newEntityToolTestHarness(t)
	entityID := mustCreateEntityID(t, ctx, exec, map[string]any{
		"flow_instance": "review/inst-1",
		"fields": map[string]any{
			"status": "open",
			"score":  10.0,
			"active": true,
		},
	})
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
	bundle := loadWave1EntityToolMultiFlowBundle(t, map[string]entityToolFlowFixture{
		"scoring": {
			SchemaYAML: `
name: scoring
mode: static
initial_state: marginal_review
states: [marginal_review, killed]
terminal_states: [killed]
`,
		},
		"validation": {
			SchemaYAML: `
name: validation
mode: static
initial_state: researching
states: [researching, killed]
terminal_states: [killed]
`,
		},
	})
	ctx, exec, db := newEntityToolTestHarnessWithBundle(t, models.AgentConfig{
		ID:    "tester",
		Role:  "operator",
		Tools: []string{"get_subject_status"},
	}, bundle)
	subjectID := uuid.NewString()
	scoringID := uuid.NewString()
	validationID := uuid.NewString()
	now := time.Now().UTC().Truncate(time.Second)
	seedEntityStateRow(t, db, scoringID, subjectID, "scoring/score-1", "vertical", "marginal_review", map[string]any{"status": "open"}, now.Add(-time.Hour))
	seedEntityStateRow(t, db, validationID, subjectID, "validation/validation-1", "validation_entity", "researching", map[string]any{"status": "open"}, now)

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
	if got := strings.TrimSpace(asString(status["latest_flow"])); got != "validation" {
		t.Fatalf("latest_flow = %q, want validation", got)
	}
	if got := strings.TrimSpace(asString(status["latest_state"])); got != "researching" {
		t.Fatalf("latest_state = %q, want researching", got)
	}
	if allTerminal, ok := status["all_terminal"].(bool); !ok || allTerminal {
		t.Fatalf("all_terminal = %#v, want false", status["all_terminal"])
	}
}

func TestEntityTools_QueryEntitiesFilterAllowsDeclaredNestedLeaf(t *testing.T) {
	ctx, exec := newEntityToolTestExecutor(t)
	_ = mustCreateEntityID(t, ctx, exec, map[string]any{
		"flow_instance": "review/inst-1",
		"fields": map[string]any{
			"status":   "open",
			"metadata": map[string]any{"region": "us"},
		},
	})

	out, err := exec.Execute(ctx, "query_entities", map[string]any{
		"filter": `metadata.region == "us"`,
		"select": []string{"status", "metadata.region"},
		"limit":  10,
	})
	if err != nil {
		t.Fatalf("query_entities with declared nested leaf: %v", err)
	}
	result, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("expected query result map, got %#v", out)
	}
	rows, ok := result["results"].([]map[string]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("unexpected query results: %#v", result["results"])
	}
	if got := strings.TrimSpace(asString(rows[0]["status"])); got != "open" {
		t.Fatalf("status = %q, want open", got)
	}
	if got := strings.TrimSpace(asString(rows[0]["metadata.region"])); got != "us" {
		t.Fatalf("metadata.region = %q, want us", got)
	}
}

func TestEntityTools_QueryEntitiesFilterRejectsUndeclaredFieldBeforeEvalWithNearestMatch(t *testing.T) {
	ctx, exec := newEntityToolTestExecutor(t)

	_, err := exec.Execute(ctx, "query_entities", map[string]any{
		"filter": `metadata.regoin == "us"`,
		"limit":  10,
	})
	re, ok := runtimetools.AsRuntimeError(err)
	if err == nil || !ok || re.Code != "invalid_tool_input" {
		t.Fatalf("expected invalid_tool_input, got %v", err)
	}
	if !strings.Contains(err.Error(), "metadata.regoin") {
		t.Fatalf("expected undeclared filter field in error, got %v", err)
	}
	if !strings.Contains(err.Error(), "did you mean metadata.region?") {
		t.Fatalf("expected nearest-match guidance, got %v", err)
	}
}

func TestEntityTools_QueryEntitiesFilterRejectsEntityScopedSelectors(t *testing.T) {
	ctx, exec := newEntityToolTestExecutor(t)

	_, err := exec.Execute(ctx, "query_entities", map[string]any{
		"filter": `entity.metadata.region == "us"`,
		"limit":  10,
	})
	re, ok := runtimetools.AsRuntimeError(err)
	if err == nil || !ok || re.Code != "invalid_tool_input" {
		t.Fatalf("expected invalid_tool_input, got %v", err)
	}
	if !strings.Contains(err.Error(), "must not use entity.metadata.region") {
		t.Fatalf("expected entity-scoped selector rejection, got %v", err)
	}
	if !strings.Contains(err.Error(), "use metadata.region instead") {
		t.Fatalf("expected direct selector guidance, got %v", err)
	}
}

func TestEntityTools_QueryMetricsFilterRejectsUndeclaredFieldBeforeEval(t *testing.T) {
	ctx, exec := newEntityToolTestExecutor(t)

	_, err := exec.Execute(ctx, "query_metrics", map[string]any{
		"metric": "count",
		"filter": `metadata.regoin == "us"`,
	})
	re, ok := runtimetools.AsRuntimeError(err)
	if err == nil || !ok || re.Code != "invalid_tool_input" {
		t.Fatalf("expected invalid_tool_input, got %v", err)
	}
	if !strings.Contains(err.Error(), "metadata.regoin") {
		t.Fatalf("expected undeclared metric filter field in error, got %v", err)
	}
}

func TestEntityTools_ReadToolsUseExplicitTargetContract(t *testing.T) {
	bundle := loadWave1EntityToolMultiFlowBundle(t, map[string]entityToolFlowFixture{
		"discovery": {
			EntitiesYAML: `
campaign:
  status: text
`,
			AgentsYAML: entityToolAgentYAML(models.AgentConfig{ID: "researcher", Role: "researcher", Tools: []string{"query_entities", "query_metrics", "search_entities"}}),
		},
		"signal-search": {
			EntitiesYAML: `
signal:
  signal_strength: integer
  processed: boolean
  status: text
`,
		},
	})
	ctx, exec, db := newEntityToolTestHarnessWithBundle(t, models.AgentConfig{
		ID:    "researcher",
		Role:  "researcher",
		Tools: []string{"query_entities", "query_metrics", "search_entities"},
	}, bundle)
	subjectID := uuid.NewString()
	seedEntityStateRow(t, db, uuid.NewString(), subjectID, "discovery/run-1", "campaign", "active", map[string]any{"status": "open"}, time.Now().UTC().Truncate(time.Second))
	seedEntityStateRow(t, db, uuid.NewString(), subjectID, "signal-search/run-1", "signal", "active", map[string]any{
		"signal_strength": 80,
		"processed":       false,
		"status":          "new",
	}, time.Now().UTC().Truncate(time.Second))

	queryOut, err := exec.Execute(ctx, "query_entities", map[string]any{
		"entity_type": "signal-search.signal",
		"filter":      `signal_strength >= 70 && processed == false`,
		"select":      []string{"signal_strength", "processed"},
		"limit":       10,
	})
	if err != nil {
		t.Fatalf("query_entities explicit target: %v", err)
	}
	queryResult, ok := queryOut.(map[string]any)
	if !ok {
		t.Fatalf("expected query result map, got %#v", queryOut)
	}
	queryRows, ok := queryResult["results"].([]map[string]any)
	if !ok || len(queryRows) != 1 {
		t.Fatalf("query results = %#v, want one signal row", queryResult["results"])
	}
	if got := fmt.Sprint(queryRows[0]["signal_strength"]); got != "80" {
		t.Fatalf("signal_strength = %q, want 80", got)
	}

	searchOut, err := exec.Execute(ctx, "search_entities", map[string]any{
		"entity_type":   "signal-search.signal",
		"current_state": "active",
		"filter":        map[string]any{"processed": false},
		"limit":         10,
	})
	if err != nil {
		t.Fatalf("search_entities explicit target: %v", err)
	}
	searchResult, ok := searchOut.(map[string]any)
	if !ok {
		t.Fatalf("expected search result map, got %#v", searchOut)
	}
	searchRows, ok := searchResult["results"].([]map[string]any)
	if !ok || len(searchRows) != 1 {
		t.Fatalf("search results = %#v, want one signal row", searchResult["results"])
	}
	if got := strings.TrimSpace(asString(searchRows[0]["entity_type"])); got != "signal" {
		t.Fatalf("search entity_type = %q, want signal", got)
	}

	metricOut, err := exec.Execute(ctx, "query_metrics", map[string]any{
		"entity_type": "signal-search.signal",
		"metric":      "sum",
		"field":       "signal_strength",
		"filter":      `processed == false`,
	})
	if err != nil {
		t.Fatalf("query_metrics explicit target: %v", err)
	}
	metricResult, ok := metricOut.(map[string]any)
	if !ok {
		t.Fatalf("expected metric result map, got %#v", metricOut)
	}
	if got := testNumericValue(metricResult["value"]); got != 80 {
		t.Fatalf("metric value = %#v, want 80", metricResult["value"])
	}
}

func TestEntityTools_BracketListTypeRefsAcrossConsumers(t *testing.T) {
	actor := models.AgentConfig{
		ID:    "validator",
		Role:  "validator",
		Tools: []string{"create_entity", "get_entity", "save_entity_field", "search_entities", "query_entities", "query_metrics"},
	}
	bundle := loadWave1EntityToolBundle(t, actor, "validation", "validation_case", `
types:
  Feature:
    name: text
  BrandCandidate:
    name: text
  MvpSpec:
    core_features: "[Feature]"
    out_of_scope: "[text]"
  Brand:
    alternatives: "[BrandCandidate]"
  ValidationKit:
    risk_flags: "[text]"
`, `
validation_case:
  mvp_spec:
    type: MvpSpec
    initial: {}
  brand:
    type: Brand
    initial: {}
  validation_kit:
    type: ValidationKit
    initial: {}
  score: integer
`)
	ctx, exec, db := newEntityToolTestHarnessWithBundle(t, actor, bundle)

	entityID := mustCreateEntityID(t, ctx, exec, map[string]any{
		"flow_instance": "validation/case-1",
		"fields": map[string]any{
			"score": 7,
		},
	})
	_ = mustCreateEntityID(t, ctx, exec, map[string]any{
		"flow_instance": "validation/case-2",
		"fields": map[string]any{
			"mvp_spec": map[string]any{
				"core_features": []any{map[string]any{"name": "Import"}},
				"out_of_scope":  []any{"mobile"},
			},
			"score": 3,
		},
	})
	if _, err := db.ExecContext(ctx, `UPDATE entity_state SET current_state = 'marginal_review' WHERE entity_id = $1::uuid`, entityID); err != nil {
		t.Fatalf("seed current_state: %v", err)
	}

	getOut, err := exec.Execute(ctx, "get_entity", map[string]any{"entity_id": entityID})
	if err != nil {
		t.Fatalf("get_entity bracket-list fields: %v", err)
	}
	getEntity := getOut.(map[string]any)
	getFields := getEntity["fields"].(map[string]any)
	if features := getFields["mvp_spec"].(map[string]any)["core_features"]; len(features.([]any)) != 0 {
		t.Fatalf("mvp_spec.core_features default = %#v, want empty list", features)
	}
	if alternatives := getFields["brand"].(map[string]any)["alternatives"]; len(alternatives.([]any)) != 0 {
		t.Fatalf("brand.alternatives default = %#v, want empty list", alternatives)
	}
	if riskFlags := getFields["validation_kit"].(map[string]any)["risk_flags"]; len(riskFlags.([]any)) != 0 {
		t.Fatalf("validation_kit.risk_flags default = %#v, want empty list", riskFlags)
	}

	if _, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_id": entityID,
		"field":     "mvp_spec.core_features",
		"value": []any{
			map[string]any{"name": "Guided setup"},
		},
	}); err != nil {
		t.Fatalf("save_entity_field bracket list-of-named: %v", err)
	}
	if _, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_id": entityID,
		"field":     "validation_kit.risk_flags",
		"value":     []any{"pricing risk"},
	}); err != nil {
		t.Fatalf("save_entity_field bracket list-of-text: %v", err)
	}

	searchOut, err := exec.Execute(ctx, "search_entities", map[string]any{
		"current_state": "marginal_review",
		"filter":        map[string]any{"score": 7},
		"limit":         10,
	})
	if err != nil {
		t.Fatalf("search_entities bracket-list materialization: %v", err)
	}
	searchRows := searchOut.(map[string]any)["results"].([]map[string]any)
	if len(searchRows) != 1 {
		t.Fatalf("search results = %#v, want one row", searchRows)
	}
	if _, err := exec.Execute(ctx, "search_entities", map[string]any{
		"filter": map[string]any{"mvp_spec.core_features": []any{map[string]any{"name": "Guided setup"}}},
		"limit":  10,
	}); err == nil || strings.Contains(err.Error(), "unsupported type") {
		t.Fatalf("search_entities composite list filter = %v, want fail-closed validation before unsupported type", err)
	}

	queryOut, err := exec.Execute(ctx, "query_entities", map[string]any{
		"filter": `score == 7`,
		"select": []string{"score"},
		"limit":  10,
	})
	if err != nil {
		t.Fatalf("query_entities bracket-list materialization: %v", err)
	}
	queryRows := queryOut.(map[string]any)["results"].([]map[string]any)
	if len(queryRows) != 1 {
		t.Fatalf("query results = %#v, want one row", queryRows)
	}

	metricOut, err := exec.Execute(ctx, "query_metrics", map[string]any{
		"metric": "sum",
		"field":  "score",
		"filter": `score == 7`,
	})
	if err != nil {
		t.Fatalf("query_metrics bracket-list materialization: %v", err)
	}
	if got := testNumericValue(metricOut.(map[string]any)["value"]); got != 7 {
		t.Fatalf("metric value = %#v, want 7", metricOut)
	}

	if _, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_id": entityID,
		"field":     "mvp_spec.core_features",
		"value":     map[string]any{"name": "not a list"},
	}); err == nil {
		t.Fatalf("save_entity_field bracket list accepted object, want invalid_tool_input")
	}
}

func TestEntityTools_ReadTargetValidationRejectsUndeclaredBeforeEval(t *testing.T) {
	bundle := loadWave1EntityToolMultiFlowBundle(t, map[string]entityToolFlowFixture{
		"discovery": {
			EntitiesYAML: `
campaign:
  status: text
`,
			AgentsYAML: entityToolAgentYAML(models.AgentConfig{ID: "researcher", Role: "researcher", Tools: []string{"query_entities", "query_metrics", "search_entities"}}),
		},
		"signal-search": {
			EntitiesYAML: `
signal:
  signal_strength: integer
  processed: boolean
`,
		},
	})
	ctx, exec, _ := newEntityToolTestHarnessWithBundle(t, models.AgentConfig{
		ID:    "researcher",
		Role:  "researcher",
		Tools: []string{"query_entities", "query_metrics", "search_entities"},
	}, bundle)

	_, err := exec.Execute(ctx, "query_entities", map[string]any{
		"entity_type": "signal-search.signal",
		"filter":      `signal_strenght >= 70`,
	})
	re, ok := runtimetools.AsRuntimeError(err)
	if err == nil || !ok || re.Code != "invalid_tool_input" {
		t.Fatalf("expected query_entities invalid_tool_input, got %v", err)
	}
	if !strings.Contains(err.Error(), "signal_strenght") || !strings.Contains(err.Error(), "did you mean signal_strength?") {
		t.Fatalf("expected nearest-match target diagnostic, got %v", err)
	}

	_, err = exec.Execute(ctx, "query_metrics", map[string]any{
		"entity_type": "signal-search.signal",
		"metric":      "sum",
		"field":       "signal_strenght",
	})
	re, ok = runtimetools.AsRuntimeError(err)
	if err == nil || !ok || re.Code != "invalid_tool_input" {
		t.Fatalf("expected query_metrics invalid_tool_input, got %v", err)
	}
	if !strings.Contains(err.Error(), "signal_strenght") {
		t.Fatalf("expected target field diagnostic, got %v", err)
	}

	_, err = exec.Execute(ctx, "search_entities", map[string]any{
		"entity_type": "signal-search.signal",
		"filter":      map[string]any{"signal_strenght": 80},
	})
	re, ok = runtimetools.AsRuntimeError(err)
	if err == nil || !ok || re.Code != "invalid_tool_input" {
		t.Fatalf("expected search_entities invalid_tool_input, got %v", err)
	}
	if !strings.Contains(err.Error(), "signal_strenght") {
		t.Fatalf("expected target object-filter field diagnostic, got %v", err)
	}

	_, err = exec.Execute(ctx, "query_entities", map[string]any{
		"filter": `signal_strength >= 70`,
	})
	re, ok = runtimetools.AsRuntimeError(err)
	if err == nil || !ok || re.Code != "invalid_tool_input" {
		t.Fatalf("expected default actor contract to reject target-only field, got %v", err)
	}
}

func TestEntityTools_GetSubjectStatusReturnsEmptyForUnknownSubject(t *testing.T) {
	ctx, exec := newEntityToolTestExecutor(t)
	subjectID := uuid.NewString()

	out, err := exec.Execute(ctx, "get_subject_status", map[string]any{"subject_id": subjectID})
	if err != nil {
		t.Fatalf("get_subject_status: %v", err)
	}
	status, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("expected status map, got %#v", out)
	}
	entities, ok := status["entities"].([]map[string]any)
	if !ok || len(entities) != 0 {
		t.Fatalf("entities = %#v, want empty", status["entities"])
	}
	if allTerminal, ok := status["all_terminal"].(bool); !ok || !allTerminal {
		t.Fatalf("all_terminal = %#v, want true", status["all_terminal"])
	}
	if got := strings.TrimSpace(asString(status["latest_flow"])); got != "" {
		t.Fatalf("latest_flow = %q, want empty", got)
	}
	if got := strings.TrimSpace(asString(status["latest_state"])); got != "" {
		t.Fatalf("latest_state = %q, want empty", got)
	}
}

func TestEntityTools_GetSubjectStatusAllTerminalTrueWhenAllFlowsTerminal(t *testing.T) {
	bundle := loadWave1EntityToolMultiFlowBundle(t, map[string]entityToolFlowFixture{
		"scoring": {
			SchemaYAML: `
name: scoring
mode: static
initial_state: queued
states: [queued, killed]
terminal_states: [killed]
`,
		},
		"validation": {
			SchemaYAML: `
name: validation
mode: static
initial_state: queued
states: [queued, killed]
terminal_states: [killed]
`,
		},
	})
	ctx, exec, db := newEntityToolTestHarnessWithBundle(t, models.AgentConfig{
		ID:    "tester",
		Role:  "operator",
		Tools: []string{"get_subject_status"},
	}, bundle)
	subjectID := uuid.NewString()
	now := time.Now().UTC().Truncate(time.Second)
	seedEntityStateRow(t, db, uuid.NewString(), subjectID, "scoring/score-1", "vertical", "killed", map[string]any{"status": "closed"}, now)
	seedEntityStateRow(t, db, uuid.NewString(), subjectID, "validation/validation-1", "validation_entity", "killed", map[string]any{"status": "closed"}, now)

	out, err := exec.Execute(ctx, "get_subject_status", map[string]any{"subject_id": subjectID})
	if err != nil {
		t.Fatalf("get_subject_status: %v", err)
	}
	status := out.(map[string]any)
	if allTerminal, ok := status["all_terminal"].(bool); !ok || !allTerminal {
		t.Fatalf("all_terminal = %#v, want true", status["all_terminal"])
	}
}

func TestEntityTools_GetSubjectStatusSingleFlowSubject(t *testing.T) {
	bundle := loadWave1EntityToolMultiFlowBundle(t, map[string]entityToolFlowFixture{
		"scoring": {
			SchemaYAML: `
name: scoring
mode: static
initial_state: active
states: [active, killed]
terminal_states: [killed]
`,
		},
	})
	ctx, exec, db := newEntityToolTestHarnessWithBundle(t, models.AgentConfig{
		ID:    "tester",
		Role:  "operator",
		Tools: []string{"get_subject_status"},
	}, bundle)
	subjectID := uuid.NewString()
	entityID := uuid.NewString()
	seedEntityStateRow(t, db, entityID, subjectID, "scoring/score-1", "vertical", "active", map[string]any{"status": "open"}, time.Now().UTC().Truncate(time.Second))

	out, err := exec.Execute(ctx, "get_subject_status", map[string]any{"subject_id": subjectID})
	if err != nil {
		t.Fatalf("get_subject_status: %v", err)
	}
	status := out.(map[string]any)
	entities, ok := status["entities"].([]map[string]any)
	if !ok || len(entities) != 1 {
		t.Fatalf("entities = %#v, want one row", status["entities"])
	}
	if got := strings.TrimSpace(asString(status["latest_flow"])); got != "scoring" {
		t.Fatalf("latest_flow = %q, want scoring", got)
	}
	if got := strings.TrimSpace(asString(status["latest_state"])); got != "active" {
		t.Fatalf("latest_state = %q, want active", got)
	}
}

func TestEntityTools_GetSubjectStatusPrefersDeeperFlowOnEqualEnteredStateAt(t *testing.T) {
	bundle := loadEntityToolFixtureBundle(t, "tests/tier11-flow-composition/test-nested-three-levels")
	ctx, exec, db := newEntityToolTestHarnessWithBundle(t, models.AgentConfig{
		ID:    "tester",
		Role:  "operator",
		Tools: []string{"get_subject_status"},
	}, bundle)
	subjectID := uuid.NewString()
	childID := uuid.NewString()
	grandchildID := uuid.NewString()
	sameMoment := time.Now().UTC().Truncate(time.Second)
	seedEntityStateRow(t, db, childID, subjectID, "child/child-1", "child_entity", "waiting", map[string]any{"status": "open"}, sameMoment)
	seedEntityStateRow(t, db, grandchildID, subjectID, "child/grandchild/grandchild-1", "grandchild_entity", "ready", map[string]any{"status": "open"}, sameMoment)

	out, err := exec.Execute(ctx, "get_subject_status", map[string]any{"subject_id": subjectID})
	if err != nil {
		t.Fatalf("get_subject_status: %v", err)
	}
	status := out.(map[string]any)
	if got := strings.TrimSpace(asString(status["latest_flow"])); got != "grandchild" {
		t.Fatalf("latest_flow = %q, want grandchild", got)
	}
	if got := strings.TrimSpace(asString(status["latest_state"])); got != "ready" {
		t.Fatalf("latest_state = %q, want ready", got)
	}
}

func TestEntityTools_InvalidField(t *testing.T) {
	ctx, exec := newEntityToolTestExecutor(t)
	entityID := mustCreateEntityID(t, ctx, exec, map[string]any{
		"flow_instance": "review/inst-1",
		"fields": map[string]any{
			"status": "open",
		},
	})
	_, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_id": entityID,
		"field":     "unknown_field",
		"value":     "x",
	})
	if err == nil || !strings.Contains(err.Error(), "invalid enum value unknown_field") {
		t.Fatalf("expected delivery-boundary invalid field rejection, got %v", err)
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

func TestEntityTools_CreateEntityRejectsCallerSuppliedEntityID(t *testing.T) {
	ctx, exec := newEntityToolTestExecutor(t)
	_, err := exec.Execute(ctx, "create_entity", map[string]any{
		"entity_id":     uuid.NewString(),
		"flow_instance": "review/inst-1",
		"fields": map[string]any{
			"status": "open",
			"score":  10.0,
			"active": true,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "entity_id is platform-minted and must not be supplied") {
		t.Fatalf("expected caller-supplied entity_id rejection, got %v", err)
	}
}

func TestEntityTools_CreateEntityRejectsCallerSuppliedEntityType(t *testing.T) {
	ctx, exec := newEntityToolTestExecutor(t)
	_, err := exec.Execute(ctx, "create_entity", map[string]any{
		"entity_type":   "accounts",
		"flow_instance": "review/inst-1",
		"fields": map[string]any{
			"status": "open",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "entity_type is inferred from flow_instance and must not be supplied") {
		t.Fatalf("expected caller-supplied entity_type rejection, got %v", err)
	}
}

func TestEntityTools_ConstrainedAllowedToolsStillPermitOnlyUniversalEntityTools(t *testing.T) {
	ctx, exec := newEntityToolTestExecutorWithActor(t, models.AgentConfig{
		ID:    "tester",
		Role:  "operator",
		Tools: []string{"emit_something"},
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

func TestEntityTools_CreateEntityRejectsFlowWithoutEntityContract(t *testing.T) {
	ctx, exec := newEntityToolTestExecutorWithBundle(t, models.AgentConfig{
		ID:    "tester",
		Role:  "operator",
		Tools: []string{"create_entity", "save_entity_field", "get_entity"},
	}, &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{InitialStage: "queued"},
	})
	_, err := exec.Execute(ctx, "create_entity", map[string]any{
		"flow_instance": "review/inst-1",
	})
	if err == nil || !strings.Contains(err.Error(), "flow_instance does not resolve to a flow-owned entity contract") {
		t.Fatalf("expected missing contract rejection, got %v", err)
	}
}

func TestEntityTools_SaveEntityFieldRejectsCrossFlowWrite(t *testing.T) {
	bundle := loadWave1EntityToolMultiFlowBundle(t, map[string]entityToolFlowFixture{
		"analyzer-flow": {
			EntitiesYAML: `
analysis:
  status: text
`,
			AgentsYAML: entityToolAgentYAML(models.AgentConfig{ID: "analyzer", Role: "analyzer", Tools: []string{"save_entity_field", "get_entity"}}),
		},
		"other-flow": {
			EntitiesYAML: `
foreign:
  status: text
  name: text
  composite_score: numeric
`,
		},
	})
	ctx, exec, db := newEntityToolTestHarnessWithBundle(t, models.AgentConfig{
		ID:    "analyzer",
		Role:  "analyzer",
		Tools: []string{"save_entity_field", "get_entity"},
	}, bundle)
	entityID := uuid.NewString()
	seedEntityStateRow(t, db, entityID, entityID, "other-flow/inst-1", "foreign", "queued", map[string]any{"status": "open"}, time.Now().UTC().Truncate(time.Second))

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
	bundle := loadWave1EntityToolMultiFlowBundle(t, map[string]entityToolFlowFixture{
		"analyzer-flow": {
			EntitiesYAML: `
analysis:
  status: text
`,
			AgentsYAML: entityToolAgentYAML(models.AgentConfig{ID: "analyzer", Role: "analyzer", Tools: []string{"create_entity", "save_entity_field", "get_entity"}}),
		},
	})
	ctx, exec, _ := newEntityToolTestHarnessWithBundle(t, models.AgentConfig{
		ID:    "analyzer",
		Role:  "analyzer",
		Tools: []string{"create_entity", "save_entity_field", "get_entity"},
	}, bundle)
	entityID := mustCreateEntityID(t, ctx, exec, map[string]any{
		"flow_instance": "analyzer-flow/inst-1",
		"fields": map[string]any{
			"status": "open",
		},
	})

	if _, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_id": entityID,
		"field":     "status",
		"value":     "closed",
	}); err != nil {
		t.Fatalf("save_entity_field same flow: %v", err)
	}
}

func TestEntityTools_SaveEntityFieldAllowsSameFlowWriteWithForeignSubject(t *testing.T) {
	bundle := loadWave1EntityToolMultiFlowBundle(t, map[string]entityToolFlowFixture{
		"analyzer-flow": {
			EntitiesYAML: `
analysis:
  status: text
`,
			AgentsYAML: entityToolAgentYAML(models.AgentConfig{ID: "analyzer", Role: "analyzer", Tools: []string{"create_entity", "save_entity_field", "get_entity"}}),
		},
	})
	ctx, exec, _ := newEntityToolTestHarnessWithBundle(t, models.AgentConfig{
		ID:    "analyzer",
		Role:  "analyzer",
		Tools: []string{"create_entity", "save_entity_field", "get_entity"},
	}, bundle)
	subjectID := uuid.NewString()
	entityID := mustCreateEntityID(t, ctx, exec, map[string]any{
		"subject_id":    subjectID,
		"flow_instance": "analyzer-flow/inst-1",
		"fields": map[string]any{
			"status": "open",
		},
	})

	if _, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_id": entityID,
		"field":     "status",
		"value":     "closed",
	}); err != nil {
		t.Fatalf("save_entity_field same flow with foreign subject: %v", err)
	}
}

func TestEntityTools_GetEntityAllowsCrossFlowRead(t *testing.T) {
	bundle := loadWave1EntityToolMultiFlowBundle(t, map[string]entityToolFlowFixture{
		"analyzer-flow": {
			EntitiesYAML: `
analysis:
  status: text
`,
			AgentsYAML: entityToolAgentYAML(models.AgentConfig{ID: "analyzer", Role: "analyzer", Tools: []string{"get_entity"}}),
		},
		"other-flow": {
			EntitiesYAML: `
foreign:
  status: text
  name: text
  composite_score: numeric
`,
		},
	})
	ctx, exec, db := newEntityToolTestHarnessWithBundle(t, models.AgentConfig{
		ID:    "analyzer",
		Role:  "analyzer",
		Tools: []string{"get_entity"},
	}, bundle)
	entityID := uuid.NewString()
	seedEntityStateRow(t, db, entityID, entityID, "other-flow/inst-1", "foreign", "open", map[string]any{"status": "foreign"}, time.Now().UTC().Truncate(time.Second))

	out, err := exec.Execute(ctx, "get_entity", map[string]any{"entity_id": entityID})
	if err != nil {
		t.Fatalf("get_entity cross-flow read: %v", err)
	}
	entity, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("expected entity map, got %#v", out)
	}
	if got := strings.TrimSpace(asString(entity["flow_instance"])); got != "other-flow/inst-1" {
		t.Fatalf("flow_instance = %q, want other-flow/inst-1", got)
	}
	if got := strings.TrimSpace(asString(entity["current_state"])); got != "open" {
		t.Fatalf("current_state = %q, want open", got)
	}
}

func TestEntityTools_FlowOwnedActorCanReadForeignEntityAndWriteOwnEntity(t *testing.T) {
	bundle := loadWave1EntityToolMultiFlowBundle(t, map[string]entityToolFlowFixture{
		"analyzer-flow": {
			EntitiesYAML: `
analysis:
  status: text
`,
			AgentsYAML: entityToolAgentYAML(models.AgentConfig{ID: "analyzer", Role: "analyzer", Tools: []string{"create_entity", "get_entity", "save_entity_field"}}),
		},
		"other-flow": {
			EntitiesYAML: `
foreign:
  status: text
  name: text
  composite_score: numeric
`,
		},
	})
	ctx, exec, db := newEntityToolTestHarnessWithBundle(t, models.AgentConfig{
		ID:    "analyzer",
		Role:  "analyzer",
		Tools: []string{"create_entity", "get_entity", "save_entity_field"},
	}, bundle)

	scoringID := uuid.NewString()
	subjectID := uuid.NewString()
	seedEntityStateRow(t, db, scoringID, subjectID, "other-flow/score-1", "foreign", "shortlisted", map[string]any{
		"name":            "Example Vertical",
		"composite_score": 72,
	}, time.Now().UTC().Truncate(time.Second))
	validationID := mustCreateEntityID(t, ctx, exec, map[string]any{
		"subject_id":    subjectID,
		"flow_instance": "analyzer-flow/validation-1",
		"initial_state": "researching",
		"fields": map[string]any{
			"status": "open",
		},
	})

	out, err := exec.Execute(ctx, "get_entity", map[string]any{"entity_id": scoringID})
	if err != nil {
		t.Fatalf("get_entity scoring context: %v", err)
	}
	entity, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("expected entity map, got %#v", out)
	}
	fields, ok := entity["fields"].(map[string]any)
	if !ok || strings.TrimSpace(asString(fields["name"])) != "Example Vertical" {
		t.Fatalf("unexpected scoring fields: %#v", entity["fields"])
	}

	if _, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_id": validationID,
		"field":     "status",
		"value":     "researched",
	}); err != nil {
		t.Fatalf("save_entity_field validation target: %v", err)
	}
}

func newEntityToolTestExecutor(t *testing.T) (context.Context, *runtimetools.Executor) {
	t.Helper()
	ctx, exec, _ := newEntityToolTestHarnessWithActor(t, models.AgentConfig{
		ID:    "tester",
		Role:  "operator",
		Tools: []string{"create_entity", "get_entity", "get_subject_status", "save_entity_field", "search_entities", "query_entities", "query_metrics"},
	})
	return ctx, exec
}

func newEntityToolTestExecutorWithActor(t *testing.T, actor models.AgentConfig) (context.Context, *runtimetools.Executor) {
	t.Helper()
	bundle := loadWave1EntityToolBundle(t, actor, "review", "accounts", `
types:
  Metadata:
    region: text
  Brief:
    summary: text
  Spec:
    headline: text
    approved: boolean
  ValidationKit:
    checklist: list<text>
`, `
accounts:
  status: text
  score: numeric
  priority:
    type: integer
    initial: 0
  active: boolean
  metadata: Metadata
  notes: text
  business_brief: Brief
  mvp_spec: Spec
  validation_kit: ValidationKit
`)
	ctx, exec, _ := newEntityToolTestHarnessWithBundle(t, actor, bundle)
	return ctx, exec
}

func newEntityToolTestExecutorWithBundle(t *testing.T, actor models.AgentConfig, bundle *runtimecontracts.WorkflowContractBundle) (context.Context, *runtimetools.Executor) {
	t.Helper()
	ctx, exec, _ := newEntityToolTestHarnessWithBundle(t, actor, bundle)
	return ctx, exec
}

func newAnnotatedEntityToolExecutor(t *testing.T) (context.Context, *runtimetools.Executor) {
	t.Helper()
	return newEntityToolTestExecutorWithBundle(t, models.AgentConfig{
		ID:    "tester",
		Role:  "operator",
		Tools: []string{"create_entity", "get_entity", "save_entity_field", "search_entities"},
	}, loadWave1EntityToolBundle(t, models.AgentConfig{
		ID:    "tester",
		Role:  "operator",
		Tools: []string{"create_entity", "get_entity", "save_entity_field", "search_entities"},
	}, "validation", "validation", `
types:
  Brief:
    summary: text
  Spec:
    headline: text
    approved: boolean
  ValidationKit:
    checklist: list<text>
`, `
validation:
  business_brief: Brief
  mvp_spec: Spec
  validation_kit: ValidationKit
`))
}

func newEntityToolTestHarness(t *testing.T) (context.Context, *runtimetools.Executor, *sql.DB) {
	t.Helper()
	return newEntityToolTestHarnessWithActor(t, models.AgentConfig{
		ID:    "tester",
		Role:  "operator",
		Tools: []string{"create_entity", "get_entity", "get_subject_status", "save_entity_field", "search_entities", "query_entities", "query_metrics"},
	})
}

func newEntityToolTestHarnessWithActor(t *testing.T, actor models.AgentConfig) (context.Context, *runtimetools.Executor, *sql.DB) {
	t.Helper()
	bundle := loadWave1EntityToolBundle(t, actor, "review", "accounts", `
types:
  Metadata:
    region: text
  Brief:
    summary: text
  Spec:
    headline: text
    approved: boolean
  ValidationKit:
    checklist: list<text>
`, `
accounts:
  status: text
  score: numeric
  priority:
    type: integer
    initial: 0
  active: boolean
  metadata: Metadata
  notes: text
  business_brief: Brief
  mvp_spec: Spec
  validation_kit: ValidationKit
`)
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

func testNumericValue(v any) float64 {
	switch n := v.(type) {
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case float64:
		return n
	default:
		return 0
	}
}

func mustCreateEntityID(t *testing.T, ctx context.Context, exec *runtimetools.Executor, input map[string]any) string {
	t.Helper()
	cloned := map[string]any{}
	for key, value := range input {
		cloned[key] = value
	}
	delete(cloned, "entity_id")
	out, err := exec.Execute(ctx, "create_entity", cloned)
	if err != nil {
		t.Fatalf("create_entity: %v", err)
	}
	created, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("unexpected create_entity output: %#v", out)
	}
	entityID := strings.TrimSpace(asString(created["entity_id"]))
	if entityID == "" {
		t.Fatalf("create_entity entity_id = %#v, want minted uuid", created["entity_id"])
	}
	return entityID
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

func loadWave1EntityToolBundle(t *testing.T, actor models.AgentConfig, flowID, entityType, typesYAML, entitiesYAML string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	root := t.TempDir()
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")

	writeEntityToolFixtureFile(t, filepath.Join(root, "package.yaml"), fmt.Sprintf(`
name: entity-tool-bundle
version: "1.0.0"
platform_version: ">=1.0.0"
flows:
  - id: %s
    flow: %s
    mode: static
`, flowID, flowID))
	writeEntityToolFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: entity-tool-bundle\n")
	if strings.TrimSpace(typesYAML) != "" {
		writeEntityToolFixtureFile(t, filepath.Join(root, "flows", flowID, "types.yaml"), typesYAML)
	}
	writeEntityToolFixtureFile(t, filepath.Join(root, "flows", flowID, "schema.yaml"), fmt.Sprintf(`
name: %s
mode: static
initial_state: queued
states: [queued, marginal_review, closed]
terminal_states: [closed]
`, flowID))
	writeEntityToolFixtureFile(t, filepath.Join(root, "flows", flowID, "entities.yaml"), entitiesYAML)
	writeEntityToolFixtureFile(t, filepath.Join(root, "flows", flowID, "agents.yaml"), entityToolAgentYAML(actor))

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides(%s): %v", root, err)
	}
	if got, _, ok := bundle.FlowOwnedEntityContract(flowID); !ok || strings.TrimSpace(got) != entityType {
		t.Fatalf("FlowOwnedEntityContract(%q) = (%q, ok=%v), want %q", flowID, got, ok, entityType)
	}
	return bundle
}

type entityToolFlowFixture struct {
	SchemaYAML   string
	TypesYAML    string
	EntitiesYAML string
	AgentsYAML   string
}

func loadWave1EntityToolMultiFlowBundle(t *testing.T, flows map[string]entityToolFlowFixture) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	root := t.TempDir()
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")

	flowIDs := make([]string, 0, len(flows))
	for flowID := range flows {
		flowIDs = append(flowIDs, strings.TrimSpace(flowID))
	}
	sort.Strings(flowIDs)

	var packageYAML strings.Builder
	packageYAML.WriteString("name: entity-tool-bundle\n")
	packageYAML.WriteString("version: \"1.0.0\"\n")
	packageYAML.WriteString("platform_version: \">=1.0.0\"\n")
	packageYAML.WriteString("flows:\n")
	for _, flowID := range flowIDs {
		packageYAML.WriteString("  - id: ")
		packageYAML.WriteString(flowID)
		packageYAML.WriteString("\n")
		packageYAML.WriteString("    flow: ")
		packageYAML.WriteString(flowID)
		packageYAML.WriteString("\n")
		packageYAML.WriteString("    mode: static\n")
	}
	writeEntityToolFixtureFile(t, filepath.Join(root, "package.yaml"), packageYAML.String())
	writeEntityToolFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: entity-tool-bundle\n")

	for _, flowID := range flowIDs {
		fixture := flows[flowID]
		schemaYAML := strings.TrimSpace(fixture.SchemaYAML)
		if schemaYAML == "" {
			schemaYAML = fmt.Sprintf(`
name: %s
mode: static
initial_state: queued
states: [queued, active, researching, marginal_review, analyzed, ready, finished, closed, killed]
terminal_states: [finished, closed, killed]
`, flowID)
		}
		writeEntityToolFixtureFile(t, filepath.Join(root, "flows", flowID, "schema.yaml"), schemaYAML)
		if strings.TrimSpace(fixture.TypesYAML) != "" {
			writeEntityToolFixtureFile(t, filepath.Join(root, "flows", flowID, "types.yaml"), fixture.TypesYAML)
		}
		if strings.TrimSpace(fixture.EntitiesYAML) != "" {
			writeEntityToolFixtureFile(t, filepath.Join(root, "flows", flowID, "entities.yaml"), fixture.EntitiesYAML)
		}
		if strings.TrimSpace(fixture.AgentsYAML) != "" {
			writeEntityToolFixtureFile(t, filepath.Join(root, "flows", flowID, "agents.yaml"), fixture.AgentsYAML)
		}
	}

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides(%s): %v", root, err)
	}
	return bundle
}

func seedEntityStateRow(t *testing.T, db *sql.DB, entityID, subjectID, flowInstance, entityType, currentState string, fields map[string]any, enteredAt time.Time) {
	t.Helper()
	if strings.TrimSpace(subjectID) == "" {
		subjectID = entityID
	}
	if enteredAt.IsZero() {
		enteredAt = time.Now().UTC()
	}
	fieldsJSON, err := json.Marshal(fields)
	if err != nil {
		t.Fatalf("json.Marshal(fields): %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO entity_state (
			entity_id, subject_id, flow_instance, entity_type, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3, $4, '',
			$5, '{}'::jsonb, $6::jsonb, '{}'::jsonb, 1,
			$7, $7, $7
		)
	`, entityID, subjectID, flowInstance, entityType, currentState, string(fieldsJSON), enteredAt); err != nil {
		t.Fatalf("seed entity_state(%s): %v", entityID, err)
	}
}

func entityToolAgentYAML(actor models.AgentConfig) string {
	var builder strings.Builder
	builder.WriteString(strings.TrimSpace(actor.ID))
	builder.WriteString(":\n")
	builder.WriteString("  id: ")
	builder.WriteString(strings.TrimSpace(actor.ID))
	builder.WriteString("\n")
	if role := strings.TrimSpace(actor.Role); role != "" {
		builder.WriteString("  role: ")
		builder.WriteString(role)
		builder.WriteString("\n")
	}
	if len(actor.Tools) > 0 {
		builder.WriteString("  tools:\n")
		for _, tool := range actor.Tools {
			tool = strings.TrimSpace(tool)
			if tool == "" {
				continue
			}
			builder.WriteString("    - ")
			builder.WriteString(tool)
			builder.WriteString("\n")
		}
	}
	return builder.String()
}

func writeEntityToolFixtureFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
}
