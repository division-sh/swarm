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

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runforkrevision "github.com/division-sh/swarm/internal/runtime/runforkrevision"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type entityToolRuntimeLogBus struct {
	logs       []runtimepipeline.RuntimeLogEntry
	lineages   []runtimecorrelation.RuntimeLineage
	hasLineage []bool
	published  []events.Event
}

func (b *entityToolRuntimeLogBus) Publish(_ context.Context, evt events.Event) error {
	b.published = append(b.published, evt)
	return nil
}

func (*entityToolRuntimeLogBus) PublishDirect(context.Context, events.Event, []string) error {
	return nil
}

func (b *entityToolRuntimeLogBus) LogRuntime(ctx context.Context, entry runtimepipeline.RuntimeLogEntry) error {
	lineage, ok := runtimecorrelation.RuntimeLineageFromContext(ctx)
	b.lineages = append(b.lineages, lineage)
	b.hasLineage = append(b.hasLineage, ok)
	b.logs = append(b.logs, entry)
	return nil
}

func selectedForkEntityToolRuntimeContext(actor models.AgentConfig) context.Context {
	const eventID = "f6d20e7c-123d-4a37-9f9b-137421a24bdb"
	ctx := runtimetools.WithActor(unmanagedToolTestContext(), actor)
	ctx = runtimecorrelation.WithRunID(ctx, entityToolTestRunID)
	ctx = runtimecorrelation.WithRuntimeLineage(ctx, runtimecorrelation.RuntimeLineage{
		Owner:               "runtime.run_fork.selected_contract_execution.fork_local_runtime_typed_lineage",
		RunID:               entityToolTestRunID,
		SubjectEventID:      eventID,
		SubjectEventType:    "validation/validation.package_ready",
		ParentEventID:       eventID,
		RowCategory:         runtimecorrelation.RuntimeLineageRowCategoryRuntimeContainer,
		SelectedForkOwner:   "runtime.run_fork.selected_contract_execution.fork_local_runtime_container",
		Classification:      runtimecorrelation.RuntimeLineageClassificationForkLocal,
		SelectedForkContext: true,
	})
	return runtimebus.WithInboundEvent(ctx, eventtest.RootIngress(
		eventID,
		events.EventType("validation/validation.package_ready"),
		"",
		"",
		[]byte(`{"entity_id":"entity-typed-lineage"}`),
		0,
		entityToolTestRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "entity-typed-lineage"),
		time.Unix(1700000000, 0).UTC(),
	))
}

func assertEntityToolDiagnosticLineage(t *testing.T, bus *entityToolRuntimeLogBus, index int) {
	t.Helper()
	if index >= len(bus.logs) || index >= len(bus.lineages) || index >= len(bus.hasLineage) {
		t.Fatalf("lineage index %d outside logs=%d lineages=%d hasLineage=%d", index, len(bus.logs), len(bus.lineages), len(bus.hasLineage))
	}
	if !bus.hasLineage[index] {
		t.Fatalf("runtime lineage missing for log %#v", bus.logs[index])
	}
	lineage := bus.lineages[index]
	if lineage.Owner != "runtime.run_fork.selected_contract_execution.fork_local_runtime_typed_lineage" ||
		lineage.RunID != entityToolTestRunID ||
		lineage.SubjectEventID != "f6d20e7c-123d-4a37-9f9b-137421a24bdb" ||
		lineage.ParentEventID != "f6d20e7c-123d-4a37-9f9b-137421a24bdb" ||
		lineage.RowCategory != runtimecorrelation.RuntimeLineageRowCategoryDiagnostic ||
		lineage.SelectedForkOwner != "runtime.run_fork.selected_contract_execution.fork_local_runtime_container" ||
		lineage.Classification != runtimecorrelation.RuntimeLineageClassificationForkLocal ||
		!lineage.SelectedForkContext {
		t.Fatalf("runtime lineage = %#v", lineage)
	}
}

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
	if _, ok := created["subject_id"]; ok {
		t.Fatalf("create_entity returned deprecated subject_id field: %#v", created)
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
	if strings.TrimSpace(asString(entity["entity_type"])) != "accounts" {
		t.Fatalf("entity_type = %#v, want accounts", entity["entity_type"])
	}
	if _, ok := entity["subject_id"]; ok {
		t.Fatalf("get_entity returned deprecated subject_id field: %#v", entity)
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

func TestRoleScopedEntityTools_OptedInActorReceivesGeneratedSurfaceOnly(t *testing.T) {
	actor := models.AgentConfig{ExecutionMode: "live", ID: "validation-orchestrator", Role: "validation_orchestrator", Tools: []string{
		"create_entity",
		"get_entity",
		"get_subject_status",
		"save_entity_field",
		"query_entities",
		"query_metrics",
		"search_entities",
	}}
	bundle := loadRoleScopedEntityToolBundle(t, actor, true)
	ctx, exec, _ := newEntityToolTestHarnessWithBundleAndLegacyAccess(t, actor, bundle, false)

	defs := exec.ToolDefinitionsForActor(actor)
	names := roleScopedToolDefinitionMap(defs)
	for _, name := range []string{
		"read_validation_case",
		"read_validation_case_status",
		"read_validation_case_business_brief",
		"save_validation_case_business_brief",
		"update_validation_case_business_brief_summary",
		"update_validation_case_business_brief_confidence",
	} {
		if _, ok := names[name]; !ok {
			t.Fatalf("expected generated role-scoped tool %q in %#v", name, sortedRoleScopedToolNames(names))
		}
	}
	for _, name := range []string{"save_validation_case_status", "update_validation_case_status_value"} {
		if _, ok := names[name]; ok {
			t.Fatalf("create-only field produced mutation tool %q in %#v", name, sortedRoleScopedToolNames(names))
		}
	}
	legacyNames := []string{
		"create_entity",
		"get_entity",
		"get_subject_status",
		"query_entities",
		"query_metrics",
		"save_entity_field",
		"search_entities",
	}
	for _, name := range legacyNames {
		if _, ok := names[name]; ok {
			t.Fatalf("legacy entity tool %q remained visible in %#v", name, sortedRoleScopedToolNames(names))
		}
	}

	saveSchema := names["save_validation_case_business_brief"].Schema.(map[string]any)
	props, _ := saveSchema["properties"].(map[string]any)
	if _, ok := props["entity_id"]; ok {
		t.Fatalf("generated save schema exposes entity_id: %#v", saveSchema)
	}
	if _, ok := props["value"]; !ok {
		t.Fatalf("generated save schema missing value: %#v", saveSchema)
	}

	capabilityNames := append([]string{"read_validation_case"}, legacyNames...)
	caps := exec.ToolCapabilitiesForActor(actor, capabilityNames, nil)
	if cap, ok := caps.Capability("read_validation_case"); !ok || !cap.Visible || !cap.Callable {
		t.Fatalf("read_validation_case capability = %#v, ok=%v; want visible/callable", cap, ok)
	}
	for _, name := range legacyNames {
		if cap, ok := caps.Capability(name); !ok || cap.Visible || cap.Callable || cap.DenialReason != "tool_not_allowed" {
			t.Fatalf("%s capability = %#v, ok=%v; want denied for opted-in actor", name, cap, ok)
		}
		_, err := exec.Execute(ctx, name, map[string]any{})
		requireToolFailure(t, err, runtimefailures.ClassAuthorizationDenied, "tool_not_allowed")
	}
}

func TestRoleScopedEntityTools_ExcludeEqualityParticipantWriteAffordances(t *testing.T) {
	actor := models.AgentConfig{ExecutionMode: "live", ID: "validation-orchestrator", Role: "validation_orchestrator", Tools: []string{"save_entity_field"}}
	bundle := loadWave1EntityToolMultiFlowBundle(t, map[string]entityToolFlowFixture{
		"validation": {
			SchemaYAML: `
name: validation
mode: static
initial_state: queued
states: [queued, closed]
terminal_states: [closed]
tool_surface:
  role_scoped_entity_tools: true
`,
			TypesYAML: `
types:
  manifest:
    component: text
    owner:
      type: text
      equal_to: component
    description: text
`,
			EntitiesYAML: `
validation_case:
  component: text
  owner:
    type: text
    equal_to: component
  manifest: manifest
  notes: text
`,
			AgentsYAML: `
validation-orchestrator:
  id: validation-orchestrator
  role: validation_orchestrator
  tools:
    - save_entity_field
  entity_writes:
    validation_case:
      save:
        - component
        - owner
        - manifest
        - notes
`,
		},
	})
	exec := runtimetools.NewExecutorWithOptions(nil, nil, runtimetools.ExecutorOptions{
		WorkflowSource: semanticview.Wrap(bundle),
	})

	names := roleScopedToolDefinitionMap(exec.ToolDefinitionsForActor(actor))
	for _, name := range []string{
		"save_validation_case_manifest",
		"update_validation_case_manifest_description",
		"save_validation_case_notes",
	} {
		if _, ok := names[name]; !ok {
			t.Fatalf("expected generated role-scoped tool %q in %#v", name, sortedRoleScopedToolNames(names))
		}
	}
	for _, name := range []string{
		"save_validation_case_component",
		"save_validation_case_owner",
		"update_validation_case_manifest_component",
		"update_validation_case_manifest_owner",
	} {
		if _, ok := names[name]; ok {
			t.Fatalf("equality participant produced mutation tool %q in %#v", name, sortedRoleScopedToolNames(names))
		}
	}
}

func TestRoleScopedEntityTools_CurrentEntityEligibilityFiltersTurnSurface(t *testing.T) {
	actor := models.AgentConfig{ExecutionMode: "live", ID: "validation-orchestrator", Role: "validation_orchestrator", Tools: []string{
		"get_entity",
		"save_entity_field",
		"query_entities",
	}}
	bundle := loadRoleScopedEntityToolBundle(t, actor, true)
	ctx, exec, db := newEntityToolTestHarnessWithBundleAndLegacyAccess(t, actor, bundle, false)
	currentID := uuid.NewString()
	foreignID := uuid.NewString()
	seedEntityStateRow(t, db, currentID, "", "validation/inst-1", "validation_case", "queued", map[string]any{
		"status":         "open",
		"business_brief": map[string]any{"summary": "valid", "confidence": 1},
	}, time.Now().UTC())
	seedEntityStateRow(t, db, foreignID, "", "run-root", "default", "queued", map[string]any{
		"status": "root",
	}, time.Now().UTC())

	validCtx := runtimebus.WithInboundEvent(ctx, eventtest.RootIngress(
		"evt-current",
		events.EventType("validation.started"),
		"",
		"",
		nil,
		0,
		entityToolTestRunID,
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, currentID), "validation/inst-1"),
		time.Time{},
	))
	invalidCtx := runtimebus.WithInboundEvent(ctx, eventtest.RootIngress(
		"evt-root",
		events.EventType("validation.started"),
		"",
		"",
		nil,
		0,
		entityToolTestRunID,
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, foreignID), "run-root"),
		time.Time{},
	))

	validNames := roleScopedToolDefinitionMap(exec.ToolDefinitionsForActorInContext(validCtx, actor))
	for _, name := range []string{"read_validation_case", "read_validation_case_status", "save_validation_case_business_brief", "update_validation_case_business_brief_summary"} {
		if _, ok := validNames[name]; !ok {
			t.Fatalf("valid current entity filtered %s from %#v", name, sortedRoleScopedToolNames(validNames))
		}
	}

	invalidNames := roleScopedToolDefinitionMap(exec.ToolDefinitionsForActorInContext(invalidCtx, actor))
	for _, name := range []string{"read_validation_case", "read_validation_case_status", "save_validation_case_business_brief", "update_validation_case_business_brief_summary"} {
		if _, ok := invalidNames[name]; ok {
			t.Fatalf("invalid current entity left %s visible in %#v", name, sortedRoleScopedToolNames(invalidNames))
		}
	}

	caps := exec.ToolCapabilitiesForActorInContext(invalidCtx, actor, []string{"read_validation_case", "save_validation_case_business_brief"}, nil)
	for _, name := range []string{"read_validation_case", "save_validation_case_business_brief"} {
		cap, ok := caps.Capability(name)
		if !ok || cap.Visible || cap.Callable || cap.DenialReason != "current_entity_contract_unavailable" {
			t.Fatalf("%s invalid-current capability = %#v ok=%v", name, cap, ok)
		}
	}
	deniedCtx := toolcapabilities.WithContext(invalidCtx, caps)
	_, err := exec.Execute(deniedCtx, "read_validation_case", map[string]any{})
	requireToolFailure(t, err, runtimefailures.ClassAuthorizationDenied, "tool_not_allowed")
}

func TestRoleScopedEntityTools_GeneratedSchemasAreClosedAndRuntimeRejectsExtras(t *testing.T) {
	actor := models.AgentConfig{ExecutionMode: "live", ID: "validation-orchestrator", Role: "validation_orchestrator", Tools: []string{"save_entity_field"}}
	bundle := loadRoleScopedEntityToolBundle(t, actor, true)
	source := semanticview.Wrap(bundle)
	if errs := runtimetools.ValidateGeneratedToolSchemaClosureForSource(source); len(errs) > 0 {
		t.Fatalf("ValidateGeneratedToolSchemaClosureForSource errors = %#v", errs)
	}
	ctx, exec, db := newEntityToolTestHarnessWithBundleAndLegacyAccess(t, actor, bundle, false)
	names := roleScopedToolDefinitionMap(exec.ToolDefinitionsForActor(actor))
	saveSchema := names["save_validation_case_business_brief"].Schema.(map[string]any)
	if err := runtimetools.ValidatePayloadAgainstSchema(saveSchema, map[string]any{
		"value": map[string]any{
			"summary":    "provider-side",
			"confidence": 7,
			"notes":      "undeclared",
		},
	}); err == nil {
		t.Fatal("provider-visible generated save schema accepted undeclared nested key")
	}

	entityID := uuid.NewString()
	seedEntityStateRow(t, db, entityID, "", "validation/inst-1", "validation_case", "queued", map[string]any{
		"status":         "open",
		"business_brief": map[string]any{"summary": "before", "confidence": 1},
	}, time.Now().UTC())
	currentCtx := runtimebus.WithInboundEvent(ctx, eventtest.RootIngress(
		"evt-current",
		events.EventType("validation.started"),
		"",
		"",
		nil,
		0,
		entityToolTestRunID,
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), "validation/inst-1"),
		time.Time{},
	))
	if _, err := exec.Execute(currentCtx, "save_validation_case_business_brief", map[string]any{
		"value": map[string]any{
			"summary":    "after",
			"confidence": 9,
			"notes":      "runtime bypass must reject",
		},
	}); err == nil {
		t.Fatal("runtime execution accepted undeclared nested key")
	}
	if got := persistedEntityField(t, db, entityID, "business_brief"); strings.Contains(got, "after") || strings.Contains(got, "runtime bypass") {
		t.Fatalf("runtime validation failure still mutated entity: %s", got)
	}
}

func TestRoleScopedEntityTools_NonOptedActorReceivesGeneratedSurfaceByDefault(t *testing.T) {
	actor := models.AgentConfig{ExecutionMode: "live", ID: "validation-orchestrator", Role: "validation_orchestrator", Tools: []string{"get_entity", "query_entities"}}
	bundle := loadRoleScopedEntityToolBundle(t, actor, false)
	_, exec, _ := newEntityToolTestHarnessWithBundleAndLegacyAccess(t, actor, bundle, false)

	names := roleScopedToolDefinitionMap(exec.ToolDefinitionsForActor(actor))
	for _, name := range []string{"get_entity", "query_entities"} {
		if _, ok := names[name]; ok {
			t.Fatalf("legacy entity tool %q remained visible without opt-in in %#v", name, sortedRoleScopedToolNames(names))
		}
	}
	if _, ok := names["read_validation_case"]; !ok {
		t.Fatalf("generated role-scoped tool was not visible by default in %#v", sortedRoleScopedToolNames(names))
	}
}

func TestRoleScopedEntityTools_CurrentEntityBindingAndBypassRejection(t *testing.T) {
	actor := models.AgentConfig{ExecutionMode: "live", ID: "validation-orchestrator", Role: "validation_orchestrator", Tools: []string{
		"create_entity",
		"get_entity",
		"save_entity_field",
		"query_entities",
	}}
	bundle := loadRoleScopedEntityToolBundle(t, actor, true)
	ctx, exec, db := newEntityToolTestHarnessWithBundleAndLegacyAccess(t, actor, bundle, false)
	currentID := uuid.NewString()
	siblingID := uuid.NewString()
	foreignID := uuid.NewString()
	seedEntityStateRow(t, db, currentID, "", "validation/inst-1", "validation_case", "queued", map[string]any{
		"status":         "open",
		"business_brief": map[string]any{"summary": "before", "confidence": 1},
	}, time.Now().UTC())
	seedEntityStateRow(t, db, siblingID, "", "validation/inst-1", "validation_case", "queued", map[string]any{
		"status":         "open",
		"business_brief": map[string]any{"summary": "sibling", "confidence": 2},
	}, time.Now().UTC())
	seedEntityStateRow(t, db, foreignID, "", "other/inst-1", "other_case", "queued", map[string]any{
		"status": "foreign",
	}, time.Now().UTC())
	currentCtx := runtimebus.WithInboundEvent(ctx, eventtest.RootIngress(
		"evt-current",
		events.EventType("validation.started"),
		"",
		"",
		nil,
		0,
		entityToolTestRunID,
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, currentID), "validation/inst-1"),
		time.Time{},
	))

	if _, err := exec.Execute(currentCtx, "save_validation_case_business_brief", map[string]any{
		"value": map[string]any{"summary": "after", "confidence": 9},
	}); err != nil {
		t.Fatalf("save_validation_case_business_brief: %v", err)
	}
	got, err := exec.Execute(currentCtx, "read_validation_case_business_brief", map[string]any{})
	if err != nil {
		t.Fatalf("read_validation_case_business_brief: %v", err)
	}
	brief, ok := got.(map[string]any)
	if !ok || strings.TrimSpace(asString(brief["summary"])) != "after" {
		t.Fatalf("read current business_brief = %#v, want updated current entity", got)
	}
	if got := persistedEntityField(t, db, siblingID, "business_brief"); strings.Contains(got, "after") {
		t.Fatalf("generated save touched sibling entity: %s", got)
	}

	if _, err := exec.Execute(currentCtx, "read_validation_case", map[string]any{"entity_id": siblingID}); err == nil {
		t.Fatalf("read_validation_case accepted caller-supplied entity_id")
	}
	if _, err := exec.Execute(ctx, "read_validation_case", map[string]any{}); err == nil {
		t.Fatalf("read_validation_case succeeded without current inbound entity")
	}
	foreignCtx := runtimebus.WithInboundEvent(ctx, eventtest.RootIngress(
		"evt-foreign",
		events.EventType("other.started"),
		"",
		"",
		nil,
		0,
		entityToolTestRunID,
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, foreignID), "other/inst-1"),
		time.Time{},
	))
	if _, err := exec.Execute(foreignCtx, "read_validation_case", map[string]any{}); err == nil {
		t.Fatalf("read_validation_case accepted foreign current entity")
	}
	for _, legacy := range []string{"get_entity", "create_entity"} {
		if _, err := exec.Execute(currentCtx, legacy, map[string]any{"entity_id": currentID}); err == nil {
			t.Fatalf("legacy tool %s remained callable for opted-in actor", legacy)
		}
	}
}

func TestRoleScopedEntityTools_ReadsLargeValidationCaseWithoutLoss(t *testing.T) {
	actor := models.AgentConfig{ExecutionMode: "live", ID: "validation-orchestrator", Role: "validation_orchestrator"}
	bundle := loadRoleScopedEntityToolBundle(t, actor, true)
	ctx, exec, db := newEntityToolTestHarnessWithBundleAndLegacyAccess(t, actor, bundle, false)
	entityID := uuid.NewString()
	brief := strings.Repeat("business brief ", 1800)
	specProblem := strings.Repeat("problem statement ", 900)
	specApproach := strings.Repeat("technical approach ", 900)
	seedEntityStateRow(t, db, entityID, "", "validation/inst-1", "validation_case", "queued", map[string]any{
		"status": "open",
		"business_brief": map[string]any{
			"summary":    brief,
			"confidence": 10,
		},
		"mvp_spec": map[string]any{
			"problem_statement":  specProblem,
			"technical_approach": specApproach,
		},
	}, time.Now().UTC())
	currentCtx := runtimebus.WithInboundEvent(ctx, eventtest.RootIngress(
		"evt-current",
		events.EventType("validation.started"),
		"",
		"",
		nil,
		0,
		entityToolTestRunID,
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), "validation/inst-1"),
		time.Time{},
	))

	whole, err := exec.Execute(currentCtx, "read_validation_case", map[string]any{})
	if err != nil {
		t.Fatalf("read_validation_case: %v", err)
	}
	rawWhole, err := json.Marshal(whole)
	if err != nil {
		t.Fatalf("marshal whole read: %v", err)
	}
	if len(rawWhole) < 40*1024 {
		t.Fatalf("whole read fixture size = %d, want >= 40KB", len(rawWhole))
	}
	if strings.Contains(string(rawWhole), `"truncated"`) || strings.Contains(string(rawWhole), `"preview"`) || strings.Contains(string(rawWhole), `"follow_up"`) {
		t.Fatalf("whole typed read contains lossy projection markers")
	}
	wholeMap, ok := whole.(map[string]any)
	if !ok {
		t.Fatalf("whole read = %T, want map", whole)
	}
	fields, _ := wholeMap["fields"].(map[string]any)
	mvp, _ := fields["mvp_spec"].(map[string]any)
	if asString(mvp["problem_statement"]) != specProblem {
		t.Fatalf("whole read mvp_spec.problem_statement length = %d, want %d", len(asString(mvp["problem_statement"])), len(specProblem))
	}

	field, err := exec.Execute(currentCtx, "read_validation_case_mvp_spec", map[string]any{})
	if err != nil {
		t.Fatalf("read_validation_case_mvp_spec: %v", err)
	}
	fieldMap, ok := field.(map[string]any)
	if !ok {
		t.Fatalf("field read = %T, want map", field)
	}
	if asString(fieldMap["technical_approach"]) != specApproach {
		t.Fatalf("field read technical_approach length = %d, want %d", len(asString(fieldMap["technical_approach"])), len(specApproach))
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
		ExecutionMode: "live",
		ID:            "tester",
		Role:          "operator",
		Tools:         []string{"create_entity", "get_entity", "save_entity_field"},
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
		{name: "read-only list selector", field: "validation_kit.checklist.size"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := exec.Execute(ctx, "save_entity_field", map[string]any{
				"entity_id": entityID,
				"field":     tc.field,
				"value":     "x",
			})
			re, ok := runtimefailures.As(err)
			if err == nil || !ok || re.Failure.Detail.Code != "invalid_tool_input" {
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
		ExecutionMode: "live",
		ID:            "tester",
		Role:          "operator",
		Tools:         []string{"create_entity", "save_entity_field", "get_entity"},
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

	_, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_id": entityID,
		"field":     "code",
		"value":     "acct-002",
	})
	immutableFailure := requireToolFailure(t, err, runtimefailures.ClassAuthorizationDenied, "immutable_field_write_forbidden")
	if immutableFailure.Detail.Attributes["field"] != "code" {
		t.Fatalf("immutable failure attributes = %#v", immutableFailure.Detail.Attributes)
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
		ExecutionMode: "live",
		ID:            "tester",
		Role:          "operator",
		Tools:         []string{"create_entity", "get_entity", "query_entities"},
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
		ExecutionMode: "live",
		ID:            "tester",
		Role:          "operator",
		Tools:         []string{"create_entity", "get_entity", "save_entity_field"},
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

func TestEntityTools_GetEntityReturnsForkLocalMaterializedRevision(t *testing.T) {
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "tester",
		Type:          "internal",
		Role:          "operator",
		Tools:         []string{"get_entity"},
	}
	bundle := loadWave1EntityToolBundle(t, actor, "review", "accounts", `
types: {}
`, `
accounts:
  status: text
`)
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	stateEventID := uuid.NewString()
	forkEventID := uuid.NewString()
	at := time.Unix(1700000710, 0).UTC()
	forkAt := at.Add(30 * time.Second)
	ctx := unmanagedToolTestContext()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_hash, bundle_source, bundle_fingerprint, started_at)
		VALUES ($1::uuid, 'running', $2, $3, $4, $5)
	`, sourceRunID, authorActivityTestBundleSourceFact.BundleHash, authorActivityTestBundleSourceFact.BundleSource, authorActivityTestBundleSourceFact.BundleFingerprint, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed source run: %v", err)
	}
	for _, fixture := range []struct {
		id        string
		eventType events.EventType
		createdAt time.Time
	}{
		{id: stateEventID, eventType: "fork.state_entry", createdAt: at},
		{id: forkEventID, eventType: "fork.field_only", createdAt: forkAt},
	} {
		storetest.InsertCanonicalEventRecord(t, ctx, db, runtimeauthoractivity.DialectPostgres, eventtest.PersistedProjectionForProducer(
			fixture.id, fixture.eventType, eventtest.Producer(events.EventProducerPlatform, "test"),
			"", []byte(`{}`), 0, sourceRunID, "",
			events.EventEnvelope{EntityID: entityID, Scope: events.EventScopeEntity}, fixture.createdAt,
		))
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', 'null'::jsonb, '"queued"'::jsonb, $3::uuid, 'platform', 'revision-test', 'seed', $5),
			($1::uuid, $2::uuid, 'status', 'null'::jsonb, '"open"'::jsonb, $4::uuid, 'platform', 'revision-test', 'field-only', $6)
	`, sourceRunID, entityID, stateEventID, forkEventID, at, forkAt); err != nil {
		t.Fatalf("seed mutations: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'review/inst-1', 'default', 'done',
			'{}'::jsonb, '{"status":"closed"}'::jsonb, '{}'::jsonb, 7, $3, $4, $3
		)
	`, sourceRunID, entityID, at.Add(time.Minute), at); err != nil {
		t.Fatalf("seed source entity_state: %v", err)
	}
	captureEntityToolRunForkRevision(t, db, sourceRunID)
	result, err := pg.MaterializeRunFork(ctx, store.RunForkMaterializeRequest{SourceRunID: sourceRunID, At: forkEventID})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}
	exec := runtimetools.NewExecutorWithOptions(nil, nil, runtimetools.ExecutorOptions{
		EntityStore:                    pg,
		WorkflowSource:                 semanticview.Wrap(bundle),
		AllowInternalLegacyEntityTools: true,
	})
	forkCtx := runtimetools.WithActor(runtimecorrelation.WithRunID(unmanagedToolTestContext(), result.ForkRunID), actor)
	out, err := exec.Execute(forkCtx, "get_entity", map[string]any{"entity_id": entityID})
	if err != nil {
		t.Fatalf("get_entity fork entity: %v", err)
	}
	entity, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("expected entity map, got %#v", out)
	}
	if got := int(testNumericValue(entity["revision"])); got != 1 {
		t.Fatalf("fork get_entity revision = %d, want fork-local revision 1", got)
	}
	fields, ok := entity["fields"].(map[string]any)
	if !ok || strings.TrimSpace(asString(fields["status"])) != "open" {
		t.Fatalf("fork get_entity fields = %#v, want status=open", entity["fields"])
	}
	if got := strings.TrimSpace(asString(entity["current_state"])); got != "queued" {
		t.Fatalf("fork get_entity current_state = %q, want queued", got)
	}
	enteredStateAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(asString(entity["entered_state_at"])))
	if err != nil {
		t.Fatalf("parse fork get_entity entered_state_at %q: %v", entity["entered_state_at"], err)
	}
	if !enteredStateAt.Equal(at) {
		t.Fatalf("fork get_entity entered_state_at = %s, want state-entry timestamp %s", enteredStateAt, at)
	}
}

func TestEntityTools_SaveEntityFieldAfterForkActivationUsesForkRunID(t *testing.T) {
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "tester",
		Type:          "internal",
		Role:          "operator",
		Tools:         []string{"get_entity", "save_entity_field"},
	}
	bundle := loadWave1EntityToolBundle(t, actor, "review", "accounts", `
types: {}
`, `
accounts:
  status: text
`)
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	stateEventID := uuid.NewString()
	forkEventID := uuid.NewString()
	at := time.Unix(1700000720, 0).UTC()
	forkAt := at.Add(30 * time.Second)
	ctx := unmanagedToolTestContext()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_hash, bundle_source, bundle_fingerprint, started_at)
		VALUES ($1::uuid, 'running', $2, $3, $4, $5)
	`, sourceRunID, authorActivityTestBundleSourceFact.BundleHash, authorActivityTestBundleSourceFact.BundleSource, authorActivityTestBundleSourceFact.BundleFingerprint, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed source run: %v", err)
	}
	for _, fixture := range []struct {
		id        string
		eventType events.EventType
		createdAt time.Time
	}{
		{id: stateEventID, eventType: "fork.state_entry", createdAt: at},
		{id: forkEventID, eventType: "fork.field_only", createdAt: forkAt},
	} {
		storetest.InsertCanonicalEventRecord(t, ctx, db, runtimeauthoractivity.DialectPostgres, eventtest.PersistedProjectionForProducer(
			fixture.id, fixture.eventType, eventtest.Producer(events.EventProducerPlatform, "test"),
			"", []byte(`{}`), 0, sourceRunID, "",
			events.EventEnvelope{EntityID: entityID, Scope: events.EventScopeEntity}, fixture.createdAt,
		))
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', 'null'::jsonb, '"queued"'::jsonb, $3::uuid, 'platform', 'activation-tool-test', 'seed', $5),
			($1::uuid, $2::uuid, 'status', 'null'::jsonb, '"open"'::jsonb, $4::uuid, 'platform', 'activation-tool-test', 'field-only', $6)
	`, sourceRunID, entityID, stateEventID, forkEventID, at, forkAt); err != nil {
		t.Fatalf("seed mutations: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'review/inst-1', 'default', 'queued',
			'{}'::jsonb, '{"status":"open"}'::jsonb, '{}'::jsonb, 1, $3, $4, $4
		)
	`, sourceRunID, entityID, at, forkAt); err != nil {
		t.Fatalf("seed source entity_state: %v", err)
	}
	captureEntityToolRunForkRevision(t, db, sourceRunID)
	materialized, err := pg.MaterializeRunFork(ctx, store.RunForkMaterializeRequest{SourceRunID: sourceRunID, At: forkEventID})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}
	activated, err := pg.ActivateRunFork(ctx, store.RunForkActivateRequest{ForkRunID: materialized.ForkRunID, ConfirmSourceFreeze: true})
	if err != nil {
		t.Fatalf("ActivateRunFork: %v", err)
	}
	if !activated.Activated || !activated.SourceFrozen {
		t.Fatalf("activation result = %#v", activated)
	}

	exec := runtimetools.NewExecutorWithOptions(nil, nil, runtimetools.ExecutorOptions{
		EntityStore:                    pg,
		WorkflowSource:                 semanticview.Wrap(bundle),
		AllowInternalLegacyEntityTools: true,
	})
	forkCtx := runtimetools.WithActor(runtimecorrelation.WithRunID(unmanagedToolTestContext(), materialized.ForkRunID), actor)
	if _, err := exec.Execute(forkCtx, "save_entity_field", map[string]any{
		"entity_id": entityID,
		"field":     "status",
		"value":     "fork-active",
	}); err != nil {
		t.Fatalf("save_entity_field on activated fork: %v", err)
	}

	var sourceStatus, forkStatus string
	var forkRevision int
	if err := db.QueryRowContext(ctx, `
		SELECT fields->>'status'
		FROM entity_state
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, sourceRunID, entityID).Scan(&sourceStatus); err != nil {
		t.Fatalf("load source entity_state: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT fields->>'status', revision
		FROM entity_state
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, materialized.ForkRunID, entityID).Scan(&forkStatus, &forkRevision); err != nil {
		t.Fatalf("load fork entity_state: %v", err)
	}
	if sourceStatus != "open" || forkStatus != "fork-active" {
		t.Fatalf("source/fork status = %s/%s, want open/fork-active", sourceStatus, forkStatus)
	}
	if forkRevision != 2 {
		t.Fatalf("fork revision = %d, want 2 after fork-local write", forkRevision)
	}
	var forkMutationCount, sourceMutationCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM entity_mutations
		WHERE run_id = $1::uuid AND entity_id = $2::uuid AND field = 'status' AND writer_type = 'agent' AND handler_step = 'save_entity_field'
	`, materialized.ForkRunID, entityID).Scan(&forkMutationCount); err != nil {
		t.Fatalf("count fork status mutation: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM entity_mutations
		WHERE run_id = $1::uuid AND entity_id = $2::uuid AND field = 'status' AND writer_type = 'agent' AND handler_step = 'save_entity_field'
	`, sourceRunID, entityID).Scan(&sourceMutationCount); err != nil {
		t.Fatalf("count source status mutation: %v", err)
	}
	if forkMutationCount != 1 || sourceMutationCount != 0 {
		t.Fatalf("fork/source save_entity_field mutation count = %d/%d, want 1/0", forkMutationCount, sourceMutationCount)
	}
}

func captureEntityToolRunForkRevision(t *testing.T, db *sql.DB, runID string) {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin entity-tool run-fork revision: %v", err)
	}
	defer tx.Rollback()
	if _, err := runforkrevision.Capture(context.Background(), tx, runID, runforkrevision.AllFamilies()...); err != nil {
		t.Fatalf("capture entity-tool run-fork revision: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit entity-tool run-fork revision: %v", err)
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
	re, ok := runtimefailures.As(err)
	if err == nil || !ok || re.Failure.Detail.Code != "invalid_tool_input" {
		t.Fatalf("expected invalid_tool_input, got %v", err)
	}
	if re.Failure.Detail.Attributes["expression"] != `metadata.regoin == "us"` {
		t.Fatalf("filter failure attributes = %#v", re.Failure.Detail.Attributes)
	}
}

func TestEntityTools_QueryEntitiesFilterRejectsEntityScopedSelectors(t *testing.T) {
	ctx, exec := newEntityToolTestExecutor(t)

	_, err := exec.Execute(ctx, "query_entities", map[string]any{
		"filter": `entity.metadata.region == "us"`,
		"limit":  10,
	})
	re, ok := runtimefailures.As(err)
	if err == nil || !ok || re.Failure.Detail.Code != "invalid_tool_input" {
		t.Fatalf("expected invalid_tool_input, got %v", err)
	}
	if re.Failure.Detail.Attributes["expression"] != `entity.metadata.region == "us"` {
		t.Fatalf("filter failure attributes = %#v", re.Failure.Detail.Attributes)
	}
}

func TestEntityTools_QueryMetricsFilterRejectsUndeclaredFieldBeforeEval(t *testing.T) {
	ctx, exec := newEntityToolTestExecutor(t)

	_, err := exec.Execute(ctx, "query_metrics", map[string]any{
		"metric": "count",
		"filter": `metadata.regoin == "us"`,
	})
	re, ok := runtimefailures.As(err)
	if err == nil || !ok || re.Failure.Detail.Code != "invalid_tool_input" {
		t.Fatalf("expected invalid_tool_input, got %v", err)
	}
	if re.Failure.Detail.Attributes["expression"] != `metadata.regoin == "us"` {
		t.Fatalf("metric filter failure attributes = %#v", re.Failure.Detail.Attributes)
	}
}

func TestEntityTools_ReadToolsRejectForeignExplicitTargetContract(t *testing.T) {
	bundle := loadWave1EntityToolMultiFlowBundle(t, map[string]entityToolFlowFixture{
		"discovery": {
			EntitiesYAML: `
campaign:
  status: text
`,
			AgentsYAML: entityToolAgentYAML(models.AgentConfig{ExecutionMode: "live", ID: "researcher", Role: "researcher", Tools: []string{"query_entities", "query_metrics", "search_entities"}}),
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
		ExecutionMode: "live",
		ID:            "researcher",
		Role:          "researcher",
		Tools:         []string{"query_entities", "query_metrics", "search_entities"},
	}, bundle)
	subjectID := uuid.NewString()
	seedEntityStateRow(t, db, uuid.NewString(), subjectID, "discovery/run-1", "campaign", "active", map[string]any{"status": "open"}, time.Now().UTC().Truncate(time.Second))
	seedEntityStateRow(t, db, uuid.NewString(), subjectID, "signal-search/run-1", "signal", "active", map[string]any{
		"signal_strength": 80,
		"processed":       false,
		"status":          "new",
	}, time.Now().UTC().Truncate(time.Second))

	queryOut, err := exec.Execute(ctx, "query_entities", map[string]any{
		"entity_type": "discovery.campaign",
		"filter":      `status == "open"`,
		"select":      []string{"status"},
		"limit":       10,
	})
	if err != nil {
		t.Fatalf("query_entities owned explicit target: %v", err)
	}
	queryResult, ok := queryOut.(map[string]any)
	if !ok {
		t.Fatalf("expected query result map, got %#v", queryOut)
	}
	queryRows, ok := queryResult["results"].([]map[string]any)
	if !ok || len(queryRows) != 1 {
		t.Fatalf("query results = %#v, want one campaign row", queryResult["results"])
	}
	if got := strings.TrimSpace(asString(queryRows[0]["status"])); got != "open" {
		t.Fatalf("status = %q, want open", got)
	}

	for _, tc := range []struct {
		name  string
		tool  string
		input map[string]any
	}{
		{
			name: "query_entities",
			tool: "query_entities",
			input: map[string]any{
				"entity_type": "signal-search.signal",
				"filter":      `status == "new"`,
				"select":      []string{"status"},
				"limit":       10,
			},
		},
		{
			name: "search_entities",
			tool: "search_entities",
			input: map[string]any{
				"entity_type":   "signal-search.signal",
				"current_state": "active",
				"filter":        map[string]any{"status": "new"},
				"limit":         10,
			},
		},
		{
			name: "query_metrics",
			tool: "query_metrics",
			input: map[string]any{
				"entity_type": "signal-search.signal",
				"metric":      "count",
				"filter":      `status == "new"`,
			},
		},
	} {
		_, err := exec.Execute(ctx, tc.tool, tc.input)
		re, ok := runtimefailures.As(err)
		if err == nil || !ok || re.Failure.Detail.Code != "invalid_tool_input" {
			t.Fatalf("%s expected invalid_tool_input, got %v", tc.name, err)
		}
		if re.Failure.Detail.Attributes["entity_type"] != "signal-search.signal" {
			t.Fatalf("%s target failure attributes = %#v", tc.name, re.Failure.Detail.Attributes)
		}
	}
}

func TestEntityTools_RootActorImplicitReadToolsDoNotReadChildFlowRows(t *testing.T) {
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "rooter",
		Role:          "rooter",
		Tools:         []string{"query_entities", "search_entities", "query_metrics"},
	}
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	root := t.TempDir()
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	writeEntityToolFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: root-read-bundle
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: child
    flow: child
    mode: static
`)
	writeEntityToolFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: root-read-bundle\n")
	writeEntityToolFixtureFile(t, filepath.Join(root, "entities.yaml"), `
root_subject:
  status: text
  score: integer
`)
	writeEntityToolFixtureFile(t, filepath.Join(root, "agents.yaml"), entityToolAgentYAML(actor))
	writeEntityToolFixtureFile(t, filepath.Join(root, "flows", "child", "schema.yaml"), `
name: child
mode: static
initial_state: active
states: [active, done]
terminal_states: [done]
`)
	writeEntityToolFixtureFile(t, filepath.Join(root, "flows", "child", "entities.yaml"), `
child_subject:
  status: text
  score: integer
`)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides(%s): %v", root, err)
	}
	ctx, exec, db := newEntityToolTestHarnessWithBundle(t, actor, bundle)
	rootID := uuid.NewString()
	childID := uuid.NewString()
	seedEntityStateRow(t, db, rootID, rootID, "", "root_subject", "active", map[string]any{
		"status": "root",
		"score":  7,
	}, time.Now().UTC().Truncate(time.Second))
	seedEntityStateRow(t, db, childID, childID, "child/run-1", "child_subject", "active", map[string]any{
		"status": "child",
		"score":  99,
	}, time.Now().UTC().Truncate(time.Second))

	queryOut, err := exec.Execute(ctx, "query_entities", map[string]any{
		"select": []string{"status", "score"},
		"limit":  10,
	})
	if err != nil {
		t.Fatalf("query_entities root actor: %v", err)
	}
	queryRows := queryOut.(map[string]any)["results"].([]map[string]any)
	if len(queryRows) != 1 || strings.TrimSpace(asString(queryRows[0]["entity_id"])) != rootID {
		t.Fatalf("query_entities rows = %#v, want only root row %s", queryRows, rootID)
	}

	searchOut, err := exec.Execute(ctx, "search_entities", map[string]any{"limit": 10})
	if err != nil {
		t.Fatalf("search_entities root actor: %v", err)
	}
	searchRows := searchOut.(map[string]any)["results"].([]map[string]any)
	if len(searchRows) != 1 || strings.TrimSpace(asString(searchRows[0]["entity_id"])) != rootID {
		t.Fatalf("search_entities rows = %#v, want only root row %s", searchRows, rootID)
	}
	if total := searchOut.(map[string]any)["total"].(int); total != 1 {
		t.Fatalf("search_entities total = %d, want 1", total)
	}

	metricOut, err := exec.Execute(ctx, "query_metrics", map[string]any{"metric": "count"})
	if err != nil {
		t.Fatalf("query_metrics root actor: %v", err)
	}
	if got := testNumericValue(metricOut.(map[string]any)["value"]); got != 1 {
		t.Fatalf("query_metrics count = %#v, want 1", metricOut)
	}
}

func TestEntityTools_BracketListTypeRefsAcrossConsumers(t *testing.T) {
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "validator",
		Role:          "validator",
		Tools:         []string{"create_entity", "get_entity", "save_entity_field", "search_entities", "query_entities", "query_metrics"},
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
			AgentsYAML: entityToolAgentYAML(models.AgentConfig{ExecutionMode: "live", ID: "researcher", Role: "researcher", Tools: []string{"query_entities", "query_metrics", "search_entities"}}),
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
		ExecutionMode: "live",
		ID:            "researcher",
		Role:          "researcher",
		Tools:         []string{"query_entities", "query_metrics", "search_entities"},
	}, bundle)

	_, err := exec.Execute(ctx, "query_entities", map[string]any{
		"entity_type": "discovery.campaign",
		"filter":      `statu == "open"`,
	})
	re, ok := runtimefailures.As(err)
	if err == nil || !ok || re.Failure.Detail.Code != "invalid_tool_input" {
		t.Fatalf("expected query_entities invalid_tool_input, got %v", err)
	}
	if re.Failure.Detail.Attributes["expression"] != `statu == "open"` {
		t.Fatalf("query filter failure attributes = %#v", re.Failure.Detail.Attributes)
	}

	_, err = exec.Execute(ctx, "query_metrics", map[string]any{
		"entity_type": "discovery.campaign",
		"metric":      "sum",
		"field":       "statu",
	})
	re, ok = runtimefailures.As(err)
	if err == nil || !ok || re.Failure.Detail.Code != "invalid_tool_input" {
		t.Fatalf("expected query_metrics invalid_tool_input, got %v", err)
	}
	if re.Failure.Detail.Attributes["field"] != "statu" {
		t.Fatalf("metric field failure attributes = %#v", re.Failure.Detail.Attributes)
	}

	_, err = exec.Execute(ctx, "search_entities", map[string]any{
		"entity_type": "discovery.campaign",
		"filter":      map[string]any{"statu": "open"},
	})
	re, ok = runtimefailures.As(err)
	if err == nil || !ok || re.Failure.Detail.Code != "invalid_tool_input" {
		t.Fatalf("expected search_entities invalid_tool_input, got %v", err)
	}
	if re.Failure.Detail.Attributes["field"] != "statu" {
		t.Fatalf("object-filter failure attributes = %#v", re.Failure.Detail.Attributes)
	}

	_, err = exec.Execute(ctx, "query_entities", map[string]any{
		"entity_type": "signal-search.signal",
		"filter":      `signal_strenght >= 70`,
	})
	re, ok = runtimefailures.As(err)
	if err == nil || !ok || re.Failure.Detail.Code != "invalid_tool_input" {
		t.Fatalf("expected foreign query_entities invalid_tool_input, got %v", err)
	}
	if re.Failure.Detail.Attributes["entity_type"] != "signal-search.signal" {
		t.Fatalf("foreign target failure attributes = %#v", re.Failure.Detail.Attributes)
	}
	if _, fieldValidated := re.Failure.Detail.Attributes["field"]; fieldValidated {
		t.Fatalf("foreign target reached field validation: %#v", re.Failure.Detail.Attributes)
	}

	_, err = exec.Execute(ctx, "query_entities", map[string]any{
		"filter": `signal_strength >= 70`,
	})
	re, ok = runtimefailures.As(err)
	if err == nil || !ok || re.Failure.Detail.Code != "invalid_tool_input" {
		t.Fatalf("expected default actor contract to reject target-only field, got %v", err)
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
	invalidField := requireToolFailure(t, err, runtimefailures.ClassSchemaInvalid, "invalid_tool_input")
	if invalidField.Detail.Attributes["field"] != "unknown_field" {
		t.Fatalf("invalid field attributes = %#v", invalidField.Detail.Attributes)
	}
}

func TestEntityTools_GetEntityNotFound(t *testing.T) {
	ctx, exec := newEntityToolTestExecutor(t)
	_, err := exec.Execute(ctx, "get_entity", map[string]any{
		"entity_id": uuid.NewString(),
	})
	re, ok := runtimefailures.As(err)
	if err == nil || !ok || re.Failure.Detail.Code != "not_found" {
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
	entityIDFailure := requireToolFailure(t, err, runtimefailures.ClassSchemaInvalid, "invalid_tool_input")
	if entityIDFailure.Detail.Attributes["field"] != "entity_id" {
		t.Fatalf("entity_id failure attributes = %#v", entityIDFailure.Detail.Attributes)
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
	entityTypeFailure := requireToolFailure(t, err, runtimefailures.ClassSchemaInvalid, "invalid_tool_input")
	if entityTypeFailure.Detail.Attributes["field"] != "entity_type" {
		t.Fatalf("entity_type failure attributes = %#v", entityTypeFailure.Detail.Attributes)
	}
}

func TestEntityTools_CreateEntityRejectsCallerSuppliedSubjectID(t *testing.T) {
	ctx, exec := newEntityToolTestExecutor(t)
	_, err := exec.Execute(ctx, "create_entity", map[string]any{
		"subject_id":    uuid.NewString(),
		"flow_instance": "review/inst-1",
		"fields": map[string]any{
			"status": "open",
		},
	})
	subjectFailure := requireToolFailure(t, err, runtimefailures.ClassSchemaInvalid, "invalid_tool_input")
	if subjectFailure.Detail.Attributes["field"] != "subject_id" {
		t.Fatalf("subject_id failure attributes = %#v", subjectFailure.Detail.Attributes)
	}
}

func TestEntityTools_ConstrainedAllowedToolsDoNotPermitLegacyEntityTools(t *testing.T) {
	ctx, exec := newEntityToolTestExecutorWithActor(t, models.AgentConfig{
		ExecutionMode: "live",
		ID:            "tester",
		Role:          "operator",
		Tools:         []string{"emit_something"},
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
	_, err := exec.Execute(ctx, "query_entities", map[string]any{})
	requireToolFailure(t, err, runtimefailures.ClassAuthorizationDenied, "tool_not_allowed")
}

func TestEntityTools_CreateEntityRejectsFlowWithoutEntityContract(t *testing.T) {
	ctx, exec := newEntityToolTestExecutorWithBundle(t, models.AgentConfig{
		ExecutionMode: "live",
		ID:            "tester",
		Role:          "operator",
		Tools:         []string{"create_entity", "save_entity_field", "get_entity"},
	}, &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{InitialStage: "queued"},
	})
	_, err := exec.Execute(ctx, "create_entity", map[string]any{
		"flow_instance": "review/inst-1",
	})
	missingContract := requireToolFailure(t, err, runtimefailures.ClassTargetUnreachable, "not_found")
	if missingContract.Detail.Attributes["flow_path"] != "review/inst-1" {
		t.Fatalf("missing contract attributes = %#v", missingContract.Detail.Attributes)
	}
}

func TestEntityTools_SaveEntityFieldRejectsCrossFlowWrite(t *testing.T) {
	bundle := loadWave1EntityToolMultiFlowBundle(t, map[string]entityToolFlowFixture{
		"analyzer-flow": {
			EntitiesYAML: `
analysis:
  status: text
`,
			AgentsYAML: entityToolAgentYAML(models.AgentConfig{ExecutionMode: "live", ID: "analyzer", Role: "analyzer", Tools: []string{"save_entity_field", "get_entity"}}),
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
		ExecutionMode: "live",
		ID:            "analyzer",
		Role:          "analyzer",
		Tools:         []string{"save_entity_field", "get_entity"},
	}, bundle)
	entityID := uuid.NewString()
	seedEntityStateRow(t, db, entityID, entityID, "other-flow/inst-1", "foreign", "queued", map[string]any{"status": "open"}, time.Now().UTC().Truncate(time.Second))

	_, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_id": entityID,
		"field":     "status",
		"value":     "closed",
	})
	re, ok := runtimefailures.As(err)
	if err == nil || !ok || re.Failure.Detail.Code != "cross_flow_write_forbidden" {
		t.Fatalf("expected cross_flow_write_forbidden, got %v", err)
	}
}

func TestEntityTools_SaveEntityFieldCrossFlowDenialPreservesSelectedForkRuntimeLineage(t *testing.T) {
	bundle := loadWave1EntityToolMultiFlowBundle(t, map[string]entityToolFlowFixture{
		"analyzer-flow": {
			EntitiesYAML: `
analysis:
  status: text
`,
			AgentsYAML: entityToolAgentYAML(models.AgentConfig{ExecutionMode: "live", ID: "analyzer", Role: "analyzer", Tools: []string{"save_entity_field", "get_entity"}}),
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
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "analyzer",
		Role:          "analyzer",
		Tools:         []string{"save_entity_field", "get_entity"},
	}
	_, db, _ := testutil.StartPostgres(t)
	ensureEntityToolTestRun(t, db)
	bus := &entityToolRuntimeLogBus{}
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	exec := runtimetools.NewExecutorWithOptions(bus, nil, runtimetools.ExecutorOptions{
		EntityStore:                    pg,
		WorkflowSource:                 semanticview.Wrap(bundle),
		AllowInternalLegacyEntityTools: true,
	})
	entityID := uuid.NewString()
	seedEntityStateRow(t, db, entityID, entityID, "other-flow/inst-1", "foreign", "queued", map[string]any{"status": "open"}, time.Now().UTC().Truncate(time.Second))

	_, err := exec.Execute(selectedForkEntityToolRuntimeContext(actor), "save_entity_field", map[string]any{
		"entity_id": entityID,
		"field":     "status",
		"value":     "closed",
	})
	re, ok := runtimefailures.As(err)
	if err == nil || !ok || re.Failure.Detail.Code != "cross_flow_write_forbidden" {
		t.Fatalf("expected cross_flow_write_forbidden, got %v", err)
	}
	if len(bus.logs) != 2 || bus.logs[0].Action != "entity_write_denied" || bus.logs[1].Action != "tool_execution_denied" {
		t.Fatalf("logs = %#v, want entity_write_denied then tool_execution_denied", bus.logs)
	}
	for i, log := range bus.logs {
		if log.Failure == nil || log.Failure.Class != runtimefailures.ClassAuthorizationDenied || log.Failure.Detail.Code != "cross_flow_write_forbidden" {
			t.Fatalf("log[%d] failure = %#v, want canonical cross-flow authorization denial", i, log.Failure)
		}
	}
	assertEntityToolDiagnosticLineage(t, bus, 0)
	assertEntityToolDiagnosticLineage(t, bus, 1)
}

func TestEntityTools_SaveEntityFieldAllowsSameFlowWrite(t *testing.T) {
	bundle := loadWave1EntityToolMultiFlowBundle(t, map[string]entityToolFlowFixture{
		"analyzer-flow": {
			EntitiesYAML: `
analysis:
  status: text
`,
			AgentsYAML: entityToolAgentYAML(models.AgentConfig{ExecutionMode: "live", ID: "analyzer", Role: "analyzer", Tools: []string{"create_entity", "save_entity_field", "get_entity"}}),
		},
	})
	ctx, exec, _ := newEntityToolTestHarnessWithBundle(t, models.AgentConfig{
		ExecutionMode: "live",
		ID:            "analyzer",
		Role:          "analyzer",
		Tools:         []string{"create_entity", "save_entity_field", "get_entity"},
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

func TestEntityTools_SaveEntityFieldAllowsSameFlowWriteWithoutSubjectLink(t *testing.T) {
	bundle := loadWave1EntityToolMultiFlowBundle(t, map[string]entityToolFlowFixture{
		"analyzer-flow": {
			EntitiesYAML: `
analysis:
  status: text
`,
			AgentsYAML: entityToolAgentYAML(models.AgentConfig{ExecutionMode: "live", ID: "analyzer", Role: "analyzer", Tools: []string{"create_entity", "save_entity_field", "get_entity"}}),
		},
	})
	ctx, exec, _ := newEntityToolTestHarnessWithBundle(t, models.AgentConfig{
		ExecutionMode: "live",
		ID:            "analyzer",
		Role:          "analyzer",
		Tools:         []string{"create_entity", "save_entity_field", "get_entity"},
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
		t.Fatalf("save_entity_field same flow without subject link: %v", err)
	}
}

func TestEntityTools_GetEntityRejectsCrossFlowRead(t *testing.T) {
	bundle := loadWave1EntityToolMultiFlowBundle(t, map[string]entityToolFlowFixture{
		"analyzer-flow": {
			EntitiesYAML: `
analysis:
  status: text
`,
			AgentsYAML: entityToolAgentYAML(models.AgentConfig{ExecutionMode: "live", ID: "analyzer", Role: "analyzer", Tools: []string{"get_entity"}}),
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
		ExecutionMode: "live",
		ID:            "analyzer",
		Role:          "analyzer",
		Tools:         []string{"get_entity"},
	}, bundle)
	entityID := uuid.NewString()
	seedEntityStateRow(t, db, entityID, entityID, "other-flow/inst-1", "foreign", "open", map[string]any{"status": "foreign"}, time.Now().UTC().Truncate(time.Second))

	_, err := exec.Execute(ctx, "get_entity", map[string]any{"entity_id": entityID})
	re, ok := runtimefailures.As(err)
	if err == nil || !ok || re.Failure.Detail.Code != "cross_flow_read_forbidden" {
		t.Fatalf("expected cross_flow_read_forbidden, got %v", err)
	}
	if re.Failure.Detail.Attributes["owner_flow_path"] != "other-flow/inst-1" {
		t.Fatalf("foreign owner attributes = %#v", re.Failure.Detail.Attributes)
	}
}

func TestEntityTools_ExistingEntityFlowInstanceAcceptsDeclaredRootAndExactPath(t *testing.T) {
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "validator",
		Role:          "validator",
		Tools:         []string{"create_entity", "get_entity", "save_entity_field", "query_entities", "query_metrics", "search_entities"},
	}
	bundle := loadWave1EntityToolMultiFlowBundle(t, map[string]entityToolFlowFixture{
		"validation": {
			EntitiesYAML: `
validation_case:
  status: text
  score: integer
`,
			AgentsYAML: entityToolAgentYAML(actor),
		},
		"operating": {
			EntitiesYAML: `
operating_case:
  status: text
`,
		},
	})
	ctx, exec, db := newEntityToolTestHarnessWithBundle(t, actor, bundle)
	firstID := mustCreateEntityID(t, ctx, exec, map[string]any{
		"flow_instance": "validation/case-1",
		"fields": map[string]any{
			"status": "open",
			"score":  10,
		},
	})
	secondID := mustCreateEntityID(t, ctx, exec, map[string]any{
		"flow_instance": "validation/case-2",
		"fields": map[string]any{
			"status": "open",
			"score":  20,
		},
	})
	deepID := uuid.NewString()
	seedEntityStateRow(t, db, deepID, deepID, "validation/nested/case-3", "validation_case", "queued", map[string]any{
		"status": "open",
		"score":  30,
	}, time.Now().UTC().Truncate(time.Second))

	for name, flowInstance := range map[string]string{
		"declared root": "validation",
		"exact path":    "validation/case-1",
	} {
		out, err := exec.Execute(ctx, "get_entity", map[string]any{
			"entity_id":     firstID,
			"flow_instance": flowInstance,
		})
		if err != nil {
			t.Fatalf("get_entity %s: %v", name, err)
		}
		entity := out.(map[string]any)
		if got := strings.TrimSpace(asString(entity["entity_id"])); got != firstID {
			t.Fatalf("get_entity %s entity_id = %q, want %s", name, got, firstID)
		}
	}
	_, err := exec.Execute(ctx, "get_entity", map[string]any{
		"entity_id":     firstID,
		"flow_instance": "operating",
	})
	ownershipFailure := requireToolFailure(t, err, runtimefailures.ClassAuthorizationDenied, "entity_flow_ownership_mismatch")
	if ownershipFailure.Detail.Attributes["requested_flow_path"] != "operating" {
		t.Fatalf("ownership failure attributes = %#v", ownershipFailure.Detail.Attributes)
	}
	deepOut, err := exec.Execute(ctx, "get_entity", map[string]any{
		"entity_id":     deepID,
		"flow_instance": "validation",
	})
	if err != nil {
		t.Fatalf("get_entity declared root for deep descendant: %v", err)
	}
	if got := strings.TrimSpace(asString(deepOut.(map[string]any)["entity_id"])); got != deepID {
		t.Fatalf("get_entity deep descendant entity_id = %q, want %s", got, deepID)
	}

	if _, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_id":     firstID,
		"flow_instance": "validation",
		"field":         "status",
		"value":         "root-saved",
	}); err != nil {
		t.Fatalf("save_entity_field declared root: %v", err)
	}
	if _, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_id":     firstID,
		"flow_instance": "validation/case-1",
		"field":         "status",
		"value":         "exact-saved",
	}); err != nil {
		t.Fatalf("save_entity_field exact path: %v", err)
	}
	if _, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_id":     firstID,
		"flow_instance": "operating",
		"field":         "status",
		"value":         "wrong-root",
	}); err == nil {
		t.Fatal("save_entity_field wrong root unexpectedly succeeded")
	} else if failure, ok := runtimefailures.As(err); !ok ||
		failure.Failure.Class != runtimefailures.ClassAuthorizationDenied ||
		failure.Failure.Detail.Code != "entity_flow_ownership_mismatch" ||
		failure.Failure.Detail.Attributes["requested_flow_path"] != "operating" ||
		failure.Failure.Detail.Attributes["owner_flow_path"] != "validation/case-1" {
		t.Fatalf("save_entity_field wrong root failure = %#v, want canonical ownership mismatch", failure)
	}

	queryOut, err := exec.Execute(ctx, "query_entities", map[string]any{
		"flow_instance": "validation",
		"select":        []string{"status", "score"},
		"limit":         10,
	})
	if err != nil {
		t.Fatalf("query_entities declared root: %v", err)
	}
	queryRows := queryOut.(map[string]any)["results"].([]map[string]any)
	if len(queryRows) != 3 {
		t.Fatalf("query_entities root rows = %#v, want 3", queryRows)
	}
	queryExactOut, err := exec.Execute(ctx, "query_entities", map[string]any{
		"flow_instance": "validation/case-2",
		"select":        []string{"status", "score"},
		"limit":         10,
	})
	if err != nil {
		t.Fatalf("query_entities exact path: %v", err)
	}
	queryExactRows := queryExactOut.(map[string]any)["results"].([]map[string]any)
	if len(queryExactRows) != 1 || strings.TrimSpace(asString(queryExactRows[0]["entity_id"])) != secondID {
		t.Fatalf("query_entities exact rows = %#v, want %s", queryExactRows, secondID)
	}
	queryWrongOut, err := exec.Execute(ctx, "query_entities", map[string]any{
		"flow_instance": "operating",
		"select":        []string{"status"},
		"limit":         10,
	})
	if err != nil {
		t.Fatalf("query_entities wrong root: %v", err)
	}
	if rows := queryWrongOut.(map[string]any)["results"].([]map[string]any); len(rows) != 0 {
		t.Fatalf("query_entities wrong root rows = %#v, want none", rows)
	}

	searchOut, err := exec.Execute(ctx, "search_entities", map[string]any{
		"flow_instance": "validation",
		"current_state": "queued",
		"limit":         10,
	})
	if err != nil {
		t.Fatalf("search_entities declared root: %v", err)
	}
	if total := searchOut.(map[string]any)["total"].(int); total != 3 {
		t.Fatalf("search_entities root total = %d, want 3", total)
	}
	searchExactOut, err := exec.Execute(ctx, "search_entities", map[string]any{
		"flow_instance": "validation/case-1",
		"limit":         10,
	})
	if err != nil {
		t.Fatalf("search_entities exact path: %v", err)
	}
	if total := searchExactOut.(map[string]any)["total"].(int); total != 1 {
		t.Fatalf("search_entities exact total = %d, want 1", total)
	}
	searchWrongOut, err := exec.Execute(ctx, "search_entities", map[string]any{
		"flow_instance": "operating",
		"limit":         10,
	})
	if err != nil {
		t.Fatalf("search_entities wrong root: %v", err)
	}
	if total := searchWrongOut.(map[string]any)["total"].(int); total != 0 {
		t.Fatalf("search_entities wrong root total = %d, want 0", total)
	}

	metricsOut, err := exec.Execute(ctx, "query_metrics", map[string]any{
		"flow_instance": "validation",
		"metric":        "count",
	})
	if err != nil {
		t.Fatalf("query_metrics declared root: %v", err)
	}
	if got := testNumericValue(metricsOut.(map[string]any)["value"]); got != 3 {
		t.Fatalf("query_metrics root count = %#v, want 3", metricsOut)
	}
	metricsExactOut, err := exec.Execute(ctx, "query_metrics", map[string]any{
		"flow_instance": "validation/case-1",
		"metric":        "count",
	})
	if err != nil {
		t.Fatalf("query_metrics exact path: %v", err)
	}
	if got := testNumericValue(metricsExactOut.(map[string]any)["value"]); got != 1 {
		t.Fatalf("query_metrics exact count = %#v, want 1", metricsExactOut)
	}
	metricsWrongOut, err := exec.Execute(ctx, "query_metrics", map[string]any{
		"flow_instance": "operating",
		"metric":        "count",
	})
	if err != nil {
		t.Fatalf("query_metrics wrong root: %v", err)
	}
	if got := testNumericValue(metricsWrongOut.(map[string]any)["value"]); got != 0 {
		t.Fatalf("query_metrics wrong root count = %#v, want 0", metricsWrongOut)
	}
}

func TestEntityTools_FlowOwnedActorCannotReadForeignEntityAndCanWriteOwnEntity(t *testing.T) {
	bundle := loadWave1EntityToolMultiFlowBundle(t, map[string]entityToolFlowFixture{
		"analyzer-flow": {
			EntitiesYAML: `
analysis:
  status: text
`,
			AgentsYAML: entityToolAgentYAML(models.AgentConfig{ExecutionMode: "live", ID: "analyzer", Role: "analyzer", Tools: []string{"create_entity", "get_entity", "save_entity_field"}}),
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
		ExecutionMode: "live",
		ID:            "analyzer",
		Role:          "analyzer",
		Tools:         []string{"create_entity", "get_entity", "save_entity_field"},
	}, bundle)

	scoringID := uuid.NewString()
	subjectID := uuid.NewString()
	seedEntityStateRow(t, db, scoringID, subjectID, "other-flow/score-1", "foreign", "shortlisted", map[string]any{
		"name":            "Example Vertical",
		"composite_score": 72,
	}, time.Now().UTC().Truncate(time.Second))
	validationID := mustCreateEntityID(t, ctx, exec, map[string]any{
		"flow_instance": "analyzer-flow/validation-1",
		"initial_state": "researching",
		"fields": map[string]any{
			"status": "open",
		},
	})

	_, err := exec.Execute(ctx, "get_entity", map[string]any{"entity_id": scoringID})
	re, ok := runtimefailures.As(err)
	if err == nil || !ok || re.Failure.Detail.Code != "cross_flow_read_forbidden" {
		t.Fatalf("expected cross_flow_read_forbidden, got %v", err)
	}
	if re.Failure.Detail.Attributes["owner_flow_path"] != "other-flow/score-1" {
		t.Fatalf("foreign owner attributes = %#v", re.Failure.Detail.Attributes)
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
		ExecutionMode: "live",
		ID:            "tester",
		Role:          "operator",
		Tools:         []string{"create_entity", "get_entity", "save_entity_field", "search_entities", "query_entities", "query_metrics"},
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
		ExecutionMode: "live",
		ID:            "tester",
		Type:          "internal",
		Role:          "operator",
		Tools:         []string{"create_entity", "get_entity", "save_entity_field", "search_entities"},
	}, loadWave1EntityToolBundle(t, models.AgentConfig{
		ExecutionMode: "live",
		ID:            "tester",
		Role:          "operator",
		Tools:         []string{"create_entity", "get_entity", "save_entity_field", "search_entities"},
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
		ExecutionMode: "live",
		ID:            "tester",
		Role:          "operator",
		Tools:         []string{"create_entity", "get_entity", "save_entity_field", "search_entities", "query_entities", "query_metrics"},
	})
}

func newEntityToolTestHarnessWithActor(t *testing.T, actor models.AgentConfig) (context.Context, *runtimetools.Executor, *sql.DB) {
	t.Helper()
	if strings.TrimSpace(actor.Type) == "" {
		actor.Type = "internal"
	}
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
	return newEntityToolTestHarnessWithBundleAndLegacyAccess(t, actor, bundle, true)
}

func newEntityToolTestHarnessWithBundleAndLegacyAccess(t *testing.T, actor models.AgentConfig, bundle *runtimecontracts.WorkflowContractBundle, allowInternalLegacy bool) (context.Context, *runtimetools.Executor, *sql.DB) {
	t.Helper()
	_, db, _ := testutil.StartPostgres(t)
	ensureEntityToolTestRun(t, db)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	ctx := runtimecorrelation.WithRunID(unmanagedToolTestContext(), entityToolTestRunID)
	exec := runtimetools.NewExecutorWithOptions(nil, nil, runtimetools.ExecutorOptions{
		EntityStore:                    pg,
		WorkflowSource:                 semanticview.Wrap(bundle),
		AllowInternalLegacyEntityTools: allowInternalLegacy,
	})
	return runtimetools.WithActor(ctx, actor), exec, db
}

const entityToolTestRunID = "11111111-1111-1111-1111-111111111111"

func ensureEntityToolTestRun(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.ExecContext(unmanagedToolTestContext(), `
		INSERT INTO runs (run_id, status, bundle_hash, bundle_source, bundle_fingerprint)
		VALUES ($1::uuid, 'running', $2, $3, $4)
		ON CONFLICT (run_id) DO NOTHING
	`, entityToolTestRunID, authorActivityTestBundleSourceFact.BundleHash, authorActivityTestBundleSourceFact.BundleSource, authorActivityTestBundleSourceFact.BundleFingerprint); err != nil {
		t.Fatalf("seed entity tool test run: %v", err)
	}
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
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
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
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)

	writeEntityToolFixtureFile(t, filepath.Join(root, "package.yaml"), fmt.Sprintf(`
name: entity-tool-bundle
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
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
	if got, _, ok := bundle.FlowPrimaryEntityContract(flowID); !ok || strings.TrimSpace(got) != entityType {
		t.Fatalf("FlowPrimaryEntityContract(%q) = (%q, ok=%v), want %q", flowID, got, ok, entityType)
	}
	return bundle
}

func loadRoleScopedEntityToolBundle(t *testing.T, actor models.AgentConfig, optIn bool) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	toolSurface := ""
	if optIn {
		toolSurface = `
tool_surface:
  role_scoped_entity_tools: true
`
	}
	return loadWave1EntityToolMultiFlowBundle(t, map[string]entityToolFlowFixture{
		"validation": {
			SchemaYAML: fmt.Sprintf(`
name: validation
mode: static
initial_state: queued
states: [queued, ready, closed]
terminal_states: [closed]
%s`, toolSurface),
			TypesYAML: `
types:
  business_brief:
    summary: text
    confidence: integer
  mvp_spec:
    problem_statement: text
    technical_approach: text
`,
			EntitiesYAML: `
validation_case:
  status: text
  business_brief: business_brief
  mvp_spec: mvp_spec
`,
			AgentsYAML: roleScopedEntityToolAgentYAML(actor),
		},
		"other": {
			EntitiesYAML: `
other_case:
  status: text
`,
		},
	})
}

func roleScopedEntityToolAgentYAML(actor models.AgentConfig) string {
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
	builder.WriteString("  memory: false\n")
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
	builder.WriteString("  entity_writes:\n")
	builder.WriteString("    validation_case:\n")
	builder.WriteString("      create:\n")
	builder.WriteString("        - status\n")
	builder.WriteString("      save:\n")
	builder.WriteString("        - business_brief\n")
	return builder.String()
}

func roleScopedToolDefinitionMap(defs []llm.ToolDefinition) map[string]llm.ToolDefinition {
	out := make(map[string]llm.ToolDefinition, len(defs))
	for _, def := range defs {
		if name := strings.TrimSpace(def.Name); name != "" {
			out[name] = def
		}
	}
	return out
}

func sortedRoleScopedToolNames(defs map[string]llm.ToolDefinition) []string {
	names := make([]string, 0, len(defs))
	for name := range defs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

type entityToolFlowFixture struct {
	SchemaYAML   string
	TypesYAML    string
	EntitiesYAML string
	AgentsYAML   string
	ToolsYAML    string
}

func loadWave1EntityToolMultiFlowBundle(t *testing.T, flows map[string]entityToolFlowFixture) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	root := t.TempDir()
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)

	flowIDs := make([]string, 0, len(flows))
	for flowID := range flows {
		flowIDs = append(flowIDs, strings.TrimSpace(flowID))
	}
	sort.Strings(flowIDs)

	var packageYAML strings.Builder
	packageYAML.WriteString("name: entity-tool-bundle\n")
	packageYAML.WriteString("version: \"1.0.0\"\n")
	packageYAML.WriteString("platform_version: \">=0.7.0 <0.8.0\"\n")
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
		if strings.TrimSpace(fixture.ToolsYAML) != "" {
			writeEntityToolFixtureFile(t, filepath.Join(root, "flows", flowID, "tools.yaml"), fixture.ToolsYAML)
		}
	}

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides(%s): %v", root, err)
	}
	return bundle
}

func seedEntityStateRow(t *testing.T, db *sql.DB, entityID, _ string, flowInstance, entityType, currentState string, fields map[string]any, enteredAt time.Time) {
	t.Helper()
	if enteredAt.IsZero() {
		enteredAt = time.Now().UTC()
	}
	fieldsJSON, err := json.Marshal(fields)
	if err != nil {
		t.Fatalf("json.Marshal(fields): %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3, $4, '',
			$5, '{}'::jsonb, $6::jsonb, '{}'::jsonb, 1,
			$7, $7, $7
		)
	`, entityToolTestRunID, entityID, flowInstance, entityType, currentState, string(fieldsJSON), enteredAt); err != nil {
		t.Fatalf("seed entity_state(%s): %v", entityID, err)
	}
}

func persistedEntityField(t *testing.T, db *sql.DB, entityID, field string) string {
	t.Helper()
	var raw []byte
	if err := db.QueryRow(`
		SELECT COALESCE(fields -> $2, 'null'::jsonb)
		FROM entity_state
		WHERE run_id = $1::uuid AND entity_id = $3::uuid
	`, entityToolTestRunID, field, entityID).Scan(&raw); err != nil {
		t.Fatalf("read persisted entity field %s.%s: %v", entityID, field, err)
	}
	return strings.TrimSpace(string(raw))
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
	builder.WriteString("  memory: false\n")
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
