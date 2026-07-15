package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type telemetryBusStub struct {
	logs       []runtimepipeline.RuntimeLogEntry
	lineages   []runtimecorrelation.RuntimeLineage
	hasLineage []bool
	publishErr error
	published  []events.Event
}

func (b *telemetryBusStub) Publish(_ context.Context, evt events.Event) error {
	b.published = append(b.published, evt)
	return b.publishErr
}

func (*telemetryBusStub) PublishDirect(context.Context, events.Event, []string) error { return nil }

func (b *telemetryBusStub) LogRuntime(ctx context.Context, entry runtimepipeline.RuntimeLogEntry) error {
	lineage, ok := runtimecorrelation.RuntimeLineageFromContext(ctx)
	b.lineages = append(b.lineages, lineage)
	b.hasLineage = append(b.hasLineage, ok)
	b.logs = append(b.logs, entry)
	return nil
}

func selectedForkToolContext(actor models.AgentConfig) context.Context {
	const (
		runID   = "1ebadcb5-66a3-4536-8c7a-06988d82b402"
		eventID = "a6a7390c-9eed-42aa-b01a-98465051f686"
	)
	ctx := models.WithActor(unmanagedToolTestContext(), actor)
	ctx = runtimecorrelation.WithRuntimeLineage(ctx, runtimecorrelation.RuntimeLineage{
		Owner:               "runtime.run_fork.selected_contract_execution.fork_local_runtime_typed_lineage",
		RunID:               runID,
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
		runID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "entity-typed-lineage"),
		testTime(),
	))
}

func assertToolExecutorDiagnosticLineage(t *testing.T, bus *telemetryBusStub, index int) {
	t.Helper()
	if index >= len(bus.logs) || index >= len(bus.lineages) || index >= len(bus.hasLineage) {
		t.Fatalf("lineage index %d outside logs=%d lineages=%d hasLineage=%d", index, len(bus.logs), len(bus.lineages), len(bus.hasLineage))
	}
	if !bus.hasLineage[index] {
		t.Fatalf("runtime lineage missing for log %#v", bus.logs[index])
	}
	lineage := bus.lineages[index]
	if lineage.Owner != "runtime.run_fork.selected_contract_execution.fork_local_runtime_typed_lineage" ||
		lineage.RunID != "1ebadcb5-66a3-4536-8c7a-06988d82b402" ||
		lineage.SubjectEventID != "a6a7390c-9eed-42aa-b01a-98465051f686" ||
		lineage.ParentEventID != "a6a7390c-9eed-42aa-b01a-98465051f686" ||
		lineage.RowCategory != runtimecorrelation.RuntimeLineageRowCategoryDiagnostic ||
		lineage.SelectedForkOwner != "runtime.run_fork.selected_contract_execution.fork_local_runtime_container" ||
		lineage.Classification != runtimecorrelation.RuntimeLineageClassificationForkLocal ||
		!lineage.SelectedForkContext {
		t.Fatalf("runtime lineage = %#v", lineage)
	}
}

func TestExecutorTelemetry_LogsSuccess(t *testing.T) {
	t.Setenv("TEST_HTTP_API_KEY", "secret-token")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"available": true})
	}))
	defer server.Close()

	bus := &telemetryBusStub{}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"check_domain": {
				Description: "Check domain availability",
				HandlerType: "http",
				InputSchema: runtimecontracts.ToolInputSchema{
					Type:     "object",
					Required: []string{"domain"},
					Properties: map[string]runtimecontracts.ToolInputSchema{
						"domain": {Type: "string"},
					},
				},
				HTTP: &runtimecontracts.HTTPToolSpec{
					Method: "GET",
					URL:    server.URL + "?domain={{input.domain}}",
				},
			},
		},
	})

	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source})
	ctx := models.WithActor(unmanagedToolTestContext(), models.AgentConfig{
		ExecutionMode: "live",
		ID:            "agent-1",
		Role:          "analysis",
		EntityID:      "entity-1",
		Tools:         []string{"check_domain"},
	})

	if _, err := exec.Execute(ctx, "check_domain", map[string]any{"domain": "example.com"}); err != nil {
		t.Fatalf("Execute(check_domain): %v", err)
	}
	if len(bus.logs) != 1 {
		t.Fatalf("runtime log count = %d, want 1", len(bus.logs))
	}
	log := bus.logs[0]
	if log.Action != "tool_execution_succeeded" {
		t.Fatalf("action = %q, want tool_execution_succeeded", log.Action)
	}
	if log.Message != "Tool check_domain executed successfully" {
		t.Fatalf("message = %q", log.Message)
	}
	if log.AgentID != "agent-1" || log.EntityID != "entity-1" {
		t.Fatalf("agent/entity = %q/%q", log.AgentID, log.EntityID)
	}
	detail, _ := log.Detail.(map[string]any)
	if strings.TrimSpace(asString(detail["tool_name"])) != "check_domain" {
		t.Fatalf("tool_name = %#v, want check_domain", detail["tool_name"])
	}
	if strings.TrimSpace(asString(detail["phase"])) != "dispatch" {
		t.Fatalf("phase = %#v, want dispatch", detail["phase"])
	}
	if ok, _ := detail["ok"].(bool); !ok {
		t.Fatalf("ok = %#v, want true", detail["ok"])
	}
}

func TestExecutorTelemetry_LogsDeniedExecution(t *testing.T) {
	bus := &telemetryBusStub{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{})
	ctx := models.WithActor(unmanagedToolTestContext(), models.AgentConfig{
		ExecutionMode: "live",
		ID:            "agent-2",
	})

	if _, err := exec.Execute(ctx, "workflow_custom_tool", map[string]any{"x": 1}); err == nil {
		t.Fatal("expected authorization denial")
	}
	if len(bus.logs) != 1 {
		t.Fatalf("runtime log count = %d, want 1", len(bus.logs))
	}
	log := bus.logs[0]
	if log.Action != "tool_execution_denied" {
		t.Fatalf("action = %q, want tool_execution_denied", log.Action)
	}
	if log.Message != "Tool workflow_custom_tool execution was denied" {
		t.Fatalf("message = %q", log.Message)
	}
	detail, _ := log.Detail.(map[string]any)
	if strings.TrimSpace(asString(detail["phase"])) != "authorize" {
		t.Fatalf("phase = %#v, want authorize", detail["phase"])
	}
	if ok, _ := detail["ok"].(bool); ok {
		t.Fatalf("ok = %#v, want false", detail["ok"])
	}
	if strings.TrimSpace(asString(detail["denial_layer"])) != "authorizer" {
		t.Fatalf("denial_layer = %#v, want authorizer", detail["denial_layer"])
	}
}

func TestExecutorTelemetry_LogsExecutionFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusBadGateway)
	}))
	defer server.Close()

	bus := &telemetryBusStub{}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"check_domain": {
				Description: "Check domain availability",
				HandlerType: "http",
				InputSchema: runtimecontracts.ToolInputSchema{
					Type:     "object",
					Required: []string{"domain"},
					Properties: map[string]runtimecontracts.ToolInputSchema{
						"domain": {Type: "string"},
					},
				},
				HTTP: &runtimecontracts.HTTPToolSpec{
					Method: "GET",
					URL:    server.URL + "?domain={{input.domain}}",
				},
			},
		},
	})

	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source})
	ctx := models.WithActor(unmanagedToolTestContext(), models.AgentConfig{
		ExecutionMode: "live",
		ID:            "agent-3",
		EntityID:      "entity-3",
		Tools:         []string{"check_domain"},
	})

	if _, err := exec.Execute(ctx, "check_domain", map[string]any{"domain": "example.com"}); err == nil {
		t.Fatal("expected execution failure")
	}
	if len(bus.logs) != 1 {
		t.Fatalf("runtime log count = %d, want 1", len(bus.logs))
	}
	log := bus.logs[0]
	if log.Action != "tool_execution_failed" {
		t.Fatalf("action = %q, want tool_execution_failed", log.Action)
	}
	if log.Message != "Tool check_domain execution failed" {
		t.Fatalf("message = %q", log.Message)
	}
	detail, _ := log.Detail.(map[string]any)
	if strings.TrimSpace(asString(detail["phase"])) != "dispatch" {
		t.Fatalf("phase = %#v, want dispatch", detail["phase"])
	}
	if ok, _ := detail["ok"].(bool); ok {
		t.Fatalf("ok = %#v, want false", detail["ok"])
	}
	if log.Failure == nil || log.Failure.Class != runtimefailures.ClassConnectorFailure || log.Failure.Detail.Code != "provider_http_status" {
		t.Fatalf("failure = %#v, want canonical connector status failure", log.Failure)
	}
}

func TestExecutorTelemetry_PreservesTypedLineageForToolDiagnostics(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"available": true})
		}))
		defer server.Close()

		bus := &telemetryBusStub{}
		source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
			Tools: map[string]runtimecontracts.ToolSchemaEntry{
				"check_domain": {
					Description: "Check domain availability",
					HandlerType: "http",
					InputSchema: runtimecontracts.ToolInputSchema{
						Type:     "object",
						Required: []string{"domain"},
						Properties: map[string]runtimecontracts.ToolInputSchema{
							"domain": {Type: "string"},
						},
					},
					HTTP: &runtimecontracts.HTTPToolSpec{
						Method: "GET",
						URL:    server.URL + "?domain={{input.domain}}",
					},
				},
			},
		})
		actor := models.AgentConfig{ExecutionMode: "live", ID: "selected-agent", EntityID: "entity-typed-lineage", Tools: []string{"check_domain"}}
		exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source})

		if _, err := exec.Execute(selectedForkToolContext(actor), "check_domain", map[string]any{"domain": "example.com"}); err != nil {
			t.Fatalf("Execute(check_domain): %v", err)
		}
		if len(bus.logs) != 1 || bus.logs[0].Action != "tool_execution_succeeded" {
			t.Fatalf("logs = %#v, want tool_execution_succeeded", bus.logs)
		}
		assertToolExecutorDiagnosticLineage(t, bus, 0)
	})

	t.Run("denied", func(t *testing.T) {
		bus := &telemetryBusStub{}
		actor := models.AgentConfig{ExecutionMode: "live", ID: "selected-agent"}
		exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{})

		if _, err := exec.Execute(selectedForkToolContext(actor), "workflow_custom_tool", map[string]any{"x": 1}); err == nil {
			t.Fatal("expected authorization denial")
		}
		if len(bus.logs) != 1 || bus.logs[0].Action != "tool_execution_denied" {
			t.Fatalf("logs = %#v, want tool_execution_denied", bus.logs)
		}
		assertToolExecutorDiagnosticLineage(t, bus, 0)
	})

	t.Run("failed", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "boom", http.StatusBadGateway)
		}))
		defer server.Close()

		bus := &telemetryBusStub{}
		source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
			Tools: map[string]runtimecontracts.ToolSchemaEntry{
				"check_domain": {
					Description: "Check domain availability",
					HandlerType: "http",
					InputSchema: runtimecontracts.ToolInputSchema{
						Type:     "object",
						Required: []string{"domain"},
						Properties: map[string]runtimecontracts.ToolInputSchema{
							"domain": {Type: "string"},
						},
					},
					HTTP: &runtimecontracts.HTTPToolSpec{
						Method: "GET",
						URL:    server.URL + "?domain={{input.domain}}",
					},
				},
			},
		})
		actor := models.AgentConfig{ExecutionMode: "live", ID: "selected-agent", EntityID: "entity-typed-lineage", Tools: []string{"check_domain"}}
		exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source})

		if _, err := exec.Execute(selectedForkToolContext(actor), "check_domain", map[string]any{"domain": "example.com"}); err == nil {
			t.Fatal("expected execution failure")
		}
		if len(bus.logs) != 1 || bus.logs[0].Action != "tool_execution_failed" {
			t.Fatalf("logs = %#v, want tool_execution_failed", bus.logs)
		}
		assertToolExecutorDiagnosticLineage(t, bus, 0)
	})
}

func TestExecutorTelemetry_EmitToolLogsStructuredPublishedOutcome(t *testing.T) {
	bus := &telemetryBusStub{}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"category.assessed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Type: "object",
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"category": {Type: "string"},
					},
				},
				Required: []string{"category"},
			},
		},
	})
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source})
	ctx := models.WithActor(unmanagedToolTestContext(), models.AgentConfig{
		ExecutionMode: "live",
		ID:            "agent-emit-1",
		EntityID:      "entity-actor",
		EmitEvents: []string{
			"category.assessed",
		},
	})
	ctx = runtimebus.WithInboundEvent(ctx, eventtest.RootIngress(
		"evt-inbound",
		events.EventType("trigger.input"),
		"",
		"task-inbound",
		[]byte(`{"entity_id":"entity-inbound"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "entity-inbound"),
		testTime(),
	))

	if _, err := exec.Execute(ctx, "emit_category_assessed", map[string]any{"category": "AP automation"}); err != nil {
		t.Fatalf("Execute(emit): %v", err)
	}
	if len(bus.logs) != 1 {
		t.Fatalf("runtime log count = %d, want 1", len(bus.logs))
	}
	log := bus.logs[0]
	if log.Action != emitToolOutcomeAction {
		t.Fatalf("action = %q, want %q", log.Action, emitToolOutcomeAction)
	}
	detail, _ := log.Detail.(map[string]any)
	if got := strings.TrimSpace(asString(detail["outcome"])); got != "published" {
		t.Fatalf("outcome = %#v, want published", detail["outcome"])
	}
	if got := strings.TrimSpace(asString(detail["requested_event_type"])); got != "category.assessed" {
		t.Fatalf("requested_event_type = %#v, want category.assessed", detail["requested_event_type"])
	}
	if got := strings.TrimSpace(asString(detail["resolved_event_type"])); got != "category.assessed" {
		t.Fatalf("resolved_event_type = %#v, want category.assessed", detail["resolved_event_type"])
	}
	pre, _ := detail["pre_validation_payload"].(map[string]any)
	post, _ := detail["post_enrichment_payload"].(map[string]any)
	if got := strings.TrimSpace(asString(pre["entity_id"])); got != "" {
		t.Fatalf("pre_validation entity_id = %#v, want empty", pre["entity_id"])
	}
	if _, ok := post["entity_id"]; ok {
		t.Fatalf("post_enrichment payload must not carry envelope entity_id: %#v", post["entity_id"])
	}
	if _, ok := post["task_id"]; ok {
		t.Fatalf("post_enrichment payload must not carry envelope task_id: %#v", post["task_id"])
	}
	if got := strings.TrimSpace(asString(detail["emitted_event_id"])); got == "" {
		t.Fatal("expected emitted_event_id")
	}
	if len(bus.published) != 1 {
		t.Fatalf("published event count = %d, want 1", len(bus.published))
	}
	if got := bus.published[0].EntityID(); got != "entity-actor" {
		t.Fatalf("published event entity_id = %q, want entity-actor", got)
	}
	if got := bus.published[0].TaskID(); got != "task-inbound" {
		t.Fatalf("published event task_id = %q, want task-inbound", got)
	}
}

func TestExecutorTelemetry_PreservesTypedLineageForEmitToolOutcome(t *testing.T) {
	bus := &telemetryBusStub{}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"category.assessed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Type: "object",
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"category": {Type: "string"},
					},
				},
				Required: []string{"category"},
			},
		},
	})
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "selected-agent",
		EntityID:      "entity-typed-lineage",
		EmitEvents:    []string{"category.assessed"},
	}

	if _, err := exec.Execute(selectedForkToolContext(actor), "emit_category_assessed", map[string]any{"category": "AP automation"}); err != nil {
		t.Fatalf("Execute(emit): %v", err)
	}
	if len(bus.logs) != 1 || bus.logs[0].Action != emitToolOutcomeAction {
		t.Fatalf("logs = %#v, want %s", bus.logs, emitToolOutcomeAction)
	}
	assertToolExecutorDiagnosticLineage(t, bus, 0)
}

func TestExecutorTelemetry_EmitToolLogsSchemaValidationFailureSeparatelyFromPublishFailure(t *testing.T) {
	bus := &telemetryBusStub{}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"category.assessed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Type: "object",
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"category": {Type: "string"},
					},
				},
				Required: []string{"category"},
			},
		},
	})
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source})
	ctx := models.WithActor(unmanagedToolTestContext(), models.AgentConfig{
		ExecutionMode: "live",
		ID:            "agent-emit-2",
		EmitEvents:    []string{"category.assessed"},
	})

	if _, err := exec.Execute(ctx, "emit_category_assessed", map[string]any{}); err == nil {
		t.Fatal("expected schema validation failure")
	}
	if len(bus.logs) != 1 {
		t.Fatalf("runtime log count = %d, want 1", len(bus.logs))
	}
	detail, _ := bus.logs[0].Detail.(map[string]any)
	if got := strings.TrimSpace(asString(detail["outcome"])); got != "schema_validation_failed" {
		t.Fatalf("outcome = %#v, want schema_validation_failed", detail["outcome"])
	}
	if got := strings.TrimSpace(asString(detail["failure_class"])); got != "validation" {
		t.Fatalf("failure_class = %#v, want validation", detail["failure_class"])
	}
	if got := strings.TrimSpace(asString(detail["failure_stage"])); got != "validate_schema" {
		t.Fatalf("failure_stage = %#v, want validate_schema", detail["failure_stage"])
	}
	if _, ok := detail["emitted_event_id"]; ok {
		t.Fatalf("unexpected emitted_event_id on schema validation failure: %#v", detail["emitted_event_id"])
	}
}

func TestExecutorTelemetry_EmitToolLogsUndeclaredFieldSchemaValidationFailure(t *testing.T) {
	bus := &telemetryBusStub{}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"category.assessed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Type: "object",
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"category": {Type: "string"},
					},
				},
				Required: []string{"category"},
			},
		},
	})
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source})
	ctx := models.WithActor(unmanagedToolTestContext(), models.AgentConfig{
		ExecutionMode: "live",
		ID:            "agent-emit-undeclared",
		EntityID:      "entity-actor",
		EmitEvents: []string{
			"category.assessed",
		},
	})
	ctx = runtimebus.WithInboundEvent(ctx, eventtest.RootIngress(
		"evt-inbound",
		events.EventType("trigger.input"),
		"",
		"task-inbound",
		[]byte(`{"entity_id":"entity-inbound"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "entity-inbound"),
		testTime(),
	))

	if _, err := exec.Execute(ctx, "emit_category_assessed", map[string]any{
		"category":   "AP automation",
		"unexpected": true,
	}); err == nil {
		t.Fatal("expected schema validation failure for undeclared payload field")
	}
	if len(bus.logs) != 1 {
		t.Fatalf("runtime log count = %d, want 1", len(bus.logs))
	}
	detail, _ := bus.logs[0].Detail.(map[string]any)
	if got := strings.TrimSpace(asString(detail["outcome"])); got != "schema_validation_failed" {
		t.Fatalf("outcome = %#v, want schema_validation_failed", detail["outcome"])
	}
	if got := strings.TrimSpace(asString(detail["failure_class"])); got != "validation" {
		t.Fatalf("failure_class = %#v, want validation", detail["failure_class"])
	}
	if got := strings.TrimSpace(asString(detail["failure_stage"])); got != "validate_schema" {
		t.Fatalf("failure_stage = %#v, want validate_schema", detail["failure_stage"])
	}
	pre, _ := detail["pre_validation_payload"].(map[string]any)
	post, _ := detail["post_enrichment_payload"].(map[string]any)
	if got, ok := pre["unexpected"].(bool); !ok || !got {
		t.Fatalf("pre_validation unexpected = %#v, want true", pre["unexpected"])
	}
	if got, ok := post["unexpected"].(bool); !ok || !got {
		t.Fatalf("post_enrichment unexpected = %#v, want true", post["unexpected"])
	}
	if _, ok := post["entity_id"]; ok {
		t.Fatalf("post_enrichment payload must not carry envelope entity_id: %#v", post["entity_id"])
	}
	if _, ok := post["task_id"]; ok {
		t.Fatalf("post_enrichment payload must not carry envelope task_id: %#v", post["task_id"])
	}
	if _, ok := detail["emitted_event_id"]; ok {
		t.Fatalf("unexpected emitted_event_id on schema validation failure: %#v", detail["emitted_event_id"])
	}
}

func TestExecutorTelemetry_EmitToolLogsPublishFailureWithCanonicalEventIdentity(t *testing.T) {
	bus := &telemetryBusStub{publishErr: errors.New("publish down")}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"category.assessed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Type: "object",
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"category": {Type: "string"},
					},
				},
				Required: []string{"category"},
			},
		},
	})
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source})
	ctx := models.WithActor(unmanagedToolTestContext(), models.AgentConfig{
		ExecutionMode: "live",
		ID:            "agent-emit-3",
		EmitEvents:    []string{"category.assessed"},
	})

	if _, err := exec.Execute(ctx, "emit_category_assessed", map[string]any{"category": "finops"}); err == nil {
		t.Fatal("expected publish failure")
	}
	if len(bus.logs) != 1 {
		t.Fatalf("runtime log count = %d, want 1", len(bus.logs))
	}
	log := bus.logs[0]
	detail, _ := log.Detail.(map[string]any)
	if got := strings.TrimSpace(asString(detail["outcome"])); got != "event_publish_failed" {
		t.Fatalf("outcome = %#v, want event_publish_failed", detail["outcome"])
	}
	if got := strings.TrimSpace(asString(detail["failure_class"])); got != "publish" {
		t.Fatalf("failure_class = %#v, want publish", detail["failure_class"])
	}
	if got := strings.TrimSpace(asString(detail["failure_stage"])); got != "publish" {
		t.Fatalf("failure_stage = %#v, want publish", detail["failure_stage"])
	}
	if got := strings.TrimSpace(asString(detail["emitted_event_id"])); got == "" {
		t.Fatal("expected emitted_event_id on publish failure")
	}
	if got := strings.TrimSpace(asString(detail["emitted_event_type"])); got != "category.assessed" {
		t.Fatalf("emitted_event_type = %#v, want category.assessed", detail["emitted_event_type"])
	}
	if log.Failure == nil || log.Failure.Class != runtimefailures.ClassDependencyUnavailable || log.Failure.Detail.Code != "event_publish_failed" {
		t.Fatalf("failure = %#v, want canonical event publish failure", log.Failure)
	}
}

func TestExecutorTelemetry_EmitToolLogsPayloadShapeFailureBeforeSchemaValidation(t *testing.T) {
	bus := &telemetryBusStub{}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"category.assessed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Type: "object",
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"category": {Type: "string"},
					},
				},
				Required: []string{"category"},
			},
		},
	})
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source})
	ctx := models.WithActor(unmanagedToolTestContext(), models.AgentConfig{
		ExecutionMode: "live",
		ID:            "agent-emit-4",
		EmitEvents:    []string{"category.assessed"},
	})

	if _, err := exec.Execute(ctx, "emit_category_assessed", func() {}); err == nil {
		t.Fatal("expected payload-shape failure")
	}
	if len(bus.logs) != 1 {
		t.Fatalf("runtime log count = %d, want 1", len(bus.logs))
	}
	log := bus.logs[0]
	if log.Action != emitToolOutcomeAction {
		t.Fatalf("action = %q, want %q", log.Action, emitToolOutcomeAction)
	}
	detail, _ := log.Detail.(map[string]any)
	if got := strings.TrimSpace(asString(detail["outcome"])); got != "payload_shape_failed" {
		t.Fatalf("outcome = %#v, want payload_shape_failed", detail["outcome"])
	}
	if got := strings.TrimSpace(asString(detail["failure_class"])); got != "payload_shape" {
		t.Fatalf("failure_class = %#v, want payload_shape", detail["failure_class"])
	}
	if got := strings.TrimSpace(asString(detail["failure_stage"])); got != "decode_input" {
		t.Fatalf("failure_stage = %#v, want decode_input", detail["failure_stage"])
	}
	if _, ok := detail["post_enrichment_payload"]; ok {
		t.Fatalf("unexpected post_enrichment_payload on payload-shape failure: %#v", detail["post_enrichment_payload"])
	}
	if _, ok := detail["emitted_event_id"]; ok {
		t.Fatalf("unexpected emitted_event_id on payload-shape failure: %#v", detail["emitted_event_id"])
	}
}

func TestExecutorTelemetry_EmitToolLogsInvalidEmitToolNameOutcome(t *testing.T) {
	bus := &telemetryBusStub{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{})
	actor := models.AgentConfig{ExecutionMode: "live", ID: "agent-emit-5"}

	if _, err := exec.handleEmitTool(unmanagedToolTestContext(), actor, "emit_not_registered", map[string]any{}); err == nil {
		t.Fatal("expected invalid emit tool name failure")
	}
	if len(bus.logs) != 1 {
		t.Fatalf("runtime log count = %d, want 1", len(bus.logs))
	}
	log := bus.logs[0]
	if log.Action != emitToolOutcomeAction {
		t.Fatalf("action = %q, want %q", log.Action, emitToolOutcomeAction)
	}
	detail, _ := log.Detail.(map[string]any)
	if got := strings.TrimSpace(asString(detail["outcome"])); got != "invalid_emit_tool_name" {
		t.Fatalf("outcome = %#v, want invalid_emit_tool_name", detail["outcome"])
	}
	if got := strings.TrimSpace(asString(detail["failure_class"])); got != "payload_shape" {
		t.Fatalf("failure_class = %#v, want payload_shape", detail["failure_class"])
	}
	if got := strings.TrimSpace(asString(detail["failure_stage"])); got != "resolve_event_type" {
		t.Fatalf("failure_stage = %#v, want resolve_event_type", detail["failure_stage"])
	}
}

func TestExecutorTelemetry_EmitToolCapsOversizedPayloadSnapshotsOnSchemaValidationFailure(t *testing.T) {
	bus := &telemetryBusStub{}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"category.assessed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Type: "object",
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"category": {Type: "string"},
						"notes":    {Type: "string"},
					},
				},
				Required: []string{"category"},
			},
		},
	})
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source})
	ctx := models.WithActor(unmanagedToolTestContext(), models.AgentConfig{
		ExecutionMode: "live",
		ID:            "agent-emit-6",
		EmitEvents:    []string{"category.assessed"},
	})
	oversizedPayload := map[string]any{"category": "finops"}
	for i := 0; i < 80; i++ {
		oversizedPayload[fmt.Sprintf("extra_%02d", i)] = strings.Repeat("x", 40)
	}

	if _, err := exec.Execute(ctx, "emit_category_assessed", oversizedPayload); err == nil {
		t.Fatal("expected schema validation failure for oversized undeclared payload")
	}
	if len(bus.logs) != 1 {
		t.Fatalf("runtime log count = %d, want 1", len(bus.logs))
	}
	detail, _ := bus.logs[0].Detail.(map[string]any)
	if got := strings.TrimSpace(asString(detail["outcome"])); got != "schema_validation_failed" {
		t.Fatalf("outcome = %#v, want schema_validation_failed", detail["outcome"])
	}
	if _, ok := detail["emitted_event_id"]; ok {
		t.Fatalf("unexpected emitted_event_id on schema validation failure: %#v", detail["emitted_event_id"])
	}
	pre, _ := detail["pre_validation_payload"].(map[string]any)
	if truncated, _ := pre["truncated"].(bool); !truncated {
		t.Fatalf("pre_validation_payload.truncated = %#v, want true", pre["truncated"])
	}
	summary := strings.TrimSpace(asString(pre["summary"]))
	if summary == "" {
		t.Fatal("expected truncated payload summary")
	}
	if len(summary) > maxToolTelemetryChars+3 {
		t.Fatalf("summary length = %d, want <= %d", len(summary), maxToolTelemetryChars+3)
	}
	post, _ := detail["post_enrichment_payload"].(map[string]any)
	if truncated, _ := post["truncated"].(bool); !truncated {
		t.Fatalf("post_enrichment_payload.truncated = %#v, want true", post["truncated"])
	}
}

func testTime() time.Time {
	return time.Unix(1700000000, 0).UTC()
}
