package tools

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	models "swarm/internal/runtime/core/actors"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
)

type telemetryBusStub struct {
	logs       []runtimepipeline.RuntimeLogEntry
	publishErr error
	published  []events.Event
}

func (b *telemetryBusStub) Publish(_ context.Context, evt events.Event) error {
	b.published = append(b.published, evt)
	return b.publishErr
}

func (*telemetryBusStub) PublishDirect(context.Context, events.Event, []string) error { return nil }

func (b *telemetryBusStub) LogRuntime(_ context.Context, entry runtimepipeline.RuntimeLogEntry) error {
	b.logs = append(b.logs, entry)
	return nil
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
	ctx := models.WithActor(context.Background(), models.AgentConfig{
		ID:       "agent-1",
		Role:     "analysis",
		EntityID: "entity-1",
		Tools:    []string{"check_domain"},
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
	ctx := models.WithActor(context.Background(), models.AgentConfig{
		ID: "agent-2",
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
	ctx := models.WithActor(context.Background(), models.AgentConfig{
		ID:       "agent-3",
		EntityID: "entity-3",
		Tools:    []string{"check_domain"},
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
	if strings.TrimSpace(log.Error) == "" {
		t.Fatal("expected error text in runtime log")
	}
}

func TestExecutorTelemetry_EmitToolLogsStructuredPublishedOutcome(t *testing.T) {
	bus := &telemetryBusStub{}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"category.assessed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Type: "object",
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"category":  {Type: "string"},
						"entity_id": {Type: "string"},
						"task_id":   {Type: "string"},
					},
				},
				Required: []string{"category"},
			},
		},
	})
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source})
	ctx := models.WithActor(context.Background(), models.AgentConfig{
		ID:       "agent-emit-1",
		EntityID: "entity-actor",
		EmitEvents: []string{
			"category.assessed",
		},
	})
	ctx = runtimebus.WithInboundEvent(ctx, events.Event{
		ID:        "evt-inbound",
		Type:      events.EventType("trigger.input"),
		TaskID:    "task-inbound",
		Payload:   []byte(`{"entity_id":"entity-inbound"}`),
		CreatedAt: testTime(),
	}.WithEntityID("entity-inbound"))

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
	if got := strings.TrimSpace(asString(post["entity_id"])); got != "entity-actor" {
		t.Fatalf("post_enrichment entity_id = %#v, want entity-actor", post["entity_id"])
	}
	if got := strings.TrimSpace(asString(post["task_id"])); got != "task-inbound" {
		t.Fatalf("post_enrichment task_id = %#v, want task-inbound", post["task_id"])
	}
	if got := strings.TrimSpace(asString(detail["emitted_event_id"])); got == "" {
		t.Fatal("expected emitted_event_id")
	}
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
	ctx := models.WithActor(context.Background(), models.AgentConfig{
		ID:         "agent-emit-2",
		EmitEvents: []string{"category.assessed"},
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
	ctx := models.WithActor(context.Background(), models.AgentConfig{
		ID:         "agent-emit-3",
		EmitEvents: []string{"category.assessed"},
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
	if strings.TrimSpace(log.Error) == "" {
		t.Fatal("expected error text on publish failure")
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
	ctx := models.WithActor(context.Background(), models.AgentConfig{
		ID:         "agent-emit-4",
		EmitEvents: []string{"category.assessed"},
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

func testTime() time.Time {
	return time.Unix(1700000000, 0).UTC()
}
