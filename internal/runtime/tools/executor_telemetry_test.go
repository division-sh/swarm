package tools

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	models "swarm/internal/runtime/core/actors"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
)

type telemetryBusStub struct {
	logs []runtimepipeline.RuntimeLogEntry
}

func (*telemetryBusStub) Publish(context.Context, events.Event) error { return nil }

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
