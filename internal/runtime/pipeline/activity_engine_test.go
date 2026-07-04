package pipeline

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestPipelineActivityDispatcherExecutesHTTPToolAndPublishesGeneratedSuccessEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("url"); got != "https://example.com/source" {
			t.Fatalf("url query = %q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"title": "Example Source"})
	}))
	defer server.Close()

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"source_scrape": {
				HandlerType: "http",
				EffectClass: string(runtimecontracts.ActivityEffectClassReadOnly),
				OutputSchema: runtimecontracts.ToolInputSchema{
					Type: "object",
					Properties: map[string]runtimecontracts.ToolInputSchema{
						"title": {Type: "string"},
					},
				},
				HTTP: &runtimecontracts.HTTPToolSpec{
					Method: "GET",
					URL:    server.URL + "?url={{input.url}}",
				},
			},
		},
	})
	bus := &recordingPipelineBus{}
	pc := NewPipelineCoordinatorWithOptions(bus, nil, PipelineCoordinatorOptions{
		Module: staticSemanticWorkflowModule{source: source},
	})
	dispatcher := pipelineActivityDispatcher{coordinator: pc}
	intent := runtimeengine.ActivityIntent{
		ActivityID:    "scanner_source_scrape",
		Tool:          "source_scrape",
		Input:         map[string]any{"url": "https://example.com/source"},
		EffectClass:   runtimecontracts.ActivityEffectClassReadOnly,
		SuccessEvent:  "research.scanner_source_scrape.succeeded",
		FailureEvent:  "research.scanner_source_scrape.failed",
		EntityID:      identity.NormalizeEntityID("entity-1"),
		NodeID:        identity.NormalizeNodeID("scanner"),
		FlowID:        identity.NormalizeFlowID("research"),
		SourceEventID: "evt-1",
		SourceRunID:   "run-1",
		SourceTaskID:  "task-1",
		ChainDepth:    4,
		Attempt:       1,
	}.Normalized()
	if err := dispatcher.DispatchActivities(context.Background(), []runtimeengine.ActivityIntent{intent}); err != nil {
		t.Fatalf("DispatchActivities: %v", err)
	}
	if len(bus.publishes) != 1 {
		t.Fatalf("published events = %d, want 1", len(bus.publishes))
	}
	evt := bus.publishes[0]
	if got := evt.Type(); got != events.EventType("research.scanner_source_scrape.succeeded") {
		t.Fatalf("event type = %q", got)
	}
	if got := evt.ParentEventID(); got != "evt-1" {
		t.Fatalf("parent event id = %q", got)
	}
	if got := evt.ChainDepth(); got != 5 {
		t.Fatalf("chain depth = %d", got)
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload(), &payload); err != nil {
		t.Fatalf("payload unmarshal: %v", err)
	}
	result, ok := payload["result"].(map[string]any)
	if !ok {
		t.Fatalf("result payload = %#v", payload["result"])
	}
	if result["title"] != "Example Source" {
		t.Fatalf("result title = %#v", result["title"])
	}
}
