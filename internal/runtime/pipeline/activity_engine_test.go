package pipeline

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestPipelineActivityIntentWriterPersistsDurableActivityRequestEvent(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	bus := &recordingPipelineBus{}
	pc := NewPipelineCoordinatorWithOptions(bus, nil, PipelineCoordinatorOptions{
		Module: staticSemanticWorkflowModule{source: source},
	})
	intent := testActivityIntent("https://example.com/source")

	writer := pipelineActivityIntentWriter{coordinator: pc}
	if err := writer.WriteActivityIntents(context.Background(), []runtimeengine.ActivityIntent{intent}); err != nil {
		t.Fatalf("WriteActivityIntents: %v", err)
	}
	if got := bus.outboxCount(); got != 1 {
		t.Fatalf("outbox intents = %d, want 1", got)
	}
	request := bus.outboxIntent(0)
	if got := request.Event.Type(); got != activityRequestEventType {
		t.Fatalf("request event type = %q, want %q", got, activityRequestEventType)
	}
	if got, want := request.Event.ID(), activityRequestEventID(intent); got != want {
		t.Fatalf("request event id = %q, want deterministic %q", got, want)
	}
	if got := request.Event.ParentEventID(); got != "evt-1" {
		t.Fatalf("request parent event id = %q, want evt-1", got)
	}
	var payload activityRequestPayload
	if err := json.Unmarshal(request.Event.Payload(), &payload); err != nil {
		t.Fatalf("payload unmarshal: %v", err)
	}
	if payload.Tool != "source_scrape" || payload.SuccessEvent != "research.scanner_source_scrape.succeeded" {
		t.Fatalf("request payload = %#v", payload)
	}
	if got := payload.Input["url"]; got != "https://example.com/source" {
		t.Fatalf("request input url = %#v", got)
	}
}

func TestPipelineActivityDispatcherDispatchesDurableActivityRequestEvent(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	bus := &recordingPipelineBus{}
	pc := NewPipelineCoordinatorWithOptions(bus, nil, PipelineCoordinatorOptions{
		Module: staticSemanticWorkflowModule{source: source},
	})

	dispatcher := pipelineActivityDispatcher{coordinator: pc}
	intent := testActivityIntent("https://example.com/source")
	if err := dispatcher.DispatchActivities(context.Background(), []runtimeengine.ActivityIntent{intent}); err != nil {
		t.Fatalf("DispatchActivities: %v", err)
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("published events = %d, want 1 request event", got)
	}
	evt := bus.publishedEvent(0)
	if got := evt.Type(); got != activityRequestEventType {
		t.Fatalf("published event type = %q, want %q", got, activityRequestEventType)
	}
	if got, want := evt.ID(), activityRequestEventID(intent); got != want {
		t.Fatalf("request event id = %q, want %q", got, want)
	}
}

func TestPipelineActivityRequestEventExecutesHTTPToolAndPublishesGeneratedSuccessEvent(t *testing.T) {
	inputURL := "https://example.com/source?a=1&label=two words"
	var sawRawQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawRawQuery = r.URL.RawQuery
		if got := r.URL.Query().Get("url"); got != inputURL {
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
	intent := testActivityIntent(inputURL)
	request, err := activityRequestEmitIntent(intent)
	if err != nil {
		t.Fatalf("activityRequestEmitIntent: %v", err)
	}
	handled, err := pc.handleEventResult(context.Background(), request.Event)
	if err != nil {
		t.Fatalf("handleEventResult: %v", err)
	}
	if !handled {
		t.Fatal("handleEventResult handled = false, want true for activity request")
	}
	if want := "url=https%3A%2F%2Fexample.com%2Fsource%3Fa%3D1%26label%3Dtwo%20words"; !strings.Contains(sawRawQuery, want) {
		t.Fatalf("raw query = %q, want encoded component containing %q", sawRawQuery, want)
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

func testActivityIntent(inputURL string) runtimeengine.ActivityIntent {
	return runtimeengine.ActivityIntent{
		ActivityID:    "scanner_source_scrape",
		Tool:          "source_scrape",
		Input:         map[string]any{"url": inputURL},
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
}
