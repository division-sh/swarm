package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
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
	if got, want := evt.ID(), activityResultEventID(intent, intent.SuccessEvent); got != want {
		t.Fatalf("result event id = %q, want deterministic %q", got, want)
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

func TestPipelineActivityRequestRetriesReadOnlyHTTPTool(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			http.Error(w, "temporary", http.StatusInternalServerError)
			return
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
				},
				HTTP: &runtimecontracts.HTTPToolSpec{
					Method: "GET",
					URL:    server.URL,
				},
			},
		},
	})
	bus := &recordingPipelineBus{}
	pc := NewPipelineCoordinatorWithOptions(bus, nil, PipelineCoordinatorOptions{
		Module: staticSemanticWorkflowModule{source: source},
	})
	intent := testActivityIntent("https://example.com/source")
	request, err := activityRequestEmitIntent(intent)
	if err != nil {
		t.Fatalf("activityRequestEmitIntent: %v", err)
	}
	handled, err := pc.handleEventResult(context.Background(), request.Event)
	if err != nil {
		t.Fatalf("handleEventResult: %v", err)
	}
	if !handled {
		t.Fatal("handleEventResult handled = false, want true")
	}
	if calls != 2 {
		t.Fatalf("server calls = %d, want retry then success", calls)
	}
	if len(bus.publishes) != 1 {
		t.Fatalf("published events = %d, want 1", len(bus.publishes))
	}
	var payload map[string]any
	if err := json.Unmarshal(bus.publishes[0].Payload(), &payload); err != nil {
		t.Fatalf("payload unmarshal: %v", err)
	}
	if got := payload["attempt"]; got != float64(2) {
		t.Fatalf("payload attempt = %#v, want 2", got)
	}
}

func TestPipelineActivityRequestFailsClosedForWriteEffectClass(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}))
	defer server.Close()

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"source_scrape": {
				HandlerType: "http",
				EffectClass: string(runtimecontracts.ActivityEffectClassIdempotentWrite),
				OutputSchema: runtimecontracts.ToolInputSchema{
					Type: "object",
				},
				HTTP: &runtimecontracts.HTTPToolSpec{
					Method: "POST",
					URL:    server.URL,
				},
			},
		},
	})
	bus := &recordingPipelineBus{}
	pc := NewPipelineCoordinatorWithOptions(bus, nil, PipelineCoordinatorOptions{
		Module: staticSemanticWorkflowModule{source: source},
	})
	intent := testActivityIntent("https://example.com/source")
	intent.EffectClass = runtimecontracts.ActivityEffectClassIdempotentWrite
	request, err := activityRequestEmitIntent(intent)
	if err != nil {
		t.Fatalf("activityRequestEmitIntent: %v", err)
	}
	handled, err := pc.handleEventResult(context.Background(), request.Event)
	if err != nil {
		t.Fatalf("handleEventResult: %v", err)
	}
	if !handled {
		t.Fatal("handleEventResult handled = false, want true")
	}
	if calls != 0 {
		t.Fatalf("server calls = %d, want no outbound write execution", calls)
	}
	if len(bus.publishes) != 1 {
		t.Fatalf("published events = %d, want 1 failure event", len(bus.publishes))
	}
	evt := bus.publishes[0]
	if got := evt.Type(); got != events.EventType("research.scanner_source_scrape.failed") {
		t.Fatalf("event type = %q, want failure", got)
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload(), &payload); err != nil {
		t.Fatalf("payload unmarshal: %v", err)
	}
	if got, _ := payload["error"].(string); !strings.Contains(got, "not executable in Stage 1") {
		t.Fatalf("error = %q, want fail-closed unsupported write effect", got)
	}
}

func TestPipelineActivityRequestExecutesNonIdempotentHTTPToolOnceWithStaticCredentials(t *testing.T) {
	ctx := context.Background()
	runID := uuid.NewString()
	sourceEventID := uuid.NewString()
	entityID := uuid.NewString()
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if got := r.Header.Get("Authorization"); got != "Bearer provider-secret" {
			t.Fatalf("Authorization = %q, want resolved static credential", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"echoed_authorization": r.Header.Get("Authorization")})
	}))
	defer server.Close()

	credentialStore := testActivityCredentialStore(t, "provider_token", "provider-secret")
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"provider_write": {
				HandlerType:  "http",
				EffectClass:  string(runtimecontracts.ActivityEffectClassNonIdempotentWrite),
				Credentials:  []string{"provider_token"},
				OutputSchema: runtimecontracts.ToolInputSchema{Type: "object"},
				HTTP: &runtimecontracts.HTTPToolSpec{
					Method:  "POST",
					URL:     server.URL,
					Headers: map[string]string{"Authorization": "Bearer {{credentials.provider_token}}"},
					Body:    map[string]any{"url": "{{input.url}}"},
				},
			},
		},
	})
	bus := &recordingPipelineBus{}
	db, store := newSQLiteActivityJournalStore(t, ctx)
	seedActivityRun(t, db, true, runID)
	pc := NewPipelineCoordinatorWithOptions(bus, db, PipelineCoordinatorOptions{
		Module:        staticSemanticWorkflowModule{source: source},
		WorkflowStore: store,
		Credentials:   credentialStore,
	})
	intent := testNonIdempotentActivityIntent(runID, sourceEventID, entityID)
	request, err := activityRequestEmitIntent(intent)
	if err != nil {
		t.Fatalf("activityRequestEmitIntent: %v", err)
	}
	handled, err := pc.handleEventResult(ctx, request.Event)
	if err != nil {
		t.Fatalf("handleEventResult: %v", err)
	}
	if !handled {
		t.Fatal("handleEventResult handled = false, want true")
	}
	if calls != 1 {
		t.Fatalf("server calls = %d, want exactly one provider write", calls)
	}
	if len(bus.publishes) != 1 {
		t.Fatalf("published events = %d, want 1", len(bus.publishes))
	}
	evt := bus.publishes[0]
	if got := evt.Type(); got != events.EventType(intent.SuccessEvent) {
		t.Fatalf("event type = %q, want success", got)
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload(), &payload); err != nil {
		t.Fatalf("payload unmarshal: %v", err)
	}
	result := payload["result"].(map[string]any)
	if got := result["echoed_authorization"]; got != "Bearer [REDACTED]" {
		t.Fatalf("redacted result authorization = %#v", got)
	}

	handled, err = pc.handleEventResult(ctx, request.Event)
	if err != nil {
		t.Fatalf("duplicate handleEventResult: %v", err)
	}
	if !handled {
		t.Fatal("duplicate handleEventResult handled = false, want true")
	}
	if calls != 1 {
		t.Fatalf("server calls after duplicate = %d, want no re-dispatch", calls)
	}
	if len(bus.publishes) != 2 {
		t.Fatalf("published events after duplicate = %d, want canonical result re-published", len(bus.publishes))
	}
	if got, want := bus.publishes[1].ID(), bus.publishes[0].ID(); got != want {
		t.Fatalf("duplicate result id = %q, want canonical journaled id %q", got, want)
	}
}

func TestPipelineActivityRequestNonIdempotentFailureDoesNotRetry(t *testing.T) {
	ctx := context.Background()
	runID := uuid.NewString()
	sourceEventID := uuid.NewString()
	entityID := uuid.NewString()
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		http.Error(w, "temporary", http.StatusInternalServerError)
	}))
	defer server.Close()

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"provider_write": {
				HandlerType:  "http",
				EffectClass:  string(runtimecontracts.ActivityEffectClassNonIdempotentWrite),
				OutputSchema: runtimecontracts.ToolInputSchema{Type: "object"},
				HTTP:         &runtimecontracts.HTTPToolSpec{Method: "POST", URL: server.URL},
			},
		},
	})
	bus := &recordingPipelineBus{}
	db, store := newSQLiteActivityJournalStore(t, ctx)
	seedActivityRun(t, db, true, runID)
	pc := NewPipelineCoordinatorWithOptions(bus, db, PipelineCoordinatorOptions{
		Module:        staticSemanticWorkflowModule{source: source},
		WorkflowStore: store,
	})
	intent := testNonIdempotentActivityIntent(runID, sourceEventID, entityID)
	intent.RetryMaxAttempts = 3
	request, err := activityRequestEmitIntent(intent)
	if err != nil {
		t.Fatalf("activityRequestEmitIntent: %v", err)
	}
	if _, err := pc.handleEventResult(ctx, request.Event); err != nil {
		t.Fatalf("handleEventResult: %v", err)
	}
	if calls != 1 {
		t.Fatalf("server calls = %d, want no automatic retry for non-idempotent write", calls)
	}
	if len(bus.publishes) != 1 || bus.publishes[0].Type() != events.EventType(intent.FailureEvent) {
		t.Fatalf("publishes = %#v, want one failure event", bus.publishes)
	}
	rec, ok, err := store.LoadActivityAttempt(ctx, activityRequestEventID(intent))
	if err != nil {
		t.Fatalf("LoadActivityAttempt: %v", err)
	}
	if !ok || rec.Status != ActivityAttemptStatusFailed {
		t.Fatalf("journal status = (%v, %q), want failed", ok, rec.Status)
	}
}

func TestPipelineActivityRequestNonIdempotentTransportErrorMarksUncertain(t *testing.T) {
	ctx := context.Background()
	runID := uuid.NewString()
	sourceEventID := uuid.NewString()
	entityID := uuid.NewString()
	var calls int
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"provider_write": {
				HandlerType:  "http",
				EffectClass:  string(runtimecontracts.ActivityEffectClassNonIdempotentWrite),
				OutputSchema: runtimecontracts.ToolInputSchema{Type: "object"},
				HTTP:         &runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://provider.test/write"},
			},
		},
	})
	bus := &recordingPipelineBus{}
	db, store := newSQLiteActivityJournalStore(t, ctx)
	seedActivityRun(t, db, true, runID)
	pc := NewPipelineCoordinatorWithOptions(bus, db, PipelineCoordinatorOptions{
		Module:        staticSemanticWorkflowModule{source: source},
		WorkflowStore: store,
	})
	client := &http.Client{Transport: activityRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		if got := req.URL.String(); got != "https://provider.test/write" {
			t.Fatalf("request URL = %q, want provider URL", got)
		}
		return nil, fmt.Errorf("connection reset after dispatch")
	})}
	dispatcher := pipelineActivityDispatcher{coordinator: pc, client: client}
	intent := testNonIdempotentActivityIntent(runID, sourceEventID, entityID)
	intent.RetryMaxAttempts = 3
	if err := dispatcher.executeActivityIntent(ctx, intent); err != nil {
		t.Fatalf("executeActivityIntent: %v", err)
	}
	if calls != 1 {
		t.Fatalf("transport calls = %d, want one post-start dispatch attempt", calls)
	}
	if len(bus.publishes) != 1 || bus.publishes[0].Type() != events.EventType(intent.FailureEvent) {
		t.Fatalf("publishes = %#v, want one failure event", bus.publishes)
	}
	var payload map[string]any
	if err := json.Unmarshal(bus.publishes[0].Payload(), &payload); err != nil {
		t.Fatalf("payload unmarshal: %v", err)
	}
	errText := strings.TrimSpace(asString(payload["error"]))
	if !strings.Contains(errText, "outcome is uncertain") || !strings.Contains(errText, "connection reset after dispatch") {
		t.Fatalf("failure error = %q, want uncertain transport outcome", errText)
	}
	rec, ok, err := store.LoadActivityAttempt(ctx, activityRequestEventID(intent))
	if err != nil {
		t.Fatalf("LoadActivityAttempt: %v", err)
	}
	if !ok || rec.Status != ActivityAttemptStatusUncertain {
		t.Fatalf("journal status = (%v, %q), want uncertain", ok, rec.Status)
	}
}

func TestPipelineActivityRequestStartedJournalBlocksProviderRedispatch(t *testing.T) {
	ctx := context.Background()
	runID := uuid.NewString()
	sourceEventID := uuid.NewString()
	entityID := uuid.NewString()
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}))
	defer server.Close()

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"provider_write": {
				HandlerType:  "http",
				EffectClass:  string(runtimecontracts.ActivityEffectClassNonIdempotentWrite),
				OutputSchema: runtimecontracts.ToolInputSchema{Type: "object"},
				HTTP:         &runtimecontracts.HTTPToolSpec{Method: "POST", URL: server.URL},
			},
		},
	})
	bus := &recordingPipelineBus{}
	db, store := newSQLiteActivityJournalStore(t, ctx)
	seedActivityRun(t, db, true, runID)
	pc := NewPipelineCoordinatorWithOptions(bus, db, PipelineCoordinatorOptions{
		Module:        staticSemanticWorkflowModule{source: source},
		WorkflowStore: store,
	})
	intent := testNonIdempotentActivityIntent(runID, sourceEventID, entityID)
	if _, inserted, err := store.StartActivityAttempt(ctx, activityAttemptStartRecord(intent, activityInputHash(intent.Input))); err != nil {
		t.Fatalf("StartActivityAttempt: %v", err)
	} else if !inserted {
		t.Fatal("StartActivityAttempt inserted = false, want seeded started row")
	}
	request, err := activityRequestEmitIntent(intent)
	if err != nil {
		t.Fatalf("activityRequestEmitIntent: %v", err)
	}
	if _, err := pc.handleEventResult(ctx, request.Event); err != nil {
		t.Fatalf("handleEventResult: %v", err)
	}
	if calls != 0 {
		t.Fatalf("server calls = %d, want started journal to block re-dispatch", calls)
	}
	rec, ok, err := store.LoadActivityAttempt(ctx, activityRequestEventID(intent))
	if err != nil {
		t.Fatalf("LoadActivityAttempt: %v", err)
	}
	if !ok || rec.Status != ActivityAttemptStatusUncertain {
		t.Fatalf("journal status = (%v, %q), want uncertain", ok, rec.Status)
	}
	if len(bus.publishes) != 1 || bus.publishes[0].Type() != events.EventType(intent.FailureEvent) {
		t.Fatalf("publishes = %#v, want one failure event from uncertain journal", bus.publishes)
	}
}

func TestPipelineActivityRequestMissingCredentialFailsBeforeJournalAndDispatch(t *testing.T) {
	ctx := context.Background()
	runID := uuid.NewString()
	sourceEventID := uuid.NewString()
	entityID := uuid.NewString()
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}))
	defer server.Close()

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"provider_write": {
				HandlerType:  "http",
				EffectClass:  string(runtimecontracts.ActivityEffectClassNonIdempotentWrite),
				Credentials:  []string{"provider_token"},
				OutputSchema: runtimecontracts.ToolInputSchema{Type: "object"},
				HTTP: &runtimecontracts.HTTPToolSpec{
					Method:  "POST",
					URL:     server.URL,
					Headers: map[string]string{"Authorization": "Bearer {{credentials.provider_token}}"},
				},
			},
		},
	})
	bus := &recordingPipelineBus{}
	db, store := newSQLiteActivityJournalStore(t, ctx)
	seedActivityRun(t, db, true, runID)
	emptyCredentials := testActivityCredentialStore(t, "", "")
	pc := NewPipelineCoordinatorWithOptions(bus, db, PipelineCoordinatorOptions{
		Module:        staticSemanticWorkflowModule{source: source},
		WorkflowStore: store,
		Credentials:   emptyCredentials,
	})
	intent := testNonIdempotentActivityIntent(runID, sourceEventID, entityID)
	request, err := activityRequestEmitIntent(intent)
	if err != nil {
		t.Fatalf("activityRequestEmitIntent: %v", err)
	}
	if _, err := pc.handleEventResult(ctx, request.Event); err != nil {
		t.Fatalf("handleEventResult: %v", err)
	}
	if calls != 0 {
		t.Fatalf("server calls = %d, want missing credential to fail before provider dispatch", calls)
	}
	if _, ok, err := store.LoadActivityAttempt(ctx, activityRequestEventID(intent)); err != nil {
		t.Fatalf("LoadActivityAttempt: %v", err)
	} else if ok {
		t.Fatal("activity attempt journal row exists, want missing credential to fail before started journal")
	}
	if len(bus.publishes) != 1 || bus.publishes[0].Type() != events.EventType(intent.FailureEvent) {
		t.Fatalf("publishes = %#v, want one failure event", bus.publishes)
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

func testNonIdempotentActivityIntent(runID, sourceEventID, entityID string) runtimeengine.ActivityIntent {
	intent := testActivityIntent("https://example.com/source")
	intent.Tool = "provider_write"
	intent.ActivityID = "scanner_provider_write"
	intent.EffectClass = runtimecontracts.ActivityEffectClassNonIdempotentWrite
	intent.SuccessEvent = "research.scanner_provider_write.succeeded"
	intent.FailureEvent = "research.scanner_provider_write.failed"
	intent.SourceRunID = runID
	intent.SourceEventID = sourceEventID
	intent.ParentEventID = sourceEventID
	intent.EntityID = identity.NormalizeEntityID(entityID)
	return intent.Normalized()
}

func testActivityCredentialStore(t *testing.T, key, value string) runtimecredentials.Store {
	t.Helper()
	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if strings.TrimSpace(key) != "" {
		if err := store.Set(context.Background(), key, value); err != nil {
			t.Fatalf("Set credential: %v", err)
		}
	}
	return store
}

type activityRoundTripFunc func(*http.Request) (*http.Response, error)

func (f activityRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func newSQLiteActivityJournalStore(t *testing.T, ctx context.Context) (*sql.DB, *WorkflowInstanceStore) {
	t.Helper()
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	db, err := sql.Open("sqlite", "file:"+name+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	createActivityJournalSQLiteSchema(t, ctx, db)
	return db, NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db, &recordingRuntimeMutationRunner{db: db})
}

func seedActivityRun(t *testing.T, db *sql.DB, sqlite bool, runID string) {
	t.Helper()
	if sqlite {
		if _, err := db.Exec(`INSERT INTO runs (run_id, status) VALUES (?, 'running')`, runID); err != nil {
			t.Fatalf("seed sqlite run: %v", err)
		}
		return
	}
	if _, err := db.Exec(`INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed postgres run: %v", err)
	}
}

func createActivityJournalSQLiteSchema(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	for _, stmt := range []string{
		`CREATE TABLE runs (
			run_id TEXT PRIMARY KEY,
			status TEXT NOT NULL DEFAULT 'running'
		)`,
		`CREATE TABLE activity_attempts (
			request_event_id TEXT PRIMARY KEY,
			run_id TEXT NOT NULL,
			source_event_id TEXT,
			parent_event_id TEXT,
			entity_id TEXT,
			flow_instance TEXT,
			node_id TEXT NOT NULL,
			handler_event_key TEXT NOT NULL,
			activity_id TEXT NOT NULL,
			tool TEXT NOT NULL,
			effect_class TEXT NOT NULL,
			attempt INTEGER NOT NULL DEFAULT 1,
			status TEXT NOT NULL,
			success_event TEXT NOT NULL,
			failure_event TEXT NOT NULL,
			result_event_id TEXT,
			result_event_type TEXT,
			result_payload TEXT,
			error TEXT,
			input_hash TEXT NOT NULL,
			started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
			completed_at TEXT,
			updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("create sqlite activity journal schema: %v", err)
		}
	}
}
