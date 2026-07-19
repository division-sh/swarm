package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestPipelineActivityIntentWriterPersistsDurableActivityRequestEvent(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	bus := &recordingPipelineBus{}
	pc := NewPipelineCoordinatorWithOptions(bus, nil, PipelineCoordinatorOptions{
		Module: staticSemanticWorkflowModule{source: source},
	})
	intent := testActivityIntent("https://example.com/source")
	intent.PlanGeneration = "sha256:" + strings.Repeat("a", 64)

	writer := pipelineActivityIntentWriter{coordinator: pc}
	if err := writer.WriteActivityIntents(testAuthorActivityContext(context.Background()), []runtimeengine.ActivityIntent{intent}); err != nil {
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
	if payload.Tool != "source_scrape" || payload.PlanGeneration != intent.PlanGeneration || payload.SuccessEvent != "research.scanner_source_scrape.succeeded" {
		t.Fatalf("request payload = %#v", payload)
	}
	recovered, err := activityIntentFromRequestEvent(request.Event)
	if err != nil {
		t.Fatalf("recover request intent: %v", err)
	}
	if recovered.PlanGeneration != intent.PlanGeneration {
		t.Fatalf("recovered plan generation = %q, want %q", recovered.PlanGeneration, intent.PlanGeneration)
	}
	semanticPayload, err := canonicaljson.Decode(request.Event.Payload())
	if err != nil {
		t.Fatalf("admit request payload: %v", err)
	}
	input, ok := semanticPayload.Lookup("input")
	if !ok {
		t.Fatal("request input is missing")
	}
	url, ok := input.Lookup("url")
	if got, isText := url.String(); !ok || !isText || got != "https://example.com/source" {
		t.Fatalf("request input url = %q (string=%v)", got, isText)
	}
}

func TestActivityRequestAndResultPreserveMockExecutionMode(t *testing.T) {
	intent := testActivityIntent("https://example.com/source")
	intent.ExecutionMode = executionmode.Mock
	request, err := activityRequestEmitIntent(intent)
	if err != nil {
		t.Fatalf("activityRequestEmitIntent: %v", err)
	}
	if request.Event.ExecutionMode() != executionmode.Mock {
		t.Fatalf("request execution mode = %q, want mock", request.Event.ExecutionMode())
	}
	recovered, err := activityIntentFromRequestEvent(request.Event)
	if err != nil {
		t.Fatalf("activityIntentFromRequestEvent: %v", err)
	}
	if recovered.ExecutionMode != executionmode.Mock {
		t.Fatalf("recovered intent execution mode = %q, want mock", recovered.ExecutionMode)
	}

	var emitted []events.Event
	ctx := context.WithValue(context.Background(), pipelineEmitCollectorKey{}, &emitted)
	if err := (pipelineActivityDispatcher{}).publishActivityResult(ctx, recovered, recovered.SuccessEvent, map[string]any{"ok": true}); err != nil {
		t.Fatalf("publishActivityResult: %v", err)
	}
	if len(emitted) != 1 || emitted[0].ExecutionMode() != executionmode.Mock {
		t.Fatalf("result events = %#v, want one mock event", emitted)
	}

}

func TestActivityExecutionContextRejectsCausalModeConflict(t *testing.T) {
	intent := testActivityIntent("https://example.com/source")
	intent.ExecutionMode = executionmode.Mock
	ctx := runtimeeffects.WithExecutionMode(context.Background(), executionmode.Live)
	if _, err := activityExecutionContext(ctx, intent); err == nil || !strings.Contains(err.Error(), "conflicts with dispatch context") {
		t.Fatalf("activityExecutionContext error = %v, want mode conflict", err)
	}
	ctx, err := activityExecutionContext(context.Background(), intent)
	if err != nil {
		t.Fatalf("activityExecutionContext mock recovery: %v", err)
	}
	if got, ok := runtimeeffects.ExecutionModeFromContext(ctx); !ok || got != executionmode.Mock {
		t.Fatalf("activity execution context mode = %q (present=%v), want mock", got, ok)
	}
}

func TestPipelineActivityIntentWriterDefersRuntimeLogUntilPostCommit(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	bus := &recordingPipelineBus{}
	pc := NewPipelineCoordinatorWithOptions(bus, nil, PipelineCoordinatorOptions{
		Module: staticSemanticWorkflowModule{source: source},
	})
	writer := pipelineActivityIntentWriter{coordinator: pc}
	postCommit := []func(){}
	ctx := WithPipelinePostCommitActions(testAuthorActivityContext(context.Background()), &postCommit)
	ctx = WithPipelineSQLTxContext(ctx, &sql.Tx{})

	if err := writer.WriteActivityIntents(ctx, []runtimeengine.ActivityIntent{testActivityIntent("https://example.com/source")}); err != nil {
		t.Fatalf("WriteActivityIntents: %v", err)
	}
	if got := len(bus.runtimeLogEntries()); got != 0 {
		t.Fatalf("runtime logs before commit = %d, want 0", got)
	}
	if got := len(postCommit); got != 1 {
		t.Fatalf("post-commit actions = %d, want 1", got)
	}
	FlushPipelinePostCommitActions(postCommit)
	logs := bus.runtimeLogEntries()
	if len(logs) != 1 || logs[0].Action != "intent_persisted" {
		t.Fatalf("runtime logs after commit = %#v, want one intent_persisted entry", logs)
	}
}

func TestLoopActivityRequestResultAndForkCarryGeneration(t *testing.T) {
	entityID := uuid.NewString()
	activation, err := loopruntime.New("source-run", entityID, "validation", "revision", "revision_id", uuid.NewString(), "review", 3, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	forked, err := loopruntime.Fork(activation, "fork-run", entityID)
	if err != nil {
		t.Fatal(err)
	}
	source := testNonIdempotentActivityIntent("source-run", uuid.NewString(), entityID)
	source.Generation, source.LoopStage = activation.Generation(), "review"
	fork := source
	fork.SourceRunID, fork.Generation = "fork-run", forked.Generation()
	if activityRequestEventID(source) == activityRequestEventID(fork) || activityResultEventID(source, source.SuccessEvent) == activityResultEventID(fork, fork.SuccessEvent) {
		t.Fatal("fork-local activity identity retained source loop generation")
	}
	emit, err := activityRequestEmitIntent(source)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip, err := activityIntentFromRequestEvent(emit.Event)
	if err != nil || !roundTrip.Generation.Equal(source.Generation) || roundTrip.LoopStage != "review" {
		t.Fatalf("activity request generation round trip = %#v err=%v", roundTrip, err)
	}
	for name, payload := range map[string]map[string]any{
		"success": activitySuccessPayload(source, map[string]any{"ok": true}),
		"failure": activityFailurePayload(source, runtimefailures.New(runtimefailures.ClassConnectorFailure, "provider_failed", "test", "activity", nil)),
	} {
		if payload[activation.RevisionField] != activation.RevisionID {
			t.Fatalf("%s payload revision = %#v, want %s", name, payload[activation.RevisionField], activation.RevisionID)
		}
	}
}

func TestActivityHTTPResponseSuccessPolicyParityCases(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		body        string
		policy      runtimecontracts.HTTPResponseSuccess
		secrets     []string
		wantClass   runtimefailures.Class
		wantDetail  string
		forbidError string
	}{
		{name: "status 2xx", status: http.StatusNoContent, policy: runtimecontracts.HTTPResponseSuccess{Kind: "http_status_2xx"}},
		{name: "status non-2xx", status: http.StatusMultipleChoices, body: `{}`, policy: runtimecontracts.HTTPResponseSuccess{Kind: "http_status_2xx"}, wantClass: runtimefailures.ClassConnectorFailure, wantDetail: "provider_http_status"},
		{name: "boolean equality", status: http.StatusOK, body: `{"ok":true}`, policy: runtimecontracts.HTTPResponseSuccess{Kind: "json_field_equals", Path: "response.body.ok", Equals: true}},
		{name: "string equality", status: http.StatusOK, body: `{"state":"accepted"}`, policy: runtimecontracts.HTTPResponseSuccess{Kind: "json_field_equals", Path: "response.body.state", Equals: "accepted"}},
		{name: "numeric equality", status: http.StatusOK, body: `{"count":2}`, policy: runtimecontracts.HTTPResponseSuccess{Kind: "json_field_equals", Path: "response.body.count", Equals: int64(2)}},
		{name: "provider failure", status: http.StatusOK, body: `{"ok":false}`, policy: runtimecontracts.HTTPResponseSuccess{Kind: "json_field_equals", Path: "response.body.ok", Equals: true}, wantClass: runtimefailures.ClassConnectorFailure, wantDetail: "provider_response_rejected"},
		{name: "unresolved path", status: http.StatusOK, body: `{"ok":true}`, policy: runtimecontracts.HTTPResponseSuccess{Kind: "json_field_equals", Path: "response.body.missing", Equals: true}, wantClass: runtimefailures.ClassConnectorFailure, wantDetail: "provider_response_rejected"},
		{name: "secret redaction", status: http.StatusOK, body: `{"state":"provider-secret"}`, policy: runtimecontracts.HTTPResponseSuccess{Kind: "json_field_equals", Path: "response.body.state", Equals: "accepted"}, secrets: []string{"provider-secret"}, wantClass: runtimefailures.ClassConnectorFailure, wantDetail: "provider_response_rejected", forbidError: "provider-secret"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.body != "" {
					w.Header().Set("Content-Type", "application/json")
				}
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()

			policy := tc.policy
			_, err := executePreparedActivityHTTPTool(testAuthorActivityContext(context.Background()), preparedActivityHTTPTool{
				toolName: "policy_probe",
				method:   http.MethodPost,
				url:      server.URL,
				timeout:  time.Second,
				client:   server.Client(),
				secrets:  tc.secrets,
				success:  &policy,
			})
			if tc.wantClass == "" {
				if err != nil {
					t.Fatalf("executePreparedActivityHTTPTool: %v", err)
				}
				return
			}
			failure, ok := runtimefailures.As(err)
			if err == nil || !ok || failure.Failure.Class != tc.wantClass || failure.Failure.Detail.Code != tc.wantDetail {
				t.Fatalf("executePreparedActivityHTTPTool failure = %#v, want %s/%s", failure, tc.wantClass, tc.wantDetail)
			}
			if tc.forbidError != "" {
				raw, marshalErr := runtimefailures.MarshalEnvelope(failure.Failure)
				if marshalErr != nil {
					t.Fatalf("marshal failure: %v", marshalErr)
				}
				if strings.Contains(string(raw), tc.forbidError) || strings.Contains(err.Error(), tc.forbidError) {
					t.Fatalf("executePreparedActivityHTTPTool failure leaked %q: %s", tc.forbidError, raw)
				}
			}
		})
	}
}

func TestActivityCredentialRedactionGuarantee(t *testing.T) {
	const secret = "provider-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"state":"provider-secret"}`)
	}))
	defer server.Close()

	policy := runtimecontracts.HTTPResponseSuccess{Kind: "json_field_equals", Path: "response.body.state", Equals: "accepted"}
	_, err := executePreparedActivityHTTPTool(testAuthorActivityContext(context.Background()), preparedActivityHTTPTool{
		toolName: "redaction_guarantee_probe",
		method:   http.MethodPost,
		url:      server.URL,
		timeout:  time.Second,
		client:   server.Client(),
		secrets:  []string{secret},
		success:  &policy,
	})
	failure, ok := runtimefailures.As(err)
	if err == nil || !ok || failure.Failure.Detail.Code != "provider_response_rejected" {
		t.Fatalf("failure = %#v, want redacted provider_response_rejected", failure)
	}
	raw, marshalErr := runtimefailures.MarshalEnvelope(failure.Failure)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	if strings.Contains(string(raw), secret) || strings.Contains(err.Error(), secret) {
		t.Fatalf("credential value leaked through failure: envelope=%s error=%v", raw, err)
	}
}

func TestActivityHTTPAppliesConnectorThenChannelResultProjection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true,"result":{"message_id":42}}`)
	}))
	defer server.Close()
	allow := false
	minimum, maximum := float64(1), float64(2147483647)
	result, err := executePreparedActivityHTTPTool(context.Background(), preparedActivityHTTPTool{
		toolName: "telegram.send_interactive",
		method:   http.MethodPost,
		url:      server.URL,
		timeout:  time.Second,
		client:   server.Client(),
		success:  &runtimecontracts.HTTPResponseSuccess{Kind: "json_field_equals", Path: "response.body.ok", Equals: true},
		responseMapping: map[string]any{
			"message_id": "{{response.body.result.message_id}}",
		},
		outputSchema: runtimecontracts.ToolInputSchema{
			Type: "object", Required: []string{"message_id"},
			AdditionalProperties: runtimecontracts.ToolAdditionalProperties{Allowed: &allow},
			Properties:           map[string]runtimecontracts.ToolInputSchema{"message_id": {Type: "integer", Minimum: &minimum, Maximum: &maximum}},
		},
		compiledResult: &runtimecontracts.CompiledResultProjection{
			Fields: map[string]runtimecontracts.CompiledResultField{
				"delivery_reference.id": {From: "result.message_id"},
			},
			OutputSchema: runtimecontracts.ToolInputSchema{
				Type: "object", Required: []string{"delivery_reference"},
				AdditionalProperties: runtimecontracts.ToolAdditionalProperties{Allowed: &allow},
				Properties: map[string]runtimecontracts.ToolInputSchema{
					"delivery_reference": {
						Type: "object", Required: []string{"id"}, AdditionalProperties: runtimecontracts.ToolAdditionalProperties{Allowed: &allow},
						Properties: map[string]runtimecontracts.ToolInputSchema{"id": {Type: "integer", Minimum: &minimum, Maximum: &maximum}},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("executePreparedActivityHTTPTool: %v", err)
	}
	want := map[string]any{"delivery_reference": map[string]any{"id": float64(42)}}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("result = %#v, want %#v", result, want)
	}
}

func TestCompiledResultProjectionHasNoConversionSeam(t *testing.T) {
	if _, ok := reflect.TypeOf(runtimecontracts.CompiledResultField{}).FieldByName("Convert"); ok {
		t.Fatal("CompiledResultField still exposes a conversion interpreter")
	}
	source, err := os.ReadFile("activity_engine.go")
	if err != nil {
		t.Fatalf("read activity_engine.go: %v", err)
	}
	for _, forbidden := range []string{"convertCompiledActivityResult", "compiled result conversion", "FieldProjectionConvertNumberToText"} {
		if strings.Contains(string(source), forbidden) {
			t.Fatalf("activity result projection retains legacy conversion seam %q", forbidden)
		}
	}
}

func TestChannelProjectedActivityResultJournalsAndReplaysAcrossSelectedStores(t *testing.T) {
	for _, tc := range activityBoringStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			runID := uuid.NewString()
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"ok":true,"result":{"message_id":42}}`)
			}))
			defer server.Close()

			db, store, sqlite := newActivityJournalStoreForCase(t, ctx, tc.kind)
			seedActivityRun(t, db, sqlite, runID)
			source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{
				"channel.ops.deliver": testCompiledChannelActivityTool(server.URL),
			}})
			bus := &recordingPipelineBus{}
			pc := NewPipelineCoordinatorWithOptions(bus, db, PipelineCoordinatorOptions{Module: staticSemanticWorkflowModule{source: source}, WorkflowStore: store})
			intent := testNonIdempotentActivityIntent(runID, uuid.NewString(), uuid.NewString())
			intent.Tool = "channel.ops.deliver"
			intent.ActivityID = "channel_deliver"
			intent.SuccessEvent = "channel.deliver.succeeded"
			intent.FailureEvent = "channel.deliver.failed"
			request, err := activityRequestEmitIntent(intent)
			if err != nil {
				t.Fatalf("activityRequestEmitIntent: %v", err)
			}
			if handled, err := pc.handleEventResult(ctx, request.Event); err != nil || !handled {
				t.Fatalf("first handle = %v, err=%v", handled, err)
			}
			if handled, err := pc.handleEventResult(ctx, request.Event); err != nil || !handled {
				t.Fatalf("replay handle = %v, err=%v", handled, err)
			}
			if calls.Load() != 1 {
				t.Fatalf("provider calls = %d, want one across replay", calls.Load())
			}
			if len(bus.publishes) != 2 || bus.publishes[0].ID() != bus.publishes[1].ID() {
				t.Fatalf("published replay events = %#v, want same journaled result twice", bus.publishes)
			}
			var payload map[string]any
			if err := json.Unmarshal(bus.publishes[1].Payload(), &payload); err != nil {
				t.Fatalf("decode replay payload: %v", err)
			}
			want := map[string]any{"delivery_reference": map[string]any{"id": float64(42)}}
			if !reflect.DeepEqual(payload["result"], want) {
				t.Fatalf("journaled channel result = %#v, want %#v", payload["result"], want)
			}
		})
	}
}

func TestChannelActivityPostCommitAcknowledgmentLossStateBlocksRedispatchAcrossSelectedStores(t *testing.T) {
	for _, tc := range activityBoringStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			runID := uuid.NewString()
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				_, _ = io.WriteString(w, `{"ok":true,"result":{"message_id":42}}`)
			}))
			defer server.Close()
			db, store, sqlite := newActivityJournalStoreForCase(t, ctx, tc.kind)
			seedActivityRun(t, db, sqlite, runID)
			source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{
				"channel.ops.deliver": testCompiledChannelActivityTool(server.URL),
			}})
			bus := &recordingPipelineBus{}
			pc := NewPipelineCoordinatorWithOptions(bus, db, PipelineCoordinatorOptions{Module: staticSemanticWorkflowModule{source: source}, WorkflowStore: store})
			intent := testNonIdempotentActivityIntent(runID, uuid.NewString(), uuid.NewString())
			intent.Tool = "channel.ops.deliver"
			intent.ActivityID = "channel_deliver"
			intent.SuccessEvent = "channel.deliver.succeeded"
			intent.FailureEvent = "channel.deliver.failed"
			if _, inserted, err := store.StartActivityAttempt(ctx, activityAttemptStartRecord(intent, activityInputHash(intent.Input))); err != nil || !inserted {
				t.Fatalf("seed committed same-key claim: inserted=%v err=%v", inserted, err)
			}
			request, err := activityRequestEmitIntent(intent)
			if err != nil {
				t.Fatalf("activityRequestEmitIntent: %v", err)
			}
			if handled, err := pc.handleEventResult(ctx, request.Event); err != nil || !handled {
				t.Fatalf("reconcile committed claim = %v, err=%v", handled, err)
			}
			if calls.Load() != 0 || len(bus.publishes) != 0 {
				t.Fatalf("committed claim redispatched provider: calls=%d publishes=%d", calls.Load(), len(bus.publishes))
			}
		})
	}
}

func newActivityJournalStoreForCase(t *testing.T, ctx context.Context, kind activityBoringStoreKind) (*sql.DB, *WorkflowInstanceStore, bool) {
	t.Helper()
	if kind == activityBoringStoreSQLite {
		db, store := newSQLiteActivityJournalStore(t, ctx)
		return db, store, true
	}
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	return db, NewWorkflowInstanceStore(db), false
}

func testCompiledChannelActivityTool(url string) runtimecontracts.ToolSchemaEntry {
	allow := false
	minimum, maximum := float64(1), float64(2147483647)
	return runtimecontracts.ToolSchemaEntry{
		HandlerType: "http", EffectClass: string(runtimecontracts.ActivityEffectClassNonIdempotentWrite),
		HTTP:            &runtimecontracts.HTTPToolSpec{Method: http.MethodPost, URL: url},
		ResponseSuccess: &runtimecontracts.HTTPResponseSuccess{Kind: "json_field_equals", Path: "response.body.ok", Equals: true},
		ResponseMapping: map[string]any{"message_id": "{{response.body.result.message_id}}"},
		OutputSchema: runtimecontracts.ToolInputSchema{
			Type: "object", Required: []string{"message_id"}, AdditionalProperties: runtimecontracts.ToolAdditionalProperties{Allowed: &allow},
			Properties: map[string]runtimecontracts.ToolInputSchema{"message_id": {Type: "integer", Minimum: &minimum, Maximum: &maximum}},
		},
		CompiledResult: &runtimecontracts.CompiledResultProjection{
			Fields: map[string]runtimecontracts.CompiledResultField{"delivery_reference.id": {From: "result.message_id"}},
			OutputSchema: runtimecontracts.ToolInputSchema{
				Type: "object", Required: []string{"delivery_reference"}, AdditionalProperties: runtimecontracts.ToolAdditionalProperties{Allowed: &allow},
				Properties: map[string]runtimecontracts.ToolInputSchema{
					"delivery_reference": {
						Type: "object", Required: []string{"id"}, AdditionalProperties: runtimecontracts.ToolAdditionalProperties{Allowed: &allow},
						Properties: map[string]runtimecontracts.ToolInputSchema{"id": {Type: "integer", Minimum: &minimum, Maximum: &maximum}},
					},
				},
			},
		},
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
	if err := dispatcher.DispatchActivities(testAuthorActivityContext(context.Background()), []runtimeengine.ActivityIntent{intent}); err != nil {
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
	intent.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-v1:activity-round-trip"}}
	request, err := activityRequestEmitIntent(intent)
	if err != nil {
		t.Fatalf("activityRequestEmitIntent: %v", err)
	}
	handled, err := pc.handleEventResult(testAuthorActivityContext(context.Background()), eventtest.ForDelivery(request.Event, intent.Context))
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
	if len(bus.publishContexts) != 1 || bus.publishContexts[0].ReplyContextID() != intent.Context.ReplyContextID() {
		t.Fatalf("published activity result contexts = %#v, want %q", bus.publishContexts, intent.Context.ReplyContextID())
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
	handled, err := pc.handleEventResult(testAuthorActivityContext(context.Background()), request.Event)
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
	handled, err := pc.handleEventResult(testAuthorActivityContext(context.Background()), request.Event)
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
	failure, _ := payload["failure"].(map[string]any)
	detail, _ := failure["detail"].(map[string]any)
	if failure["class"] != string(runtimefailures.ClassSchemaInvalid) || detail["code"] != "activity_effect_class_unsupported" {
		t.Fatalf("failure = %#v, want fail-closed unsupported write effect", failure)
	}
}

func TestPipelineActivityRequestExecutesNonIdempotentHTTPToolOnceWithStaticCredentials(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
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
		t.Fatalf("server calls = %d, want exactly one provider write; published=%#v", calls, bus.publishes)
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

func TestPipelineActivityRequestMockFlowLocalProviderConnectorUsesGeneratedResponseAndJournal(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	runID := uuid.NewString()
	entityID := uuid.NewString()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		calls.Add(1)
	}))
	defer server.Close()

	tool := testTelegramConnectorTool(server.URL)
	tool.OutputSchema = runtimecontracts.ToolInputSchema{
		Type: "object",
		Properties: map[string]runtimecontracts.ToolInputSchema{
			"ok": {Type: "boolean"},
		},
		Required: []string{"ok"},
	}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{
		"telegram.send_message": tool,
	}})
	plan, err := providerconnectors.CompileMockResponsePlan(source)
	if err != nil {
		t.Fatalf("CompileMockResponsePlan: %v", err)
	}
	bus := &recordingPipelineBus{}
	db, store := newSQLiteActivityJournalStore(t, ctx)
	seedActivityRun(t, db, true, runID)
	credentialStore := &countingActivityCredentialStore{}
	pc := NewPipelineCoordinatorWithOptions(bus, db, PipelineCoordinatorOptions{
		Module:                 staticSemanticWorkflowModule{source: source},
		WorkflowStore:          store,
		MockConnectorResponses: plan,
		Credentials:            credentialStore,
	})
	intent := testNonIdempotentActivityIntent(runID, uuid.NewString(), entityID)
	intent.Tool = "telegram.send_message"
	intent.ActivityID = "telegram_send_message"
	intent.ExecutionMode = executionmode.Mock
	intent.Input = mustActivityInput(map[string]any{"chat_id": "42", "text": "hello"})

	for delivery := 1; delivery <= 2; delivery++ {
		if err := (pipelineActivityDispatcher{coordinator: pc}).executeActivityIntent(ctx, intent); err != nil {
			t.Fatalf("execute mock activity delivery %d: %v", delivery, err)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("HTTP calls = %d, want zero", calls.Load())
	}
	if credentialStore.reads.Load() != 0 {
		t.Fatalf("credential reads = %d, want zero", credentialStore.reads.Load())
	}
	record, ok, err := store.LoadActivityAttempt(ctx, activityRequestEventID(intent))
	if err != nil || !ok {
		t.Fatalf("LoadActivityAttempt found=%v err=%v publications=%#v", ok, err, bus.publishes)
	}
	if record.Status != ActivityAttemptStatusSucceeded || record.ExecutionMode != executionmode.Mock {
		t.Fatalf("mock attempt = %#v", record)
	}
	result, _ := record.ResultPayload["result"].(map[string]any)
	if result["ok"] != false {
		t.Fatalf("mock result = %#v", record.ResultPayload)
	}
	if len(bus.publishes) != 2 || bus.publishes[0].ID() != bus.publishes[1].ID() {
		t.Fatalf("mock duplicate publications = %#v", bus.publishes)
	}
	var storyModes int
	if err := db.QueryRow(`SELECT COUNT(*) FROM author_activity_occurrences WHERE json_extract(projection, '$.execution_mode') = 'mock' AND source_owner = 'activity_attempts'`).Scan(&storyModes); err != nil {
		t.Fatalf("query mock author activity: %v", err)
	}
	if storyModes != 2 {
		t.Fatalf("mock author activity rows = %d, want started and succeeded", storyModes)
	}
}

func TestPipelineActivityRequestMockTerminalReplayDoesNotRequireCurrentResponsePlan(t *testing.T) {
	backends := []struct {
		name  string
		store func(t *testing.T, ctx context.Context) (*sql.DB, *WorkflowInstanceStore, bool)
	}{
		{name: "sqlite", store: func(t *testing.T, ctx context.Context) (*sql.DB, *WorkflowInstanceStore, bool) {
			db, journal := newSQLiteActivityJournalStore(t, ctx)
			return db, journal, true
		}},
		{name: "postgres", store: func(t *testing.T, _ context.Context) (*sql.DB, *WorkflowInstanceStore, bool) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			return db, NewWorkflowInstanceStore(db), false
		}},
	}
	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			db, journal, sqlite := backend.store(t, ctx)
			runID := uuid.NewString()
			seedActivityRun(t, db, sqlite, runID)
			var httpCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				httpCalls.Add(1)
			}))
			defer server.Close()
			tool := testTelegramConnectorTool(server.URL)
			tool.OutputSchema = runtimecontracts.ToolInputSchema{
				Type:       "object",
				Properties: map[string]runtimecontracts.ToolInputSchema{"ok": {Type: "boolean"}},
				Required:   []string{"ok"},
			}
			source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{
				"telegram.send_message": tool,
			}})
			plan, err := providerconnectors.NewMockResponsePlan(map[string]map[string]any{
				"telegram.send_message": {"ok": true},
			})
			if err != nil {
				t.Fatalf("NewMockResponsePlan: %v", err)
			}
			intent := testNonIdempotentActivityIntent(runID, uuid.NewString(), uuid.NewString())
			intent.Tool = "telegram.send_message"
			intent.ActivityID = "telegram_send_message"
			intent.ExecutionMode = executionmode.Mock
			intent.Input = mustActivityInput(map[string]any{"chat_id": "42", "text": "hello"})

			firstBus := &recordingPipelineBus{}
			first := NewPipelineCoordinatorWithOptions(firstBus, db, PipelineCoordinatorOptions{
				Module: staticSemanticWorkflowModule{source: source}, WorkflowStore: journal,
				MockConnectorResponses: plan,
			})
			if err := (pipelineActivityDispatcher{coordinator: first}).executeActivityIntent(ctx, intent); err != nil {
				t.Fatalf("execute initial mock activity: %v", err)
			}
			if len(firstBus.publishes) != 1 {
				t.Fatalf("initial publications = %#v", firstBus.publishes)
			}

			restartBus := &recordingPipelineBus{}
			credentials := &countingActivityCredentialStore{}
			restarted := NewPipelineCoordinatorWithOptions(restartBus, db, PipelineCoordinatorOptions{
				Module: staticSemanticWorkflowModule{source: source}, WorkflowStore: journal,
				Credentials: credentials,
			})
			if err := (pipelineActivityDispatcher{coordinator: restarted}).executeActivityIntent(ctx, intent); err != nil {
				t.Fatalf("replay terminal mock activity without current plan: %v", err)
			}
			if len(restartBus.publishes) != 1 || restartBus.publishes[0].ID() != firstBus.publishes[0].ID() {
				t.Fatalf("restart publications = %#v, want journaled event %q", restartBus.publishes, firstBus.publishes[0].ID())
			}
			if httpCalls.Load() != 0 || credentials.reads.Load() != 0 {
				t.Fatalf("replay launched live dependencies: HTTP=%d credentials=%d", httpCalls.Load(), credentials.reads.Load())
			}
		})
	}
}

func TestPipelineActivityRequestMockAdmissionFailsBeforeJournalCredentialsAndHTTP(t *testing.T) {
	backends := []struct {
		name  string
		store func(t *testing.T, ctx context.Context) (*sql.DB, *WorkflowInstanceStore, bool)
	}{
		{name: "sqlite", store: func(t *testing.T, ctx context.Context) (*sql.DB, *WorkflowInstanceStore, bool) {
			db, journal := newSQLiteActivityJournalStore(t, ctx)
			return db, journal, true
		}},
		{name: "postgres", store: func(t *testing.T, _ context.Context) (*sql.DB, *WorkflowInstanceStore, bool) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			return db, NewWorkflowInstanceStore(db), false
		}},
	}
	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			db, store, sqlite := backend.store(t, ctx)
			for _, tc := range []struct {
				name      string
				tool      runtimecontracts.ToolSchemaEntry
				responses map[string]map[string]any
				wantCode  string
			}{
				{name: "missing response", tool: testTelegramConnectorTool("http://127.0.0.1:1"), responses: nil, wantCode: "mock_connector_response_not_admitted"},
				{name: "non provider", tool: runtimecontracts.ToolSchemaEntry{HandlerType: "http", EffectClass: "non_idempotent_write", OutputSchema: runtimecontracts.ToolInputSchema{Type: "object"}}, responses: map[string]map[string]any{"telegram.send_message": {"ok": true}}, wantCode: "mock_connector_response_not_admitted"},
				{name: "read only", tool: runtimecontracts.ToolSchemaEntry{Category: providerconnectors.Category, HandlerType: "http", EffectClass: "read_only", OutputSchema: runtimecontracts.ToolInputSchema{Type: "object"}}, responses: map[string]map[string]any{"telegram.send_message": {"ok": true}}, wantCode: "mock_activity_effect_class_unsupported"},
				{name: "invalid response", tool: runtimecontracts.ToolSchemaEntry{Category: providerconnectors.Category, HandlerType: "http", EffectClass: "non_idempotent_write", HTTP: &runtimecontracts.HTTPToolSpec{Method: "POST", URL: "http://127.0.0.1:1"}, OutputSchema: runtimecontracts.ToolInputSchema{Type: "object", Properties: map[string]runtimecontracts.ToolInputSchema{"ok": {Type: "boolean"}}, Required: []string{"ok"}}}, responses: map[string]map[string]any{"telegram.send_message": {"ok": "invalid"}}, wantCode: "mock_connector_response_not_admitted"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					runID := uuid.NewString()
					seedActivityRun(t, db, sqlite, runID)
					tc.tool.Credentials = []string{"must_not_read"}
					plan, err := providerconnectors.NewMockResponsePlan(tc.responses)
					if err != nil {
						t.Fatalf("NewMockResponsePlan: %v", err)
					}
					source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{"telegram.send_message": tc.tool}})
					bus := &recordingPipelineBus{}
					credentialStore := &countingActivityCredentialStore{}
					pc := NewPipelineCoordinatorWithOptions(bus, db, PipelineCoordinatorOptions{
						Module: staticSemanticWorkflowModule{source: source}, WorkflowStore: store,
						MockConnectorResponses: plan, Credentials: credentialStore,
					})
					intent := testNonIdempotentActivityIntent(runID, uuid.NewString(), uuid.NewString())
					intent.Tool = "telegram.send_message"
					intent.ExecutionMode = executionmode.Mock
					intent.EffectClass = runtimecontracts.NormalizeActivityEffectClass(tc.tool.EffectClass)
					if err := (pipelineActivityDispatcher{coordinator: pc}).executeActivityIntent(ctx, intent); err != nil {
						t.Fatalf("execute rejected mock activity: %v", err)
					}
					var attempts int
					if err := db.QueryRow(`SELECT COUNT(*) FROM activity_attempts WHERE run_id = `+activityTestPlaceholder(sqlite, 1), runID).Scan(&attempts); err != nil {
						t.Fatalf("count activity attempts: %v", err)
					}
					if attempts != 0 {
						t.Fatalf("activity attempts = %d, want zero", attempts)
					}
					if credentialStore.reads.Load() != 0 {
						t.Fatalf("credential reads = %d, want zero", credentialStore.reads.Load())
					}
					if len(bus.publishes) != 1 || !strings.Contains(string(bus.publishes[0].Payload()), tc.wantCode) {
						t.Fatalf("failure publications = %#v, want code %q", bus.publishes, tc.wantCode)
					}
				})
			}
		})
	}
}

func TestGeneratedSyntheticConnectorUsesCanonicalActivityJournalOnReplay(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	artifacts, err := providerconnectors.GenerateCatalog(os.DirFS("../../providerconnectors"))
	if err != nil {
		t.Fatalf("GenerateCatalog: %v", err)
	}
	var tool runtimecontracts.ToolSchemaEntry
	for _, artifact := range artifacts {
		if artifact.Manifest.Provider == "acme" {
			tool = artifact.Manifest.Tools["acme.create_widget"]
			break
		}
	}
	if tool.HTTP == nil {
		t.Fatal("generated synthetic connector acme.create_widget is missing")
	}

	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if got := r.URL.Path; got != "/accounts/account-7/widgets" {
			t.Fatalf("request path = %q", got)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer acme-secret" {
			t.Fatalf("Authorization = %q", got)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		if got := body["name"]; got != "proof widget" {
			t.Fatalf("request name = %#v", got)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": "widget-1"})
	}))
	defer server.Close()

	httpSpec := *tool.HTTP
	httpSpec.URL = strings.Replace(httpSpec.URL, "https://api.acme.test", server.URL, 1)
	tool.HTTP = &httpSpec
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{
		"acme.create_widget": tool,
	}})
	runID := uuid.NewString()
	sourceEventID := uuid.NewString()
	entityID := uuid.NewString()
	db, store := newSQLiteActivityJournalStore(t, ctx)
	seedActivityRun(t, db, true, runID)
	bus := &recordingPipelineBus{}
	pc := NewPipelineCoordinatorWithOptions(bus, db, PipelineCoordinatorOptions{
		Module:        staticSemanticWorkflowModule{source: source},
		WorkflowStore: store,
		Credentials:   testActivityCredentialStore(t, "acme_api_key", "acme-secret"),
	})
	intent := testNonIdempotentActivityIntent(runID, sourceEventID, entityID)
	intent.Tool = "acme.create_widget"
	intent.ActivityID = "acme_create_widget"
	intent.Input = mustActivityInput(map[string]any{"account_id": "account-7", "name": "proof widget"})
	request, err := activityRequestEmitIntent(intent)
	if err != nil {
		t.Fatalf("activityRequestEmitIntent: %v", err)
	}

	for attempt := 1; attempt <= 2; attempt++ {
		handled, err := pc.handleEventResult(ctx, request.Event)
		if err != nil {
			t.Fatalf("handleEventResult attempt %d: %v", attempt, err)
		}
		if !handled {
			t.Fatalf("handleEventResult attempt %d handled = false", attempt)
		}
	}
	if calls != 1 {
		t.Fatalf("provider calls after replay = %d, want exactly one", calls)
	}
	if len(bus.publishes) != 2 || bus.publishes[0].ID() != bus.publishes[1].ID() {
		t.Fatalf("journal replay publications = %#v, want same canonical result twice", bus.publishes)
	}
}

func TestPipelineActivityRequestTelegramConnectorRoundTripThroughInboundDelivery(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, ctx context.Context) (*sql.DB, *WorkflowInstanceStore, bool)
	}{
		{
			name: "sqlite",
			setup: func(t *testing.T, ctx context.Context) (*sql.DB, *WorkflowInstanceStore, bool) {
				db, store := newSQLiteActivityJournalStore(t, ctx)
				return db, store, true
			},
		},
		{
			name: "postgres",
			setup: func(t *testing.T, ctx context.Context) (*sql.DB, *WorkflowInstanceStore, bool) {
				_, db, _ := testutil.StartPostgres(t)
				return db, NewWorkflowInstanceStore(db), false
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, store, sqlite := tc.setup(t, ctx)
			runTelegramConnectorRoundTripThroughInboundDelivery(t, ctx, db, store, sqlite)
		})
	}
}

func runTelegramConnectorRoundTripThroughInboundDelivery(t *testing.T, ctx context.Context, db *sql.DB, store *WorkflowInstanceStore, sqlite bool) {
	t.Helper()
	runID := uuid.NewString()
	entityID := uuid.NewString()
	seedActivityRun(t, db, sqlite, runID)

	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if got := r.URL.Path; got != "/botprovider-secret/sendMessage" {
			t.Fatalf("telegram path = %q, want token path sendMessage", got)
		}
		if got := r.Header.Get("Content-Type"); !strings.Contains(got, "application/json") {
			t.Fatalf("Content-Type = %q, want application/json", got)
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll request body: %v", err)
		}
		var body map[string]any
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Fatalf("decode Telegram request body %s: %v", raw, err)
		}
		if got := asString(body["chat_id"]); got != "42" {
			t.Fatalf("chat_id = %#v, want 42", body["chat_id"])
		}
		if got := asString(body["text"]); got != "reply: hello from telegram" {
			t.Fatalf("text = %#v, want reply text", body["text"])
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok": true,
			"result": map[string]any{
				"message_id": 99,
				"chat":       map[string]any{"id": 42},
				"text":       "reply: hello from telegram",
			},
		})
	}))
	defer server.Close()

	delivery, inboundEvent := acceptedTelegramInboundDeliveryEvent(t, entityID, runID)
	chatID, incomingText := telegramInboundChatAndText(t, delivery.Events[0].Payload)
	credentialStore := testActivityCredentialStore(t, "telegram_bot_token", "provider-secret")
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"telegram.send_message": testTelegramConnectorTool(server.URL),
		},
	})
	bus := &recordingPipelineBus{}
	pc := NewPipelineCoordinatorWithOptions(bus, db, PipelineCoordinatorOptions{
		Module:        staticSemanticWorkflowModule{source: source},
		WorkflowStore: store,
		Credentials:   credentialStore,
	})
	intent := testNonIdempotentActivityIntent(runID, inboundEvent.ID(), entityID)
	intent.Tool = "telegram.send_message"
	intent.ActivityID = "telegram_send_message"
	intent.Input = mustActivityInput(map[string]any{"chat_id": chatID, "text": "reply: " + incomingText})
	intent.SuccessEvent = "telegram.send_message.succeeded"
	intent.FailureEvent = "telegram.send_message.failed"
	intent.SourceTaskID = inboundEvent.TaskID()
	intent.ParentEventID = inboundEvent.ID()
	intent.ChainDepth = inboundEvent.ChainDepth()
	intent = intent.Normalized()
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
		t.Fatalf("Telegram calls = %d, want exactly one", calls)
	}
	if len(bus.publishes) != 1 {
		t.Fatalf("published events = %d, want one generated activity result", len(bus.publishes))
	}
	resultEvent := bus.publishes[0]
	if resultEvent.Type() != events.EventType(intent.SuccessEvent) {
		t.Fatalf("result event type = %q, want %q", resultEvent.Type(), intent.SuccessEvent)
	}
	if resultEvent.ParentEventID() != inboundEvent.ID() {
		t.Fatalf("result parent event id = %q, want inbound event id %q", resultEvent.ParentEventID(), inboundEvent.ID())
	}
	if strings.Contains(string(resultEvent.Payload()), "provider-secret") {
		t.Fatalf("result payload leaked credential: %s", resultEvent.Payload())
	}
	rec, ok, err := store.LoadActivityAttempt(ctx, activityRequestEventID(intent))
	if err != nil {
		t.Fatalf("LoadActivityAttempt: %v", err)
	}
	if !ok || rec.Status != ActivityAttemptStatusSucceeded {
		t.Fatalf("activity attempt = (%v, %q), want succeeded", ok, rec.Status)
	}

	handled, err = pc.handleEventResult(ctx, request.Event)
	if err != nil {
		t.Fatalf("duplicate handleEventResult: %v", err)
	}
	if !handled {
		t.Fatal("duplicate handleEventResult handled = false, want true")
	}
	if calls != 1 {
		t.Fatalf("Telegram calls after duplicate = %d, want no redispatch", calls)
	}
	if len(bus.publishes) != 2 {
		t.Fatalf("published events after duplicate = %d, want journaled result replay", len(bus.publishes))
	}
	if got, want := bus.publishes[1].ID(), resultEvent.ID(); got != want {
		t.Fatalf("duplicate result id = %q, want journaled id %q", got, want)
	}
}

func TestPipelineActivityRequestNonIdempotentFailureDoesNotRetry(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
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
	ctx := testAuthorActivityContext(context.Background())
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
	failure, _ := payload["failure"].(map[string]any)
	detail, _ := failure["detail"].(map[string]any)
	if failure["class"] != string(runtimefailures.ClassOutcomeUncertain) || detail["code"] != "activity_provider_outcome_uncertain" {
		t.Fatalf("failure = %#v, want uncertain transport outcome", failure)
	}
	if got := failure["message"]; got != "Operation outcome is uncertain (activity_provider_outcome_uncertain)." {
		t.Fatalf("failure message = %#v, want registry-owned operation wording", got)
	}
	if got := failure["remediation"]; got != "Reconcile the authoritative operation state before retrying (activity_provider_outcome_uncertain)." {
		t.Fatalf("failure remediation = %#v, want registry-owned operation wording", got)
	}
	rec, ok, err := store.LoadActivityAttempt(ctx, activityRequestEventID(intent))
	if err != nil {
		t.Fatalf("LoadActivityAttempt: %v", err)
	}
	if !ok || rec.Status != ActivityAttemptStatusUncertain {
		t.Fatalf("journal status = (%v, %q), want uncertain", ok, rec.Status)
	}
}

func TestPipelineActivityRequestStartedJournalBlocksProviderRedispatchWithoutTerminalizing(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
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
	if !ok || rec.Status != ActivityAttemptStatusStarted {
		t.Fatalf("journal status = (%v, %q), want started", ok, rec.Status)
	}
	if len(bus.publishes) != 0 {
		t.Fatalf("publishes = %#v, want started duplicate to wait for the in-flight owner", bus.publishes)
	}
}

func TestLoopActivityClaimCommitAcknowledgmentLossReconcilesWithoutDispatch(t *testing.T) {
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES (?, 'running')`, runID); err != nil {
		t.Fatal(err)
	}
	runner := &activityCommitAckLossRunner{db: db}
	store := NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db, runner)
	activation, entityID := seedLoopActivityInstance(t, store, ctx, "review")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}))
	defer server.Close()
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{
		"provider_write": {
			HandlerType: "http", EffectClass: string(runtimecontracts.ActivityEffectClassNonIdempotentWrite),
			OutputSchema: runtimecontracts.ToolInputSchema{Type: "object"}, HTTP: &runtimecontracts.HTTPToolSpec{Method: "POST", URL: server.URL},
		},
	}})
	bus := &recordingPipelineBus{}
	pc := NewPipelineCoordinatorWithOptions(bus, db, PipelineCoordinatorOptions{Module: staticSemanticWorkflowModule{source: source}, WorkflowStore: store})
	intent := testNonIdempotentActivityIntent(runID, uuid.NewString(), entityID)
	intent.Generation, intent.LoopStage = activation.Generation(), activation.CurrentStage
	runner.failNext.Store(true)
	if err := (pipelineActivityDispatcher{coordinator: pc}).executeActivityIntent(ctx, intent); err != nil {
		t.Fatalf("execute after commit acknowledgment loss: %v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("provider calls = %d, want no blind dispatch", calls.Load())
	}
	record, ok, err := store.LoadActivityAttempt(ctx, activityRequestEventID(intent))
	if err != nil || !ok || record.Status != ActivityAttemptStatusStarted || !record.Generation.Equal(intent.Generation) {
		t.Fatalf("reconciled claim = %#v found=%v err=%v", record, ok, err)
	}
	if len(bus.publishes) != 0 {
		t.Fatalf("published results after indeterminate claim: %#v", bus.publishes)
	}
}

type activityCommitAckLossRunner struct {
	db       *sql.DB
	failNext atomic.Bool
}

func (r *activityCommitAckLossRunner) RunRuntimeMutationContext(ctx context.Context, fn func(context.Context) error) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	postCommit := make([]func(), 0, 2)
	txctx := withPipelinePostCommitActions(WithPipelineSQLTxContext(ctx, tx), &postCommit)
	storyctx, err := runtimeauthoractivity.Begin(txctx, tx, runtimeauthoractivity.DialectSQLite)
	if err != nil {
		return err
	}
	if err := fn(storyctx); err != nil {
		return err
	}
	if err := runtimeauthoractivity.Finalize(storyctx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	flushPipelinePostCommitActions(postCommit)
	if r.failNext.Swap(false) {
		return errors.New("simulated commit acknowledgment loss")
	}
	return nil
}

func TestPipelineActivityRequestConcurrentDuplicatePreservesOriginalTerminalResult(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	runID := uuid.NewString()
	sourceEventID := uuid.NewString()
	entityID := uuid.NewString()
	var calls atomic.Int32
	var released atomic.Bool
	providerEntered := make(chan struct{})
	releaseProvider := make(chan struct{})
	release := func() {
		if released.CompareAndSwap(false, true) {
			close(releaseProvider)
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			close(providerEntered)
		}
		<-releaseProvider
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}))
	defer server.Close()
	defer release()

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
	request, err := activityRequestEmitIntent(intent)
	if err != nil {
		t.Fatalf("activityRequestEmitIntent: %v", err)
	}
	firstDone := make(chan error, 1)
	go func() {
		_, err := pc.handleEventResult(ctx, request.Event)
		firstDone <- err
	}()
	<-providerEntered

	if _, err := pc.handleEventResult(ctx, request.Event); err != nil {
		t.Fatalf("duplicate handleEventResult: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("provider calls after duplicate = %d, want exactly one in-flight dispatch", got)
	}
	rec, ok, err := store.LoadActivityAttempt(ctx, activityRequestEventID(intent))
	if err != nil {
		t.Fatalf("LoadActivityAttempt before release: %v", err)
	}
	if !ok || rec.Status != ActivityAttemptStatusStarted {
		t.Fatalf("journal before release = (%v, %q), want started", ok, rec.Status)
	}
	if len(bus.publishes) != 0 {
		t.Fatalf("publishes before release = %#v, want duplicate to avoid terminalizing active attempt", bus.publishes)
	}

	release()
	if err := <-firstDone; err != nil {
		t.Fatalf("first handleEventResult: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("provider calls after success = %d, want exactly one dispatch", got)
	}
	rec, ok, err = store.LoadActivityAttempt(ctx, activityRequestEventID(intent))
	if err != nil {
		t.Fatalf("LoadActivityAttempt after release: %v", err)
	}
	if !ok || rec.Status != ActivityAttemptStatusSucceeded {
		t.Fatalf("journal after release = (%v, %q), want succeeded", ok, rec.Status)
	}
	if len(bus.publishes) != 1 || bus.publishes[0].Type() != events.EventType(intent.SuccessEvent) {
		t.Fatalf("publishes after release = %#v, want one success event", bus.publishes)
	}
}

func TestPipelineActivityRequestMissingCredentialFailsAfterClaimBeforeDispatch(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
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
	if rec, ok, err := store.LoadActivityAttempt(ctx, activityRequestEventID(intent)); err != nil {
		t.Fatalf("LoadActivityAttempt: %v", err)
	} else if !ok || rec.Status != ActivityAttemptStatusFailed {
		t.Fatalf("activity attempt = %#v found=%v, want journaled failed claim", rec, ok)
	}
	if len(bus.publishes) != 1 || bus.publishes[0].Type() != events.EventType(intent.FailureEvent) {
		t.Fatalf("publishes = %#v, want one failure event", bus.publishes)
	}
	failure := requireActivityEventFailure(t, bus.publishes[0])
	if failure.Class != runtimefailures.ClassAuthenticationNeeded || failure.Detail.Code != "activity_credential_required" {
		t.Fatalf("failure = %s/%s, want authentication_required/activity_credential_required", failure.Class, failure.Detail.Code)
	}
}

func TestPipelineActivityRequestTelegramConnectorMissingTokenFailsAfterClaimBeforeDispatch(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
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
			"telegram.send_message": testTelegramConnectorTool(server.URL),
		},
	})
	bus := &recordingPipelineBus{}
	db, store := newSQLiteActivityJournalStore(t, ctx)
	seedActivityRun(t, db, true, runID)
	pc := NewPipelineCoordinatorWithOptions(bus, db, PipelineCoordinatorOptions{
		Module:        staticSemanticWorkflowModule{source: source},
		WorkflowStore: store,
		Credentials:   testActivityCredentialStore(t, "", ""),
	})
	intent := testNonIdempotentActivityIntent(runID, sourceEventID, entityID)
	intent.Tool = "telegram.send_message"
	intent.ActivityID = "telegram_send_message"
	intent.Input = mustActivityInput(map[string]any{"chat_id": "42", "text": "reply"})
	intent.SuccessEvent = "telegram.send_message.succeeded"
	intent.FailureEvent = "telegram.send_message.failed"
	intent = intent.Normalized()
	request, err := activityRequestEmitIntent(intent)
	if err != nil {
		t.Fatalf("activityRequestEmitIntent: %v", err)
	}
	if _, err := pc.handleEventResult(ctx, request.Event); err != nil {
		t.Fatalf("handleEventResult: %v", err)
	}
	if calls != 0 {
		t.Fatalf("Telegram calls = %d, want missing token to fail before dispatch", calls)
	}
	if rec, ok, err := store.LoadActivityAttempt(ctx, activityRequestEventID(intent)); err != nil {
		t.Fatalf("LoadActivityAttempt: %v", err)
	} else if !ok || rec.Status != ActivityAttemptStatusFailed {
		t.Fatalf("activity attempt = %#v found=%v, want journaled failed claim", rec, ok)
	}
	if len(bus.publishes) != 1 || bus.publishes[0].Type() != events.EventType(intent.FailureEvent) {
		t.Fatalf("publishes = %#v, want one failure event", bus.publishes)
	}
	failure := requireActivityEventFailure(t, bus.publishes[0])
	if failure.Class != runtimefailures.ClassAuthenticationNeeded || failure.Detail.Code != "activity_credential_required" {
		t.Fatalf("failure = %s/%s, want authentication_required/activity_credential_required", failure.Class, failure.Detail.Code)
	}
	if strings.Contains(string(bus.publishes[0].Payload()), "provider-secret") {
		t.Fatalf("failure payload leaked credential: %s", bus.publishes[0].Payload())
	}
}

func requireActivityEventFailure(t testing.TB, evt events.Event) runtimefailures.Envelope {
	t.Helper()
	var payload struct {
		Failure json.RawMessage `json:"failure"`
	}
	if err := json.Unmarshal(evt.Payload(), &payload); err != nil {
		t.Fatalf("unmarshal activity event payload: %v", err)
	}
	failure, err := runtimefailures.UnmarshalEnvelope(payload.Failure)
	if err != nil {
		t.Fatalf("decode activity failure envelope: %v", err)
	}
	return failure
}

func testActivityIntent(inputURL string) runtimeengine.ActivityIntent {
	return runtimeengine.ActivityIntent{
		ActivityID:    "scanner_source_scrape",
		Tool:          "source_scrape",
		Input:         mustActivityInput(map[string]any{"url": inputURL}),
		EffectClass:   runtimecontracts.ActivityEffectClassReadOnly,
		SuccessEvent:  "research.scanner_source_scrape.succeeded",
		FailureEvent:  "research.scanner_source_scrape.failed",
		EntityID:      identity.NormalizeEntityID("entity-1"),
		NodeID:        identity.NormalizeNodeID("scanner"),
		FlowID:        identity.NormalizeFlowID("research"),
		FlowInstance:  "research/entity-1",
		SourceEventID: "evt-1",
		SourceRunID:   "run-1",
		SourceTaskID:  "task-1",
		ChainDepth:    4,
		Attempt:       1,
		ExecutionMode: executionmode.Live,
	}.Normalized()
}

func mustActivityInput(input map[string]any) semanticvalue.Value {
	value, err := canonicaljson.FromGo(input)
	if err != nil {
		panic(err)
	}
	return value
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
		if err := store.Set(testAuthorActivityContext(context.Background()), key, value); err != nil {
			t.Fatalf("Set credential: %v", err)
		}
	}
	return store
}

func testTelegramConnectorTool(baseURL string) runtimecontracts.ToolSchemaEntry {
	return runtimecontracts.ToolSchemaEntry{
		Category:    "provider_connector",
		Description: "send Telegram messages",
		HandlerType: "http",
		EffectClass: string(runtimecontracts.ActivityEffectClassNonIdempotentWrite),
		Credentials: []string{"telegram_bot_token"},
		InputSchema: runtimecontracts.ToolInputSchema{
			Type: "object",
			Properties: map[string]runtimecontracts.ToolInputSchema{
				"chat_id": {Type: "string"},
				"text":    {Type: "string"},
			},
			Required: []string{"chat_id", "text"},
		},
		OutputSchema: runtimecontracts.ToolInputSchema{
			Type: "object",
		},
		ResponseSuccess: &runtimecontracts.HTTPResponseSuccess{
			Kind: "http_status_2xx",
		},
		HTTP: &runtimecontracts.HTTPToolSpec{
			Method: "POST",
			URL:    strings.TrimRight(baseURL, "/") + "/bot{{credentials.telegram_bot_token}}/sendMessage",
			Body: map[string]any{
				"chat_id": "{{input.chat_id}}",
				"text":    "{{input.text}}",
			},
		},
	}
}

func acceptedTelegramInboundDeliveryEvent(t *testing.T, entityID, runID string) (providertriggers.Delivery, events.Event) {
	t.Helper()
	payload := map[string]any{
		"update_id": float64(123456789),
		"message": map[string]any{
			"message_id": float64(7),
			"chat":       map[string]any{"id": float64(42)},
			"text":       "hello from telegram",
		},
	}
	body := []byte(`{"update_id":123456789,"message":{"message_id":7,"chat":{"id":42},"text":"hello from telegram"}}`)
	req := providertriggers.Request{
		Provider: "telegram",
		Target: providertriggers.Target{
			EntityID:      entityID,
			WebhookSecret: "telegram-secret",
		},
		Body:      body,
		Headers:   make(http.Header),
		Payload:   payload,
		Received:  time.Unix(1710000000, 0).UTC(),
		UserAgent: "telegram-test",
	}
	req.Headers.Set("X-Telegram-Bot-Api-Secret-Token", "telegram-secret")
	catalog, _, err := providertriggers.NewCatalogSnapshotFromPackDirs("0.7.0", []string{filepath.Join("..", "..", "..", "packs", "provider-triggers", "telegram")}, nil)
	if err != nil {
		t.Fatalf("load Telegram provider trigger pack: %v", err)
	}
	plan, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{
		Alias: "telegram-chat", Provider: "telegram", SigningSecret: "webhook_signing.telegram",
	})
	if err != nil {
		t.Fatalf("compile Telegram inbound admission: %v", err)
	}
	delivery, err := plan.Accept(req)
	if err != nil {
		t.Fatalf("Accept Telegram inbound delivery: %v", err)
	}
	if delivery.Events[0].Name != "inbound.telegram" || delivery.ProviderEventID != "123456789" {
		t.Fatalf("delivery = %+v, want inbound.telegram update", delivery)
	}
	raw, err := json.Marshal(delivery.Events[0].Payload)
	if err != nil {
		t.Fatalf("marshal inbound delivery payload: %v", err)
	}
	evt := eventtest.RootIngress(
		uuid.NewSHA1(uuid.NameSpaceURL, []byte("swarm:telegram-inbound:"+delivery.ProviderEventID)).String(),
		delivery.Events[0].Name,
		"telegram",
		"telegram-update-7",
		raw,
		1,
		runID,
		"",
		events.EventEnvelope{
			EntityID: entityID,
			Source: events.RouteIdentity{
				FlowID:   "telegram",
				EntityID: entityID,
			},
		},
		time.Unix(1710000000, 0).UTC(),
	)
	return delivery, evt
}

func telegramInboundChatAndText(t *testing.T, payload map[string]any) (string, string) {
	t.Helper()
	rawPayload, ok := payload["payload"].(map[string]any)
	if !ok {
		t.Fatalf("inbound payload = %#v, want payload object", payload["payload"])
	}
	message, ok := rawPayload["message"].(map[string]any)
	if !ok {
		t.Fatalf("telegram message = %#v, want object", rawPayload["message"])
	}
	chat, ok := message["chat"].(map[string]any)
	if !ok {
		t.Fatalf("telegram chat = %#v, want object", message["chat"])
	}
	chatID := asString(chat["id"])
	text := asString(message["text"])
	if chatID == "" || text == "" {
		t.Fatalf("telegram chat/text = %q/%q, want non-empty", chatID, text)
	}
	return chatID, text
}

type activityRoundTripFunc func(*http.Request) (*http.Response, error)

func (f activityRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type countingActivityCredentialStore struct {
	reads atomic.Int32
}

func (s *countingActivityCredentialStore) Get(context.Context, string) (string, bool, error) {
	s.reads.Add(1)
	return "", false, nil
}

func (*countingActivityCredentialStore) Set(context.Context, string, string) error { return nil }
func (*countingActivityCredentialStore) List(context.Context) ([]string, error)    { return nil, nil }
func (*countingActivityCredentialStore) Delete(context.Context, string) error      { return nil }

func activityTestPlaceholder(sqlite bool, position int) string {
	if sqlite {
		return "?"
	}
	return fmt.Sprintf("$%d::uuid", position)
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
			execution_mode TEXT NOT NULL CHECK (execution_mode IN ('live', 'mock')),
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
			failure TEXT,
			input_hash TEXT NOT NULL,
			loop_generation TEXT NOT NULL DEFAULT '{}',
			loop_stage TEXT,
			reply_context_id TEXT,
			started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
			completed_at TEXT,
			updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE author_activity_order (
			singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
			last_sequence BIGINT NOT NULL CHECK (last_sequence >= 0)
		)`,
		`CREATE TABLE author_activity_occurrences (
			occurrence_id TEXT PRIMARY KEY,
			sequence BIGINT NOT NULL UNIQUE CHECK (sequence > 0),
			kind TEXT NOT NULL,
			version INTEGER NOT NULL CHECK (version = 2),
			transition TEXT NOT NULL,
			source_owner TEXT NOT NULL,
			source_identity TEXT NOT NULL,
			dedup_key TEXT NOT NULL UNIQUE,
			run_id TEXT,
			entity_id TEXT,
			agent_id TEXT,
			flow_id TEXT,
			scope_kind TEXT NOT NULL,
			runtime_instance_id TEXT,
			bundle_hash TEXT,
			author_safe_summary TEXT,
			projection TEXT NOT NULL DEFAULT '{}',
			failure TEXT,
			occurred_at TIMESTAMP NOT NULL
		)`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("create sqlite activity journal schema: %v", err)
		}
	}
}
