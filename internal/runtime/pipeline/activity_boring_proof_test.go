package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactivityidentity "github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func VerifyNativeActivityBoringProofHandAuthoredFlowDispatchesOutsideTransactionAndReusesRecordedResultForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range activityBoringStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			var fixture nativeActivityBoringFixtureForTest
			entityID := uuid.NewString()
			inputURL := "https://example.com/source"
			sourceEvent := nativeActivityBoringSourceEventForTest(entityID, testPipelineRunID, inputURL)
			expected := activityBoringExpectedIntentForSourceEvent(sourceEvent, inputURL)

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if exists, locked := activityBoringEntityLockState(fixture.pc, entityID); !exists || locked {
					t.Errorf("activity HTTP call entity lock exists=%v locked=%v, want existing unlocked lock", exists, locked)
				}
				calls.Add(1)
				_ = json.NewEncoder(w).Encode(map[string]any{"title": "Example Source"})
			}))
			defer server.Close()

			fixture = nativeActivityBoringFixture(t, tc.name, server.URL, true, open)
			ctx := seedNativeActivityBoringSourceFlowForTest(t, fixture, sourceEvent)
			fixture.pc.SetTestWorkflowNodeHandlerStartHook(func(ctx context.Context, nodeID string, evt events.Event) error {
				if nodeID != mustActivityBoringNode("scanner").Key() || evt.ID() != sourceEvent.ID() {
					return nil
				}
				if got := calls.Load(); got != 0 {
					return errors.New("activity HTTP call happened before the node handler started")
				}
				assertNativeActivityBoringEventCountForTest(t, fixture, activityRequestEventID(expected), 0)
				assertNativeActivityBoringEventCountForTest(t, fixture, activityBoringSuccessEventID(expected), 0)
				return nil
			})
			fixture.bus.beforeActivityRequestHandle = func(ctx context.Context, evt events.Event) error {
				if fixture.native.Transactions().Active != 0 {
					return errors.New("activity request delivered while handler SQL transaction is still active")
				}
				if exists, locked := activityBoringEntityLockState(fixture.pc, entityID); !exists || locked {
					return fmt.Errorf("activity request delivered with entity lock exists=%v locked=%v, want existing unlocked lock", exists, locked)
				}
				if got := calls.Load(); got != 0 {
					return fmt.Errorf("activity HTTP call count before activity request delivery = %d, want 0", got)
				}
				assertNativeActivityBoringEventCountForTest(t, fixture, activityRequestEventID(expected), 1)
				assertNativeActivityBoringEventCountForTest(t, fixture, activityBoringSuccessEventID(expected), 0)
				return nil
			}

			ctx = withWorkflowNodeDeliveryRoute(ctx, activityBoringNodeRoute(sourceEvent, "scanner"))
			handled, outcome, err := fixture.pc.handleEventResult(ctx, sourceEvent)
			if err != nil {
				t.Fatalf("hand-authored source handleEventResult: %v", err)
			}
			if disposition, ok := outcome.Disposition(); ok {
				t.Fatalf("hand-authored source disposition = %s/%s failure=%#v", disposition.Kind(), disposition.ReasonCode(), disposition.Failure())
			}
			if !handled {
				t.Fatal("hand-authored source handleEventResult handled = false, want true")
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("server calls after supported flow = %d, want 1", got)
			}
			assertNativeActivityBoringEventCountForTest(t, fixture, activityRequestEventID(expected), 1)
			assertNativeActivityBoringEventCountForTest(t, fixture, activityBoringSuccessEventID(expected), 1)
			assertNativeActivityBoringDeliveryForTest(t, fixture, sourceEvent)

			request := fixture.bus.persistedPublishedEvent(t, fixture.native, ctx, 0)
			handled, _, err = fixture.pc.handleEventResult(ctx, request)
			if err != nil {
				t.Fatalf("duplicate supported activity request handleEventResult: %v", err)
			}
			if !handled {
				t.Fatal("duplicate supported activity request handled = false, want true")
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("server calls after duplicate supported activity request = %d, want recorded result reuse", got)
			}
			found := false
			for _, log := range fixture.bus.runtimeLogEntries() {
				if log.Action == "result_reused" {
					found = true
				}
			}
			if !found {
				t.Fatal("native activity replay omitted result_reused trace")
			}
		})
	}
}

func VerifyNativeActivityBoringProofHandAuthoredFlowCrashAfterRequestBeforeResultCompletesOncePostgresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	var calls atomic.Int32
	entityID := uuid.NewString()
	inputURL := "https://example.com/source"
	sourceEvent := nativeActivityBoringSourceEventForTest(entityID, testPipelineRunID, inputURL)
	expected := activityBoringExpectedIntentForSourceEvent(sourceEvent, inputURL)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"title": "Recovered Source"})
	}))
	defer server.Close()

	fixture := nativeActivityBoringFixture(t, "postgres", server.URL, false, open)
	ctx := seedNativeActivityBoringSourceFlowForTest(t, fixture, sourceEvent)
	ctx = withWorkflowNodeDeliveryRoute(ctx, activityBoringNodeRoute(sourceEvent, "scanner"))
	handled, _, err := fixture.pc.handleEventResult(ctx, sourceEvent)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("source handleEventResult before crash: %v", err)
	}
	if !handled {
		t.Fatal("source handleEventResult before crash handled = false, want true")
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("server calls after supported request persistence = %d, want 0 before crash recovery", got)
	}
	assertNativeActivityBoringEventCountForTest(t, fixture, activityRequestEventID(expected), 1)
	assertNativeActivityBoringEventCountForTest(t, fixture, activityBoringSuccessEventID(expected), 0)
	assertNativeActivityBoringDeliveryForTest(t, fixture, sourceEvent)

	restarted := fixture.reopen(t, true)
	request, err := restarted.native.PublishedEvent(restarted.ctx, activityRequestEventID(expected))
	if err != nil {
		t.Fatal(err)
	}
	ctx = restarted.ctx
	assertActivityBoringPersistedRequestMatches(t, request, expected)
	handled, _, err = restarted.pc.handleEventResult(ctx, request)
	if err != nil {
		t.Fatalf("restart supported activity request handleEventResult: %v", err)
	}
	if !handled {
		t.Fatal("restart supported activity request handled = false, want true")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("server calls after restart completion = %d, want 1", got)
	}
	assertNativeActivityBoringEventCountForTest(t, restarted, activityBoringSuccessEventID(expected), 1)

	handled, _, err = restarted.pc.handleEventResult(ctx, request)
	if err != nil {
		t.Fatalf("duplicate post-restart supported request handleEventResult: %v", err)
	}
	if !handled {
		t.Fatal("duplicate post-restart supported request handled = false, want true")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("server calls after duplicate completed supported request = %d, want recorded result reuse", got)
	}
}

func VerifyNativeActivityBoringProofHandAuthoredReadOnlyForkReexecuteUsesForkLocalIdentityForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range activityBoringStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			entityID := uuid.NewString()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				_ = json.NewEncoder(w).Encode(map[string]any{"title": "Example Source"})
			}))
			defer server.Close()

			fixture := nativeActivityBoringFixture(t, tc.name, server.URL, true, open)
			sourceEvent := nativeActivityBoringSourceEventForTest(entityID, uuid.NewString(), "https://example.com/source")
			forkEvent := nativeActivityBoringSourceEventForTest(entityID, uuid.NewString(), "https://example.com/source")
			sourceExpected := activityBoringExpectedIntentForSourceEvent(sourceEvent, "https://example.com/source")
			forkExpected := activityBoringExpectedIntentForSourceEvent(forkEvent, "https://example.com/source")
			if activityRequestEventID(sourceExpected) == activityRequestEventID(forkExpected) {
				t.Fatal("fork-local hand-authored request identity did not change")
			}

			for _, evt := range []events.Event{sourceEvent, forkEvent} {
				ctx := seedNativeActivityBoringSourceFlowForTest(t, fixture, evt)
				ctx = withWorkflowNodeDeliveryRoute(ctx, activityBoringNodeRoute(evt, "scanner"))
				handled, _, err := fixture.pc.handleEventResult(ctx, evt)
				if err != nil {
					t.Fatalf("hand-authored fork source handleEventResult(%s): %v", evt.RunID(), err)
				}
				if !handled {
					t.Fatalf("hand-authored fork source handleEventResult(%s) handled = false, want true", evt.RunID())
				}
			}
			if got := calls.Load(); got != 2 {
				t.Fatalf("server calls across source+fork hand-authored read_only execution = %d, want declared reexecute_read call per identity", got)
			}
			if activityBoringSuccessEventID(sourceExpected) == activityBoringSuccessEventID(forkExpected) {
				t.Fatal("fork-local hand-authored result identity did not change")
			}
		})
	}
}

func VerifyNativeActivityBoringProofDuplicateRequestReusesRecordedReadResultForTest(t *testing.T, open func(*testing.T, string) WorkflowActivityNativeFixtureForTest) {
	for _, tc := range activityBoringStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				_ = json.NewEncoder(w).Encode(map[string]any{"title": "Example Source"})
			}))
			defer server.Close()

			fixture := newActivityBoringFixture(t, tc.name, server.URL, open)
			intent := newActivityBoringIntent("https://example.com/source", testPipelineRunID)
			ctx := runtimecorrelation.WithRunID(fixture.native.Context, intent.SourceRunID)
			if err := fixture.native.RequireRun(ctx, intent.SourceRunID); err != nil {
				t.Fatal(err)
			}
			request, err := activityRequestEmitIntent(intent)
			if err != nil {
				t.Fatalf("activityRequestEmitIntent: %v", err)
			}
			persistNativeActivityBoringRequestForTest(t, fixture, ctx, intent, request.Event)

			handled, _, err := fixture.pc.handleEventResult(ctx, request.Event)
			if err != nil {
				t.Fatalf("first handleEventResult: %v", err)
			}
			if !handled {
				t.Fatal("first handleEventResult handled = false, want true")
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("server calls after first request = %d, want 1", got)
			}
			if count := fixture.native.EventIDCount(ctx, activityBoringSuccessEventID(intent)); count != 1 {
				t.Fatalf("native activity result count=%d, want 1", count)
			}

			handled, _, err = fixture.pc.handleEventResult(ctx, request.Event)
			if err != nil {
				t.Fatalf("second handleEventResult: %v", err)
			}
			if !handled {
				t.Fatal("second handleEventResult handled = false, want true")
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("server calls after duplicate request = %d, want recorded result reuse", got)
			}
			assertActivityBoringRuntimeLogAction(t, fixture.bus, "result_reused")
		})
	}
}

func VerifyNativeActivityBoringProofReadOnlyForkReexecuteUsesForkLocalRequestIdentityForTest(t *testing.T, open func(*testing.T, string) WorkflowActivityNativeFixtureForTest) {
	for _, tc := range activityBoringStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				_ = json.NewEncoder(w).Encode(map[string]any{"title": "Example Source"})
			}))
			defer server.Close()

			fixture := newActivityBoringFixture(t, tc.name, server.URL, open)
			sourceRunID := uuid.NewString()
			forkRunID := uuid.NewString()
			sourceIntent := newActivityBoringIntent("https://example.com/source", sourceRunID)
			forkIntent := sourceIntent
			forkIntent.SourceRunID = forkRunID
			forkIntent.ForkPolicy = runtimecontracts.ActivityForkReexecuteRead
			if activityRequestEventID(sourceIntent) == activityRequestEventID(forkIntent) {
				t.Fatal("fork-local request identity did not change")
			}
			if activityBoringSuccessEventID(sourceIntent) == activityBoringSuccessEventID(forkIntent) {
				t.Fatal("fork-local result identity did not change")
			}
			// The identity controls above isolate the run dimension. Durable
			// execution needs a fork-local causal event, not a second owner of
			// the source run's immutable event receipt.
			forkIntent.SourceEventID = uuid.NewString()

			for index, intent := range []runtimeengine.ActivityIntent{sourceIntent, forkIntent} {
				request, err := activityRequestEmitIntent(intent)
				if err != nil {
					t.Fatalf("activityRequestEmitIntent: %v", err)
				}
				ctx := runtimecorrelation.WithRunID(fixture.native.Context, intent.SourceRunID)
				if err := fixture.native.RequireRun(ctx, intent.SourceRunID); err != nil {
					t.Fatal(err)
				}
				if index == 0 {
					persistNativeActivityBoringRequestForTest(t, fixture, ctx, intent, request.Event)
				} else {
					sourceCtx := runtimecorrelation.WithRunID(ctx, sourceRunID)
					if _, _, err := fixture.native.Runs.ForkRunSource(sourceCtx, runlifecycle.ForkSourceRequest{
						RunID: sourceRunID, ContinuedAsRunID: forkRunID, EndedAt: time.Now().UTC(),
					}); err != nil {
						t.Fatal(err)
					}
					persistNativeActivityBoringRequestForTest(t, fixture, ctx, intent, request.Event)
				}
				handled, _, err := fixture.pc.handleEventResult(ctx, request.Event)
				if err != nil {
					t.Fatalf("handleEventResult(%s): %v", intent.SourceRunID, err)
				}
				if !handled {
					t.Fatalf("handleEventResult(%s) handled = false, want true", intent.SourceRunID)
				}
			}
			if got := calls.Load(); got != 2 {
				t.Fatalf("server calls across source+fork read_only execution = %d, want declared reexecute_read call per identity", got)
			}
			if activityBoringSuccessEventID(sourceIntent) == activityBoringSuccessEventID(forkIntent) {
				t.Fatal("fork-local result identity did not change")
			}
		})
	}
}

func VerifyNativeActivityBoringProofRetryIsBoundedAndTracedForTest(t *testing.T, open func(*testing.T, string) WorkflowActivityNativeFixtureForTest) {
	for _, tc := range activityBoringStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				call := calls.Add(1)
				if call < 3 {
					http.Error(w, "temporary", http.StatusInternalServerError)
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"title": "Example Source"})
			}))
			defer server.Close()

			fixture := newActivityBoringFixture(t, tc.name, server.URL, open)
			intent := newActivityBoringIntent("https://example.com/source", testPipelineRunID)
			intent.RetryMaxAttempts = 3
			intent.RetryBackoff = "none"
			ctx := runtimecorrelation.WithRunID(fixture.native.Context, intent.SourceRunID)
			if err := fixture.native.RequireRun(ctx, intent.SourceRunID); err != nil {
				t.Fatal(err)
			}
			request, err := activityRequestEmitIntent(intent)
			if err != nil {
				t.Fatalf("activityRequestEmitIntent: %v", err)
			}
			persistNativeActivityBoringRequestForTest(t, fixture, ctx, intent, request.Event)

			handled, _, err := fixture.pc.handleEventResult(ctx, request.Event)
			if err != nil {
				t.Fatalf("handleEventResult: %v", err)
			}
			if !handled {
				t.Fatal("handleEventResult handled = false, want true")
			}
			if got := calls.Load(); got != 3 {
				t.Fatalf("server calls = %d, want bounded retry success on third attempt", got)
			}
			assertActivityBoringRuntimeLogActionCount(t, fixture.bus, "attempt_started", 3)
			assertActivityBoringRuntimeLogAction(t, fixture.bus, "result_published")
		})
	}
}

func TestActivityBoringProofRuntimeLogFailureDoesNotBlockReadOnlyActivity(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"title": "Example Source"})
	}))
	defer server.Close()

	bus := &recordingPipelineBus{runtimeLogErr: errors.New("runtime log unavailable")}
	pc := newPreviewPipelineCoordinatorForTest(bus, PipelineCoordinatorOptions{
		Module: staticSemanticWorkflowModule{source: activityBoringSource(server.URL)},
	})
	intent := newActivityBoringIntent("https://example.com/source", testPipelineRunID)
	request, err := activityRequestEmitIntent(intent)
	if err != nil {
		t.Fatalf("activityRequestEmitIntent: %v", err)
	}
	handled, _, err := pc.handleEventResult(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), intent.SourceRunID), request.Event)
	if err != nil {
		t.Fatalf("handleEventResult with failing runtime log: %v", err)
	}
	if !handled {
		t.Fatal("handleEventResult handled = false, want true")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("server calls = %d, want activity execution despite runtime log failure", got)
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("published events = %d, want generated result despite runtime log failure", got)
	}
}

type activityBoringStoreCase struct {
	name string
}

func activityBoringStoreCases() []activityBoringStoreCase {
	return []activityBoringStoreCase{
		{name: "sqlite"},
		{name: "postgres"},
	}
}

type activityBoringFixture struct {
	native WorkflowActivityNativeFixtureForTest
	bus    *nativePipelineDeliveryBusObservationForTest
	pc     *PipelineCoordinator
}

func newActivityBoringFixture(t *testing.T, backend, serverURL string, open func(*testing.T, string) WorkflowActivityNativeFixtureForTest) activityBoringFixture {
	t.Helper()
	native := open(t, backend)
	// These direct activity-request controls do not execute workflow nodes or
	// acquire node delivery authority. The journal/result contract is separate.
	pc := native.NewCoordinator(nil, PipelineCoordinatorOptions{
		Module: staticSemanticWorkflowModule{source: semanticview.Wrap(activityBoringFullFlowBundle(t, serverURL))},
	})
	bus := observeNativePipelineDeliveryBusForTest(t, pc)
	return activityBoringFixture{native: native, bus: bus, pc: pc}
}

func persistNativeActivityBoringRequestForTest(t *testing.T, fixture activityBoringFixture, ctx context.Context, intent runtimeengine.ActivityIntent, request events.Event) {
	t.Helper()
	entity := intent.EntityID.String()
	source := eventtest.ExistingRunRootIngressWithRoutingSource(intent.SourceEventID, "research/source.requested", "", intent.SourceTaskID,
		[]byte(`{}`), intent.ChainDepth, intent.SourceRunID, events.EventEnvelope{}, testWorkflowRoutingSource("research", intent.FlowInstance, entity), request.CreatedAt())
	fixture.native.Publish(ctx, source)
	fixture.native.Publish(ctx, request)
}

func activityBoringSource(serverURL string) semanticview.Source {
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"source_scrape": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("http")), runtimecontracts.WithToolEffect(runtimecontracts.NormalizeActivityEffectClass(string(runtimecontracts.ActivityEffectClassReadOnly))), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object"))), runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{
				Method: "GET",
				URL:    strings.TrimRight(serverURL, "/") + "?url={{input.url}}",
			})),
		},
	})
}

func activityBoringFullFlowBundle(t *testing.T, serverURL string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	return loadWorkflowTempBundle(t, map[string]string{
		"schema.yaml":            "name: activity-boring-proof\nstages: []\n",
		"research/schema.yaml":   "name: research\ninstance: marker\nstages:\n  pending: {}\n",
		"research/entities.yaml": "test_entity:\n  marker: text\n",
		"research/events.yaml":   "source.requested:\n  url: text\n",
		"research/nodes.yaml": `scanner:
  subscribes_to: [source.requested]
  event_handlers:
    source.requested:
      activity:
        tool: source_scrape
        input:
          url: payload.url
`,
		"tools.yaml": fmt.Sprintf(`source_scrape:
  description: Read a source.
  handler_type: http
  effect_class: read_only
  http:
    method: GET
    url: %q
  input_schema:
    type: object
    required: [url]
    properties:
      url: {type: string}
  output_schema:
    type: object
    properties:
      title: {type: string}
`, strings.TrimRight(serverURL, "/")+"?url={{input.url}}"),
	})
}

func newActivityBoringSourceEvent(entityID, runID, inputURL string) events.Event {
	if strings.TrimSpace(runID) == "" {
		runID = testPipelineRunID
	}
	envelope := events.EnvelopeForFlowInstance(events.EventEnvelope{}, "research/"+strings.TrimSpace(entityID))
	envelope = events.EnvelopeForEntityID(envelope, entityID)
	return eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("source.requested"),
		"source",
		"task-activity-boring-proof",
		mustJSON(map[string]any{"url": inputURL}),
		2,
		runID,
		"",
		envelope,
		time.Now().UTC(),
	)
}

func activityBoringExpectedIntentForSourceEvent(evt events.Event, inputURL string) runtimeengine.ActivityIntent {
	flowInstance := "research/" + evt.EntityID()
	routingSource := mustActivityBoringRoutingSource(evt.EntityID())
	site := runtimecontracts.ActivitySite{
		Node:            mustActivityBoringNode("scanner"),
		HandlerEventKey: "source.requested",
		RuleIndex:       -1,
		Spec: runtimecontracts.ActivitySpec{
			Tool: "source_scrape",
			Input: map[string]runtimecontracts.ExpressionValue{
				"url": runtimecontracts.CELExpression("payload.url"),
			},
		},
	}
	resultEvents := runtimecontracts.ActivityResultEventsForSite(site)
	defaults := runtimecontracts.ActivityRetryDefaultsForEffectClass(runtimecontracts.ActivityEffectClassReadOnly)
	return runtimeengine.ActivityIntent{
		RoutingSource:    routingSource,
		ActivityID:       resultEvents.ActivityID,
		Tool:             "source_scrape",
		Input:            mustActivityInput(map[string]any{"url": inputURL}),
		EffectClass:      runtimecontracts.ActivityEffectClassReadOnly,
		SuccessEvent:     resultEvents.SuccessEvent,
		FailureEvent:     resultEvents.FailureEvent,
		RetryMaxAttempts: defaults.MaxAttempts,
		RetryBackoff:     defaults.Backoff,
		ForkPolicy:       runtimecontracts.ActivityForkPolicyForEffectClass(runtimecontracts.ActivityEffectClassReadOnly),
		EntityID:         identity.NormalizeEntityID(evt.EntityID()),
		Owner:            runtimeactivityidentity.MustNodeOwner(mustActivityBoringNode("scanner")),
		ExecutionFlowID:  identity.NormalizeFlowID("research"),
		FlowInstance:     flowInstance,
		HandlerEventKey:  "source.requested",
		SourceEventID:    evt.ID(),
		SourceRunID:      evt.RunID(),
		SourceTaskID:     evt.TaskID(),
		ParentEventID:    evt.ParentEventID(),
		ChainDepth:       evt.ChainDepth(),
		Attempt:          1,
		ExecutionMode:    evt.ExecutionMode(),
	}.Normalized()
}

// This fixture always executes the research template. Keep its expected
// publication independent of the runtime projection under test.
func activityBoringSuccessEventID(intent runtimeengine.ActivityIntent) string {
	local := strings.TrimPrefix(intent.SuccessEvent, "research/")
	return activityResultEventID(intent, "research/"+intent.EntityID.String()+"/"+local)
}

func newActivityBoringIntent(inputURL, runID string) runtimeengine.ActivityIntent {
	if strings.TrimSpace(runID) == "" {
		runID = testPipelineRunID
	}
	entityID := uuid.NewString()
	resultEvents := runtimecontracts.ActivityResultEventsForSite(runtimecontracts.ActivitySite{
		Node: mustActivityBoringNode("scanner"), HandlerEventKey: "source.requested", RuleIndex: -1,
		Spec: runtimecontracts.ActivitySpec{Tool: "source_scrape"},
	})
	return runtimeengine.ActivityIntent{
		RoutingSource:    mustActivityBoringRoutingSource(entityID),
		ActivityID:       "scanner_source_scrape",
		Tool:             "source_scrape",
		Input:            mustActivityInput(map[string]any{"url": inputURL}),
		EffectClass:      runtimecontracts.ActivityEffectClassReadOnly,
		SuccessEvent:     resultEvents.SuccessEvent,
		FailureEvent:     resultEvents.FailureEvent,
		RetryMaxAttempts: 3,
		RetryBackoff:     "none",
		ForkPolicy:       runtimecontracts.ActivityForkReexecuteRead,
		EntityID:         identity.NormalizeEntityID(entityID),
		Owner:            runtimeactivityidentity.MustNodeOwner(mustActivityBoringNode("scanner")),
		ExecutionFlowID:  identity.NormalizeFlowID("research"),
		FlowInstance:     "research/" + entityID,
		HandlerEventKey:  "source.requested",
		SourceEventID:    uuid.NewString(),
		SourceRunID:      runID,
		SourceTaskID:     "task-activity-boring-proof",
		ChainDepth:       4,
		Attempt:          1,
		ExecutionMode:    executionmode.Live,
	}.Normalized()
}

func mustActivityBoringRoutingSource(entityID string) events.RoutingSource {
	source, err := events.NewConcreteTemplateInstanceRoutingSource(events.RouteIdentity{
		FlowID: "research", FlowInstance: "research/" + strings.TrimSpace(entityID), EntityID: strings.TrimSpace(entityID),
	})
	if err != nil {
		panic(err)
	}
	return source
}

func activityBoringNodeRoute(evt events.Event, nodeID string) events.DeliveryRoute {
	return events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(mustActivityBoringNode(nodeID)), Target: events.MustExistingEntityTarget(events.RouteIdentity{
		FlowID: "research", FlowInstance: "research/" + strings.TrimSpace(evt.EntityID()), EntityID: strings.TrimSpace(evt.EntityID()),
	}),
	}
}

func mustActivityBoringNode(nodeID string) identity.ExecutableNode {
	node, err := identity.AdmitExecutableNodeDeclaration("research", strings.TrimSpace(nodeID))
	if err != nil {
		panic(err)
	}
	return node
}

func assertActivityBoringPersistedRequestMatches(t *testing.T, evt events.Event, want runtimeengine.ActivityIntent) {
	t.Helper()
	want = want.Normalized()
	if evt.Type() != activityRequestEventType {
		t.Fatalf("persisted event type = %s, want %s", evt.Type(), activityRequestEventType)
	}
	if evt.ID() != activityRequestEventID(want) {
		t.Fatalf("persisted request id = %s, want %s", evt.ID(), activityRequestEventID(want))
	}
	if evt.RunID() != want.SourceRunID {
		t.Fatalf("persisted request run_id = %s, want %s", evt.RunID(), want.SourceRunID)
	}
	if evt.EntityID() != want.EntityID.String() {
		t.Fatalf("persisted request entity_id = %s, want %s", evt.EntityID(), want.EntityID.String())
	}
	got, err := activityIntentFromRequestEvent(evt)
	if err != nil {
		t.Fatalf("decode persisted activity request %s: %v", evt.ID(), err)
	}
	if activityRequestEventID(got) != activityRequestEventID(want) {
		t.Fatalf("decoded persisted request identity = %s, want %s", activityRequestEventID(got), activityRequestEventID(want))
	}
	if got.Tool != want.Tool || got.ActivityID != want.ActivityID || got.SuccessEvent != want.SuccessEvent || got.FailureEvent != want.FailureEvent {
		t.Fatalf("decoded persisted request mismatch: got=%#v want=%#v", got, want)
	}
}

func activityBoringEntityLockState(pc *PipelineCoordinator, entityID string) (exists bool, locked bool) {
	if pc == nil {
		return false, false
	}
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return false, false
	}
	pc.entityLockMu.Lock()
	lock := pc.entityLocks[entityID]
	pc.entityLockMu.Unlock()
	if lock == nil {
		return false, false
	}
	if lock.TryLock() {
		lock.Unlock()
		return true, false
	}
	return true, true
}

func assertActivityBoringRuntimeLogAction(t *testing.T, bus interface{ runtimeLogEntries() []RuntimeLogEntry }, action string) {
	t.Helper()
	if got := countActivityBoringRuntimeLogAction(bus, action); got == 0 {
		t.Fatalf("runtime log action %q missing; logs=%#v", action, bus.runtimeLogEntries())
	}
}

func assertActivityBoringRuntimeLogActionCount(t *testing.T, bus interface{ runtimeLogEntries() []RuntimeLogEntry }, action string, want int) {
	t.Helper()
	if got := countActivityBoringRuntimeLogAction(bus, action); got != want {
		t.Fatalf("runtime log action %q count = %d, want %d; logs=%#v", action, got, want, bus.runtimeLogEntries())
	}
}

func countActivityBoringRuntimeLogAction(bus interface{ runtimeLogEntries() []RuntimeLogEntry }, action string) int {
	if bus == nil {
		return 0
	}
	var count int
	for _, entry := range bus.runtimeLogEntries() {
		if entry.Action == action {
			count++
		}
	}
	return count
}
