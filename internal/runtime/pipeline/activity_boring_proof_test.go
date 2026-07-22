package pipeline

import (
	"context"
	"database/sql"
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
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestActivityBoringProofHandAuthoredFlowDispatchesOutsideTransactionAndReusesRecordedResult(t *testing.T) {
	for _, tc := range activityBoringStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			var fixture activityBoringFixture
			entityID := uuid.NewString()
			inputURL := "https://example.com/source"
			sourceEvent := newActivityBoringSourceEvent(entityID, testPipelineRunID, inputURL)
			expected := activityBoringExpectedIntentForSourceEvent(sourceEvent, inputURL)

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if exists, locked := activityBoringEntityLockState(fixture.pc, entityID); !exists || locked {
					t.Errorf("activity HTTP call entity lock exists=%v locked=%v, want existing unlocked lock", exists, locked)
				}
				calls.Add(1)
				_ = json.NewEncoder(w).Encode(map[string]any{"title": "Example Source"})
			}))
			defer server.Close()

			fixture = newActivityBoringFullFlowFixture(t, tc.kind, server.URL, true)
			seedActivityBoringSourceFlow(t, fixture, tc.kind, sourceEvent)
			fixture.pc.SetTestWorkflowNodeHandlerStartHook(func(ctx context.Context, nodeID string, evt events.Event) error {
				if nodeID != "scanner" || evt.ID() != sourceEvent.ID() {
					return nil
				}
				if got := calls.Load(); got != 0 {
					return errors.New("activity HTTP call happened before the node handler started")
				}
				assertActivityBoringEventCount(t, fixture.db, tc.kind, activityRequestEventID(expected), 0)
				assertActivityBoringEventCount(t, fixture.db, tc.kind, activityResultEventID(expected, expected.SuccessEvent), 0)
				return nil
			})
			fixture.bus.beforeActivityRequestHandle = func(ctx context.Context, evt events.Event) error {
				if _, ok := PipelineSQLTxFromContext(ctx); ok {
					return errors.New("activity request delivered while handler SQL transaction is still active")
				}
				if exists, locked := activityBoringEntityLockState(fixture.pc, entityID); !exists || locked {
					return fmt.Errorf("activity request delivered with entity lock exists=%v locked=%v, want existing unlocked lock", exists, locked)
				}
				if got := calls.Load(); got != 0 {
					return fmt.Errorf("activity HTTP call count before activity request delivery = %d, want 0", got)
				}
				assertActivityBoringEventCount(t, fixture.db, tc.kind, activityRequestEventID(expected), 1)
				assertActivityBoringEventCount(t, fixture.db, tc.kind, activityResultEventID(expected, expected.SuccessEvent), 0)
				return nil
			}

			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), sourceEvent.RunID())
			ctx = withWorkflowNodeDeliveryRoute(ctx, activityBoringNodeRoute(sourceEvent, "scanner"))
			handled, err := fixture.pc.handleEventResult(ctx, sourceEvent)
			if err != nil {
				t.Fatalf("hand-authored source handleEventResult: %v", err)
			}
			if !handled {
				t.Fatal("hand-authored source handleEventResult handled = false, want true")
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("server calls after supported flow = %d, want 1", got)
			}
			assertActivityBoringEventCount(t, fixture.db, tc.kind, activityRequestEventID(expected), 1)
			assertActivityBoringEventCount(t, fixture.db, tc.kind, activityResultEventID(expected, expected.SuccessEvent), 1)
			assertActivityBoringDeliveryStatus(t, fixture.db, tc.kind, sourceEvent.ID(), "scanner", "delivered")

			request := fixture.bus.outboxIntent(0)
			handled, err = fixture.pc.handleEventResult(ctx, request.Event)
			if err != nil {
				t.Fatalf("duplicate supported activity request handleEventResult: %v", err)
			}
			if !handled {
				t.Fatal("duplicate supported activity request handled = false, want true")
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("server calls after duplicate supported activity request = %d, want recorded result reuse", got)
			}
			assertActivityBoringRuntimeLogAction(t, fixture.bus, "result_reused")
		})
	}
}

func TestActivityBoringProofHandAuthoredFlowCrashAfterRequestBeforeResultCompletesOncePostgres(t *testing.T) {
	var calls atomic.Int32
	entityID := uuid.NewString()
	inputURL := "https://example.com/source"
	sourceEvent := newActivityBoringSourceEvent(entityID, testPipelineRunID, inputURL)
	expected := activityBoringExpectedIntentForSourceEvent(sourceEvent, inputURL)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"title": "Recovered Source"})
	}))
	defer server.Close()

	fixture := newActivityBoringFullFlowFixture(t, activityBoringStorePostgres, server.URL, false)
	seedActivityBoringSourceFlow(t, fixture, activityBoringStorePostgres, sourceEvent)
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), sourceEvent.RunID())
	ctx = withWorkflowNodeDeliveryRoute(ctx, activityBoringNodeRoute(sourceEvent, "scanner"))
	handled, err := fixture.pc.handleEventResult(ctx, sourceEvent)
	if err != nil {
		t.Fatalf("source handleEventResult before crash: %v", err)
	}
	if !handled {
		t.Fatal("source handleEventResult before crash handled = false, want true")
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("server calls after supported request persistence = %d, want 0 before crash recovery", got)
	}
	assertActivityBoringEventCount(t, fixture.db, activityBoringStorePostgres, activityRequestEventID(expected), 1)
	assertActivityBoringEventCount(t, fixture.db, activityBoringStorePostgres, activityResultEventID(expected, expected.SuccessEvent), 0)

	restarted := newActivityBoringFullFlowCoordinator(t, fixture.db, activityBoringStorePostgres, server.URL, true)
	request := loadActivityBoringPersistedEvent(t, fixture.db, activityBoringStorePostgres, activityRequestEventID(expected))
	assertActivityBoringPersistedRequestMatches(t, request, expected)
	handled, err = restarted.pc.handleEventResult(ctx, request)
	if err != nil {
		t.Fatalf("restart supported activity request handleEventResult: %v", err)
	}
	if !handled {
		t.Fatal("restart supported activity request handled = false, want true")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("server calls after restart completion = %d, want 1", got)
	}
	assertActivityBoringEventCount(t, fixture.db, activityBoringStorePostgres, activityResultEventID(expected, expected.SuccessEvent), 1)

	handled, err = restarted.pc.handleEventResult(ctx, request)
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

func TestActivityBoringProofHandAuthoredReadOnlyForkReexecuteUsesForkLocalIdentity(t *testing.T) {
	for _, tc := range activityBoringStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			entityID := uuid.NewString()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				_ = json.NewEncoder(w).Encode(map[string]any{"title": "Example Source"})
			}))
			defer server.Close()

			fixture := newActivityBoringFullFlowFixture(t, tc.kind, server.URL, true)
			sourceEvent := newActivityBoringSourceEvent(entityID, uuid.NewString(), "https://example.com/source")
			forkEvent := newActivityBoringSourceEvent(entityID, uuid.NewString(), "https://example.com/source")
			sourceExpected := activityBoringExpectedIntentForSourceEvent(sourceEvent, "https://example.com/source")
			forkExpected := activityBoringExpectedIntentForSourceEvent(forkEvent, "https://example.com/source")
			if activityRequestEventID(sourceExpected) == activityRequestEventID(forkExpected) {
				t.Fatal("fork-local hand-authored request identity did not change")
			}

			for _, evt := range []events.Event{sourceEvent, forkEvent} {
				seedActivityBoringSourceFlow(t, fixture, tc.kind, evt)
				ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), evt.RunID())
				ctx = withWorkflowNodeDeliveryRoute(ctx, activityBoringNodeRoute(evt, "scanner"))
				handled, err := fixture.pc.handleEventResult(ctx, evt)
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
			if activityResultEventID(sourceExpected, sourceExpected.SuccessEvent) == activityResultEventID(forkExpected, forkExpected.SuccessEvent) {
				t.Fatal("fork-local hand-authored result identity did not change")
			}
		})
	}
}

func TestActivityBoringProofDuplicateRequestReusesRecordedReadResult(t *testing.T) {
	for _, tc := range activityBoringStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				_ = json.NewEncoder(w).Encode(map[string]any{"title": "Example Source"})
			}))
			defer server.Close()

			fixture := newActivityBoringFixture(t, tc.kind, server.URL)
			intent := newActivityBoringIntent("https://example.com/source", testPipelineRunID)
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), intent.SourceRunID)
			request, err := activityRequestEmitIntent(intent)
			if err != nil {
				t.Fatalf("activityRequestEmitIntent: %v", err)
			}

			handled, err := fixture.pc.handleEventResult(ctx, request.Event)
			if err != nil {
				t.Fatalf("first handleEventResult: %v", err)
			}
			if !handled {
				t.Fatal("first handleEventResult handled = false, want true")
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("server calls after first request = %d, want 1", got)
			}
			assertActivityBoringEventCount(t, fixture.db, tc.kind, activityResultEventID(intent, intent.SuccessEvent), 1)

			handled, err = fixture.pc.handleEventResult(ctx, request.Event)
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

func TestActivityBoringProofCrashAfterIntentBeforeResultCompletesOncePostgres(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"title": "Recovered Source"})
	}))
	defer server.Close()

	fixture := newActivityBoringFixture(t, activityBoringStorePostgres, server.URL)
	intent := newActivityBoringIntent("https://example.com/source", testPipelineRunID)
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), intent.SourceRunID)
	writer := pipelineActivityIntentWriter{coordinator: fixture.pc}
	if err := writer.WriteActivityIntents(ctx, []runtimeengine.ActivityIntent{intent}); err != nil {
		t.Fatalf("WriteActivityIntents: %v", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("server calls after request persistence = %d, want 0 before post-crash restart", got)
	}
	requestID := activityRequestEventID(intent)
	resultID := activityResultEventID(intent, intent.SuccessEvent)
	assertActivityBoringEventCount(t, fixture.db, activityBoringStorePostgres, requestID, 1)
	assertActivityBoringEventCount(t, fixture.db, activityBoringStorePostgres, resultID, 0)
	assertActivityBoringRuntimeLogAction(t, fixture.bus, "intent_persisted")

	restarted := newActivityBoringCoordinator(t, fixture.db, activityBoringStorePostgres, server.URL)
	request := loadActivityBoringPersistedEvent(t, fixture.db, activityBoringStorePostgres, requestID)
	assertActivityBoringPersistedRequestMatches(t, request, intent)
	handled, err := restarted.pc.handleEventResult(ctx, request)
	if err != nil {
		t.Fatalf("restart handleEventResult: %v", err)
	}
	if !handled {
		t.Fatal("restart handleEventResult handled = false, want true")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("server calls after restart completion = %d, want 1", got)
	}
	assertActivityBoringEventCount(t, fixture.db, activityBoringStorePostgres, resultID, 1)

	handled, err = restarted.pc.handleEventResult(ctx, request)
	if err != nil {
		t.Fatalf("duplicate post-restart handleEventResult: %v", err)
	}
	if !handled {
		t.Fatal("duplicate post-restart handled = false, want true")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("server calls after duplicate completed request = %d, want recorded result reuse", got)
	}
}

func TestActivityBoringProofReadOnlyForkReexecuteUsesForkLocalRequestIdentity(t *testing.T) {
	for _, tc := range activityBoringStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				_ = json.NewEncoder(w).Encode(map[string]any{"title": "Example Source"})
			}))
			defer server.Close()

			fixture := newActivityBoringFixture(t, tc.kind, server.URL)
			sourceRunID := uuid.NewString()
			forkRunID := uuid.NewString()
			sourceIntent := newActivityBoringIntent("https://example.com/source", sourceRunID)
			forkIntent := sourceIntent
			forkIntent.SourceRunID = forkRunID
			forkIntent.ForkPolicy = runtimecontracts.ActivityForkReexecuteRead
			if activityRequestEventID(sourceIntent) == activityRequestEventID(forkIntent) {
				t.Fatal("fork-local request identity did not change")
			}

			for _, intent := range []runtimeengine.ActivityIntent{sourceIntent, forkIntent} {
				request, err := activityRequestEmitIntent(intent)
				if err != nil {
					t.Fatalf("activityRequestEmitIntent: %v", err)
				}
				ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), intent.SourceRunID)
				handled, err := fixture.pc.handleEventResult(ctx, request.Event)
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
			if activityResultEventID(sourceIntent, sourceIntent.SuccessEvent) == activityResultEventID(forkIntent, forkIntent.SuccessEvent) {
				t.Fatal("fork-local result identity did not change")
			}
		})
	}
}

func TestActivityBoringProofRetryIsBoundedAndTraced(t *testing.T) {
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

			fixture := newActivityBoringFixture(t, tc.kind, server.URL)
			intent := newActivityBoringIntent("https://example.com/source", testPipelineRunID)
			intent.RetryMaxAttempts = 3
			intent.RetryBackoff = "none"
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), intent.SourceRunID)
			request, err := activityRequestEmitIntent(intent)
			if err != nil {
				t.Fatalf("activityRequestEmitIntent: %v", err)
			}

			handled, err := fixture.pc.handleEventResult(ctx, request.Event)
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
	pc := NewPipelineCoordinatorWithOptions(bus, nil, PipelineCoordinatorOptions{
		Module: staticSemanticWorkflowModule{source: activityBoringSource(server.URL)},
	})
	intent := newActivityBoringIntent("https://example.com/source", testPipelineRunID)
	request, err := activityRequestEmitIntent(intent)
	if err != nil {
		t.Fatalf("activityRequestEmitIntent: %v", err)
	}
	handled, err := pc.handleEventResult(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), intent.SourceRunID), request.Event)
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

type activityBoringStoreKind string

const (
	activityBoringStoreSQLite   activityBoringStoreKind = "sqlite"
	activityBoringStorePostgres activityBoringStoreKind = "postgres"
)

type activityBoringStoreCase struct {
	name string
	kind activityBoringStoreKind
}

func activityBoringStoreCases() []activityBoringStoreCase {
	return []activityBoringStoreCase{
		{name: "sqlite", kind: activityBoringStoreSQLite},
		{name: "postgres", kind: activityBoringStorePostgres},
	}
}

type activityBoringFixture struct {
	db  *sql.DB
	bus *persistingActivityBoringBus
	pc  *PipelineCoordinator
}

func newActivityBoringFixture(t *testing.T, kind activityBoringStoreKind, serverURL string) activityBoringFixture {
	t.Helper()
	switch kind {
	case activityBoringStoreSQLite:
		db := newSQLiteWorkflowInstanceStoreTestDB(t)
		bus := &persistingActivityBoringBus{appendEvent: func(ctx context.Context, evt events.Event) error {
			return appendActivityBoringEvent(ctx, db, kind, evt)
		}}
		pc := NewPipelineCoordinatorWithOptions(bus, db, PipelineCoordinatorOptions{
			Module:        staticSemanticWorkflowModule{source: activityBoringSource(serverURL)},
			WorkflowStore: NewSQLiteWorkflowInstanceStore(db),
		})
		return activityBoringFixture{db: db, bus: bus, pc: pc}
	case activityBoringStorePostgres:
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		return newActivityBoringCoordinator(t, db, kind, serverURL)
	default:
		t.Fatalf("unknown activity boring store kind %q", kind)
		return activityBoringFixture{}
	}
}

func newActivityBoringCoordinator(t *testing.T, db *sql.DB, kind activityBoringStoreKind, serverURL string) activityBoringFixture {
	t.Helper()
	bus := &persistingActivityBoringBus{appendEvent: func(ctx context.Context, evt events.Event) error {
		return appendActivityBoringEvent(ctx, db, kind, evt)
	}}
	store := NewWorkflowInstanceStore(db)
	if kind == activityBoringStoreSQLite {
		store = newSQLiteWorkflowInstanceStoreForTest(t, db)
	}
	deliveryStore := newPipelineTestDeliveryOwnerForDB(t, db)
	pc := NewPipelineCoordinatorWithOptions(bus, db, PipelineCoordinatorOptions{
		Module:        staticSemanticWorkflowModule{source: activityBoringSource(serverURL)},
		WorkflowStore: store,
		DeliveryStore: deliveryStore,
	})
	return activityBoringFixture{db: db, bus: bus, pc: pc}
}

func newActivityBoringFullFlowFixture(t *testing.T, kind activityBoringStoreKind, serverURL string, handleActivityRequests bool) activityBoringFixture {
	t.Helper()
	switch kind {
	case activityBoringStoreSQLite:
		db := newSQLiteWorkflowInstanceStoreTestDB(t)
		return newActivityBoringFullFlowCoordinator(t, db, kind, serverURL, handleActivityRequests)
	case activityBoringStorePostgres:
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		return newActivityBoringFullFlowCoordinator(t, db, kind, serverURL, handleActivityRequests)
	default:
		t.Fatalf("unknown activity boring store kind %q", kind)
		return activityBoringFixture{}
	}
}

func newActivityBoringFullFlowCoordinator(t *testing.T, db *sql.DB, kind activityBoringStoreKind, serverURL string, handleActivityRequests bool) activityBoringFixture {
	t.Helper()
	bus := &persistingActivityBoringBus{
		appendEvent: func(ctx context.Context, evt events.Event) error {
			return appendActivityBoringEvent(ctx, db, kind, evt)
		},
		handleActivityRequests: handleActivityRequests,
	}
	store := NewWorkflowInstanceStore(db)
	if kind == activityBoringStoreSQLite {
		store = newSQLiteWorkflowInstanceStoreForTest(t, db)
	}
	bundle := activityBoringFullFlowBundle(serverURL)
	deliveryStore := newPipelineTestDeliveryOwnerForDB(t, db)
	pc := NewPipelineCoordinatorWithOptions(bus, db, PipelineCoordinatorOptions{
		Module: &previewWorkflowModule{
			bundle:   bundle,
			workflow: NewWorkflowDefinition("research", []WorkflowStage{{Name: "pending"}}, nil),
			workflowNodes: []WorkflowNode{{
				ID:            "scanner",
				Subscriptions: []events.EventType{"source.requested"},
				Produces:      []events.EventType{"research.scanner_source_requested_source_scrape.succeeded", "research.scanner_source_requested_source_scrape.failed"},
				ExecutionType: runtimecontracts.SystemNodeExecutionType,
				Policies: map[string]WorkflowEventPolicy{
					"source.requested": {Consume: true},
				},
			}},
		},
		WorkflowStore: store,
		DeliveryStore: deliveryStore,
	})
	bus.coordinator = pc
	return activityBoringFixture{db: db, bus: bus, pc: pc}
}

func activityBoringSource(serverURL string) semanticview.Source {
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"source_scrape": {
				HandlerType: "http",
				EffectClass: string(runtimecontracts.ActivityEffectClassReadOnly),
				OutputSchema: runtimecontracts.ToolInputSchema{
					Type: "object",
				},
				HTTP: &runtimecontracts.HTTPToolSpec{
					Method: "GET",
					URL:    strings.TrimRight(serverURL, "/") + "?url={{input.url}}",
				},
			},
		},
	})
}

func activityBoringFullFlowSource(serverURL string) semanticview.Source {
	return semanticview.Wrap(activityBoringFullFlowBundle(serverURL))
}

func activityBoringFullFlowBundle(serverURL string) *runtimecontracts.WorkflowContractBundle {
	handler := runtimecontracts.SystemNodeEventHandler{
		Activity: runtimecontracts.ActivitySpec{
			Tool: "source_scrape",
			Input: map[string]runtimecontracts.ExpressionValue{
				"url": runtimecontracts.CELExpression("payload.url"),
			},
		},
	}
	node := runtimecontracts.SystemNodeContract{
		ID:            "scanner",
		ExecutionType: runtimecontracts.SystemNodeExecutionType,
		SubscribesTo:  []string{"source.requested"},
		EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
			"source.requested": handler,
		},
	}
	flow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{
			ID:         "research",
			PackageKey: "activity-boring-proof",
			Dir:        "flows/research",
		},
		Schema: runtimecontracts.FlowSchemaDocument{
			Name:         "research",
			InitialState: "pending",
			States:       []string{"pending"},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"scanner": node,
		},
		Path: "research",
	}
	return &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:         "research",
			Version:      "v-test",
			InitialStage: "pending",
			FlowInitial:  map[string]string{"research": "pending"},
			FlowStates:   map[string][]string{"research": []string{"pending"}},
			EventOwners: map[string][]string{
				"source.requested": {"scanner"},
			},
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"scanner": {
					"source.requested": handler,
				},
			},
			EffectiveNodes: map[string]runtimecontracts.SystemNodeEffectiveSemantics{
				"scanner": {
					ID:                   "scanner",
					ExecutionType:        runtimecontracts.SystemNodeExecutionType,
					RuntimeSubscriptions: []string{"source.requested"},
					Produces:             []string{"research.scanner_source_requested_source_scrape.succeeded", "research.scanner_source_requested_source_scrape.failed"},
				},
			},
		},
		FlowTree: runtimecontracts.FlowTree{
			Root:   &flow,
			ByPath: map[string]*runtimecontracts.FlowContractView{"research": &flow},
			ByID:   map[string]*runtimecontracts.FlowContractView{"research": &flow},
		},
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"source_scrape": {
				HandlerType: "http",
				EffectClass: string(runtimecontracts.ActivityEffectClassReadOnly),
				InputSchema: runtimecontracts.ToolInputSchema{
					Type:     "object",
					Required: []string{"url"},
					Properties: map[string]runtimecontracts.ToolInputSchema{
						"url": {Type: "string"},
					},
				},
				OutputSchema: runtimecontracts.ToolInputSchema{
					Type: "object",
					Properties: map[string]runtimecontracts.ToolInputSchema{
						"title": {Type: "string"},
					},
				},
				HTTP: &runtimecontracts.HTTPToolSpec{
					Method: "GET",
					URL:    strings.TrimRight(serverURL, "/") + "?url={{input.url}}",
				},
			},
		},
	}
}

func newActivityBoringSourceEvent(entityID, runID, inputURL string) events.Event {
	if strings.TrimSpace(runID) == "" {
		runID = testPipelineRunID
	}
	return eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("source.requested"),
		"source",
		"task-activity-boring-proof",
		mustJSON(map[string]any{"url": inputURL}),
		2,
		runID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
		time.Now().UTC(),
	)
}

func activityBoringExpectedIntentForSourceEvent(evt events.Event, inputURL string) runtimeengine.ActivityIntent {
	flowInstance := "research/" + evt.EntityID()
	site := runtimecontracts.ActivitySite{
		FlowID:          "research",
		NodeID:          "scanner",
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
		NodeID:           identity.NormalizeNodeID("scanner"),
		FlowID:           identity.NormalizeFlowID("research"),
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

func newActivityBoringIntent(inputURL, runID string) runtimeengine.ActivityIntent {
	if strings.TrimSpace(runID) == "" {
		runID = testPipelineRunID
	}
	entityID := uuid.NewString()
	return runtimeengine.ActivityIntent{
		ActivityID:       "scanner_source_scrape",
		Tool:             "source_scrape",
		Input:            mustActivityInput(map[string]any{"url": inputURL}),
		EffectClass:      runtimecontracts.ActivityEffectClassReadOnly,
		SuccessEvent:     "research.scanner_source_scrape.succeeded",
		FailureEvent:     "research.scanner_source_scrape.failed",
		RetryMaxAttempts: 3,
		RetryBackoff:     "none",
		ForkPolicy:       runtimecontracts.ActivityForkReexecuteRead,
		EntityID:         identity.NormalizeEntityID(entityID),
		NodeID:           identity.NormalizeNodeID("scanner"),
		FlowID:           identity.NormalizeFlowID("research"),
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

type persistingActivityBoringBus struct {
	recordingPipelineBus
	appendEvent                 func(context.Context, events.Event) error
	coordinator                 *PipelineCoordinator
	handleActivityRequests      bool
	beforeActivityRequestHandle func(context.Context, events.Event) error
}

type persistingActivityBoringOutbox struct {
	bus *persistingActivityBoringBus
}

type persistingActivityBoringDispatcher struct {
	bus *persistingActivityBoringBus
}

func (b *persistingActivityBoringBus) Publish(ctx context.Context, evt events.Event) error {
	if b.appendEvent != nil {
		if err := b.appendEvent(ctx, evt); err != nil {
			return err
		}
	}
	return b.recordingPipelineBus.Publish(ctx, evt)
}

func (b *persistingActivityBoringBus) EngineOutbox() runtimeengine.OutboxWriter {
	return persistingActivityBoringOutbox{bus: b}
}

func (b *persistingActivityBoringBus) EngineDispatcher() runtimeengine.PostCommitDispatcher {
	return persistingActivityBoringDispatcher{bus: b}
}

func (o persistingActivityBoringOutbox) WriteOutbox(ctx context.Context, intents []runtimeengine.EmitIntent) error {
	if o.bus == nil {
		return nil
	}
	if o.bus.outboxErr != nil {
		return o.bus.outboxErr
	}
	for _, intent := range intents {
		if o.bus.appendEvent != nil {
			if err := o.bus.appendEvent(ctx, intent.Event); err != nil {
				return err
			}
		}
	}
	o.bus.mu.Lock()
	defer o.bus.mu.Unlock()
	o.bus.outboxIntents = append(o.bus.outboxIntents, cloneEmitIntents(intents)...)
	return nil
}

func (d persistingActivityBoringDispatcher) DispatchPostCommit(ctx context.Context, intents []runtimeengine.EmitIntent) error {
	if d.bus == nil {
		return nil
	}
	if CollectPipelineEmitIntents(ctx, intents) {
		return nil
	}
	for _, intent := range intents {
		if len(intent.Recipients) > 0 {
			if err := d.bus.recordingPipelineBus.PublishDirect(ctx, intent.Event, intent.Recipients); err != nil {
				return err
			}
			continue
		}
		if err := d.bus.recordingPipelineBus.Publish(ctx, intent.Event); err != nil {
			return err
		}
		if d.bus.handleActivityRequests && intent.Event.Type() == activityRequestEventType && d.bus.coordinator != nil {
			if d.bus.beforeActivityRequestHandle != nil {
				if err := d.bus.beforeActivityRequestHandle(ctx, intent.Event); err != nil {
					return err
				}
			}
			handled, err := d.bus.coordinator.handleEventResult(ctx, intent.Event)
			if err != nil {
				return err
			}
			if !handled {
				return fmt.Errorf("post-commit activity request %s was not handled", intent.Event.ID())
			}
		}
	}
	return nil
}

func appendActivityBoringEvent(ctx context.Context, db *sql.DB, kind activityBoringStoreKind, evt events.Event) error {
	if db == nil {
		return nil
	}
	execer := interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
		QueryRowContext(context.Context, string, ...any) *sql.Row
	}(db)
	if tx, ok := PipelineSQLTxFromContext(ctx); ok {
		execer = tx
	}
	runID := strings.TrimSpace(evt.RunID())
	if runID == "" {
		runID = strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx))
	}
	if runID == "" {
		runID = testPipelineRunID
	}
	createdAt := evt.CreatedAt()
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	var dialect runtimeauthoractivity.Dialect
	switch kind {
	case activityBoringStoreSQLite:
		dialect = runtimeauthoractivity.DialectSQLite
		if _, err := execer.ExecContext(ctx, `
			INSERT OR IGNORE INTO runs (run_id, status, started_at)
			VALUES (?, 'running', ?)
		`, runID, createdAt); err != nil {
			return err
		}
	case activityBoringStorePostgres:
		dialect = runtimeauthoractivity.DialectPostgres
		if _, err := execer.ExecContext(ctx, `
			INSERT INTO runs (run_id, status, started_at)
			VALUES ($1::uuid, 'running', $2)
			ON CONFLICT (run_id) DO NOTHING
		`, runID, createdAt); err != nil {
			return err
		}
	default:
		return nil
	}
	return eventfixture.Insert(ctx, execer, dialect, evt)
}

func seedActivityBoringSourceFlow(t *testing.T, fixture activityBoringFixture, kind activityBoringStoreKind, evt events.Event) {
	t.Helper()
	if fixture.pc == nil || fixture.db == nil {
		t.Fatal("activity boring source flow fixture requires coordinator and db")
	}
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), evt.RunID())
	if err := appendActivityBoringEvent(ctx, fixture.db, kind, evt); err != nil {
		t.Fatalf("seed activity boring source event: %v", err)
	}
	owner := configurePipelineTestDeliveryOwner(t, fixture.pc)
	if err := owner.commitInitial(ctx, evt, activityBoringNodeRoute(evt, "scanner")); err != nil {
		t.Fatalf("seed activity boring node delivery: %v", err)
	}
	entityID := strings.TrimSpace(evt.EntityID())
	if entityID == "" {
		t.Fatal("activity boring source event requires entity id")
	}
	if err := fixture.pc.workflowStore.Upsert(ctx, WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "research",
		WorkflowVersion: "v-test",
		CurrentState:    "pending",
		Metadata: map[string]any{
			"entity_id":   entityID,
			"flow_path":   "research/" + entityID,
			"instance_id": entityID,
		},
	}); err != nil {
		t.Fatalf("seed activity boring workflow instance: %v", err)
	}
}

func activityBoringNodeRoute(evt events.Event, nodeID string) events.DeliveryRoute {
	return events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   strings.TrimSpace(nodeID),
		Target:         events.RouteIdentity{EntityID: strings.TrimSpace(evt.EntityID())},
	}
}

func assertActivityBoringEventCount(t *testing.T, db *sql.DB, kind activityBoringStoreKind, eventID string, want int) {
	t.Helper()
	var (
		got int
		err error
	)
	switch kind {
	case activityBoringStoreSQLite:
		err = db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_id = ?`, eventID).Scan(&got)
	case activityBoringStorePostgres:
		err = db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_id = $1::uuid`, eventID).Scan(&got)
	default:
		t.Fatalf("unknown store kind %q", kind)
	}
	if err != nil {
		t.Fatalf("count event %s: %v", eventID, err)
	}
	if got != want {
		t.Fatalf("event %s count = %d, want %d", eventID, got, want)
	}
}

func loadActivityBoringPersistedEvent(t *testing.T, db *sql.DB, kind activityBoringStoreKind, eventID string) events.Event {
	t.Helper()
	if kind != activityBoringStorePostgres {
		t.Fatalf("persisted crash readback proof requires Postgres, got %s", kind)
	}
	event, err := eventfixture.Load(context.Background(), db, runtimeauthoractivity.DialectPostgres, eventID)
	if err != nil {
		t.Fatalf("load persisted event %s: %v", eventID, err)
	}
	return event
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

func assertActivityBoringDeliveryStatus(t *testing.T, db *sql.DB, kind activityBoringStoreKind, eventID, nodeID, want string) {
	t.Helper()
	var (
		got sql.NullString
		err error
	)
	switch kind {
	case activityBoringStoreSQLite:
		err = db.QueryRow(`
			SELECT status
			FROM event_deliveries
			WHERE event_id = ? AND subscriber_type = 'node' AND subscriber_id = ?
		`, eventID, nodeID).Scan(&got)
	case activityBoringStorePostgres:
		err = db.QueryRow(`
			SELECT status
			FROM event_deliveries
			WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2
		`, eventID, nodeID).Scan(&got)
	default:
		t.Fatalf("unknown store kind %q", kind)
	}
	if err != nil {
		t.Fatalf("read delivery status for %s/%s: %v", eventID, nodeID, err)
	}
	if !got.Valid || got.String != want {
		t.Fatalf("delivery status for %s/%s = %q valid=%v, want %q", eventID, nodeID, got.String, got.Valid, want)
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

func assertActivityBoringRuntimeLogAction(t *testing.T, bus *persistingActivityBoringBus, action string) {
	t.Helper()
	if got := countActivityBoringRuntimeLogAction(bus, action); got == 0 {
		t.Fatalf("runtime log action %q missing; logs=%#v", action, bus.runtimeLogEntries())
	}
}

func assertActivityBoringRuntimeLogActionCount(t *testing.T, bus *persistingActivityBoringBus, action string, want int) {
	t.Helper()
	if got := countActivityBoringRuntimeLogAction(bus, action); got != want {
		t.Fatalf("runtime log action %q count = %d, want %d; logs=%#v", action, got, want, bus.runtimeLogEntries())
	}
}

func countActivityBoringRuntimeLogAction(bus *persistingActivityBoringBus, action string) int {
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
