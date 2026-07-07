package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

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
			ctx := runtimecorrelation.WithRunID(context.Background(), intent.SourceRunID)
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
	ctx := runtimecorrelation.WithRunID(context.Background(), intent.SourceRunID)
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
	request := fixture.bus.outboxIntent(0)
	handled, err := restarted.pc.handleEventResult(ctx, request.Event)
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

	handled, err = restarted.pc.handleEventResult(ctx, request.Event)
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
				ctx := runtimecorrelation.WithRunID(context.Background(), intent.SourceRunID)
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
			ctx := runtimecorrelation.WithRunID(context.Background(), intent.SourceRunID)
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
	handled, err := pc.handleEventResult(runtimecorrelation.WithRunID(context.Background(), intent.SourceRunID), request.Event)
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
		store = NewSQLiteWorkflowInstanceStore(db)
	}
	pc := NewPipelineCoordinatorWithOptions(bus, db, PipelineCoordinatorOptions{
		Module:        staticSemanticWorkflowModule{source: activityBoringSource(serverURL)},
		WorkflowStore: store,
	})
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

func newActivityBoringIntent(inputURL, runID string) runtimeengine.ActivityIntent {
	if strings.TrimSpace(runID) == "" {
		runID = testPipelineRunID
	}
	return runtimeengine.ActivityIntent{
		ActivityID:       "scanner_source_scrape",
		Tool:             "source_scrape",
		Input:            map[string]any{"url": inputURL},
		EffectClass:      runtimecontracts.ActivityEffectClassReadOnly,
		SuccessEvent:     "research.scanner_source_scrape.succeeded",
		FailureEvent:     "research.scanner_source_scrape.failed",
		RetryMaxAttempts: 3,
		RetryBackoff:     "none",
		ForkPolicy:       runtimecontracts.ActivityForkReexecuteRead,
		EntityID:         identity.NormalizeEntityID(uuid.NewString()),
		NodeID:           identity.NormalizeNodeID("scanner"),
		FlowID:           identity.NormalizeFlowID("research"),
		HandlerEventKey:  "source.requested",
		SourceEventID:    uuid.NewString(),
		SourceRunID:      runID,
		SourceTaskID:     "task-activity-boring-proof",
		ChainDepth:       4,
		Attempt:          1,
	}.Normalized()
}

type persistingActivityBoringBus struct {
	recordingPipelineBus
	appendEvent func(context.Context, events.Event) error
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
			if err := d.bus.PublishDirect(ctx, intent.Event, intent.Recipients); err != nil {
				return err
			}
			continue
		}
		if err := d.bus.Publish(ctx, intent.Event); err != nil {
			return err
		}
	}
	return nil
}

func appendActivityBoringEvent(ctx context.Context, db *sql.DB, kind activityBoringStoreKind, evt events.Event) error {
	if db == nil {
		return nil
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
	payload := strings.TrimSpace(string(evt.Payload()))
	if payload == "" {
		payload = "{}"
	}
	scope := strings.TrimSpace(string(evt.Scope()))
	if scope == "" {
		scope = "global"
	}
	flowInstance := strings.TrimSpace(evt.FlowInstance())
	if flowInstance == "" {
		flowInstance = "runtime"
	}
	var entityID any
	if raw := strings.TrimSpace(evt.EntityID()); raw != "" {
		if _, err := uuid.Parse(raw); err == nil {
			entityID = raw
		}
	}
	switch kind {
	case activityBoringStoreSQLite:
		if _, err := db.ExecContext(ctx, `
			INSERT OR IGNORE INTO runs (run_id, status, started_at)
			VALUES (?, 'running', ?)
		`, runID, createdAt); err != nil {
			return err
		}
		_, err := db.ExecContext(ctx, `
			INSERT OR IGNORE INTO events (
				event_id, run_id, event_name, entity_id, flow_instance, scope,
				payload, chain_depth, produced_by_type, created_at
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'platform', ?)
		`, evt.ID(), runID, strings.TrimSpace(string(evt.Type())), entityID, flowInstance, scope, payload, evt.ChainDepth(), createdAt)
		return err
	case activityBoringStorePostgres:
		if _, err := db.ExecContext(ctx, `
			INSERT INTO runs (run_id, status, started_at)
			VALUES ($1::uuid, 'running', $2)
			ON CONFLICT (run_id) DO NOTHING
		`, runID, createdAt); err != nil {
			return err
		}
		_, err := db.ExecContext(ctx, `
			INSERT INTO events (
				event_id, run_id, event_name, entity_id, flow_instance, scope,
				payload, chain_depth, produced_by_type, created_at
			)
			VALUES ($1::uuid, $2::uuid, $3, $4::uuid, $5, $6, $7::jsonb, $8, 'platform', $9)
			ON CONFLICT (event_id) DO NOTHING
		`, evt.ID(), runID, strings.TrimSpace(string(evt.Type())), entityID, flowInstance, scope, payload, evt.ChainDepth(), createdAt)
		return err
	default:
		return nil
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
