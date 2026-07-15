package bus_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/core/ownership"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

const eventBusTestRunID = "99999999-9999-9999-9999-999999999999"

func eventBusTestRunContext(t *testing.T, db *sql.DB) context.Context {
	t.Helper()
	ctx := runtimecorrelation.WithRunID(context.Background(), eventBusTestRunID)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES ($1::uuid, 'running')
		ON CONFLICT (run_id) DO NOTHING
	`, eventBusTestRunID); err != nil {
		t.Fatalf("seed event bus test run: %v", err)
	}
	return ctx
}

func TestEventBusRejectsTerminalRunEventsThroughEveryPublishOwnerPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	runID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := pg.MarkRunTerminal(ctx, runID, "cancelled", nil, time.Now().UTC()); err != nil {
		t.Fatalf("mark run cancelled: %v", err)
	}
	assertEventBusTerminalRunRefusal(t, pg, runID, "cancelled", pg.RunEventMutation, func(eventID string) (string, int, int, error) {
		var status string
		var eventCount, deliveryCount int
		if err := db.QueryRowContext(ctx, `SELECT COALESCE(status, '') FROM runs WHERE run_id = $1::uuid`, runID).Scan(&status); err != nil {
			return "", 0, 0, err
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id = $1::uuid`, eventID).Scan(&eventCount); err != nil {
			return "", 0, 0, err
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1::uuid`, eventID).Scan(&deliveryCount); err != nil {
			return "", 0, 0, err
		}
		return status, eventCount, deliveryCount, nil
	})
}

func TestEventBusRejectsTerminalRunEventsThroughEveryPublishOwnerSQLite(t *testing.T) {
	sqliteStore := storetest.StartSQLiteRuntimeStore(t)
	ctx := context.Background()
	runID := uuid.NewString()
	if _, err := sqliteStore.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES (?, 'running', ?)`, runID, time.Now().UTC()); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := sqliteStore.MarkRunTerminal(ctx, runID, "cancelled", nil, time.Now().UTC()); err != nil {
		t.Fatalf("mark run cancelled: %v", err)
	}
	assertEventBusTerminalRunRefusal(t, sqliteStore, runID, "cancelled", sqliteStore.RunEventMutation, func(eventID string) (string, int, int, error) {
		var status string
		var eventCount, deliveryCount int
		if err := sqliteStore.DB.QueryRowContext(ctx, `SELECT COALESCE(status, '') FROM runs WHERE run_id = ?`, runID).Scan(&status); err != nil {
			return "", 0, 0, err
		}
		if err := sqliteStore.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id = ?`, eventID).Scan(&eventCount); err != nil {
			return "", 0, 0, err
		}
		if err := sqliteStore.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id = ?`, eventID).Scan(&deliveryCount); err != nil {
			return "", 0, 0, err
		}
		return status, eventCount, deliveryCount, nil
	})
}

type eventBusExactDuplicateState struct {
	Status        string
	RunEventCount int
	EventRows     int
	DeliveryRows  int
	ReceiptRows   int
}

func TestEventBusExactDuplicateIsOperationNoOpPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	runID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	evt := exactDuplicateEventBusEvent(runID)
	if err := pg.PersistEventWithDeliveriesAndScope(ctx, evt, []string{"agent-original"}, runtimereplayclaim.CommittedReplayScopeDirect); err != nil {
		t.Fatalf("seed event with committed side effects: %v", err)
	}
	assertEventBusExactDuplicateIsOperationNoOp(t, pg, pg.RunEventMutation, evt, func() (eventBusExactDuplicateState, error) {
		var state eventBusExactDuplicateState
		if err := db.QueryRowContext(ctx, `SELECT COALESCE(status, ''), COALESCE(event_count, 0) FROM runs WHERE run_id = $1::uuid`, runID).Scan(&state.Status, &state.RunEventCount); err != nil {
			return state, err
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id = $1::uuid`, evt.ID()).Scan(&state.EventRows); err != nil {
			return state, err
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1::uuid`, evt.ID()).Scan(&state.DeliveryRows); err != nil {
			return state, err
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid`, evt.ID()).Scan(&state.ReceiptRows); err != nil {
			return state, err
		}
		return state, nil
	}, func() error {
		if _, err := db.ExecContext(ctx, `
			UPDATE event_deliveries
			SET status = 'delivered', delivered_at = now()
			WHERE event_id = $1::uuid
		`, evt.ID()); err != nil {
			return err
		}
		_, err := pg.MarkRunTerminal(ctx, runID, "cancelled", nil, time.Now().UTC())
		return err
	})
}

func TestEventBusExactDuplicateIsOperationNoOpSQLite(t *testing.T) {
	sqliteStore := storetest.StartSQLiteRuntimeStore(t)
	ctx := context.Background()
	runID := uuid.NewString()
	if _, err := sqliteStore.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES (?, 'running', ?)`, runID, time.Now().UTC()); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	evt := exactDuplicateEventBusEvent(runID)
	if err := sqliteStore.PersistEventWithDeliveriesAndScope(ctx, evt, []string{"agent-original"}, runtimereplayclaim.CommittedReplayScopeDirect); err != nil {
		t.Fatalf("seed event with committed side effects: %v", err)
	}
	assertEventBusExactDuplicateIsOperationNoOp(t, sqliteStore, sqliteStore.RunEventMutation, evt, func() (eventBusExactDuplicateState, error) {
		var state eventBusExactDuplicateState
		if err := sqliteStore.DB.QueryRowContext(ctx, `SELECT COALESCE(status, ''), COALESCE(event_count, 0) FROM runs WHERE run_id = ?`, runID).Scan(&state.Status, &state.RunEventCount); err != nil {
			return state, err
		}
		if err := sqliteStore.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id = ?`, evt.ID()).Scan(&state.EventRows); err != nil {
			return state, err
		}
		if err := sqliteStore.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id = ?`, evt.ID()).Scan(&state.DeliveryRows); err != nil {
			return state, err
		}
		if err := sqliteStore.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id = ?`, evt.ID()).Scan(&state.ReceiptRows); err != nil {
			return state, err
		}
		return state, nil
	}, func() error {
		if _, err := sqliteStore.DB.ExecContext(ctx, `
			UPDATE event_deliveries
			SET status = 'delivered', delivered_at = ?
			WHERE event_id = ?
		`, time.Now().UTC(), evt.ID()); err != nil {
			return err
		}
		_, err := sqliteStore.MarkRunTerminal(ctx, runID, "cancelled", nil, time.Now().UTC())
		return err
	})
}

func assertEventBusExactDuplicateIsOperationNoOp(
	t *testing.T,
	eventStore runtimebus.EventStore,
	runMutation func(context.Context, func(runtimebus.EventMutation) error) error,
	evt events.Event,
	loadState func() (eventBusExactDuplicateState, error),
	markTerminal func() error,
) {
	t.Helper()
	eb, err := runtimebus.NewEventBusWithOptions(eventStore, runtimebus.EventBusOptions{})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	original := eb.Subscribe("agent-original", evt.Type())
	ch := eb.Subscribe("agent-expansion", evt.Type())
	writers := map[string]func(context.Context, events.Event) error{
		"publish":              eb.Publish,
		"publish_acknowledged": eb.PublishAcknowledged,
		"publish_direct": func(ctx context.Context, event events.Event) error {
			return eb.PublishDirect(ctx, event, []string{"agent-expansion"})
		},
		"publish_in_mutation": func(ctx context.Context, event events.Event) error {
			return runMutation(ctx, func(mutation runtimebus.EventMutation) error {
				return eb.PublishInMutation(mutation.Context(), event)
			})
		},
	}
	assertPhase := func(t *testing.T) {
		t.Helper()
		for name, publish := range writers {
			name, publish := name, publish
			t.Run(name, func(t *testing.T) {
				before, err := loadState()
				if err != nil {
					t.Fatalf("load state before duplicate: %v", err)
				}
				if err := publish(context.Background(), evt); err != nil {
					t.Fatalf("publish exact duplicate: %v", err)
				}
				after, err := loadState()
				if err != nil {
					t.Fatalf("load state after duplicate: %v", err)
				}
				if after != before {
					t.Fatalf("exact duplicate mutated operation state: before=%+v after=%+v", before, after)
				}
				requireNoBusEvent(t, original, "exact duplicate committed-recipient dispatch")
				requireNoBusEvent(t, ch, "exact duplicate process-local dispatch")
			})
		}
	}
	t.Run("active_run", assertPhase)
	if err := markTerminal(); err != nil {
		t.Fatalf("mark terminal: %v", err)
	}
	t.Run("terminal_run", assertPhase)
}

func exactDuplicateEventBusEvent(runID string) events.Event {
	return eventtest.RootIngress(
		uuid.NewString(), events.EventType("custom.exact_duplicate_noop"), "api.v1", "", []byte(`{"attempt":"duplicate"}`),
		0, runID, "", events.EventEnvelope{}, time.Now().UTC().Truncate(time.Microsecond),
	)
}

func TestEventBusRejectsDiagnosticDirectEventsThroughEveryPublishOwnerPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	assertEventBusDiagnosticDirectRefusal(t, pg, pg.RunEventMutation, func(eventID string) (int, error) {
		var count int
		err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_id = $1::uuid`, eventID).Scan(&count)
		return count, err
	})
}

func TestEventBusRejectsDiagnosticDirectEventsThroughEveryPublishOwnerSQLite(t *testing.T) {
	sqliteStore := storetest.StartSQLiteRuntimeStore(t)
	assertEventBusDiagnosticDirectRefusal(t, sqliteStore, sqliteStore.RunEventMutation, func(eventID string) (int, error) {
		var count int
		err := sqliteStore.DB.QueryRow(`SELECT COUNT(*) FROM events WHERE event_id = ?`, eventID).Scan(&count)
		return count, err
	})
}

func assertEventBusDiagnosticDirectRefusal(
	t *testing.T,
	eventStore runtimebus.EventStore,
	runMutation func(context.Context, func(runtimebus.EventMutation) error) error,
	loadEventCount func(string) (int, error),
) {
	t.Helper()
	eb, err := runtimebus.NewEventBusWithOptions(eventStore, runtimebus.EventBusOptions{})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	writers := map[string]func(context.Context, events.Event) error{
		"publish":              eb.Publish,
		"publish_acknowledged": eb.PublishAcknowledged,
		"publish_direct": func(ctx context.Context, evt events.Event) error {
			return eb.PublishDirect(ctx, evt, []string{"agent-1"})
		},
		"publish_in_mutation": func(ctx context.Context, evt events.Event) error {
			return runMutation(ctx, func(mutation runtimebus.EventMutation) error {
				return eb.PublishInMutation(mutation.Context(), evt)
			})
		},
	}
	for _, eventType := range events.DiagnosticDirectEventTypes() {
		eventType := eventType
		t.Run(string(eventType), func(t *testing.T) {
			for name, publish := range writers {
				name, publish := name, publish
				t.Run(name, func(t *testing.T) {
					eventID := uuid.NewString()
					evt := eventtest.DiagnosticDirect(
						eventID, eventType, "runtime", "", []byte(`{"evidence":"typed-owner-only"}`),
						0, "", "", events.EventEnvelope{}, time.Now().UTC(),
					)
					err := publish(context.Background(), evt)
					if err == nil || !strings.Contains(err.Error(), "diagnostic-direct event") {
						t.Fatalf("publish error = %v, want diagnostic-direct refusal", err)
					}
					count, err := loadEventCount(eventID)
					if err != nil {
						t.Fatalf("load event count: %v", err)
					}
					if count != 0 {
						t.Fatalf("persisted diagnostic-direct events = %d, want 0", count)
					}
				})
			}
		})
	}
}

func assertEventBusTerminalRunRefusal(
	t *testing.T,
	eventStore runtimebus.EventStore,
	runID string,
	wantStatus string,
	runMutation func(context.Context, func(runtimebus.EventMutation) error) error,
	loadState func(string) (string, int, int, error),
) {
	t.Helper()
	eb, err := runtimebus.NewEventBusWithOptions(eventStore, runtimebus.EventBusOptions{})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ctx := context.Background()
	writers := map[string]func(context.Context, events.Event) error{
		"publish":              eb.Publish,
		"publish_acknowledged": eb.PublishAcknowledged,
		"publish_direct": func(ctx context.Context, evt events.Event) error {
			return eb.PublishDirect(ctx, evt, []string{"agent-1"})
		},
		"publish_in_mutation": func(ctx context.Context, evt events.Event) error {
			return runMutation(ctx, func(mutation runtimebus.EventMutation) error {
				return eb.PublishInMutation(mutation.Context(), evt)
			})
		},
	}
	for name, publish := range writers {
		name, publish := name, publish
		t.Run(name, func(t *testing.T) {
			eventID := uuid.NewString()
			evt := eventtest.RootIngress(
				eventID,
				events.EventType("custom.terminal_refusal"),
				"api.v1",
				"",
				[]byte(`{"attempt":"terminal"}`),
				0,
				runID,
				"",
				events.EventEnvelope{},
				time.Now().UTC(),
			)
			if err := publish(ctx, evt); !errors.Is(err, storerunlifecycle.ErrRunNotActive) {
				t.Fatalf("publish error = %v, want inactive-run rejection", err)
			}
			status, eventCount, deliveryCount, err := loadState(eventID)
			if err != nil {
				t.Fatalf("load state: %v", err)
			}
			if status != wantStatus || eventCount != 0 || deliveryCount != 0 {
				t.Fatalf("state after refusal = status=%s events=%d deliveries=%d", status, eventCount, deliveryCount)
			}
		})
	}
}

type fixtureWorkflowModule struct {
	source         semanticview.Source
	workflow       *runtimepipeline.WorkflowDefinition
	workflowNodes  []runtimepipeline.WorkflowNode
	guardRegistry  runtimepipeline.GuardRegistry
	actionRegistry runtimepipeline.ActionRegistry
}

func (m *fixtureWorkflowModule) SemanticSource() semanticview.Source {
	return m.source
}

func (m *fixtureWorkflowModule) WorkflowDefinition() *runtimepipeline.WorkflowDefinition {
	return m.workflow
}

func (m *fixtureWorkflowModule) WorkflowNodes() []runtimepipeline.WorkflowNode {
	return append([]runtimepipeline.WorkflowNode(nil), m.workflowNodes...)
}

func (m *fixtureWorkflowModule) GuardRegistry() runtimepipeline.GuardRegistry {
	return m.guardRegistry
}

func (m *fixtureWorkflowModule) ActionRegistry() runtimepipeline.ActionRegistry {
	return m.actionRegistry
}

func newFixtureWorkflowModule(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle) runtimepipeline.WorkflowModule {
	t.Helper()
	source := semanticview.Wrap(bundle)
	workflow, err := runtimepipeline.LoadWorkflowDefinition(source)
	if err != nil {
		t.Fatalf("LoadWorkflowDefinition: %v", err)
	}
	workflowNodes, err := runtimepipeline.LoadWorkflowNodes(source)
	if err != nil {
		t.Fatalf("LoadWorkflowNodes: %v", err)
	}
	return &fixtureWorkflowModule{
		source:         source,
		workflow:       workflow,
		workflowNodes:  workflowNodes,
		guardRegistry:  runtimepipeline.NewContractGuardRegistry(source),
		actionRegistry: runtimepipeline.NewContractActionRegistry(source),
	}
}

type waitInterceptor struct {
	started chan struct{}
	release chan struct{}
}

func (w waitInterceptor) Intercept(_ context.Context, _ events.Event) (bool, []events.Event, error) {
	select {
	case w.started <- struct{}{}:
	default:
	}
	<-w.release
	return true, nil, nil
}

type deferredChainInterceptor struct{}

func (deferredChainInterceptor) Intercept(_ context.Context, evt events.Event) (bool, []events.Event, error) {
	next := ""
	switch evt.Type() {
	case events.EventType("custom.root"):
		next = "custom.middle"
	case events.EventType("custom.middle"):
		next = "custom.leaf"
	case events.EventType("custom.leaf"):
		next = "custom.final"
	default:
		return true, nil, nil
	}
	return false, []events.Event{eventtest.RootIngress("", events.EventType(next), "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, evt.EntityID()), time.Now().UTC())}, nil
}

type singleDeferredInterceptor struct{}

func (singleDeferredInterceptor) Intercept(_ context.Context, evt events.Event) (bool, []events.Event, error) {
	if evt.Type() != events.EventType("custom.root") {
		return true, nil, nil
	}
	return false, []events.Event{eventtest.RootIngress(
		"",
		events.EventType("custom.middle"),
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, evt.EntityID()),
		time.Now().UTC(),
	)}, nil
}

type deliveryContextDeferredInterceptor struct {
	t    *testing.T
	want events.DeliveryContext
}

func (i deliveryContextDeferredInterceptor) Intercept(ctx context.Context, evt events.Event) (bool, []events.Event, error) {
	i.t.Helper()
	switch evt.Type() {
	case events.EventType("custom.root"):
		next := eventtest.RootIngress(
			"",
			events.EventType("custom.middle"),
			"",
			"",
			nil,
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, evt.EntityID()),
			time.Now().UTC(),
		).WithDeliveryContext(i.want)
		return false, []events.Event{next}, nil
	case events.EventType("custom.middle"):
		if got := events.DeliveryContextFromContext(ctx).ReplyContextID(); got != i.want.ReplyContextID() {
			i.t.Fatalf("deferred publish reply context = %q, want %q", got, i.want.ReplyContextID())
		}
	}
	return true, nil, nil
}

type nonTransactionalPersistedBeforeInterceptor struct {
	t     *testing.T
	store *recordingEventStore
}

func (i nonTransactionalPersistedBeforeInterceptor) Intercept(ctx context.Context, evt events.Event) (bool, []events.Event, error) {
	i.t.Helper()
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		i.t.Fatal("non-transactional interceptor unexpectedly ran with sql tx")
	}
	for _, got := range i.store.eventTypes() {
		if got == string(evt.Type()) {
			return true, nil, nil
		}
	}
	i.t.Fatalf("event type %s was not persisted before non-transactional interceptor ran; persisted=%v", evt.Type(), i.store.eventTypes())
	return true, nil, nil
}

type postCommitTxAbsentInterceptor struct {
	t              *testing.T
	store          *store.PostgresStore
	eventID        string
	called         chan struct{}
	wantRecipients []string
	wantScope      runtimereplayclaim.CommittedReplayScope
}

func (i postCommitTxAbsentInterceptor) Intercept(ctx context.Context, _ events.Event) (bool, []events.Event, error) {
	i.t.Helper()
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		i.t.Fatal("post-commit interceptor ran with sql tx still in context")
	}
	ok, err := i.store.EventExists(ctx, i.eventID)
	if err != nil {
		i.t.Fatalf("EventExists(%s): %v", i.eventID, err)
	}
	if !ok {
		i.t.Fatalf("expected event %s to be committed before interceptor ran", i.eventID)
	}
	if i.wantRecipients != nil {
		got, err := i.store.ListEventDeliveryRecipients(ctx, i.eventID)
		if err != nil {
			i.t.Fatalf("ListEventDeliveryRecipients(%s): %v", i.eventID, err)
		}
		assertSortedStringsEqual(i.t, got, i.wantRecipients)
	}
	if i.wantScope != "" {
		got, err := i.store.LoadCommittedReplayScope(ctx, i.eventID)
		if err != nil {
			i.t.Fatalf("LoadCommittedReplayScope(%s): %v", i.eventID, err)
		}
		if got != i.wantScope {
			i.t.Fatalf("committed replay scope = %q, want %q", got, i.wantScope)
		}
	}
	select {
	case i.called <- struct{}{}:
	default:
	}
	return true, nil, nil
}

type postCommitErrorInterceptor struct {
	t       *testing.T
	store   *store.PostgresStore
	eventID string
	err     error
}

func (i postCommitErrorInterceptor) Intercept(ctx context.Context, _ events.Event) (bool, []events.Event, error) {
	i.t.Helper()
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		i.t.Fatal("post-commit error interceptor ran with sql tx still in context")
	}
	ok, err := i.store.EventExists(ctx, i.eventID)
	if err != nil {
		i.t.Fatalf("EventExists(%s): %v", i.eventID, err)
	}
	if !ok {
		i.t.Fatalf("expected event %s to be committed before interceptor error", i.eventID)
	}
	return false, nil, i.err
}

type deferredEventVisibleInterceptor struct {
	t        *testing.T
	store    *store.PostgresStore
	eventID  string
	checkFor events.EventType
}

func (i deferredEventVisibleInterceptor) Intercept(ctx context.Context, evt events.Event) (bool, []events.Event, error) {
	i.t.Helper()
	if evt.Type() == events.EventType("custom.root") {
		return false, []events.Event{
			eventtest.RootIngress(i.eventID, i.checkFor, "", "", []byte(`{"entity_id":"ent-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
		}, nil
	}
	if evt.Type() != i.checkFor {
		return true, nil, nil
	}
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		i.t.Fatal("deferred event interceptor ran with sql tx still in context")
	}
	ok, err := i.store.EventExists(ctx, i.eventID)
	if err != nil {
		i.t.Fatalf("EventExists(%s): %v", i.eventID, err)
	}
	if !ok {
		i.t.Fatalf("expected deferred event %s to be persisted before interceptors ran", i.eventID)
	}
	return true, nil, nil
}

type recordingLoggerHook struct {
	entries []recordedLogEntry
}

type recordedLogEntry struct {
	Action     string
	Detail     any
	Lineage    runtimecorrelation.RuntimeLineage
	HasLineage bool
	SourceFact runtimecorrelation.BundleSourceFact
	HasSource  bool
}

func (h *recordingLoggerHook) Log(ctx context.Context, _ diaglog.Level, _, _, action, _, _, _, _, _ string, _ map[string]string, detail any, _ *runtimefailures.Envelope, _ int) error {
	lineage, ok := runtimecorrelation.RuntimeLineageFromContext(ctx)
	sourceFact, hasSource := runtimecorrelation.BundleSourceFactFromContext(ctx)
	h.entries = append(h.entries, recordedLogEntry{Action: action, Detail: detail, Lineage: lineage, HasLineage: ok, SourceFact: sourceFact, HasSource: hasSource})
	return nil
}

type descriptorAwareEventStore struct {
	mu          sync.Mutex
	descriptors []runtimebus.ActiveAgentDescriptor
	deliveries  []string
	listErr     error
}

type routeSetEventStore struct {
	runtimebus.InMemoryEventStore
	mu     sync.Mutex
	routes map[string][]events.DeliveryRoute
}

type replayCapableAtomicStoreMissingScope struct {
	mu         sync.Mutex
	deliveries []string
}

func newRouteSetEventStore() *routeSetEventStore {
	return &routeSetEventStore{
		routes: map[string][]events.DeliveryRoute{},
	}
}

func (*descriptorAwareEventStore) AppendEvent(context.Context, events.Event) error { return nil }

func (s *descriptorAwareEventStore) InsertEventDeliveries(_ context.Context, _ string, agentIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deliveries = append([]string(nil), agentIDs...)
	return nil
}

func (s *descriptorAwareEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return s.persistedDeliveries(), nil
}

func (s *descriptorAwareEventStore) ListActiveAgentDescriptors(context.Context) ([]runtimebus.ActiveAgentDescriptor, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	return append([]runtimebus.ActiveAgentDescriptor(nil), s.descriptors...), nil
}

func (s *descriptorAwareEventStore) persistedDeliveries() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.deliveries...)
}

func (s *routeSetEventStore) PersistEventWithDeliveryRouteSetAndScope(_ context.Context, evt events.Event, routes []events.DeliveryRoute, _ runtimereplayclaim.CommittedReplayScope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.routes == nil {
		s.routes = map[string][]events.DeliveryRoute{}
	}
	s.routes[evt.ID()] = events.NormalizeDeliveryRoutes(routes)
	return nil
}

func (s *routeSetEventStore) ListEventDeliveryRoutes(_ context.Context, eventID string) ([]events.DeliveryRoute, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]events.DeliveryRoute(nil), s.routes[eventID]...), nil
}

func (*replayCapableAtomicStoreMissingScope) AppendEvent(context.Context, events.Event) error {
	return nil
}

func (s *replayCapableAtomicStoreMissingScope) InsertEventDeliveries(_ context.Context, _ string, agentIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deliveries = append([]string(nil), agentIDs...)
	return nil
}

func (s *replayCapableAtomicStoreMissingScope) PersistEventWithDeliveries(ctx context.Context, evt events.Event, agentIDs []string) error {
	return s.InsertEventDeliveries(ctx, evt.ID(), agentIDs)
}

func (s *replayCapableAtomicStoreMissingScope) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.deliveries...), nil
}

func (*replayCapableAtomicStoreMissingScope) ListEventsMissingPipelineReceipt(context.Context, time.Time, int) ([]events.PersistedReplayEvent, error) {
	return nil, nil
}

func (*replayCapableAtomicStoreMissingScope) ClaimPipelineReplay(context.Context, string) (runtimeownership.Lease, bool, error) {
	return nil, false, nil
}

func (*replayCapableAtomicStoreMissingScope) ClaimPipelinePublication(context.Context, string) (runtimeownership.Lease, bool, error) {
	return sweeperClaimLease{}, true, nil
}

func assertSortedStringsEqual(t *testing.T, got, want []string) {
	t.Helper()
	got = append([]string(nil), got...)
	want = append([]string(nil), want...)
	slices.Sort(got)
	slices.Sort(want)
	if len(got) != len(want) {
		t.Fatalf("strings = %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("strings = %v, want %v", got, want)
		}
	}
}

func seedActiveRuntimeBusAgent(t *testing.T, ctx context.Context, pg *store.PostgresStore, agentID string) {
	t.Helper()
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:            agentID,
			Role:          "observer",
			FlowID:        "global",
			Type:          "stub",
			Model:         "regular",
			ExecutionMode: "live",
			Config:        []byte(`{}`),
		},
		Status:    "active",
		HiredBy:   "test",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent(%s): %v", agentID, err)
	}
}

func loadRunStateForEvent(t *testing.T, ctx context.Context, db *sql.DB, eventID string) (string, string, string) {
	t.Helper()
	var runID, runStatus, triggerEventType string
	if err := db.QueryRowContext(ctx, `
		SELECT
			COALESCE(r.run_id::text, ''),
			COALESCE(r.status, ''),
			COALESCE(r.trigger_event_type, '')
		FROM events e
		INNER JOIN runs r ON r.run_id = e.run_id
		WHERE e.event_id = $1::uuid
	`, eventID).Scan(&runID, &runStatus, &triggerEventType); err != nil {
		t.Fatalf("load run state for %s: %v", eventID, err)
	}
	return runID, runStatus, triggerEventType
}

func countEventDeliveriesForEvent(t *testing.T, ctx context.Context, db *sql.DB, eventID string) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
	`, eventID).Scan(&count); err != nil {
		t.Fatalf("count event deliveries for %s: %v", eventID, err)
	}
	return count
}

func countPipelineReceiptsForEvent(t *testing.T, ctx context.Context, db *sql.DB, eventID string) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, eventID).Scan(&count); err != nil {
		t.Fatalf("count pipeline receipts for %s: %v", eventID, err)
	}
	return count
}

func TestEventBusPublishTransactionalPostCommitReceiptFailureIsRecoverable(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	ctx := eventBusTestRunContext(t, db)
	pg := &store.PostgresStore{DB: db}
	failing := &failStandalonePipelineReceiptOnceStore{
		PostgresStore: pg,
		err:           errors.New("simulated post-commit receipt failure"),
	}
	logger := &recordingLoggerHook{}
	eb, err := runtimebus.NewEventBusWithOptions(failing, runtimebus.EventBusOptions{Logger: logger})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}

	agentID := "agent-post-commit-receipt"
	eventID := "21000000-0000-0000-0000-000000000011"
	seedActiveRuntimeBusAgent(t, ctx, pg, agentID)
	ch := eb.Subscribe(agentID, events.EventType("custom.receipt_failure"))
	defer eb.Unsubscribe(agentID)

	if err := eb.Publish(ctx, eventtest.RootIngress(
		eventID,
		events.EventType("custom.receipt_failure"),
		"api.v1",
		"",
		[]byte(`{"entity_id":"21000000-0000-0000-0000-000000000012"}`),
		0,
		eventBusTestRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "21000000-0000-0000-0000-000000000012"),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish with post-commit receipt failure: %v", err)
	}
	ok, err := pg.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !ok {
		t.Fatalf("event %s was not persisted", eventID)
	}
	if got := countEventDeliveriesForEvent(t, ctx, db, eventID); got != 1 {
		t.Fatalf("event deliveries = %d, want 1", got)
	}
	if got := countPipelineReceiptsForEvent(t, ctx, db, eventID); got != 0 {
		t.Fatalf("pipeline receipts = %d, want 0 after injected failure", got)
	}
	missing, err := pg.ListEventsMissingPipelineReceipt(ctx, time.Now().Add(-time.Hour), 10)
	if err != nil {
		t.Fatalf("ListEventsMissingPipelineReceipt: %v", err)
	}
	if !containsMissingPipelineReceiptEvent(missing, eventID) {
		t.Fatalf("missing pipeline receipt events = %#v, want %s", missing, eventID)
	}
	if !hasRuntimeLogAction(logger.entries, "pipeline_receipt_persist_failed") {
		t.Fatalf("logger entries = %#v, want pipeline_receipt_persist_failed", logger.entries)
	}
	got := requireBusEvent(t, ch, "post-commit receipt failure delivery")
	if got.ID() != eventID {
		t.Fatalf("delivered event = %s, want %s", got.ID(), eventID)
	}
}

func TestEventBusPublishTransactionalPostCommitCompletionFailureIsRecoverable(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	ctx := eventBusTestRunContext(t, db)
	pg := &store.PostgresStore{DB: db}
	failing := &failNormalRunCompletionStore{
		PostgresStore: pg,
		err:           errors.New("simulated normal-run completion failure"),
	}
	logger := &recordingLoggerHook{}
	eb, err := runtimebus.NewEventBusWithOptions(failing, runtimebus.EventBusOptions{Logger: logger})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}

	agentID := "agent-post-commit-completion"
	eventID := "21000000-0000-0000-0000-000000000021"
	seedActiveRuntimeBusAgent(t, ctx, pg, agentID)
	ch := eb.Subscribe(agentID, events.EventType("custom.completion_failure"))
	defer eb.Unsubscribe(agentID)

	if err := eb.Publish(ctx, eventtest.RootIngress(
		eventID,
		events.EventType("custom.completion_failure"),
		"api.v1",
		"",
		[]byte(`{"entity_id":"21000000-0000-0000-0000-000000000022"}`),
		0,
		eventBusTestRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "21000000-0000-0000-0000-000000000022"),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish with post-commit completion failure: %v", err)
	}
	ok, err := pg.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !ok {
		t.Fatalf("event %s was not persisted", eventID)
	}
	if got := countEventDeliveriesForEvent(t, ctx, db, eventID); got != 1 {
		t.Fatalf("event deliveries = %d, want 1", got)
	}
	outcome, failure := loadPipelineReceiptOutcomeAndFailure(t, ctx, db, eventID)
	if outcome != "dead_letter" || failure == nil || failure.Class != runtimefailures.ClassDependencyUnavailable || failure.Detail.Code != "normal_run_completion_failed" {
		t.Fatalf("pipeline receipt outcome=%q failure=%#v, want dead_letter with canonical failure", outcome, failure)
	}
	if !hasRuntimeLogAction(logger.entries, "publish_post_commit_convergence_failed") {
		t.Fatalf("logger entries = %#v, want publish_post_commit_convergence_failed", logger.entries)
	}
	got := requireBusEvent(t, ch, "post-commit completion failure delivery")
	if got.ID() != eventID {
		t.Fatalf("delivered event = %s, want %s", got.ID(), eventID)
	}
}

type failStandalonePipelineReceiptOnceStore struct {
	*store.PostgresStore
	err error
}

func (s *failStandalonePipelineReceiptOnceStore) UpsertPipelineReceipt(ctx context.Context, eventID, status string, failure *runtimefailures.Envelope) error {
	return s.UpsertPipelineReceiptTx(ctx, nil, eventID, status, failure)
}

func (s *failStandalonePipelineReceiptOnceStore) UpsertPipelineReceiptTx(ctx context.Context, tx *sql.Tx, eventID, status string, failure *runtimefailures.Envelope) error {
	if tx == nil && s.err != nil {
		err := s.err
		s.err = nil
		return err
	}
	return s.PostgresStore.UpsertPipelineReceiptTx(ctx, tx, eventID, status, failure)
}

type failNormalRunCompletionStore struct {
	*store.PostgresStore
	err error
}

func (s *failNormalRunCompletionStore) ConvergeNormalRunCompletion(context.Context, string, []string, map[string][]string) error {
	return s.err
}

func loadPipelineReceiptOutcomeAndFailure(t *testing.T, ctx context.Context, db *sql.DB, eventID string) (string, *runtimefailures.Envelope) {
	t.Helper()
	var outcome string
	var raw []byte
	if err := db.QueryRowContext(ctx, `
		SELECT outcome, failure
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, eventID).Scan(&outcome, &raw); err != nil {
		t.Fatalf("load pipeline receipt for %s: %v", eventID, err)
	}
	if len(raw) == 0 {
		return outcome, nil
	}
	failure, err := runtimefailures.UnmarshalEnvelope(raw)
	if err != nil {
		t.Fatalf("decode pipeline receipt failure for %s: %v", eventID, err)
	}
	return outcome, &failure
}

func containsMissingPipelineReceiptEvent(items []events.PersistedReplayEvent, eventID string) bool {
	for _, evt := range items {
		if strings.TrimSpace(evt.Event.ID()) == strings.TrimSpace(eventID) {
			return true
		}
	}
	return false
}

func hasRuntimeLogAction(entries []recordedLogEntry, action string) bool {
	for _, entry := range entries {
		if entry.Action == action {
			return true
		}
	}
	return false
}

func loadAgentDeliveryForEvent(t *testing.T, ctx context.Context, db *sql.DB, eventID, agentID string) (string, string) {
	t.Helper()
	var status, runStatus string
	if err := db.QueryRowContext(ctx, `
		SELECT
			COALESCE(d.status, ''),
			COALESCE(r.status, '')
		FROM event_deliveries d
		INNER JOIN runs r ON r.run_id = d.run_id
		WHERE d.event_id = $1::uuid
		  AND d.subscriber_type = 'agent'
		  AND d.subscriber_id = $2
	`, eventID, agentID).Scan(&status, &runStatus); err != nil {
		t.Fatalf("load delivery state for %s/%s: %v", eventID, agentID, err)
	}
	return status, runStatus
}

func TestEventBusPublish_LogsQueuedDeliveryLifecycleTransition(t *testing.T) {
	logger := &recordingLoggerHook{}
	bus, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{Logger: logger})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ch := bus.Subscribe("agent-1", events.EventType("task.requested"))

	evt := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("task.requested"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Now().UTC(),
	)

	if err := bus.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	_ = requireBusEvent(t, ch, "queued delivery lifecycle transition")

	var found bool
	for _, entry := range logger.entries {
		if entry.Action != "delivery_lifecycle_transition" {
			continue
		}
		detail, ok := entry.Detail.(map[string]any)
		if !ok {
			t.Fatalf("detail type = %T", entry.Detail)
		}
		if detail["delivery_state"] == "queued" && detail["subscriber_id"] == "agent-1" && detail["delivery_reason"] == "matched_agent_subscription" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("queued delivery lifecycle transition not found in logs: %#v", logger.entries)
	}
}

func TestEventBusPublish_AttachesTypedRuntimeDiagnosticLineage(t *testing.T) {
	logger := &recordingLoggerHook{}
	bus, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{Logger: logger})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	eventID := uuid.NewString()
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRuntimeLineage(context.Background(), runtimecorrelation.RuntimeLineage{
		Owner:               "runtime.run_fork.selected_contract_execution.fork_local_runtime_typed_lineage",
		RunID:               runID,
		RowCategory:         runtimecorrelation.RuntimeLineageRowCategoryRuntimeContainer,
		SelectedForkOwner:   "runtime.run_fork.selected_contract_execution.fork_local_runtime_container",
		Classification:      runtimecorrelation.RuntimeLineageClassificationForkLocal,
		SelectedForkContext: true,
	})
	ctx = runtimecorrelation.WithRunID(ctx, runID)

	if err := bus.Publish(ctx, eventtest.RootIngress(
		eventID,
		events.EventType("task.requested"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1"}`),
		0,
		runID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	var published recordedLogEntry
	for _, entry := range logger.entries {
		if entry.Action == "published" {
			published = entry
			break
		}
	}
	if !published.HasLineage {
		t.Fatalf("logger entries = %#v, want typed lineage on published diagnostic", logger.entries)
	}
	if published.Lineage.Owner != "runtime.run_fork.selected_contract_execution.fork_local_runtime_typed_lineage" ||
		published.Lineage.RowCategory != runtimecorrelation.RuntimeLineageRowCategoryDiagnostic ||
		published.Lineage.SubjectEventID != eventID ||
		published.Lineage.ParentEventID != eventID ||
		published.Lineage.Classification != runtimecorrelation.RuntimeLineageClassificationForkLocal {
		t.Fatalf("published diagnostic lineage = %#v", published.Lineage)
	}
}

func TestEventBusPublish_AttachesBundleSourceFactToRuntimeLogs(t *testing.T) {
	logger := &recordingLoggerHook{}
	sourceFact := runtimecorrelation.BundleSourceFact{
		BundleHash:        "bundle-v1:sha256:1111111111111111111111111111111111111111111111111111111111111111",
		BundleSource:      "persisted",
		BundleFingerprint: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
	}
	bus, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		Logger:           logger,
		BundleSourceFact: sourceFact,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ch := bus.Subscribe("agent-1", events.EventType("task.requested"))
	if err := bus.Publish(context.Background(), eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("task.requested"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1"}`),
		0,
		uuid.NewString(),
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	_ = requireBusEvent(t, ch, "bundle source fact delivery")
	for _, entry := range logger.entries {
		if entry.HasSource && entry.SourceFact == sourceFact.Normalized() {
			return
		}
	}
	t.Fatalf("bundle source fact not found in runtime logs: %#v", logger.entries)
}

func TestEventBusPublish_UsesPayloadValidator(t *testing.T) {
	eb, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		PayloadValidator: func(_ context.Context, eventType string, payload []byte) error {
			if strings.TrimSpace(eventType) != "task.completed" {
				t.Fatalf("unexpected event type %q", eventType)
			}
			if string(payload) != `{"ok":true}` {
				t.Fatalf("unexpected payload %s", string(payload))
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.Publish(context.Background(), eventtest.RootIngress("", "task.completed", "", "", []byte(`{"ok":true}`), 0, "", "", events.EventEnvelope{}, time.Time{})); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

func TestEventBusPublish_PayloadValidatorFailureAbortsPublish(t *testing.T) {
	eb, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		PayloadValidator: func(context.Context, string, []byte) error {
			return context.DeadlineExceeded
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	err = eb.Publish(context.Background(), eventtest.RootIngress("", "task.completed", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{}))
	if err == nil || !errors.Is(err, runtimebus.ErrPayloadValidation) {
		t.Fatalf("expected payload validator failure, got %v", err)
	}
}

func TestEventBusPublish_FailsClosedWhenReplayCapableAtomicStoreOmitsCommittedReplayScope(t *testing.T) {
	store := &replayCapableAtomicStoreMissingScope{}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	eb.Subscribe("agent-a", events.EventType("custom.replay.checked"))

	err = eb.Publish(context.Background(), eventtest.RootIngress(uuid.NewString(),
		events.EventType("custom.replay.checked"), "", "", []byte(`{"ok":true}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC()))
	if !errors.Is(err, runtimereplayclaim.ErrMissingCommittedReplayScope) {
		t.Fatalf("Publish error = %v, want missing committed replay scope", err)
	}
}

func TestEventBusPublishDirect_PayloadValidatorFailureAbortsPublish(t *testing.T) {
	eb, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		PayloadValidator: func(context.Context, string, []byte) error {
			return context.DeadlineExceeded
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	err = eb.PublishDirect(context.Background(), eventtest.RootIngress("", "task.completed", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{}), []string{"agent-a"})
	if err == nil || !errors.Is(err, runtimebus.ErrPayloadValidation) {
		t.Fatalf("expected payload validator failure, got %v", err)
	}
}

func TestEventBusCheckDirectRecipients_PayloadValidatorFailureAbortsBeforeRecipientPlanning(t *testing.T) {
	eb, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		PayloadValidator: func(context.Context, string, []byte) error {
			return context.DeadlineExceeded
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}

	status, err := eb.CheckDirectRecipients(context.Background(), eventtest.RootIngress("", "task.completed", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{}), []string{"agent-a"})
	if err == nil || !errors.Is(err, runtimebus.ErrPayloadValidation) {
		t.Fatalf("expected payload validator failure, got %v", err)
	}
	if !slices.Equal(status.Requested, []string{"agent-a"}) {
		t.Fatalf("requested recipients = %#v, want agent-a", status.Requested)
	}
	if len(status.Recipients) != 0 || len(status.Filtered) != 0 || len(status.Missing) != 0 {
		t.Fatalf("recipient status after validation failure = %#v, want no planning result", status)
	}
}

func TestEventBusCheckPublishRecipientPlanReportsSubscribedPublishWithoutDelivery(t *testing.T) {
	eb, err := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := eb.Subscribe("agent-a", events.EventType("task.completed"))

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), eventtest.RootIngress("", "task.completed", "", "", []byte(`{"ok":true}`), 0, "", "", events.EventEnvelope{}, time.Time{}))
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if !slices.Equal(plan.Recipients, []string{"agent-a"}) {
		t.Fatalf("recipients = %#v, want agent-a", plan.Recipients)
	}
	if !slices.Equal(plan.PersistedRecipients, []string{"agent-a"}) {
		t.Fatalf("persisted recipients = %#v, want agent-a", plan.PersistedRecipients)
	}
	if !slices.Equal(plan.SubscriptionRecipients, []string{"agent-a"}) {
		t.Fatalf("subscription recipients = %#v, want agent-a", plan.SubscriptionRecipients)
	}
	select {
	case got := <-ch:
		t.Fatalf("CheckPublishRecipientPlan delivered event %#v", got)
	default:
	}
}

func TestEventBusPublishDirect_PersistsButDoesNotMarkDeliveredBeforeRealFanOut(t *testing.T) {
	store := &descriptorAwareEventStore{
		descriptors: []runtimebus.ActiveAgentDescriptor{
			{AgentID: "agent-a"},
		},
	}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}

	err = eb.PublishDirect(context.Background(), eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("custom.direct"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Now().UTC(),
	),
		[]string{"agent-a"})
	failure, ok := runtimefailures.As(err)
	if !ok || failure.Failure.Class != runtimefailures.ClassTargetUnreachable || failure.Failure.Detail.Code != "authoritative_delivery_incomplete" {
		t.Fatalf("PublishDirect failure = %#v, want authoritative delivery incomplete", failure)
	}
	assertSortedStringsEqual(t, store.persistedDeliveries(), []string{"agent-a"})
}

func TestEventBusPublishDirect_PreservesContextOnPersistedAndLiveDelivery(t *testing.T) {
	store := newRouteSetEventStore()
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	eb.RegisterRuntimeActiveAgentDescriptor(runtimebus.ActiveAgentDescriptor{AgentID: "agent-a"})
	ch := eb.Subscribe("agent-a", events.EventType("custom.direct"))
	eventID := uuid.NewString()
	deliveryContext := events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-v1:direct-context"}}
	ctx := events.WithDeliveryContext(context.Background(), deliveryContext)

	if err := eb.PublishDirect(ctx, eventtest.RootIngress(
		eventID,
		events.EventType("custom.direct"),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EventEnvelope{},
		time.Now().UTC(),
	), []string{"agent-a"}); err != nil {
		t.Fatalf("PublishDirect: %v", err)
	}
	got := requireBusEvent(t, ch, "context-carrying direct delivery")
	if got.DeliveryContext().ReplyContextID() != deliveryContext.ReplyContextID() {
		t.Fatalf("live direct context = %#v, want %#v", got.DeliveryContext(), deliveryContext)
	}
	store.mu.Lock()
	routes := append([]events.DeliveryRoute(nil), store.routes[eventID]...)
	store.mu.Unlock()
	if len(routes) != 1 || routes[0].Context.ReplyContextID() != deliveryContext.ReplyContextID() {
		t.Fatalf("persisted direct routes = %#v, want one context-carrying route", routes)
	}
}

func TestEventBusPublishDirect_FiltersEntityScopedRecipientsByExplicitMetadata(t *testing.T) {
	store := &descriptorAwareEventStore{
		descriptors: []runtimebus.ActiveAgentDescriptor{
			{AgentID: "control-plane"},
			{AgentID: "reviewer-ent-1", EntityID: "ent-1"},
			{AgentID: "reviewer-ent-2", EntityID: "ent-2"},
		},
	}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	controlCh := eb.Subscribe("control-plane")
	matchCh := eb.Subscribe("reviewer-ent-1")
	otherCh := eb.Subscribe("reviewer-ent-2")

	err = eb.PublishDirect(context.Background(), eventtest.RootIngress(
		"",
		events.EventType("custom.direct"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Now().UTC(),
	),
		[]string{"control-plane", "reviewer-ent-1", "reviewer-ent-2", "missing-agent"})
	if err != nil {
		t.Fatalf("PublishDirect: %v", err)
	}

	evt := requireBusEvent(t, controlCh, "direct delivery to control-plane")
	if got := evt.EntityID(); got != "ent-1" {
		t.Fatalf("control event entity_id = %q, want ent-1", got)
	}
	evt = requireBusEvent(t, matchCh, "direct delivery to matching entity-scoped reviewer")
	if got := evt.EntityID(); got != "ent-1" {
		t.Fatalf("matched event entity_id = %q, want ent-1", got)
	}
	requireNoBusEvent(t, otherCh, "direct delivery to filtered entity-scoped reviewer")

	assertSortedStringsEqual(t, store.persistedDeliveries(), []string{"control-plane", "reviewer-ent-1"})
}

func TestEventBusPublish_FiltersEntityScopedRecipientsByExplicitMetadata(t *testing.T) {
	store := &descriptorAwareEventStore{
		descriptors: []runtimebus.ActiveAgentDescriptor{
			{AgentID: "control-plane"},
			{AgentID: "reviewer-ent-1", EntityID: "ent-1"},
			{AgentID: "reviewer-ent-2", EntityID: "ent-2"},
		},
	}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	controlCh := eb.Subscribe("control-plane", events.EventType("custom.trigger"))
	matchCh := eb.Subscribe("reviewer-ent-1", events.EventType("custom.trigger"))
	otherCh := eb.Subscribe("reviewer-ent-2", events.EventType("custom.trigger"))

	if err := eb.Publish(context.Background(), eventtest.RootIngress(
		"",
		events.EventType("custom.trigger"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	evt := requireBusEvent(t, controlCh, "explicit metadata delivery to control-plane")
	if got := evt.EntityID(); got != "ent-1" {
		t.Fatalf("control event entity_id = %q, want ent-1", got)
	}
	evt = requireBusEvent(t, matchCh, "explicit metadata delivery to entity-scoped reviewer")
	if got := evt.EntityID(); got != "ent-1" {
		t.Fatalf("matched event entity_id = %q, want ent-1", got)
	}
	requireNoBusEvent(t, otherCh, "explicit metadata delivery to filtered entity-scoped reviewer")

	assertSortedStringsEqual(t, store.persistedDeliveries(), []string{"control-plane", "reviewer-ent-1"})
}

func TestEventBusPublish_FiltersEntityScopedRecipientsByTypedEnvelopeNotPayload(t *testing.T) {
	store := &descriptorAwareEventStore{
		descriptors: []runtimebus.ActiveAgentDescriptor{
			{AgentID: "reviewer-ent-1", EntityID: "ent-1"},
			{AgentID: "reviewer-ent-2", EntityID: "ent-2"},
		},
	}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	matchCh := eb.Subscribe("reviewer-ent-1", events.EventType("custom.trigger"))
	otherCh := eb.Subscribe("reviewer-ent-2", events.EventType("custom.trigger"))

	err = eb.Publish(context.Background(), eventtest.RootIngress(
		"",
		events.EventType("custom.trigger"),
		"",
		"",
		[]byte(`{"entity_id":"ent-2"}`),
		0,
		"",
		"",
		events.EventEnvelope{EntityID: "ent-1"},
		time.Now().UTC(),
	))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	evt := requireBusEvent(t, matchCh, "typed-envelope delivery to entity-scoped reviewer")
	if got := evt.EntityID(); got != "ent-1" {
		t.Fatalf("matched event entity_id = %q, want ent-1", got)
	}
	requireNoBusEvent(t, otherCh, "typed-envelope delivery to filtered entity-scoped reviewer")
	assertSortedStringsEqual(t, store.persistedDeliveries(), []string{"reviewer-ent-1"})
}

func TestEventBusPublish_DropsRecipientsMissingExplicitDescriptor(t *testing.T) {
	store := &descriptorAwareEventStore{
		descriptors: []runtimebus.ActiveAgentDescriptor{
			{AgentID: "control-plane"},
		},
	}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	controlCh := eb.Subscribe("control-plane", events.EventType("custom.trigger"))
	missingCh := eb.Subscribe("reviewer-ent-1", events.EventType("custom.trigger"))

	if err := eb.Publish(context.Background(), eventtest.RootIngress(
		"",
		events.EventType("custom.trigger"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	_ = requireBusEvent(t, controlCh, "descriptor delivery to control-plane")
	requireNoBusEvent(t, missingCh, "descriptor delivery to missing reviewer")

	assertSortedStringsEqual(t, store.persistedDeliveries(), []string{"control-plane"})
}

func TestEventBusPublish_KeepsInternalSubscribersLiveOnlyUnderDescriptorPlanning(t *testing.T) {
	store := &descriptorAwareEventStore{
		descriptors: []runtimebus.ActiveAgentDescriptor{
			{AgentID: "agent-a"},
		},
	}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	workflowCh := eb.SubscribeInternal("workflow-runtime", events.EventType("custom.trigger"))
	nodeCh := eb.SubscribeInternal("scan-orchestrator", events.EventType("custom.trigger"))
	agentCh := eb.Subscribe("agent-a", events.EventType("custom.trigger"))
	missingCh := eb.Subscribe("agent-missing", events.EventType("custom.trigger"))

	if err := eb.Publish(context.Background(), eventtest.RootIngress(
		"",
		events.EventType("custom.trigger"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	evt := requireBusEvent(t, workflowCh, "internal workflow-runtime descriptor delivery")
	if got := evt.EntityID(); got != "ent-1" {
		t.Fatalf("workflow-runtime event entity_id = %q, want ent-1", got)
	}
	evt = requireBusEvent(t, nodeCh, "internal system-node descriptor delivery")
	if got := evt.EntityID(); got != "ent-1" {
		t.Fatalf("system node event entity_id = %q, want ent-1", got)
	}
	evt = requireBusEvent(t, agentCh, "agent descriptor delivery")
	if got := evt.EntityID(); got != "ent-1" {
		t.Fatalf("agent event entity_id = %q, want ent-1", got)
	}
	requireNoBusEvent(t, missingCh, "descriptor delivery to missing agent")

	assertSortedStringsEqual(t, store.persistedDeliveries(), []string{"agent-a"})
}

func TestEventBusPublishDeferred_UsesCanonicalSubscribedRecipientFiltering(t *testing.T) {
	store := &descriptorAwareEventStore{
		descriptors: []runtimebus.ActiveAgentDescriptor{
			{AgentID: "agent-a"},
			{AgentID: "agent-b", EntityID: "ent-2"},
		},
	}
	eb, err := runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{singleDeferredInterceptor{}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	workflowCh := eb.SubscribeInternal("workflow-runtime", events.EventType("custom.middle"))
	agentCh := eb.Subscribe("agent-a", events.EventType("custom.middle"))
	otherCh := eb.Subscribe("agent-b", events.EventType("custom.middle"))

	if err := eb.Publish(context.Background(), eventtest.RootIngress(
		"",
		events.EventType("custom.root"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	evt := requireBusEvent(t, workflowCh, "deferred delivery to workflow-runtime")
	if got := evt.EntityID(); got != "ent-1" {
		t.Fatalf("workflow-runtime event entity_id = %q, want ent-1", got)
	}
	evt = requireBusEvent(t, agentCh, "deferred delivery to agent")
	if got := evt.EntityID(); got != "ent-1" {
		t.Fatalf("agent event entity_id = %q, want ent-1", got)
	}
	requireNoBusEvent(t, otherCh, "deferred delivery to filtered agent")

	assertSortedStringsEqual(t, store.persistedDeliveries(), []string{"agent-a"})
}

func TestEventBusPublishDeferredRestoresEventDeliveryContext(t *testing.T) {
	store := &recordingEventStore{}
	want := events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-v1:deferred-publish"}}
	eb, err := runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{deliveryContextDeferredInterceptor{t: t, want: want}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.Publish(context.Background(), eventtest.RootIngress(
		"",
		events.EventType("custom.root"),
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := eb.WaitForQuiescence(ctx); err != nil {
		t.Fatalf("WaitForQuiescence: %v", err)
	}
}

func TestEventBusPublish_FailsClosedWhenDescriptorLookupFails(t *testing.T) {
	store := &descriptorAwareEventStore{
		listErr: errors.New("descriptor lookup failed"),
	}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := eb.Subscribe("reviewer-ent-1", events.EventType("custom.trigger"))

	err = eb.Publish(context.Background(), eventtest.RootIngress(
		"",
		events.EventType("custom.trigger"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Now().UTC(),
	))
	if err == nil || !strings.Contains(err.Error(), "descriptor lookup failed") {
		t.Fatalf("Publish error = %v, want descriptor lookup failure", err)
	}
	requireNoBusEvent(t, ch, "descriptor lookup failure delivery")
	if got := store.persistedDeliveries(); len(got) != 0 {
		t.Fatalf("persisted deliveries = %v, want none", got)
	}
}

func TestEventBusWaitForQuiescenceWaitsForPublishCompletion(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	eb, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{waitInterceptor{started: started, release: release}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}

	publishDone := make(chan error, 1)
	go func() {
		publishDone <- eb.Publish(context.Background(), eventtest.RootIngress("", "task.completed", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{}))
	}()

	requireSignalBefore(t, started, 500*time.Millisecond, "interceptor start")

	waitCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := eb.WaitForQuiescence(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitForQuiescence error = %v, want deadline exceeded while publish is blocked", err)
	}

	close(release)
	if err := requireErrorBefore(t, publishDone, 500*time.Millisecond, "publish completion"); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	waitCtx, cancel = context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if err := eb.WaitForQuiescence(waitCtx); err != nil {
		t.Fatalf("WaitForQuiescence after publish completion: %v", err)
	}
}

func TestEventBusPublishAcknowledgedReturnsBeforePostCommitDispatchCompletes(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := eventBusTestRunContext(t, db)
	pg := &store.PostgresStore{DB: db}
	const eventID = "11111111-1111-1111-1111-111111111136"
	const agentID = "agent-acknowledged-publish"
	seedActiveRuntimeBusAgent(t, ctx, pg, agentID)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	eb, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{waitInterceptor{started: started, release: release}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ch := eb.Subscribe(agentID, events.EventType("task.completed"))
	defer eb.Unsubscribe(agentID)

	publishDone := make(chan error, 1)
	go func() {
		publishDone <- eb.PublishAcknowledged(ctx, eventtest.RootIngress(
			eventID,
			events.EventType("task.completed"),
			"api.v1",
			"",
			[]byte(`{"entity_id":"11111111-1111-1111-1111-111111111137"}`),
			0,
			eventBusTestRunID,
			"",
			events.EventEnvelope{EntityID: "11111111-1111-1111-1111-111111111137"},
			time.Now().UTC()))
	}()
	requireSignalBefore(t, started, 5*time.Second, "async post-commit interceptor start")
	if err := requireErrorBefore(t, publishDone, 5*time.Second, "acknowledged publish return before interceptor release"); err != nil {
		t.Fatalf("PublishAcknowledged: %v", err)
	}

	ok, err := pg.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists(%s): %v", eventID, err)
	}
	if !ok {
		t.Fatalf("event %s was not persisted before acknowledged publish returned", eventID)
	}
	gotRecipients, err := pg.ListEventDeliveryRecipients(ctx, eventID)
	if err != nil {
		t.Fatalf("ListEventDeliveryRecipients(%s): %v", eventID, err)
	}
	assertSortedStringsEqual(t, gotRecipients, []string{agentID})
	gotScope, err := pg.LoadCommittedReplayScope(ctx, eventID)
	if err != nil {
		t.Fatalf("LoadCommittedReplayScope(%s): %v", eventID, err)
	}
	if gotScope != runtimereplayclaim.CommittedReplayScopeSubscribed {
		t.Fatalf("committed replay scope = %q, want %q", gotScope, runtimereplayclaim.CommittedReplayScopeSubscribed)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := eb.WaitForQuiescence(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitForQuiescence while async dispatch blocked = %v, want deadline exceeded", err)
	}
	requireNoBusEvent(t, ch, "acknowledged publish before interceptor release")

	close(release)
	deliveryTimer := time.NewTimer(time.Second)
	defer deliveryTimer.Stop()
	var got events.Event
	select {
	case got = <-ch:
	case <-deliveryTimer.C:
		t.Fatal("acknowledged publish delivery: timed out waiting for queued bus event")
	}
	if got.ID() != eventID {
		t.Fatalf("delivered event id = %s, want %s", got.ID(), eventID)
	}
	waitCtx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := eb.WaitForQuiescence(waitCtx); err != nil {
		t.Fatalf("WaitForQuiescence after acknowledged dispatch completion: %v", err)
	}
}

func TestEventBusForegroundPublicationClaimBlocksSiblingReplayOnSQLiteAndPostgres(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) (runtimebus.EventStore, runtimebus.EventStore, *sql.DB, string)
	}{
		{
			name: "sqlite",
			open: func(t *testing.T) (runtimebus.EventStore, runtimebus.EventStore, *sql.DB, string) {
				selected := storetest.StartSQLiteRuntimeStore(t)
				return selected, selected, selected.DB, "?"
			},
		},
		{
			name: "postgres",
			open: func(t *testing.T) (runtimebus.EventStore, runtimebus.EventStore, *sql.DB, string) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				return &store.PostgresStore{DB: db}, &store.PostgresStore{DB: db}, db, "$1::uuid"
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			foregroundStore, siblingStore, db, runPlaceholder := tc.open(t)
			runID, eventID, entityID := uuid.NewString(), uuid.NewString(), uuid.NewString()
			if _, err := db.ExecContext(context.Background(), "INSERT INTO runs (run_id, status) VALUES ("+runPlaceholder+", 'running')", runID); err != nil {
				t.Fatalf("insert run: %v", err)
			}
			started := make(chan struct{}, 1)
			release := make(chan struct{})
			foreground, err := runtimebus.NewEventBusWithOptions(foregroundStore, runtimebus.EventBusOptions{
				Interceptors: []runtimebus.EventInterceptor{waitInterceptor{started: started, release: release}},
			})
			if err != nil {
				t.Fatal(err)
			}
			sibling, err := runtimebus.NewEventBus(siblingStore)
			if err != nil {
				t.Fatal(err)
			}
			publishDone := make(chan error, 1)
			go func() {
				publishDone <- foreground.PublishAcknowledged(context.Background(), eventtest.RootIngress(
					eventID, events.EventType("custom.shared_claim"), "test", "", []byte(`{}`), 0, runID, "",
					events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Now().UTC()))
			}()
			requireSignalBefore(t, started, 5*time.Second, "foreground dispatch start")
			if err := requireErrorBefore(t, publishDone, 5*time.Second, "acknowledged foreground publish"); err != nil {
				t.Fatal(err)
			}

			if got, err := sibling.SweepUndispatched(context.Background(), time.Hour, 10); err != nil || got != 0 {
				t.Fatalf("sibling sweep while foreground owns event = %d, %v; want 0, nil", got, err)
			}
			close(release)
			waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := foreground.WaitForQuiescence(waitCtx); err != nil {
				t.Fatalf("wait foreground completion: %v", err)
			}
			if got, err := sibling.SweepUndispatched(context.Background(), time.Hour, 10); err != nil || got != 0 {
				t.Fatalf("sibling sweep after foreground settlement = %d, %v; want 0, nil", got, err)
			}
		})
	}
}

type publicationClaimBarrierStore struct {
	*store.PostgresStore
	claimed chan<- struct{}
	release <-chan struct{}
}

type replayClaimBarrierStore struct {
	*store.PostgresStore
	eventID string
	claimed chan<- struct{}
	release <-chan struct{}
}

func (s *replayClaimBarrierStore) awaitClaim(ctx context.Context, lease runtimeownership.Lease, claimed bool, err error) (runtimeownership.Lease, bool, error) {
	if err != nil || !claimed {
		return lease, claimed, err
	}
	select {
	case s.claimed <- struct{}{}:
	case <-ctx.Done():
		_ = lease.Release(context.WithoutCancel(ctx))
		return nil, false, ctx.Err()
	}
	select {
	case <-s.release:
		return lease, true, nil
	case <-ctx.Done():
		_ = lease.Release(context.WithoutCancel(ctx))
		return nil, false, ctx.Err()
	}
}

func (s *replayClaimBarrierStore) ClaimPipelineReplay(ctx context.Context, eventID string) (runtimeownership.Lease, bool, error) {
	lease, claimed, err := s.PostgresStore.ClaimPipelineReplay(ctx, eventID)
	return s.awaitClaim(ctx, lease, claimed, err)
}

func (s *replayClaimBarrierStore) ClaimPipelineSettlement(ctx context.Context, eventID string) (runtimeownership.Lease, bool, error) {
	lease, claimed, err := s.PostgresStore.ClaimPipelineSettlement(ctx, eventID)
	return s.awaitClaim(ctx, lease, claimed, err)
}

func (s *replayClaimBarrierStore) ListEventsMissingPipelineReceipt(ctx context.Context, since time.Time, limit int) ([]events.PersistedReplayEvent, error) {
	records, err := s.PostgresStore.ListEventsMissingPipelineReceipt(ctx, since, 200)
	return filterReplayPoolRecords(records, s.eventID), err
}

func (s *replayClaimBarrierStore) ListEventsMissingPipelineReceiptForRun(ctx context.Context, runID string, since time.Time, limit int) ([]events.PersistedReplayEvent, error) {
	records, err := s.PostgresStore.ListEventsMissingPipelineReceiptForRun(ctx, runID, since, 200)
	return filterReplayPoolRecords(records, s.eventID), err
}

func (s *replayClaimBarrierStore) ListDueDecisionRouteObligations(ctx context.Context, now time.Time, limit int) ([]events.PersistedReplayEvent, error) {
	records, err := s.PostgresStore.ListDueDecisionRouteObligations(ctx, now, 200)
	return filterReplayPoolRecords(records, s.eventID), err
}

func filterReplayPoolRecords(records []events.PersistedReplayEvent, eventID string) []events.PersistedReplayEvent {
	for _, record := range records {
		if record.Event.ID() == eventID {
			return []events.PersistedReplayEvent{record}
		}
	}
	return nil
}

func (s *publicationClaimBarrierStore) ClaimPipelinePublication(ctx context.Context, eventID string) (runtimeownership.Lease, bool, error) {
	lease, claimed, err := s.PostgresStore.ClaimPipelinePublication(ctx, eventID)
	if err != nil || !claimed {
		return lease, claimed, err
	}
	select {
	case s.claimed <- struct{}{}:
	case <-ctx.Done():
		_ = lease.Release(context.WithoutCancel(ctx))
		return nil, false, ctx.Err()
	}
	select {
	case <-s.release:
		return lease, true, nil
	case <-ctx.Done():
		_ = lease.Release(context.WithoutCancel(ctx))
		return nil, false, ctx.Err()
	}
}

func TestEventBusPostgresPublicationClaimsDoNotExhaustPersistencePool(t *testing.T) {
	const poolSize = 4
	for _, form := range []string{"synchronous", "acknowledged", "mutation_bound"} {
		t.Run(form, func(t *testing.T) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			pg := &store.PostgresStore{DB: db}
			db.SetMaxOpenConns(poolSize)
			db.SetMaxIdleConns(poolSize)

			claimed := make(chan struct{}, poolSize)
			release := make(chan struct{})
			selected := &publicationClaimBarrierStore{PostgresStore: pg, claimed: claimed, release: release}
			bus, err := runtimebus.NewEventBus(selected)
			if err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			start := make(chan struct{})
			eventIDs := make([]string, poolSize)
			runIDs := make([]string, poolSize)
			errs := make(chan error, poolSize)
			for i := 0; i < poolSize; i++ {
				eventIDs[i] = uuid.NewString()
				runIDs[i] = uuid.NewString()
				if _, err := db.ExecContext(context.Background(), `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runIDs[i]); err != nil {
					t.Fatalf("insert run: %v", err)
				}
				eventID := eventIDs[i]
				runID := runIDs[i]
				go func() {
					<-start
					evt := eventtest.RootIngress(
						eventID, events.EventType("custom.pool_saturation"), "test", "", []byte(`{}`), 0, runID, "",
						events.EnvelopeForEntityID(events.EventEnvelope{}, uuid.NewString()), time.Now().UTC(),
					)
					switch form {
					case "synchronous":
						errs <- bus.Publish(ctx, evt)
					case "acknowledged":
						errs <- bus.PublishAcknowledged(ctx, evt)
					case "mutation_bound":
						errs <- selected.RunEventMutation(ctx, func(mutation runtimebus.EventMutation) error {
							return bus.PublishInMutation(mutation.Context(), evt)
						})
					}
				}()
			}
			close(start)
			if form == "mutation_bound" {
				// Mutation-bound publications own the global author-story order
				// lock before acquiring event-specific publication claims. Release
				// the first story so the remaining stories can enter in order.
				requireSignalBefore(t, claimed, 5*time.Second, "first serialized PostgreSQL publication claim")
				close(release)
				for i := 1; i < poolSize; i++ {
					requireSignalBefore(t, claimed, 5*time.Second, "subsequent serialized PostgreSQL publication claim")
				}
			} else {
				for i := 0; i < poolSize; i++ {
					requireSignalBefore(t, claimed, 5*time.Second, "aligned PostgreSQL publication claim")
				}
				close(release)
			}
			for i := 0; i < poolSize; i++ {
				if err := requireErrorBefore(t, errs, 10*time.Second, "pool-saturated publication"); err != nil {
					t.Fatal(err)
				}
			}
			waitCtx, waitCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer waitCancel()
			if err := bus.WaitForQuiescence(waitCtx); err != nil {
				t.Fatalf("wait for publication settlement: %v", err)
			}
			for _, eventID := range eventIDs {
				var count int
				if err := db.QueryRowContext(context.Background(), `
					SELECT COUNT(*)
					FROM event_receipts
					WHERE event_id = $1::uuid
					  AND subscriber_type = 'platform'
					  AND subscriber_id = 'pipeline'
				`, eventID).Scan(&count); err != nil {
					t.Fatalf("count pipeline receipt for %s: %v", eventID, err)
				}
				if count != 1 {
					t.Fatalf("pipeline receipt count for %s = %d, want 1", eventID, count)
				}
			}
		})
	}
}

func TestEventBusPostgresReplayClaimsDoNotExhaustPersistencePool(t *testing.T) {
	const poolSize = 4
	for _, surface := range []string{"generic_periodic", "decision_periodic", "run_queue", "startup"} {
		t.Run(surface, func(t *testing.T) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			seedStore := &store.PostgresStore{DB: db}
			eventIDs := make([]string, poolSize)
			runIDs := make([]string, poolSize)
			decisionRoute := surface == "decision_periodic"
			for i := 0; i < poolSize; i++ {
				runIDs[i] = uuid.NewString()
				if _, err := db.ExecContext(context.Background(), `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runIDs[i]); err != nil {
					t.Fatalf("insert run: %v", err)
				}
				eventIDs[i] = seedReplayPoolEvent(t, seedStore, runIDs[i], decisionRoute)
			}
			db.SetMaxOpenConns(poolSize)
			db.SetMaxIdleConns(poolSize)

			claimed := make(chan struct{}, poolSize)
			release := make(chan struct{})
			start := make(chan struct{})
			errs := make(chan error, poolSize)
			for i := 0; i < poolSize; i++ {
				selected := &replayClaimBarrierStore{
					PostgresStore: &store.PostgresStore{DB: db}, eventID: eventIDs[i], claimed: claimed, release: release,
				}
				bus, err := runtimebus.NewEventBus(selected)
				if err != nil {
					t.Fatal(err)
				}
				runID := runIDs[i]
				go func() {
					<-start
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer cancel()
					switch surface {
					case "generic_periodic", "decision_periodic":
						_, err := bus.SweepUndispatched(ctx, time.Hour, 10)
						errs <- err
					case "run_queue":
						_, err := bus.ReleaseRunQueue(ctx, runID, time.Hour, 10)
						errs <- err
					case "startup":
						errs <- runtimepipeline.NewRecoveryManagerWith(selected, bus).Recover(ctx)
					}
				}()
			}
			close(start)
			for i := 0; i < poolSize; i++ {
				requireSignalBefore(t, claimed, 5*time.Second, "aligned PostgreSQL replay claim")
			}
			close(release)
			for i := 0; i < poolSize; i++ {
				if err := requireErrorBefore(t, errs, 10*time.Second, "pool-saturated replay"); err != nil {
					t.Fatal(err)
				}
			}
			for _, eventID := range eventIDs {
				var count int
				if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`, eventID).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != 1 {
					t.Fatalf("pipeline receipt count for %s = %d, want 1", eventID, count)
				}
				if decisionRoute {
					var status string
					if err := db.QueryRowContext(context.Background(), `SELECT status FROM decision_card_route_obligations WHERE event_id = $1::uuid`, eventID).Scan(&status); err != nil || status != "completed" {
						t.Fatalf("decision route status for %s = %q, %v; want completed", eventID, status, err)
					}
				}
			}
		})
	}
}

func seedReplayPoolEvent(t *testing.T, selected *store.PostgresStore, runID string, decisionRoute bool) string {
	t.Helper()
	eventID, entityID := uuid.NewString(), uuid.NewString()
	eventType := events.EventType("custom.replay_pool_saturation")
	payload := []byte(`{}`)
	if decisionRoute {
		snapshot, err := decisioncard.FreezeSnapshot("launch_review", "", nil, map[string]runtimecontracts.WorkflowGateOutcomePlan{
			"approve": {Verdict: "approve", AdvancesTo: "operating"},
		})
		if err != nil {
			t.Fatal(err)
		}
		anchor, err := decisioncard.NewStageGateAnchor(decisioncard.StageGateAnchor{
			FlowInstance: "launch/replay-pool", FlowID: "launch", EntityID: entityID,
			Stage: "awaiting_review", StageActivationID: uuid.NewString(),
		})
		if err != nil {
			t.Fatal(err)
		}
		card, err := decisioncard.New(decisioncard.Card{
			CardID: uuid.NewString(), RunID: runID, Anchor: anchor,
			ExecutionMode:    "live",
			Snapshot:         snapshot,
			BundleHash:       "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			EffectiveCadence: decisioncard.Cadence{ReminderInterval: "24h", InputDraftTTL: "15m"}, CreatedAt: time.Now().UTC(),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := selected.CreateDecisionCard(context.Background(), card); err != nil {
			t.Fatal(err)
		}
		if _, err := selected.DecideDecisionCard(context.Background(), decisioncard.DecideRequest{
			CardID: card.CardID, Verdict: "approve", ActorTokenID: "operator", ObservedContentHash: card.CardContentHash,
			DecisionEventID: eventID, Now: time.Now().UTC(),
		}); err != nil {
			t.Fatal(err)
		}
		eventType = events.EventType("mailbox.card_decided")
		payload = []byte(`{"card_id":"` + card.CardID + `"}`)
	}
	evt := eventtest.RootIngress(eventID, eventType, "test", "", payload, 0, runID, "",
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Now().UTC())
	if err := selected.AppendEvent(context.Background(), evt); err != nil {
		t.Fatal(err)
	}
	if err := selected.UpsertCommittedReplayScope(context.Background(), eventID, runtimereplayclaim.CommittedReplayScopeSubscribed); err != nil {
		t.Fatal(err)
	}
	return eventID
}

func TestEventBusPublish_InterceptsMultiHopDeferredChains(t *testing.T) {
	store := &recordingEventStore{}
	eb, err := runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{deferredChainInterceptor{}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.Publish(context.Background(), eventtest.RootIngress("", events.EventType("custom.root"), "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"), time.Now().UTC())); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := store.eventTypes(); len(got) < 4 || got[0] != "custom.root" || got[1] != "custom.middle" || got[2] != "custom.leaf" || got[3] != "custom.final" {
		t.Fatalf("persisted event types prefix = %v, want prefix [custom.root custom.middle custom.leaf custom.final]", got)
	}
}

func TestEventBusPublishNonTransactional_PersistsBeforeInterceptorsRun(t *testing.T) {
	store := &recordingEventStore{}
	eb, err := runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{nonTransactionalPersistedBeforeInterceptor{t: t, store: store}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.Publish(context.Background(), eventtest.RootIngress(
		"",
		events.EventType("custom.non_transactional"),
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

func TestEventBusPublishTransactional_RunsInterceptorsAfterCommit(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := eventBusTestRunContext(t, db)
	pg := &store.PostgresStore{DB: db}
	eventID := "11111111-1111-1111-1111-111111111111"
	agentID := "agent-post-commit-publish"
	seedActiveRuntimeBusAgent(t, ctx, pg, agentID)
	called := make(chan struct{}, 1)
	eb, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{postCommitTxAbsentInterceptor{
			t:              t,
			store:          pg,
			eventID:        eventID,
			called:         called,
			wantRecipients: []string{agentID},
			wantScope:      runtimereplayclaim.CommittedReplayScopeSubscribed,
		}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ch := eb.Subscribe(agentID, events.EventType("task.completed"))
	defer eb.Unsubscribe(agentID)
	if err := eb.Publish(ctx, eventtest.RootIngress(
		eventID,
		events.EventType("task.completed"),
		"api.v1",
		"",
		[]byte(`{"entity_id":"11111111-1111-1111-1111-111111111114"}`),
		0,
		eventBusTestRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "11111111-1111-1111-1111-111111111114"),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	requireSignalBefore(t, called, time.Second, "post-commit interceptor")
	got := requireBusEvent(t, ch, "post-commit delivery")
	if got.ID() != eventID {
		t.Fatalf("delivered event id = %s, want %s", got.ID(), eventID)
	}
}

func TestEventBusPublishTransactional_ReturnsPostCommitInterceptorErrorAndRecordsReceipt(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := eventBusTestRunContext(t, db)
	pg := &store.PostgresStore{DB: db}
	eventID := "11111111-1111-1111-1111-111111111112"
	wantErr := errors.New("post-commit interceptor failure")
	eb, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{postCommitErrorInterceptor{
			t:       t,
			store:   pg,
			eventID: eventID,
			err:     wantErr,
		}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	err = eb.Publish(ctx, eventtest.RootIngress(
		eventID,
		events.EventType("task.failed"),
		"",
		"",
		[]byte(`{"entity_id":"11111111-1111-1111-1111-111111111114"}`),
		0,
		eventBusTestRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "11111111-1111-1111-1111-111111111114"),
		time.Now().UTC(),
	))
	if !errors.Is(err, wantErr) {
		t.Fatalf("Publish error = %v, want %v", err, wantErr)
	}
	var outcome, failureClass, detailCode string
	if err := db.QueryRowContext(ctx, `
		SELECT outcome, COALESCE(failure->>'class', ''), COALESCE(failure->'detail'->>'code', '')
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, eventID).Scan(&outcome, &failureClass, &detailCode); err != nil {
		t.Fatalf("load pipeline receipt: %v", err)
	}
	if outcome != "dead_letter" || failureClass != string(runtimefailures.ClassInternalFailure) || detailCode != "event_interceptor_failed" {
		t.Fatalf("pipeline receipt = outcome:%q class:%q detail:%q, want canonical event_interceptor_failed", outcome, failureClass, detailCode)
	}
}

func TestEventBusPublishInMutationRunsInterceptorsAfterMutationCommit(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := eventBusTestRunContext(t, db)
	pg := &store.PostgresStore{DB: db}
	eventID := "11111111-1111-1111-1111-111111111112"
	called := make(chan struct{}, 1)
	eb, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{postCommitTxAbsentInterceptor{
			t:       t,
			store:   pg,
			eventID: eventID,
			called:  called,
		}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := pg.RunEventMutation(ctx, func(mutation runtimebus.EventMutation) error {
		if err := eb.PublishInMutation(mutation.Context(), eventtest.RootIngress(
			eventID,
			events.EventType("custom.publish_mutation_post_commit"),
			"api.v1",
			"",
			[]byte(`{"entity_id":"11111111-1111-1111-1111-111111111113"}`),
			0,
			eventBusTestRunID,
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "11111111-1111-1111-1111-111111111113"),
			time.Now().UTC(),
		)); err != nil {
			return err
		}
		select {
		case <-called:
			t.Fatal("interceptor ran before mutation committed")
		default:
		}
		ok, err := pg.EventExists(ctx, eventID)
		if err != nil {
			t.Fatalf("EventExists before commit: %v", err)
		}
		if ok {
			t.Fatal("event visible outside mutation before commit")
		}
		return nil
	}); err != nil {
		t.Fatalf("RunEventMutation: %v", err)
	}
	requireSignalBefore(t, called, time.Second, "post-commit interceptor")
	if got := countPipelineReceiptsForEvent(t, ctx, db, eventID); got != 1 {
		t.Fatalf("pipeline receipts = %d, want 1", got)
	}
}

func TestEventBusPublishTransactional_RecordsTargetFailureDeadLetter(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	eb, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ctx := context.Background()
	eventID := uuid.NewString()
	targetEntityID := uuid.NewString()
	if err := pg.RunEventMutation(ctx, func(mutation runtimebus.EventMutation) error {
		return eb.PublishInMutation(mutation.Context(), eventtest.RootIngress(
			eventID,
			events.EventType("child/output.done"),
			"",
			"",
			[]byte(`{}`),
			0,
			"",
			"",
			events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{EntityID: targetEntityID, FlowInstance: "missing-flow"}),
			time.Now().UTC(),
		))
	}); err != nil {
		t.Fatalf("PublishInMutation: %v", err)
	}

	var reason, targetContext string
	if err := db.QueryRowContext(ctx, `
		SELECT failure->'detail'->>'code', COALESCE((failure->'detail'->'attributes'->'target')::text, '')
		FROM dead_letters
		WHERE original_event_id = $1::uuid
		  AND failure->>'class' = 'platform.target_unreachable'
		  AND handler_node = 'pin_routing'
	`, eventID).Scan(&reason, &targetContext); err != nil {
		t.Fatalf("query dead_letters: %v", err)
	}
	if reason != "target_unreachable_terminated" {
		t.Fatalf("target failure reason = %q, want target_unreachable_terminated", reason)
	}
	if !strings.Contains(targetContext, "missing-flow") {
		t.Fatalf("target context = %s, want missing-flow", targetContext)
	}
}

func TestEventBusPublishInMutationSQLiteRecordsTargetFailureDeadLetter(t *testing.T) {
	sqliteStore := storetest.StartSQLiteRuntimeStore(t)
	eb, err := runtimebus.NewEventBusWithOptions(sqliteStore, runtimebus.EventBusOptions{})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	eb.RegisterRuntimeActiveAgentDescriptor(runtimebus.ActiveAgentDescriptor{
		AgentID:      "live-other",
		EntityID:     uuid.NewString(),
		FlowInstance: "other-flow",
	})
	ctx := context.Background()
	descriptors, err := eb.PinRoutingDescriptors(ctx)
	if err != nil {
		t.Fatalf("PinRoutingDescriptors: %v", err)
	}
	if len(descriptors) == 0 {
		t.Fatal("PinRoutingDescriptors returned no runtime descriptors")
	}
	if descriptors[0].ID != "live-other" {
		t.Fatalf("PinRoutingDescriptors[0].ID = %q, want live-other", descriptors[0].ID)
	}
	_ = eb.Subscribe("live-other", events.EventType("task.completed"))
	if got := eb.ResolveSubscribedRecipients("task.completed"); !slices.Equal(got, []string{"live-other"}) {
		t.Fatalf("ResolveSubscribedRecipients = %#v, want live-other", got)
	}
	eventID := uuid.NewString()
	targetEntityID := uuid.NewString()
	evt := eventtest.RootIngress(
		eventID,
		events.EventType("task.completed"),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{EntityID: targetEntityID, FlowInstance: "missing-flow"}),
		time.Now().UTC(),
	)

	if !evt.HasTargetRoute() {
		t.Fatalf("event target route missing after construction: envelope=%#v", evt.NormalizedEnvelope())
	}
	plan, err := eb.CheckPublishRecipientPlan(ctx, evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if plan.TargetFailure != "target_unreachable_terminated" {
		t.Fatalf("target failure = %q, want target_unreachable_terminated: plan=%#v", plan.TargetFailure, plan)
	}
	if err := sqliteStore.RunEventMutation(ctx, func(mutation runtimebus.EventMutation) error {
		return eb.PublishInMutation(mutation.Context(), evt)
	}); err != nil {
		t.Fatalf("PublishInMutation: %v", err)
	}

	var reason, targetContext string
	if err := sqliteStore.DB.QueryRowContext(ctx, `
		SELECT COALESCE(json_extract(failure, '$.detail.code'), ''), COALESCE(json_extract(failure, '$.detail.attributes.target'), '')
		FROM dead_letters
		WHERE original_event_id = ?
		  AND json_extract(failure, '$.class') = 'platform.target_unreachable'
		  AND handler_node = 'pin_routing'
	`, eventID).Scan(&reason, &targetContext); err != nil {
		t.Fatalf("query sqlite dead_letters: %v", err)
	}
	if reason != "target_unreachable_terminated" {
		t.Fatalf("target failure reason = %q, want target_unreachable_terminated", reason)
	}
	if !strings.Contains(targetContext, "missing-flow") {
		t.Fatalf("target context = %s, want missing-flow", targetContext)
	}
	var pipelineReceipts int
	if err := sqliteStore.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE event_id = ?
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, eventID).Scan(&pipelineReceipts); err != nil {
		t.Fatalf("query sqlite pipeline receipt: %v", err)
	}
	if pipelineReceipts != 1 {
		t.Fatalf("sqlite pipeline receipts = %d, want 1", pipelineReceipts)
	}
}

func TestEventBusPublish_ClassifiesRunBundleSourceThroughRunLifecycleOwner(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	runID := uuid.NewString()
	fingerprint := "sha256:3333333333333333333333333333333333333333333333333333333333333333"
	eb, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		BundleFingerprint: fingerprint,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.Publish(context.Background(), eventtest.RootIngress(uuid.NewString(),

		events.EventType("scan.requested"),
		"test", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	var bundleHash, bundleSource, legacyFingerprint string
	if err := db.QueryRowContext(context.Background(), `
		SELECT COALESCE(bundle_hash, ''), bundle_source, COALESCE(bundle_fingerprint, '')
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&bundleHash, &bundleSource, &legacyFingerprint); err != nil {
		t.Fatalf("load run bundle source: %v", err)
	}
	if bundleHash != "" || bundleSource != "legacy" || legacyFingerprint != fingerprint {
		t.Fatalf("bundle identity = hash:%q source:%q fingerprint:%q, want legacy with compatibility fingerprint", bundleHash, bundleSource, legacyFingerprint)
	}
}

func TestEventBusPublishDirect_StampsBundleSourceFactOnRunRow(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	runID := uuid.NewString()
	sourceFact := runtimecorrelation.BundleSourceFact{
		BundleHash:        "bundle-v1:sha256:4444444444444444444444444444444444444444444444444444444444444444",
		BundleSource:      "persisted",
		BundleFingerprint: "sha256:4444444444444444444444444444444444444444444444444444444444444444",
	}
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json)
		VALUES ($1, 'name: test', '{}'::jsonb)
	`, sourceFact.BundleHash); err != nil {
		t.Fatalf("seed bundle row: %v", err)
	}
	eb, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		BundleSourceFact: sourceFact,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.PublishDirect(context.Background(), eventtest.RootIngress(uuid.NewString(),

		events.EventType("scan.requested"),
		"test", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC()),

		[]string{"agent-a"}); err != nil {
		t.Fatalf("PublishDirect: %v", err)
	}
	var bundleHash, bundleSource, legacyFingerprint string
	if err := db.QueryRowContext(context.Background(), `
		SELECT COALESCE(bundle_hash, ''), bundle_source, COALESCE(bundle_fingerprint, '')
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&bundleHash, &bundleSource, &legacyFingerprint); err != nil {
		t.Fatalf("load run bundle source: %v", err)
	}
	if bundleHash != sourceFact.BundleHash || bundleSource != sourceFact.BundleSource || legacyFingerprint != sourceFact.BundleFingerprint {
		t.Fatalf("bundle identity = hash:%q source:%q fingerprint:%q, want canonical source fact %#v", bundleHash, bundleSource, legacyFingerprint, sourceFact)
	}
}

func TestEventBusPublishDeferred_RunsInterceptorsAfterDeferredEventCommit(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	eventID := "22222222-2222-2222-2222-222222222222"
	eb, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{deferredEventVisibleInterceptor{
			t:        t,
			store:    pg,
			eventID:  eventID,
			checkFor: events.EventType("custom.middle"),
		}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.Publish(context.Background(), eventtest.RootIngress("11111111-1111-1111-1111-111111111111",
		events.EventType("custom.root"), "", "", []byte(`{"entity_id":"ent-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
	); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

func TestEventBusPublish_InheritsRunAndParentFromInboundContext(t *testing.T) {
	store := &recordingEventStore{}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ctx := runtimecorrelation.WithInboundEvent(context.Background(), eventtest.RootIngress("evt-parent",
		events.EventType("task.started"), "", "", nil, 0, "run-abc", "", events.EventEnvelope{}, time.Time{}))
	if err := eb.Publish(ctx, eventtest.RootIngress("evt-child",
		events.EventType("task.completed"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{})); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(store.events) != 1 {
		found := false
		for _, evt := range store.events {
			if evt.ID() != "evt-child" {
				continue
			}
			found = true
			if got := evt.RunID(); got != "run-abc" {
				t.Fatalf("persisted run_id = %q, want run-abc", got)
			}
			if got := evt.ParentEventID(); got != "evt-parent" {
				t.Fatalf("persisted parent_event_id = %q, want evt-parent", got)
			}
		}
		if !found {
			t.Fatalf("persisted events = %#v, want child event", store.events)
		}
		return
	}
	if got := store.events[0].RunID(); got != "run-abc" {
		t.Fatalf("persisted run_id = %q, want run-abc", got)
	}
	if got := store.events[0].ParentEventID(); got != "evt-parent" {
		t.Fatalf("persisted parent_event_id = %q, want evt-parent", got)
	}
}

func TestEventBusPublish_ZeroRecipientsDoesNotEmitContradiction(t *testing.T) {
	store := &recordingEventStore{}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	if err := eb.Publish(context.Background(), eventtest.RootIngress("evt-zero",
		events.EventType("custom.no_subscribers"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{})); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := store.eventTypes()
	if len(got) != 1 || got[0] != "custom.no_subscribers" {
		t.Fatalf("persisted event types = %v, want [custom.no_subscribers]", got)
	}
}

func TestPrepareInboundDeliveryBatchRollsBackAllDerivedEventsWithCallerMutation(t *testing.T) {
	testCases := []struct {
		name  string
		setup func(*testing.T) (context.Context, *sql.DB, runtimebus.EventStore, runtimebus.EventMutationRunner)
		count func(*testing.T, context.Context, *sql.DB, string, string) (int, int)
	}{
		{
			name: "postgres",
			setup: func(t *testing.T) (context.Context, *sql.DB, runtimebus.EventStore, runtimebus.EventMutationRunner) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				ctx := eventBusTestRunContext(t, db)
				pg := &store.PostgresStore{DB: db}
				return ctx, db, pg, pg
			},
			count: countPostgresInboundBatchRows,
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) (context.Context, *sql.DB, runtimebus.EventStore, runtimebus.EventMutationRunner) {
				sqliteStore := storetest.StartSQLiteRuntimeStore(t)
				ctx := runtimecorrelation.WithRunID(context.Background(), eventBusTestRunID)
				if _, err := sqliteStore.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES (?, 'running')`, eventBusTestRunID); err != nil {
					t.Fatalf("seed SQLite event bus test run: %v", err)
				}
				return ctx, sqliteStore.DB, sqliteStore, sqliteStore
			},
			count: countSQLiteInboundBatchRows,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, db, eventStore, runner := tc.setup(t)
			rawID := uuid.NewString()
			normalizedID := uuid.NewString()
			entityID := uuid.NewString()
			authorization := runtimeprovideroutput.Authorization{
				Provider: "proof-provider", Event: "inbound.proof.normalized", PackID: "provider.proof",
				PackVersion: "1.0.0", ManifestHash: "sha256:" + strings.Repeat("a", 64), GenerationID: "proof-generation",
			}
			source := inboundBatchAuthorizedSource{
				Source: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), authorizations: []runtimeprovideroutput.Authorization{authorization},
			}
			eb, err := runtimebus.NewEventBusWithOptions(eventStore, runtimebus.EventBusOptions{
				ContractBundle: source, ProviderOutputVerifier: source,
			})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			batch := runtimebus.InboundDeliveryBatch{
				Provider: "proof-provider",
				Events: []runtimebus.InboundDeliveryEvent{
					{Event: eventtest.RootIngress(rawID, events.EventType("inbound.proof"), "inbound-gateway", "", []byte(`{"raw":true}`), 0, eventBusTestRunID, "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Now().UTC()), Kind: runtimeprovideroutput.KindRaw},
					{
						Event:         eventtest.RootIngress(normalizedID, events.EventType("inbound.proof.normalized"), "inbound-gateway", "", []byte(`{"normalized":true}`), 0, eventBusTestRunID, "", events.EventEnvelope{}, time.Now().UTC()),
						Kind:          runtimeprovideroutput.KindNormalized,
						Authorization: authorization,
					},
				},
			}

			err = runner.RunEventMutation(ctx, func(mutation runtimebus.EventMutation) error {
				wrapped := &failingInboundBatchMutation{EventMutation: mutation, failAppend: 2}
				_, prepareErr := eb.PrepareInboundDeliveryBatchInMutation(wrapped.Context(), batch)
				return prepareErr
			})
			if err == nil || !strings.Contains(err.Error(), "injected normalized append failure") {
				t.Fatalf("PrepareInboundDeliveryBatchInMutation error = %v, want injected normalized append failure", err)
			}
			eventsCount, markerCount := tc.count(t, ctx, db, rawID, normalizedID)
			if eventsCount != 0 || markerCount != 0 {
				t.Fatalf("rolled-back provider batch retained events=%d markers=%d, want zero", eventsCount, markerCount)
			}
		})
	}
}

type inboundBatchAuthorizedSource struct {
	semanticview.Source
	authorizations []runtimeprovideroutput.Authorization
}

func (s inboundBatchAuthorizedSource) ProviderTriggerTargetFreeAuthorizations() []runtimeprovideroutput.Authorization {
	return append([]runtimeprovideroutput.Authorization(nil), s.authorizations...)
}

func (s inboundBatchAuthorizedSource) VerifyProviderOutputAuthorization(actual runtimeprovideroutput.Authorization) error {
	for _, expected := range s.authorizations {
		if expected.Matches(actual) {
			return nil
		}
	}
	return errors.New("authorization does not match test catalog owner")
}

type failingInboundBatchMutation struct {
	runtimebus.EventMutation
	appendCount int
	failAppend  int
}

func (m *failingInboundBatchMutation) Context() context.Context {
	return runtimebus.WithEventMutationContext(m.EventMutation.Context(), m)
}

func (m *failingInboundBatchMutation) AppendEvent(ctx context.Context, evt events.Event) error {
	_, err := m.AppendEventOutcome(ctx, evt)
	return err
}

func (m *failingInboundBatchMutation) AppendEventOutcome(ctx context.Context, evt events.Event) (runtimebus.EventAppendOutcome, error) {
	m.appendCount++
	if m.appendCount == m.failAppend {
		return runtimebus.EventAppendOutcomeUnknown, errors.New("injected normalized append failure")
	}
	return m.EventMutation.AppendEventOutcome(ctx, evt)
}

func countPostgresInboundBatchRows(t *testing.T, ctx context.Context, db *sql.DB, rawID, normalizedID string) (int, int) {
	t.Helper()
	var eventsCount, markerCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id IN ($1::uuid, $2::uuid)`, rawID, normalizedID).Scan(&eventsCount); err != nil {
		t.Fatalf("count Postgres provider events: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_name = 'platform.inbound_recorded'`).Scan(&markerCount); err != nil {
		t.Fatalf("count Postgres provider marker: %v", err)
	}
	return eventsCount, markerCount
}

func countSQLiteInboundBatchRows(t *testing.T, ctx context.Context, db *sql.DB, rawID, normalizedID string) (int, int) {
	t.Helper()
	var eventsCount, markerCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id IN (?, ?)`, rawID, normalizedID).Scan(&eventsCount); err != nil {
		t.Fatalf("count SQLite provider events: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_name = 'platform.inbound_recorded'`).Scan(&markerCount); err != nil {
		t.Fatalf("count SQLite provider marker: %v", err)
	}
	return eventsCount, markerCount
}

func TestEventBusPublish_RuntimeLogBypassesContradictionRouting(t *testing.T) {
	store := &recordingEventStore{}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	if err := eb.Publish(context.Background(), eventtest.RootIngress("evt-log",
		events.EventType("platform.runtime_log"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{})); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := store.eventTypes()
	if len(got) != 1 || got[0] != "platform.runtime_log" {
		t.Fatalf("persisted event types = %v, want [platform.runtime_log]", got)
	}
}

func TestEventBusPublish_RuntimeOwnedStandalonePlatformRunsConvergeWithoutPersistedDeliveries(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	pg := &store.PostgresStore{DB: db}
	eb, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}

	testCases := []struct {
		name      string
		eventID   string
		eventType events.EventType
		event     func(id string, eventType events.EventType) events.Event
	}{
		{
			name:      "platform.boot",
			eventID:   "10000000-0000-0000-0000-000000000001",
			eventType: events.EventType("platform.boot"),
			event: func(id string, eventType events.EventType) events.Event {
				return eventtest.RuntimeControl(id, eventType, "runtime", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
			},
		},
		{
			name:      "platform.recovery_failed",
			eventID:   "10000000-0000-0000-0000-000000000002",
			eventType: events.EventType("platform.recovery_failed"),
			event: func(id string, eventType events.EventType) events.Event {
				return eventtest.RuntimeDiagnostic(id, eventType, "runtime", "", platformSignalFixturePayload(t, eventType), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			internal := eb.SubscribeInternal("internal-"+string(tc.eventType), tc.eventType)
			defer eb.Unsubscribe("internal-" + string(tc.eventType))

			if err := eb.Publish(ctx, tc.event(tc.eventID, tc.eventType)); err != nil {
				t.Fatalf("Publish(%s): %v", tc.eventType, err)
			}

			got := requireBusEvent(t, internal, "standalone platform event internal delivery")
			if got.ID() != tc.eventID {
				t.Fatalf("internal delivery event_id = %q, want %q", got.ID(), tc.eventID)
			}

			runID, runStatus, triggerEventType := loadRunStateForEvent(t, ctx, db, tc.eventID)
			if strings.TrimSpace(runID) == "" {
				t.Fatalf("run_id missing for %s", tc.eventType)
			}
			if runStatus != "completed" {
				t.Fatalf("run status for %s = %q, want completed", tc.eventType, runStatus)
			}
			if triggerEventType != string(tc.eventType) {
				t.Fatalf("trigger_event_type for %s = %q, want %q", tc.eventType, triggerEventType, tc.eventType)
			}
			if got := countEventDeliveriesForEvent(t, ctx, db, tc.eventID); got != 0 {
				t.Fatalf("event_deliveries for %s = %d, want 0", tc.eventType, got)
			}
		})
	}
}

func TestEventBusPublish_RuntimeOwnedStandalonePlatformRunsConvergeAfterFinalReceipt(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	pg := &store.PostgresStore{DB: db}
	eb, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}

	agentID := "agent-runtime-owned-platform"
	seedActiveRuntimeBusAgent(t, ctx, pg, agentID)

	testCases := []struct {
		name      string
		eventID   string
		eventType events.EventType
		event     func(id string, eventType events.EventType) events.Event
	}{
		{
			name:      "manager platform.agent_failed",
			eventID:   "20000000-0000-0000-0000-000000000001",
			eventType: events.EventType("platform.agent_failed"),
			event: func(id string, eventType events.EventType) events.Event {
				return eventtest.RuntimeDiagnostic(id, eventType, "runtime", "", platformSignalFixturePayload(t, eventType), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
			},
		},
		{
			name:      "receipts platform.paused",
			eventID:   "20000000-0000-0000-0000-000000000002",
			eventType: events.EventType("platform.paused"),
			event: func(id string, eventType events.EventType) events.Event {
				return eventtest.RuntimeControl(id, eventType, "runtime", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
			},
		},
		{
			name:      "budget platform.budget_threshold_crossed",
			eventID:   "20000000-0000-0000-0000-000000000003",
			eventType: events.EventType("platform.budget_threshold_crossed"),
			event: func(id string, eventType events.EventType) events.Event {
				return eventtest.RuntimeDiagnostic(id, eventType, "runtime", "", platformSignalFixturePayload(t, eventType), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
			},
		},
		{
			name:      "run lifecycle platform.run_stalled",
			eventID:   "20000000-0000-0000-0000-000000000004",
			eventType: events.EventType("platform.run_stalled"),
			event: func(id string, eventType events.EventType) events.Event {
				return eventtest.RuntimeDiagnostic(id, eventType, "runtime", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			subscription := eb.Subscribe(agentID, tc.eventType)
			defer eb.Unsubscribe(agentID)

			if err := eb.Publish(ctx, tc.event(tc.eventID, tc.eventType)); err != nil {
				t.Fatalf("Publish(%s): %v", tc.eventType, err)
			}

			got := requireBusEvent(t, subscription, "standalone agent event delivery")
			if got.ID() != tc.eventID {
				t.Fatalf("delivered event_id = %q, want %q", got.ID(), tc.eventID)
			}

			if got := countEventDeliveriesForEvent(t, ctx, db, tc.eventID); got != 1 {
				t.Fatalf("event_deliveries for %s = %d, want 1", tc.eventType, got)
			}
			if deliveryStatus, runStatus := loadAgentDeliveryForEvent(t, ctx, db, tc.eventID, agentID); deliveryStatus != "pending" || runStatus != "running" {
				t.Fatalf("pre-receipt state for %s = delivery:%q run:%q, want pending/running", tc.eventType, deliveryStatus, runStatus)
			}

			if err := pg.UpsertEventReceipt(ctx, tc.eventID, agentID, runtimemanager.ReceiptStatusProcessed, nil); err != nil {
				t.Fatalf("UpsertEventReceipt(%s): %v", tc.eventType, err)
			}

			deliveryStatus, runStatus := loadAgentDeliveryForEvent(t, ctx, db, tc.eventID, agentID)
			if deliveryStatus != "delivered" {
				t.Fatalf("delivery status for %s = %q, want delivered", tc.eventType, deliveryStatus)
			}
			if runStatus != "completed" {
				t.Fatalf("run status for %s = %q, want completed", tc.eventType, runStatus)
			}
		})
	}
}

func platformSignalFixturePayload(t *testing.T, eventType events.EventType) []byte {
	t.Helper()
	payload := map[string]any{}
	switch eventType {
	case events.EventType("platform.agent_failed"), events.EventType("platform.recovery_failed"):
		failure := runtimefailures.Normalize(runtimefailures.New(
			runtimefailures.ClassInternalFailure,
			"unclassified_runtime_error",
			"eventbus-test",
			"publish_platform_signal",
			nil,
		), "eventbus-test", "publish_platform_signal")
		payload["failure"] = &failure
	case events.EventType("platform.budget_threshold_crossed"):
		payload["level"] = "warning"
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal platform signal fixture %s: %v", eventType, err)
	}
	return raw
}

func TestEventBusRuntimeIngressPauseQueuesAndResumeReleases(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	ctx := eventBusTestRunContext(t, db)
	pg := &store.PostgresStore{DB: db}
	eb, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	controller := runtimeingress.NewController(pg, eb, runtimeingress.Options{})
	t.Cleanup(runtimebus.ResumeRuntimeIngress)
	eb.SetRuntimeIngressDispatchGate(controller)

	agentID := "agent-paused-queue"
	eventType := events.EventType("custom.paused")
	seedActiveRuntimeBusAgent(t, ctx, pg, agentID)
	ch := eb.Subscribe(agentID, eventType)
	defer eb.Unsubscribe(agentID)

	if _, err := controller.Pause(context.Background(), runtimeingress.TransitionRequest{
		Reason:       "test_pause",
		ControlledBy: "test",
	}); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	eventID := "21000000-0000-0000-0000-000000000001"
	if err := eb.Publish(ctx, eventtest.RootIngress(
		eventID,
		eventType,
		"api.v1",
		"",
		[]byte(`{"entity_id":"21000000-0000-0000-0000-000000000002"}`),
		0,
		eventBusTestRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "21000000-0000-0000-0000-000000000002"),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish while paused: %v", err)
	}

	requireNoBusEvent(t, ch, "paused runtime before resume")
	if got := countEventDeliveriesForEvent(t, ctx, db, eventID); got != 1 {
		t.Fatalf("event deliveries while paused = %d, want 1", got)
	}
	if got := countPipelineReceiptsForEvent(t, ctx, db, eventID); got != 0 {
		t.Fatalf("pipeline receipts while paused = %d, want 0", got)
	}

	resumed, err := controller.Resume(context.Background(), runtimeingress.TransitionRequest{
		Reason:       "test_resume",
		ControlledBy: "test",
	})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if resumed.ReleasedCount != 1 {
		t.Fatalf("released count = %d, want 1", resumed.ReleasedCount)
	}
	got := requireBusEvent(t, ch, "queued event release after resume")
	if got.ID() != eventID {
		t.Fatalf("delivered event = %s, want %s", got.ID(), eventID)
	}
	if got := countPipelineReceiptsForEvent(t, ctx, db, eventID); got != 1 {
		t.Fatalf("pipeline receipts after resume = %d, want 1", got)
	}
}

func TestEventBusPublish_HumanTaskEventsRouteBySubscriptionOnly(t *testing.T) {
	eb, err := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := eb.Subscribe("requester")
	defer eb.Unsubscribe("requester")

	if err := eb.Publish(context.Background(), eventtest.RootIngress("", events.EventType("human_task.approved"), "", "", []byte(`{"requesting_agent":"requester"}`), 0, "", "", events.EventEnvelope{}, time.Time{})); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	requireNoBusEvent(t, ch, "human task event without subscription")
}

func TestEventBusPublish_DoesNotLogRoutedRecipientForRetiredSiblingAutoWire(t *testing.T) {
	producer := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "producer", Flow: "producer"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{Events: []string{"scan.requested"}},
			},
		},
		Path: "producer",
	}
	discovery := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "discovery", Flow: "discovery"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{"scan.requested"}},
			},
		},
		Path: "discovery",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"scan-orchestrator": {
				ID:           "scan-orchestrator",
				SubscribesTo: []string{"scan.requested"},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{producer, discovery}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"producer":  &root.Children[0],
				"discovery": &root.Children[1],
			},
		},
	}
	hook := &recordingLoggerHook{}
	eb, err := runtimebus.NewEventBusWithOptions(newRouteSetEventStore(), runtimebus.EventBusOptions{
		Logger:         hook,
		ContractBundle: semanticview.Wrap(bundle),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	eb.Subscribe("direct-agent", events.EventType("producer/scan.requested"))
	eb.Subscribe("scan-orchestrator")
	defer eb.Unsubscribe("direct-agent")
	defer eb.Unsubscribe("scan-orchestrator")

	if err := eb.Publish(context.Background(), eventtest.RootIngress("", events.EventType("producer/scan.requested"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{})); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	var delivered any
	for _, entry := range hook.entries {
		if entry.Action == "delivered" {
			delivered = entry.Detail
		}
	}
	if delivered == nil {
		t.Fatal("expected delivered log entry")
	}
	detail, ok := delivered.(map[string]any)
	if !ok {
		t.Fatalf("delivered detail type = %T, want map[string]any", delivered)
	}
	routed, _ := detail["routed_recipients"].([]map[string]any)
	if len(routed) == 0 {
		// logger detail may pass through as []any after interface widening
		if raw, ok := detail["routed_recipients"].([]any); ok {
			routed = make([]map[string]any, 0, len(raw))
			for _, item := range raw {
				if cast, ok := item.(map[string]any); ok {
					routed = append(routed, cast)
				}
			}
		}
	}
	if len(routed) != 0 {
		t.Fatalf("routed_recipients = %#v, want none for retired sibling auto-wire", detail["routed_recipients"])
	}
	subs, _ := detail["subscription_recipients"].([]string)
	if len(subs) == 0 {
		if raw, ok := detail["subscription_recipients"].([]any); ok {
			subs = make([]string, 0, len(raw))
			for _, item := range raw {
				if cast, ok := item.(string); ok {
					subs = append(subs, cast)
				}
			}
		}
	}
	if len(subs) != 1 || subs[0] != "direct-agent" {
		t.Fatalf("subscription_recipients = %#v, want [direct-agent]", detail["subscription_recipients"])
	}
}

func TestEventBusPublish_RecordsNoRoutedDiagnosticsForRetiredSiblingAutoWire(t *testing.T) {
	producer := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "producer", Flow: "producer"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{Events: []string{"scan.requested"}},
			},
		},
		Path: "producer",
	}
	discovery := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "discovery", Flow: "discovery"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{"scan.requested"}},
			},
		},
		Path: "discovery",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"scan-orchestrator": {
				ID:           "scan-orchestrator",
				SubscribesTo: []string{"scan.requested"},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{producer, discovery}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"producer":  &root.Children[0],
				"discovery": &root.Children[1],
			},
		},
	}
	eb, err := runtimebus.NewEventBusWithOptions(newRouteSetEventStore(), runtimebus.EventBusOptions{
		ContractBundle: semanticview.Wrap(bundle),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	eb.Subscribe("scan-orchestrator")
	defer eb.Unsubscribe("scan-orchestrator")
	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx := runtimebus.WithEmittedEventsRecorder(context.Background(), recorder)
	if err := eb.Publish(ctx, eventtest.RootIngress("", "producer/scan.requested", "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"), time.Time{})); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	diags := recorder.SnapshotPublishes()
	if len(diags) != 1 {
		t.Fatalf("publish diagnostics = %#v", diags)
	}
	if diags[0].EventType != "producer/scan.requested" {
		t.Fatalf("event_type = %q", diags[0].EventType)
	}
	if len(diags[0].RoutedRecipients) != 0 {
		t.Fatalf("routed_recipients = %#v, want none for retired sibling auto-wire", diags[0].RoutedRecipients)
	}
}

func TestEventBusPublish_NestedDescendantCompletionDoesNotEmitChildContinuation(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-nested-three-levels")
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	module := newFixtureWorkflowModule(t, bundle)
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg := &store.PostgresStore{DB: db}
	var pc *runtimepipeline.PipelineCoordinator
	eb, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		ContractBundle: semanticview.Wrap(bundle),
		InterceptorProvider: func() []runtimebus.EventInterceptor {
			if pc == nil {
				return nil
			}
			return []runtimebus.EventInterceptor{pc}
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	pc = runtimepipeline.NewPipelineCoordinatorWithOptions(eb, db, runtimepipeline.PipelineCoordinatorOptions{
		Module:                  module,
		EventReceiptsCapability: pg.CanonicalEventReceiptsCapability,
	})
	if pc == nil {
		t.Fatal("expected coordinator")
	}

	const rootEntityID = "11111111-1111-1111-1111-111111111111"
	childEntityID := runtimepipeline.FlowInstanceEntityID("child/inst-1")
	grandchildEntityID := runtimepipeline.FlowInstanceEntityID("child/grandchild/inst-1")
	store := runtimepipeline.NewWorkflowInstanceStore(db)
	ctx := eventBusTestRunContext(t, db)
	for _, instance := range []runtimepipeline.WorkflowInstance{
		{
			InstanceID:      rootEntityID,
			StorageRef:      rootEntityID,
			WorkflowName:    bundle.WorkflowName(),
			WorkflowVersion: bundle.WorkflowVersion(),
			CurrentState:    "idle",
			Metadata: map[string]any{
				"entity_id": rootEntityID,
			},
		},
		{
			InstanceID:      childEntityID,
			StorageRef:      "child/inst-1",
			WorkflowName:    "child",
			WorkflowVersion: bundle.WorkflowVersion(),
			CurrentState:    "waiting",
			Metadata: map[string]any{
				"entity_id":        childEntityID,
				"flow_path":        "child/inst-1",
				"parent_entity_id": rootEntityID,
			},
		},
		{
			InstanceID:      grandchildEntityID,
			StorageRef:      "child/grandchild/inst-1",
			WorkflowName:    "grandchild",
			WorkflowVersion: bundle.WorkflowVersion(),
			CurrentState:    "finished",
			Metadata: map[string]any{
				"entity_id":        grandchildEntityID,
				"flow_path":        "child/grandchild/inst-1",
				"parent_entity_id": childEntityID,
			},
		},
	} {
		if err := store.Upsert(ctx, instance); err != nil {
			t.Fatalf("seed workflow instance %q: %v", instance.InstanceID, err)
		}
	}

	if err := eb.Publish(ctx, eventtest.RootIngress(
		"11111111-2222-3333-4444-555555555555",
		events.EventType("child/grandchild/micro.done"),
		"cataloge2e",
		"",
		[]byte(`{"entity_id":"`+grandchildEntityID+`"}`),
		0,
		eventBusTestRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, grandchildEntityID),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := eb.WaitForQuiescence(ctx); err != nil {
		t.Fatalf("WaitForQuiescence: %v", err)
	}

	child, found, err := store.Load(ctx, childEntityID)
	if err != nil {
		t.Fatalf("load child instance: %v", err)
	}
	if !found {
		t.Fatal("expected child instance")
	}
	if got := strings.TrimSpace(child.CurrentState); got != "waiting" {
		t.Fatalf("child current_state = %q, want waiting", got)
	}

	root, found, err := store.Load(ctx, rootEntityID)
	if err != nil {
		t.Fatalf("load root instance: %v", err)
	}
	if !found {
		t.Fatal("expected root instance")
	}
	if got := strings.TrimSpace(root.CurrentState); got != "idle" {
		t.Fatalf("root current_state = %q, want idle without subject-link back-propagation", got)
	}

	var emitted []string
	rows, err := db.QueryContext(context.Background(), `SELECT event_name FROM events ORDER BY created_at ASC, event_id ASC`)
	if err != nil {
		t.Fatalf("query events: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan event: %v", err)
		}
		emitted = append(emitted, strings.TrimSpace(name))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate events: %v", err)
	}
	if contains(emitted, "child/step.result") {
		t.Fatalf("events = %v, do not want child/step.result", emitted)
	}
	if contains(emitted, "pipeline.complete") {
		t.Fatalf("events = %v, do not want pipeline.complete without subject-link back-propagation", emitted)
	}
}

func contains(items []string, want string) bool {
	for _, item := range items {
		if strings.TrimSpace(item) == strings.TrimSpace(want) {
			return true
		}
	}
	return false
}

func TestEventBusPublish_MixedEmptyAndTargetedNodeRoutesExecuteAndSettle(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := eventBusTestRunContext(t, db)
	pg := &store.PostgresStore{DB: db}
	module, bundle := mixedNodeRouteWorkflowModule(t)
	const eventType = "child/child.start"
	const rootEntityID = "11111111-1111-1111-1111-222222222222"
	const childEntityID = "11111111-1111-1111-1111-333333333333"
	emptyRoute := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "project-observer",
	}
	targetRoute := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "child-intake",
		Target: events.RouteIdentity{
			FlowInstance: "child",
			EntityID:     childEntityID,
		},
	}

	var pc *runtimepipeline.PipelineCoordinator
	eb, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		ContractBundle: semanticview.Wrap(bundle),
		RecipientPlanMaterializer: func(context.Context, events.Event, runtimebus.PublishRecipientPlan) ([]events.DeliveryRoute, error) {
			return []events.DeliveryRoute{emptyRoute, targetRoute}, nil
		},
		InterceptorProvider: func() []runtimebus.EventInterceptor {
			if pc == nil {
				return nil
			}
			return []runtimebus.EventInterceptor{pc}
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	pc = runtimepipeline.NewPipelineCoordinatorWithOptions(eb, db, runtimepipeline.PipelineCoordinatorOptions{
		Module:                  module,
		EventReceiptsCapability: pg.CanonicalEventReceiptsCapability,
	})
	if _, ok := any(pc).(runtimebus.DeliveryRouteInterceptor); !ok {
		t.Fatal("PipelineCoordinator does not implement DeliveryRouteInterceptor")
	}
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	for _, instance := range []runtimepipeline.WorkflowInstance{
		{
			InstanceID:      rootEntityID,
			StorageRef:      rootEntityID,
			WorkflowName:    "mixed-route",
			WorkflowVersion: "v-test",
			CurrentState:    "active",
		},
		{
			InstanceID:      childEntityID,
			StorageRef:      "child",
			WorkflowName:    "child",
			WorkflowVersion: "v-test",
			CurrentState:    "active",
		},
	} {
		if err := workflowStore.Upsert(ctx, instance); err != nil {
			t.Fatalf("seed workflow instance %s: %v", instance.InstanceID, err)
		}
	}

	live := eb.SubscribeInternal("workflow-runtime", events.EventType(eventType))
	defer eb.Unsubscribe("workflow-runtime")
	evt := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType(eventType),
		"source",
		"",
		[]byte(`{"entity_id":"`+rootEntityID+`"}`),
		0,
		eventBusTestRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, rootEntityID),
		time.Now().UTC(),
	)
	plan, err := eb.CheckPublishRecipientPlan(ctx, evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if !deliveryRoutesContain(plan.DeliveryRoutes, emptyRoute) || !deliveryRoutesContain(plan.DeliveryRoutes, targetRoute) {
		t.Fatalf("delivery routes = %#v, want empty route %#v and target route %#v", plan.DeliveryRoutes, emptyRoute, targetRoute)
	}
	if err := eb.Publish(ctx, evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := eb.WaitForQuiescence(ctx); err != nil {
		t.Fatalf("WaitForQuiescence: %v", err)
	}
	assertNodeDeliveryStatus(t, db, evt.ID(), emptyRoute.SubscriberID, "delivered")
	assertNodeDeliveryStatus(t, db, evt.ID(), targetRoute.SubscriberID, "delivered")
	select {
	case got := <-live:
		t.Fatalf("consumed mixed node route event leaked to workflow-runtime carrier: %#v", got)
	default:
	}
}

func mixedNodeRouteWorkflowModule(t *testing.T) (runtimepipeline.WorkflowModule, *runtimecontracts.WorkflowContractBundle) {
	t.Helper()
	handler := runtimecontracts.SystemNodeEventHandler{
		Rules: []runtimecontracts.HandlerRuleEntry{{
			ID:        "accept",
			Condition: "true",
		}},
	}
	child := runtimecontracts.FlowContractView{
		Path:  "child",
		Paths: runtimecontracts.FlowContractPaths{ID: "child", Flow: "child"},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"child.start": {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"child-intake": {
				ID:            "child-intake",
				ExecutionType: "system_node",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"child.start": handler,
				},
			},
		},
	}
	root := runtimecontracts.FlowContractView{
		Path:  "",
		Paths: runtimecontracts.FlowContractPaths{ID: "mixed-route"},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"child/child.start": {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"project-observer": {
				ID:            "project-observer",
				ExecutionType: "system_node",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"child/child.start": handler,
				},
			},
		},
		Children: []runtimecontracts.FlowContractView{child},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:    "mixed-route",
			Version: "v-test",
			EffectiveNodes: map[string]runtimecontracts.SystemNodeEffectiveSemantics{
				"project-observer": {ID: "project-observer", ExecutionType: "system_node"},
				"child-intake":     {ID: "child-intake", ExecutionType: "system_node"},
			},
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"project-observer": {
					"child/child.start": handler,
				},
				"child-intake": {
					"child.start": handler,
				},
			},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"mixed-route": &root,
				"child":       &root.Children[0],
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"mixed-route": {},
			"child":       {},
		},
	}
	source := semanticview.Wrap(bundle)
	return &fixtureWorkflowModule{
		source: source,
		workflow: runtimepipeline.NewWorkflowDefinition("mixed-route", []runtimepipeline.WorkflowStage{
			{Name: "active"},
		}, nil),
		workflowNodes: []runtimepipeline.WorkflowNode{
			{
				ID:            "project-observer",
				Subscriptions: []events.EventType{"child/child.start"},
				Policies: map[string]runtimepipeline.WorkflowEventPolicy{
					"child/child.start": {Consume: true},
				},
			},
			{
				ID:            "child-intake",
				Subscriptions: []events.EventType{"child/child.start"},
				Policies: map[string]runtimepipeline.WorkflowEventPolicy{
					"child/child.start": {Consume: true},
				},
			},
		},
		guardRegistry:  runtimepipeline.NewContractGuardRegistry(source),
		actionRegistry: runtimepipeline.NewContractActionRegistry(source),
	}, bundle
}

func assertNodeDeliveryStatus(t *testing.T, db *sql.DB, eventID, nodeID, want string) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = $2
		  AND status = $3
	`, eventID, nodeID, want).Scan(&count); err != nil {
		t.Fatalf("query delivery status for %s: %v", nodeID, err)
	}
	if count != 1 {
		rows, err := db.QueryContext(context.Background(), `
			SELECT subscriber_id, COALESCE(status, ''), COALESCE(reason_code, ''), COALESCE(delivery_target_route::text, '')
			FROM event_deliveries
			WHERE event_id = $1::uuid
			ORDER BY subscriber_id, delivery_target_route::text
		`, eventID)
		if err != nil {
			t.Fatalf("delivery rows for %s status %q = %d, want 1; dump query failed: %v", nodeID, want, count, err)
		}
		defer rows.Close()
		dump := make([]string, 0)
		for rows.Next() {
			var subscriber, status, reason, target string
			if err := rows.Scan(&subscriber, &status, &reason, &target); err != nil {
				t.Fatalf("scan delivery dump: %v", err)
			}
			dump = append(dump, subscriber+" status="+status+" reason="+reason+" target="+target)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate delivery dump: %v", err)
		}
		deadLetters := make([]string, 0)
		deadRows, err := db.QueryContext(context.Background(), `
			SELECT COALESCE(handler_node, ''), COALESCE(failure->>'class', ''), COALESCE(failure->'detail'->>'code', '')
			FROM dead_letters
			WHERE original_event_id = $1::uuid
			ORDER BY created_at ASC
		`, eventID)
		if err == nil {
			defer deadRows.Close()
			for deadRows.Next() {
				var node, failure, message string
				if err := deadRows.Scan(&node, &failure, &message); err != nil {
					t.Fatalf("scan dead letter dump: %v", err)
				}
				deadLetters = append(deadLetters, node+" failure="+failure+" detail="+message)
			}
			if err := deadRows.Err(); err != nil {
				t.Fatalf("iterate dead letter dump: %v", err)
			}
		}
		t.Fatalf("delivery rows for %s status %q = %d, want 1; rows=%v dead_letters=%v", nodeID, want, count, dump, deadLetters)
	}
}

func TestEventBusPublish_NestedThreeLevelChain_FromRootStartCompletesWithoutChildContinuation(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-nested-three-levels")
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	module := newFixtureWorkflowModule(t, bundle)
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg := &store.PostgresStore{DB: db}
	var pc *runtimepipeline.PipelineCoordinator
	eb, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		ContractBundle: semanticview.Wrap(bundle),
		InterceptorProvider: func() []runtimebus.EventInterceptor {
			if pc == nil {
				return nil
			}
			return []runtimebus.EventInterceptor{pc}
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	pc = runtimepipeline.NewPipelineCoordinatorWithOptions(eb, db, runtimepipeline.PipelineCoordinatorOptions{
		Module:                  module,
		EventReceiptsCapability: pg.CanonicalEventReceiptsCapability,
	})
	if pc == nil {
		t.Fatal("expected coordinator")
	}

	const rootEntityID = "11111111-1111-1111-1111-111111111111"
	ctx := eventBusTestRunContext(t, db)
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	if err := workflowStore.Upsert(ctx, runtimepipeline.WorkflowInstance{
		InstanceID:      rootEntityID,
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "idle",
	}); err != nil {
		t.Fatalf("seed root instance: %v", err)
	}

	if err := eb.Publish(ctx, eventtest.RootIngress(
		"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
		events.EventType("pipeline.start"),
		"cataloge2e",
		"",
		[]byte(`{"entity_id":"`+rootEntityID+`"}`),
		0,
		eventBusTestRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, rootEntityID),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := eb.WaitForQuiescence(ctx); err != nil {
		t.Fatalf("WaitForQuiescence: %v", err)
	}

	root, found, err := workflowStore.Load(ctx, rootEntityID)
	if err != nil {
		t.Fatalf("load root instance: %v", err)
	}
	if !found {
		t.Fatal("expected root instance")
	}
	if got := strings.TrimSpace(root.CurrentState); got != "idle" {
		rows, _ := db.QueryContext(context.Background(), `SELECT event_name, COALESCE(entity_id::text,''), COALESCE(flow_instance,'') FROM events ORDER BY created_at ASC, event_id ASC`)
		dump := make([]string, 0)
		if rows != nil {
			defer rows.Close()
			for rows.Next() {
				var name, entityID, flowInstance string
				if scanErr := rows.Scan(&name, &entityID, &flowInstance); scanErr == nil {
					dump = append(dump, name+" entity="+entityID+" flow="+flowInstance)
				}
			}
		}
		instances, _ := workflowStore.List(ctx)
		t.Fatalf("root current_state = %q, want idle without subject-link back-propagation; events=%v instances=%#v", got, dump, instances)
	}

	instances, err := workflowStore.List(ctx)
	if err != nil {
		t.Fatalf("list workflow instances: %v", err)
	}
	var (
		childState      string
		grandchildState string
	)
	var emitted []string
	rows, err := db.QueryContext(context.Background(), `SELECT event_name FROM events ORDER BY created_at ASC, event_id ASC`)
	if err != nil {
		t.Fatalf("query events: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan event: %v", err)
		}
		emitted = append(emitted, strings.TrimSpace(name))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate events: %v", err)
	}
	for _, instance := range instances {
		switch strings.TrimSpace(instance.WorkflowName) {
		case "child":
			childState = strings.TrimSpace(instance.CurrentState)
		case "grandchild":
			grandchildState = strings.TrimSpace(instance.CurrentState)
		}
	}
	if childState != "completed" {
		t.Fatalf("child current_state = %q, want completed", childState)
	}
	if grandchildState != "finished" {
		t.Fatalf("grandchild current_state = %q, want finished", grandchildState)
	}
	if contains(emitted, "child/step.result") {
		t.Fatalf("events = %v, do not want child/step.result", emitted)
	}
	if contains(emitted, "pipeline.complete") {
		t.Fatalf("events = %v, do not want pipeline.complete without subject-link back-propagation", emitted)
	}
}

func TestEventBusPublish_GatedChildFlowCompletionAdvancesRoot(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-gates-in-child-flow")
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	module := newFixtureWorkflowModule(t, bundle)
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg := &store.PostgresStore{DB: db}
	var pc *runtimepipeline.PipelineCoordinator
	eb, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		ContractBundle: semanticview.Wrap(bundle),
		InterceptorProvider: func() []runtimebus.EventInterceptor {
			if pc == nil {
				return nil
			}
			return []runtimebus.EventInterceptor{pc}
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	pc = runtimepipeline.NewPipelineCoordinatorWithOptions(eb, db, runtimepipeline.PipelineCoordinatorOptions{
		Module:                  module,
		EventReceiptsCapability: pg.CanonicalEventReceiptsCapability,
	})
	if pc == nil {
		t.Fatal("expected coordinator")
	}

	const rootEntityID = "11111111-1111-1111-1111-111111111111"
	ctx := eventBusTestRunContext(t, db)
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	if err := workflowStore.Upsert(ctx, runtimepipeline.WorkflowInstance{
		InstanceID:      rootEntityID,
		StorageRef:      rootEntityID,
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "pending",
		Metadata: map[string]any{
			"entity_id": rootEntityID,
		},
	}); err != nil {
		t.Fatalf("seed root instance: %v", err)
	}

	if err := eb.Publish(ctx, eventtest.RootIngress(
		"11111111-2222-3333-4444-555555555555",
		events.EventType("validate.requested"),
		"cataloge2e",
		"",
		[]byte(`{"entity_id":"`+rootEntityID+`"}`),
		0,
		eventBusTestRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, rootEntityID),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := eb.WaitForQuiescence(ctx); err != nil {
		t.Fatalf("WaitForQuiescence: %v", err)
	}

	root, found, err := workflowStore.Load(ctx, rootEntityID)
	if err != nil {
		t.Fatalf("load root instance: %v", err)
	}
	if !found {
		t.Fatal("expected root instance")
	}
	if got := strings.TrimSpace(root.CurrentState); got != "pending" {
		rows, _ := db.QueryContext(context.Background(), `SELECT event_name, COALESCE(entity_id::text,''), COALESCE(flow_instance,''), COALESCE(payload::text,'') FROM events ORDER BY created_at ASC, event_id ASC`)
		dump := make([]string, 0)
		if rows != nil {
			defer rows.Close()
			for rows.Next() {
				var name, entityID, flowInstance, payload string
				if scanErr := rows.Scan(&name, &entityID, &flowInstance, &payload); scanErr == nil {
					dump = append(dump, name+" entity="+entityID+" flow="+flowInstance+" payload="+payload)
				}
			}
		}
		instances, _ := workflowStore.List(ctx)
		t.Fatalf("root current_state = %q, want pending without subject-link back-propagation; root metadata=%#v events=%v instances=%#v", got, root.Metadata, dump, instances)
	}
}

func TestEventBusPublish_RecordsNestedDescendantLocalizedEvent(t *testing.T) {
	grandchild := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "grandchild", Flow: "grandchild"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{Events: []string{"micro.done"}},
			},
		},
		Path: "child/grandchild",
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"micro.done": {},
		},
	}
	child := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "child", Flow: "child"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{Events: []string{"step.result"}},
			},
		},
		Path: "child",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"child-aggregator": {
				ID:           "child-aggregator",
				SubscribesTo: []string{"grandchild/micro.done"},
			},
		},
		Children: []runtimecontracts.FlowContractView{grandchild},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{child}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"child":      &root.Children[0],
				"grandchild": &root.Children[0].Children[0],
			},
		},
	}
	eb, err := runtimebus.NewEventBusWithOptions(newRouteSetEventStore(), runtimebus.EventBusOptions{
		ContractBundle: semanticview.Wrap(bundle),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	eb.Subscribe("child-aggregator")
	defer eb.Unsubscribe("child-aggregator")
	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx := runtimebus.WithEmittedEventsRecorder(context.Background(), recorder)
	if err := eb.Publish(ctx, eventtest.RootIngress("", "child/grandchild/micro.done", "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-grandchild"), time.Time{})); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	diags := recorder.SnapshotPublishes()
	if len(diags) != 1 || len(diags[0].RoutedRecipients) != 1 {
		t.Fatalf("publish diagnostics = %#v", diags)
	}
	if got := diags[0].RoutedRecipients[0].LocalizedEvent; got != "grandchild/micro.done" {
		t.Fatalf("localized_event = %q, want grandchild/micro.done", got)
	}
}

func TestEventBusPublish_RecordsNestedTemplateInstanceLocalizedEvent(t *testing.T) {
	grandchild := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "grandchild", Flow: "grandchild"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: "template",
		},
		Path: "child/grandchild",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"worker": {
				ID:           "worker-{instance_id}",
				SubscribesTo: []string{"micro.done"},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"micro.done": {},
		},
	}
	child := runtimecontracts.FlowContractView{
		Paths:    runtimecontracts.FlowContractPaths{ID: "child", Flow: "child"},
		Path:     "child",
		Children: []runtimecontracts.FlowContractView{grandchild},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{child}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"child":      &root.Children[0],
				"grandchild": &root.Children[0].Children[0],
			},
		},
	}
	eb, err := runtimebus.NewEventBusWithOptions(newRouteSetEventStore(), runtimebus.EventBusOptions{
		ContractBundle: semanticview.Wrap(bundle),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("child/grandchild", "inst-1")}); err != nil {
		t.Fatalf("AddFlowInstance: %v", err)
	}
	eb.Subscribe("worker-inst-1")
	defer eb.Unsubscribe("worker-inst-1")
	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx := runtimebus.WithEmittedEventsRecorder(context.Background(), recorder)
	if err := eb.Publish(ctx, eventtest.RootIngress(
		"",
		"child/grandchild/inst-1/micro.done",
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-grandchild"),
		time.Time{},
	)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	diags := recorder.SnapshotPublishes()
	if len(diags) != 1 || len(diags[0].RoutedRecipients) != 1 {
		t.Fatalf("publish diagnostics = %#v", diags)
	}
	if got := diags[0].RoutedRecipients[0].LocalizedEvent; got != "micro.done" {
		t.Fatalf("localized_event = %q, want micro.done", got)
	}
}
