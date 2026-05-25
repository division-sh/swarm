package bus_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimeactors "swarm/internal/runtime/core/actors"
	runtimeflowidentity "swarm/internal/runtime/core/flowidentity"
	runtimeownership "swarm/internal/runtime/core/ownership"
	runtimecorrelation "swarm/internal/runtime/correlation"
	"swarm/internal/runtime/diaglog"
	"swarm/internal/runtime/flowmodel"
	runtimeingress "swarm/internal/runtime/ingress"
	runtimemanager "swarm/internal/runtime/manager"
	runtimepipeline "swarm/internal/runtime/pipeline"
	runtimereplayclaim "swarm/internal/runtime/replayclaim"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/store"
	"swarm/internal/testutil"
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
	switch evt.Type {
	case events.EventType("custom.root"):
		next = "custom.middle"
	case events.EventType("custom.middle"):
		next = "custom.leaf"
	case events.EventType("custom.leaf"):
		next = "custom.final"
	default:
		return true, nil, nil
	}
	return false, []events.Event{(events.Event{
		Type:      events.EventType(next),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID(evt.EntityID())}, nil
}

type singleDeferredInterceptor struct{}

func (singleDeferredInterceptor) Intercept(_ context.Context, evt events.Event) (bool, []events.Event, error) {
	if evt.Type != events.EventType("custom.root") {
		return true, nil, nil
	}
	return false, []events.Event{(events.Event{
		Type:      events.EventType("custom.middle"),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID(evt.EntityID())}, nil
}

type eventVisibleInTxInterceptor struct {
	t       *testing.T
	eventID string
}

func (i eventVisibleInTxInterceptor) Intercept(ctx context.Context, _ events.Event) (bool, []events.Event, error) {
	i.t.Helper()
	tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx)
	if !ok || tx == nil {
		i.t.Fatal("expected transactional publish context to expose sql tx")
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id = $1::uuid`, i.eventID).Scan(&count); err != nil {
		i.t.Fatalf("query inbound event inside interceptor tx: %v", err)
	}
	if count != 1 {
		i.t.Fatalf("inbound event visible inside interceptor tx count=%d, want 1", count)
	}
	return true, nil, nil
}

type postCommitTxAbsentInterceptor struct {
	t       *testing.T
	store   *store.PostgresStore
	eventID string
	called  chan struct{}
}

func (i postCommitTxAbsentInterceptor) Intercept(ctx context.Context, _ events.Event) (bool, []events.Event, error) {
	i.t.Helper()
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		i.t.Fatal("transactional PublishTx interceptor ran with caller sql tx still in context")
	}
	ok, err := i.store.EventExists(ctx, i.eventID)
	if err != nil {
		i.t.Fatalf("EventExists(%s): %v", i.eventID, err)
	}
	if !ok {
		i.t.Fatalf("expected event %s to be committed before interceptor ran", i.eventID)
	}
	select {
	case i.called <- struct{}{}:
	default:
	}
	return true, nil, nil
}

type deferredEventVisibleInterceptor struct {
	t        *testing.T
	store    *store.PostgresStore
	eventID  string
	checkFor events.EventType
}

func (i deferredEventVisibleInterceptor) Intercept(ctx context.Context, evt events.Event) (bool, []events.Event, error) {
	i.t.Helper()
	if evt.Type == events.EventType("custom.root") {
		return false, []events.Event{{
			ID:        i.eventID,
			Type:      i.checkFor,
			CreatedAt: time.Now().UTC(),
			Payload:   []byte(`{"entity_id":"ent-1"}`),
		}}, nil
	}
	if evt.Type != i.checkFor {
		return true, nil, nil
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
}

func (h *recordingLoggerHook) Log(ctx context.Context, _ diaglog.Level, _, _, action, _, _, _, _, _ string, _ map[string]string, detail any, _ string, _ int) error {
	lineage, ok := runtimecorrelation.RuntimeLineageFromContext(ctx)
	h.entries = append(h.entries, recordedLogEntry{Action: action, Detail: detail, Lineage: lineage, HasLineage: ok})
	return nil
}

type descriptorAwareEventStore struct {
	mu          sync.Mutex
	descriptors []runtimebus.ActiveAgentDescriptor
	deliveries  []string
	listErr     error
}

type replayCapableAtomicStoreMissingScope struct {
	mu         sync.Mutex
	deliveries []string
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
	return s.InsertEventDeliveries(ctx, evt.ID, agentIDs)
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

func waitForNoEvent(t *testing.T, ch <-chan events.Event) {
	t.Helper()
	select {
	case evt := <-ch:
		t.Fatalf("unexpected event delivered: %#v", evt)
	case <-time.After(25 * time.Millisecond):
	}
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
			ID:     agentID,
			Role:   "observer",
			Mode:   "global",
			Type:   "stub",
			Config: []byte(`{}`),
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

	if err := eb.Publish(ctx, events.Event{
		ID:          eventID,
		RunID:       eventBusTestRunID,
		Type:        events.EventType("custom.receipt_failure"),
		SourceAgent: "api.v1",
		Payload:     []byte(`{"entity_id":"21000000-0000-0000-0000-000000000012"}`),
		CreatedAt:   time.Now().UTC(),
	}.WithEntityID("21000000-0000-0000-0000-000000000012")); err != nil {
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
	select {
	case got := <-ch:
		if got.ID != eventID {
			t.Fatalf("delivered event = %s, want %s", got.ID, eventID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delivered event")
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

	if err := eb.Publish(ctx, events.Event{
		ID:          eventID,
		RunID:       eventBusTestRunID,
		Type:        events.EventType("custom.completion_failure"),
		SourceAgent: "api.v1",
		Payload:     []byte(`{"entity_id":"21000000-0000-0000-0000-000000000022"}`),
		CreatedAt:   time.Now().UTC(),
	}.WithEntityID("21000000-0000-0000-0000-000000000022")); err != nil {
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
	outcome, errText := loadPipelineReceiptOutcomeAndError(t, ctx, db, eventID)
	if outcome != "dead_letter" || !strings.Contains(errText, "simulated normal-run completion failure") {
		t.Fatalf("pipeline receipt outcome=%q error=%q, want dead_letter with completion failure", outcome, errText)
	}
	if !hasRuntimeLogAction(logger.entries, "publish_post_commit_convergence_failed") {
		t.Fatalf("logger entries = %#v, want publish_post_commit_convergence_failed", logger.entries)
	}
	select {
	case got := <-ch:
		if got.ID != eventID {
			t.Fatalf("delivered event = %s, want %s", got.ID, eventID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delivered event")
	}
}

type failStandalonePipelineReceiptOnceStore struct {
	*store.PostgresStore
	err error
}

func (s *failStandalonePipelineReceiptOnceStore) UpsertPipelineReceipt(ctx context.Context, eventID, status, errText string) error {
	return s.UpsertPipelineReceiptTx(ctx, nil, eventID, status, errText)
}

func (s *failStandalonePipelineReceiptOnceStore) UpsertPipelineReceiptTx(ctx context.Context, tx *sql.Tx, eventID, status, errText string) error {
	if tx == nil && s.err != nil {
		err := s.err
		s.err = nil
		return err
	}
	return s.PostgresStore.UpsertPipelineReceiptTx(ctx, tx, eventID, status, errText)
}

type failNormalRunCompletionStore struct {
	*store.PostgresStore
	err error
}

func (s *failNormalRunCompletionStore) ConvergeNormalRunCompletion(context.Context, string, []string, map[string][]string) error {
	return s.err
}

func loadPipelineReceiptOutcomeAndError(t *testing.T, ctx context.Context, db *sql.DB, eventID string) (string, string) {
	t.Helper()
	var outcome, errText string
	if err := db.QueryRowContext(ctx, `
		SELECT outcome, COALESCE(side_effects->>'error', '')
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, eventID).Scan(&outcome, &errText); err != nil {
		t.Fatalf("load pipeline receipt for %s: %v", eventID, err)
	}
	return outcome, errText
}

func containsMissingPipelineReceiptEvent(items []events.PersistedReplayEvent, eventID string) bool {
	for _, evt := range items {
		if strings.TrimSpace(evt.Event.ID) == strings.TrimSpace(eventID) {
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

	evt := events.Event{
		ID:        uuid.NewString(),
		Type:      events.EventType("task.requested"),
		Payload:   []byte(`{"entity_id":"ent-1"}`),
		CreatedAt: time.Now().UTC(),
	}.WithEntityID("ent-1")
	if err := bus.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delivered event")
	}

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

	if err := bus.Publish(ctx, events.Event{
		ID:        eventID,
		Type:      events.EventType("task.requested"),
		RunID:     runID,
		Payload:   []byte(`{"entity_id":"ent-1"}`),
		CreatedAt: time.Now().UTC(),
	}.WithEntityID("ent-1")); err != nil {
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

func TestEventBusPublish_UsesPayloadValidator(t *testing.T) {
	eb, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		PayloadValidator: func(eventType string, payload []byte) error {
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
	if err := eb.Publish(context.Background(), events.Event{
		Type:    "task.completed",
		Payload: []byte(`{"ok":true}`),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

func TestEventBusPublish_PayloadValidatorFailureAbortsPublish(t *testing.T) {
	eb, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		PayloadValidator: func(string, []byte) error {
			return context.DeadlineExceeded
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	err = eb.Publish(context.Background(), events.Event{
		Type:    "task.completed",
		Payload: []byte(`{}`),
	})
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

	err = eb.Publish(context.Background(), events.Event{
		ID:        uuid.NewString(),
		Type:      events.EventType("custom.replay.checked"),
		Payload:   []byte(`{"ok":true}`),
		CreatedAt: time.Now().UTC(),
	})
	if !errors.Is(err, runtimereplayclaim.ErrMissingCommittedReplayScope) {
		t.Fatalf("Publish error = %v, want missing committed replay scope", err)
	}
}

func TestEventBusPublishDirect_PayloadValidatorFailureAbortsPublish(t *testing.T) {
	eb, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		PayloadValidator: func(string, []byte) error {
			return context.DeadlineExceeded
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	err = eb.PublishDirect(context.Background(), events.Event{
		Type:    "task.completed",
		Payload: []byte(`{}`),
	}, []string{"agent-a"})
	if err == nil || !errors.Is(err, runtimebus.ErrPayloadValidation) {
		t.Fatalf("expected payload validator failure, got %v", err)
	}
}

func TestEventBusCheckDirectRecipients_PayloadValidatorFailureAbortsBeforeRecipientPlanning(t *testing.T) {
	eb, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		PayloadValidator: func(string, []byte) error {
			return context.DeadlineExceeded
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}

	status, err := eb.CheckDirectRecipients(context.Background(), events.Event{
		Type:    "task.completed",
		Payload: []byte(`{}`),
	}, []string{"agent-a"})
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

	err = eb.PublishDirect(context.Background(), (events.Event{
		ID:        uuid.NewString(),
		Type:      events.EventType("custom.direct"),
		Payload:   []byte(`{"entity_id":"ent-1"}`),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID("ent-1"), []string{"agent-a"})
	if err == nil || !strings.Contains(err.Error(), "authoritative delivery incomplete") {
		t.Fatalf("PublishDirect missing recipient error = %v, want authoritative delivery incomplete", err)
	}
	assertSortedStringsEqual(t, store.persistedDeliveries(), []string{"agent-a"})
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

	err = eb.PublishDirect(context.Background(), events.Event{
		Type:      events.EventType("custom.direct"),
		Payload:   []byte(`{"entity_id":"ent-1"}`),
		CreatedAt: time.Now().UTC(),
	}.WithEntityID("ent-1"), []string{"control-plane", "reviewer-ent-1", "reviewer-ent-2", "missing-agent"})
	if err != nil {
		t.Fatalf("PublishDirect: %v", err)
	}

	select {
	case evt := <-controlCh:
		if got := evt.EntityID(); got != "ent-1" {
			t.Fatalf("control event entity_id = %q, want ent-1", got)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("control-plane did not receive direct event")
	}
	select {
	case evt := <-matchCh:
		if got := evt.EntityID(); got != "ent-1" {
			t.Fatalf("matched event entity_id = %q, want ent-1", got)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("matching entity-scoped reviewer did not receive direct event")
	}
	waitForNoEvent(t, otherCh)

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

	if err := eb.Publish(context.Background(), events.Event{
		Type:      events.EventType("custom.trigger"),
		Payload:   []byte(`{"entity_id":"ent-1"}`),
		CreatedAt: time.Now().UTC(),
	}.WithEntityID("ent-1")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case evt := <-controlCh:
		if got := evt.EntityID(); got != "ent-1" {
			t.Fatalf("control event entity_id = %q, want ent-1", got)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("control-plane did not receive event")
	}
	select {
	case evt := <-matchCh:
		if got := evt.EntityID(); got != "ent-1" {
			t.Fatalf("matched event entity_id = %q, want ent-1", got)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("entity-scoped reviewer did not receive matching event")
	}
	waitForNoEvent(t, otherCh)

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

	err = eb.Publish(context.Background(), (events.Event{
		Type:      events.EventType("custom.trigger"),
		Payload:   []byte(`{"entity_id":"ent-2"}`),
		CreatedAt: time.Now().UTC(),
	}).WithEnvelope(events.EventEnvelope{EntityID: "ent-1"}))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case evt := <-matchCh:
		if got := evt.EntityID(); got != "ent-1" {
			t.Fatalf("matched event entity_id = %q, want ent-1", got)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("entity-scoped reviewer did not receive typed-envelope match")
	}
	waitForNoEvent(t, otherCh)
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

	if err := eb.Publish(context.Background(), events.Event{
		Type:      events.EventType("custom.trigger"),
		Payload:   []byte(`{"entity_id":"ent-1"}`),
		CreatedAt: time.Now().UTC(),
	}.WithEntityID("ent-1")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-controlCh:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("control-plane did not receive event")
	}
	waitForNoEvent(t, missingCh)

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

	if err := eb.Publish(context.Background(), events.Event{
		Type:      events.EventType("custom.trigger"),
		Payload:   []byte(`{"entity_id":"ent-1"}`),
		CreatedAt: time.Now().UTC(),
	}.WithEntityID("ent-1")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case evt := <-workflowCh:
		if got := evt.EntityID(); got != "ent-1" {
			t.Fatalf("workflow-runtime event entity_id = %q, want ent-1", got)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("workflow-runtime did not receive event")
	}
	select {
	case evt := <-nodeCh:
		if got := evt.EntityID(); got != "ent-1" {
			t.Fatalf("system node event entity_id = %q, want ent-1", got)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("system node did not receive event")
	}
	select {
	case evt := <-agentCh:
		if got := evt.EntityID(); got != "ent-1" {
			t.Fatalf("agent event entity_id = %q, want ent-1", got)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("agent did not receive event")
	}
	waitForNoEvent(t, missingCh)

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

	if err := eb.Publish(context.Background(), (events.Event{
		Type:      events.EventType("custom.root"),
		Payload:   []byte(`{"entity_id":"ent-1"}`),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID("ent-1")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case evt := <-workflowCh:
		if got := evt.EntityID(); got != "ent-1" {
			t.Fatalf("workflow-runtime event entity_id = %q, want ent-1", got)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("workflow-runtime did not receive deferred event")
	}
	select {
	case evt := <-agentCh:
		if got := evt.EntityID(); got != "ent-1" {
			t.Fatalf("agent event entity_id = %q, want ent-1", got)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("agent did not receive deferred event")
	}
	waitForNoEvent(t, otherCh)

	assertSortedStringsEqual(t, store.persistedDeliveries(), []string{"agent-a"})
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

	err = eb.Publish(context.Background(), events.Event{
		Type:      events.EventType("custom.trigger"),
		Payload:   []byte(`{"entity_id":"ent-1"}`),
		CreatedAt: time.Now().UTC(),
	}.WithEntityID("ent-1"))
	if err == nil || !strings.Contains(err.Error(), "descriptor lookup failed") {
		t.Fatalf("Publish error = %v, want descriptor lookup failure", err)
	}
	waitForNoEvent(t, ch)
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
		publishDone <- eb.Publish(context.Background(), events.Event{Type: "task.completed", Payload: []byte(`{}`)})
	}()

	select {
	case <-started:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("interceptor did not start")
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := eb.WaitForQuiescence(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitForQuiescence error = %v, want deadline exceeded while publish is blocked", err)
	}

	close(release)
	select {
	case err := <-publishDone:
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("publish did not finish")
	}

	waitCtx, cancel = context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if err := eb.WaitForQuiescence(waitCtx); err != nil {
		t.Fatalf("WaitForQuiescence after publish completion: %v", err)
	}
}

func TestEventBusPublish_InterceptsMultiHopDeferredChains(t *testing.T) {
	store := &recordingEventStore{}
	eb, err := runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{deferredChainInterceptor{}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.Publish(context.Background(), (events.Event{
		Type:      events.EventType("custom.root"),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID("ent-1")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := store.eventTypes(); len(got) < 4 || got[0] != "custom.root" || got[1] != "custom.middle" || got[2] != "custom.leaf" || got[3] != "custom.final" {
		t.Fatalf("persisted event types prefix = %v, want prefix [custom.root custom.middle custom.leaf custom.final]", got)
	}
}

func TestEventBusPublishTransactional_PersistsInboundEventBeforeInterceptorsRun(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	eventID := "11111111-1111-1111-1111-111111111111"
	eb, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{eventVisibleInTxInterceptor{t: t, eventID: eventID}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.Publish(context.Background(), events.Event{
		ID:        eventID,
		Type:      events.EventType("task.completed"),
		CreatedAt: time.Now().UTC(),
		Payload:   []byte(`{}`),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

func TestEventBusPublishTxRunsInterceptorsAfterCallerCommit(t *testing.T) {
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
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	postCommitActions := make([]func(), 0, 1)
	txctx := runtimepipeline.WithPipelinePostCommitActions(ctx, &postCommitActions)
	if err := eb.PublishTx(txctx, tx, events.Event{
		ID:          eventID,
		RunID:       eventBusTestRunID,
		Type:        events.EventType("custom.publish_tx_post_commit"),
		SourceAgent: "api.v1",
		Payload:     []byte(`{"entity_id":"11111111-1111-1111-1111-111111111113"}`),
		CreatedAt:   time.Now().UTC(),
	}.WithEntityID("11111111-1111-1111-1111-111111111113")); err != nil {
		_ = tx.Rollback()
		t.Fatalf("PublishTx: %v", err)
	}
	select {
	case <-called:
		_ = tx.Rollback()
		t.Fatal("interceptor ran before caller committed")
	default:
	}
	if len(postCommitActions) != 1 {
		_ = tx.Rollback()
		t.Fatalf("post-commit actions = %d, want 1", len(postCommitActions))
	}
	ok, err := pg.EventExists(ctx, eventID)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("EventExists before commit: %v", err)
	}
	if ok {
		_ = tx.Rollback()
		t.Fatal("event visible outside caller transaction before commit")
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	runtimepipeline.FlushPipelinePostCommitActions(postCommitActions)
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for post-commit interceptor")
	}
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
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	eventID := uuid.NewString()
	targetEntityID := uuid.NewString()
	err = eb.PublishTx(ctx, tx, (events.Event{
		ID:        eventID,
		Type:      events.EventType("child/output.done"),
		CreatedAt: time.Now().UTC(),
		Payload:   []byte(`{}`),
	}).WithTargetRoute(events.RouteIdentity{EntityID: targetEntityID, FlowInstance: "missing-flow"}))
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("PublishTx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	var reason, targetContext string
	if err := db.QueryRowContext(ctx, `
		SELECT target_failure_reason, target_context::text
		FROM dead_letters
		WHERE original_event_id = $1::uuid
		  AND failure_type = 'target_resolution_failed'
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
	if err := eb.Publish(context.Background(), events.Event{
		ID:          uuid.NewString(),
		RunID:       runID,
		Type:        events.EventType("scan.requested"),
		SourceAgent: "test",
		CreatedAt:   time.Now().UTC(),
		Payload:     []byte(`{}`),
	}); err != nil {
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

func TestEventBusPublishDeferred_PersistsInboundEventBeforeInterceptorsRun(t *testing.T) {
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
	if err := eb.Publish(context.Background(), events.Event{
		ID:        "11111111-1111-1111-1111-111111111111",
		Type:      events.EventType("custom.root"),
		CreatedAt: time.Now().UTC(),
		Payload:   []byte(`{"entity_id":"ent-1"}`),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

func TestEventBusPublish_InheritsRunAndParentFromInboundContext(t *testing.T) {
	store := &recordingEventStore{}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ctx := runtimecorrelation.WithInboundEvent(context.Background(), events.Event{
		ID:    "evt-parent",
		Type:  events.EventType("task.started"),
		RunID: "run-abc",
	})
	if err := eb.Publish(ctx, events.Event{
		ID:   "evt-child",
		Type: events.EventType("task.completed"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(store.events) != 1 {
		found := false
		for _, evt := range store.events {
			if evt.ID != "evt-child" {
				continue
			}
			found = true
			if got := evt.RunID; got != "run-abc" {
				t.Fatalf("persisted run_id = %q, want run-abc", got)
			}
			if got := evt.ParentEventID; got != "evt-parent" {
				t.Fatalf("persisted parent_event_id = %q, want evt-parent", got)
			}
		}
		if !found {
			t.Fatalf("persisted events = %#v, want child event", store.events)
		}
		return
	}
	if got := store.events[0].RunID; got != "run-abc" {
		t.Fatalf("persisted run_id = %q, want run-abc", got)
	}
	if got := store.events[0].ParentEventID; got != "evt-parent" {
		t.Fatalf("persisted parent_event_id = %q, want evt-parent", got)
	}
}

func TestEventBusPublish_ZeroRecipientsDoesNotEmitContradiction(t *testing.T) {
	store := &recordingEventStore{}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	if err := eb.Publish(context.Background(), events.Event{
		ID:   "evt-zero",
		Type: events.EventType("custom.no_subscribers"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := store.eventTypes()
	if len(got) != 1 || got[0] != "custom.no_subscribers" {
		t.Fatalf("persisted event types = %v, want [custom.no_subscribers]", got)
	}
}

func TestEventBusPublish_RuntimeLogBypassesContradictionRouting(t *testing.T) {
	store := &recordingEventStore{}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	if err := eb.Publish(context.Background(), events.Event{
		ID:   "evt-log",
		Type: events.EventType("platform.runtime_log"),
	}); err != nil {
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
	}{
		{
			name:      "platform.boot",
			eventID:   "10000000-0000-0000-0000-000000000001",
			eventType: events.EventType("platform.boot"),
		},
		{
			name:      "platform.recovery_failed",
			eventID:   "10000000-0000-0000-0000-000000000002",
			eventType: events.EventType("platform.recovery_failed"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			internal := eb.SubscribeInternal("internal-"+string(tc.eventType), tc.eventType)
			defer eb.Unsubscribe("internal-" + string(tc.eventType))

			if err := eb.Publish(ctx, events.Event{
				ID:          tc.eventID,
				Type:        tc.eventType,
				SourceAgent: "runtime",
				Payload:     []byte(`{}`),
				CreatedAt:   time.Now().UTC(),
			}); err != nil {
				t.Fatalf("Publish(%s): %v", tc.eventType, err)
			}

			select {
			case got := <-internal:
				if got.ID != tc.eventID {
					t.Fatalf("internal delivery event_id = %q, want %q", got.ID, tc.eventID)
				}
			case <-time.After(250 * time.Millisecond):
				t.Fatalf("timed out waiting for internal %s delivery", tc.eventType)
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
	}{
		{
			name:      "manager platform.agent_failed",
			eventID:   "20000000-0000-0000-0000-000000000001",
			eventType: events.EventType("platform.agent_failed"),
		},
		{
			name:      "receipts platform.paused",
			eventID:   "20000000-0000-0000-0000-000000000002",
			eventType: events.EventType("platform.paused"),
		},
		{
			name:      "budget platform.budget_threshold_crossed",
			eventID:   "20000000-0000-0000-0000-000000000003",
			eventType: events.EventType("platform.budget_threshold_crossed"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			subscription := eb.Subscribe(agentID, tc.eventType)
			defer eb.Unsubscribe(agentID)

			if err := eb.Publish(ctx, events.Event{
				ID:          tc.eventID,
				Type:        tc.eventType,
				SourceAgent: "runtime",
				Payload:     []byte(`{}`),
				CreatedAt:   time.Now().UTC(),
			}); err != nil {
				t.Fatalf("Publish(%s): %v", tc.eventType, err)
			}

			select {
			case got := <-subscription:
				if got.ID != tc.eventID {
					t.Fatalf("delivered event_id = %q, want %q", got.ID, tc.eventID)
				}
			case <-time.After(250 * time.Millisecond):
				t.Fatalf("timed out waiting for agent delivery of %s", tc.eventType)
			}

			if got := countEventDeliveriesForEvent(t, ctx, db, tc.eventID); got != 1 {
				t.Fatalf("event_deliveries for %s = %d, want 1", tc.eventType, got)
			}
			if deliveryStatus, runStatus := loadAgentDeliveryForEvent(t, ctx, db, tc.eventID, agentID); deliveryStatus != "pending" || runStatus != "running" {
				t.Fatalf("pre-receipt state for %s = delivery:%q run:%q, want pending/running", tc.eventType, deliveryStatus, runStatus)
			}

			if err := pg.UpsertEventReceipt(ctx, tc.eventID, agentID, runtimemanager.ReceiptStatusProcessed, ""); err != nil {
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

	if _, err := controller.Pause(ctx, runtimeingress.TransitionRequest{
		Reason:       "test_pause",
		ControlledBy: "test",
	}); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	eventID := "21000000-0000-0000-0000-000000000001"
	if err := eb.Publish(ctx, events.Event{
		ID:          eventID,
		RunID:       eventBusTestRunID,
		Type:        eventType,
		SourceAgent: "api.v1",
		Payload:     []byte(`{"entity_id":"21000000-0000-0000-0000-000000000002"}`),
		CreatedAt:   time.Now().UTC(),
	}.WithEntityID("21000000-0000-0000-0000-000000000002")); err != nil {
		t.Fatalf("Publish while paused: %v", err)
	}

	select {
	case got := <-ch:
		t.Fatalf("paused runtime delivered event %s before resume", got.ID)
	case <-time.After(150 * time.Millisecond):
	}
	if got := countEventDeliveriesForEvent(t, ctx, db, eventID); got != 1 {
		t.Fatalf("event deliveries while paused = %d, want 1", got)
	}
	if got := countPipelineReceiptsForEvent(t, ctx, db, eventID); got != 0 {
		t.Fatalf("pipeline receipts while paused = %d, want 0", got)
	}

	resumed, err := controller.Resume(ctx, runtimeingress.TransitionRequest{
		Reason:       "test_resume",
		ControlledBy: "test",
	})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if resumed.ReleasedCount != 1 {
		t.Fatalf("released count = %d, want 1", resumed.ReleasedCount)
	}
	select {
	case got := <-ch:
		if got.ID != eventID {
			t.Fatalf("delivered event = %s, want %s", got.ID, eventID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queued event release")
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

	if err := eb.Publish(context.Background(), events.Event{
		Type:    events.EventType("human_task.approved"),
		Payload: []byte(`{"requesting_agent":"requester"}`),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case evt := <-ch:
		t.Fatalf("unexpected delivery without subscription: %#v", evt)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestEventBusPublish_LogsRoutedAndSubscribedRecipientsSeparately(t *testing.T) {
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
	eb, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
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

	if err := eb.Publish(context.Background(), events.Event{
		Type: events.EventType("producer/scan.requested"),
	}); err != nil {
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
	if len(routed) == 0 || routed[0]["id"] != "scan-orchestrator" {
		t.Fatalf("routed_recipients = %#v, want scan-orchestrator", detail["routed_recipients"])
	}
	if got := routed[0]["matched_pattern"]; got != "producer/scan.requested" {
		t.Fatalf("matched_pattern = %#v, want producer/scan.requested", got)
	}
	if got := routed[0]["route_source"]; got != "pin_auto_wire" {
		t.Fatalf("route_source = %#v, want pin_auto_wire", got)
	}
	if got := routed[0]["localized_event"]; got != "scan.requested" {
		t.Fatalf("localized_event = %#v, want scan.requested", got)
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

func TestEventBusPublish_RecordsPublishDiagnosticsInTurnRecorder(t *testing.T) {
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
	eb, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		ContractBundle: semanticview.Wrap(bundle),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	eb.Subscribe("scan-orchestrator")
	defer eb.Unsubscribe("scan-orchestrator")
	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx := runtimebus.WithEmittedEventsRecorder(context.Background(), recorder)
	if err := eb.Publish(ctx, (events.Event{
		Type: "producer/scan.requested",
	}).WithEntityID("ent-1")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	diags := recorder.SnapshotPublishes()
	if len(diags) != 1 {
		t.Fatalf("publish diagnostics = %#v", diags)
	}
	if diags[0].EventType != "producer/scan.requested" {
		t.Fatalf("event_type = %q", diags[0].EventType)
	}
	if len(diags[0].RoutedRecipients) != 1 {
		t.Fatalf("routed_recipients = %#v", diags[0].RoutedRecipients)
	}
	if diags[0].RoutedRecipients[0].RouteSource != "pin_auto_wire" {
		t.Fatalf("route_source = %q", diags[0].RoutedRecipients[0].RouteSource)
	}
	if diags[0].RoutedRecipients[0].LocalizedEvent != "scan.requested" {
		t.Fatalf("localized_event = %q", diags[0].RoutedRecipients[0].LocalizedEvent)
	}
}

func TestEventBusPublish_NestedDescendantCompletionDoesNotEmitChildContinuation(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-nested-three-levels")
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
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
		Module: module,
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

	if err := eb.Publish(ctx, (events.Event{
		ID:          "11111111-2222-3333-4444-555555555555",
		Type:        events.EventType("child/grandchild/micro.done"),
		SourceAgent: "cataloge2e",
		RunID:       eventBusTestRunID,
		Payload:     []byte(`{"entity_id":"` + grandchildEntityID + `"}`),
		CreatedAt:   time.Now().UTC(),
	}).WithEntityID(grandchildEntityID)); err != nil {
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

func TestEventBusPublish_NestedThreeLevelChain_FromRootStartCompletesWithoutChildContinuation(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-nested-three-levels")
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
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
		Module: module,
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

	if err := eb.Publish(ctx, (events.Event{
		ID:          "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
		Type:        events.EventType("pipeline.start"),
		SourceAgent: "cataloge2e",
		RunID:       eventBusTestRunID,
		Payload:     []byte(`{"entity_id":"` + rootEntityID + `"}`),
		CreatedAt:   time.Now().UTC(),
	}).WithEntityID(rootEntityID)); err != nil {
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
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
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
		Module: module,
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

	if err := eb.Publish(ctx, (events.Event{
		ID:          "11111111-2222-3333-4444-555555555555",
		Type:        events.EventType("validate.requested"),
		SourceAgent: "cataloge2e",
		RunID:       eventBusTestRunID,
		Payload:     []byte(`{"entity_id":"` + rootEntityID + `"}`),
		CreatedAt:   time.Now().UTC(),
	}).WithEntityID(rootEntityID)); err != nil {
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
	eb, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		ContractBundle: semanticview.Wrap(bundle),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	eb.Subscribe("child-aggregator")
	defer eb.Unsubscribe("child-aggregator")
	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx := runtimebus.WithEmittedEventsRecorder(context.Background(), recorder)
	if err := eb.Publish(ctx, (events.Event{
		Type: "child/grandchild/micro.done",
	}).WithEntityID("ent-grandchild")); err != nil {
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
	eb, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		ContractBundle: semanticview.Wrap(bundle),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(runtimecontracts.SystemNodeContract{}, runtimeflowidentity.DeriveRoute("child/grandchild", "inst-1")); err != nil {
		t.Fatalf("AddFlowInstance: %v", err)
	}
	eb.Subscribe("worker-inst-1")
	defer eb.Unsubscribe("worker-inst-1")
	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx := runtimebus.WithEmittedEventsRecorder(context.Background(), recorder)
	if err := eb.Publish(ctx, (events.Event{
		Type: "child/grandchild/inst-1/micro.done",
	}).WithEntityID("ent-grandchild")); err != nil {
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
