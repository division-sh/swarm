package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/google/uuid"
)

func TestSQLiteRuntimeStoreSelectedCoreContracts(t *testing.T) {
	ctx := testAuthorActivityContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)

	evtID := uuid.NewString()
	evt := eventtest.RunCreatingRootIngress(evtID,

		events.EventType("test.started"),
		"agent-1", "", json.RawMessage(`{"ok":true}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())

	if err := commitSemanticEventFixtureWithAgents(ctx, store, evt, []string{"agent-1"}); err != nil {
		t.Fatalf("AppendEvent with exact delivery: %v", err)
	}
	recipients, err := store.ListEventDeliveryRecipients(ctx, evtID)
	if err != nil {
		t.Fatalf("ListEventDeliveryRecipients: %v", err)
	}
	if len(recipients) != 1 || recipients[0] != "agent-1" {
		t.Fatalf("recipients = %#v, want agent-1", recipients)
	}

	if err := store.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:            "agent-1",
			Role:          "operator",
			FlowID:        "global",
			Model:         "regular",
			LLMBackend:    "anthropic",
			ExecutionMode: "live",
			Memory:        agentmemory.PlatformDefault(),
			Config:        json.RawMessage(`{"system_prompt":"You are an operator.","tools":[]}`),
		},
		Status:    "active",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}
	agents, err := store.LoadAgents(ctx)
	if err != nil {
		t.Fatalf("LoadAgents: %v", err)
	}
	if len(agents) != 1 || agents[0].Config.ID != "agent-1" {
		t.Fatalf("agents = %#v, want persisted agent-1", agents)
	}

	entityID := uuid.NewString()
	if _, err := store.DB.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, slug, name, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		) VALUES (?, ?, 'test-flow', 'test_entity', 'test', 'Test Entity', 'active',
			'{}', '{"score":1}', '{}', 1, ?, ?, ?)
	`, runID, entityID, time.Now().UTC(), time.Now().UTC(), time.Now().UTC()); err != nil {
		t.Fatalf("seed sqlite entity_state: %v", err)
	}
	itemID, err := store.InsertMailboxItem(ctx, runtimetools.MailboxItem{
		EventID:   evtID,
		EntityID:  entityID,
		FromAgent: "agent-1",
		Type:      "review_notice",
		Priority:  "critical",
		Status:    "pending",
		Summary:   "needs decision",
		Context:   json.RawMessage(`{"reason":"test"}`),
	})
	if err != nil {
		t.Fatalf("InsertMailboxItem: %v", err)
	}
	count, err := store.CountMailboxItems(ctx, "pending")
	if err != nil {
		t.Fatalf("CountMailboxItems: %v", err)
	}
	if count != 1 {
		t.Fatalf("pending mailbox count = %d, want 1", count)
	}
	item, err := store.GetMailboxItem(ctx, itemID)
	if err != nil {
		t.Fatalf("GetMailboxItem: %v", err)
	}
	if item.Status != "pending" || item.Type != "review_notice" {
		t.Fatalf("mailbox item status=%q type=%q, want pending review notice", item.Status, item.Type)
	}

	schedule := runtimepipeline.Schedule{
		RunID:        runID,
		AgentID:      "agent-1",
		EventType:    "timer.fired",
		Mode:         "once",
		At:           time.Now().UTC().Add(time.Hour),
		EntityID:     entityID,
		FlowInstance: "test-flow",
		TaskID:       "task-1",
		Payload:      json.RawMessage(`{"__schedule_task_id":"task-1"}`),
	}
	if err := store.UpsertSchedule(ctx, schedule); err != nil {
		t.Fatalf("UpsertSchedule: %v", err)
	}
	schedules, err := store.LoadActiveSchedules(ctx)
	if err != nil {
		t.Fatalf("LoadActiveSchedules: %v", err)
	}
	if len(schedules) != 1 || schedules[0].TaskID != "task-1" {
		t.Fatalf("active schedules = %#v, want task-1", schedules)
	}
	if err := store.MarkScheduleFiredExact(ctx, schedule); err != nil {
		t.Fatalf("MarkScheduleFiredExact: %v", err)
	}
	schedules, err = store.LoadActiveSchedules(ctx)
	if err != nil {
		t.Fatalf("LoadActiveSchedules after fire: %v", err)
	}
	if len(schedules) != 0 {
		t.Fatalf("active schedules after fire = %#v, want empty", schedules)
	}

	ingressState, err := store.EnsureRuntimeIngressState(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("EnsureRuntimeIngressState: %v", err)
	}
	if ingressState.Status != runtimeingress.StatusRunning {
		t.Fatalf("ingress status = %s, want running", ingressState.Status)
	}
	pausedIngress, changed, err := store.TransitionRuntimeIngressState(ctx, runtimeingress.StatusPaused, "test", "operator", time.Now().UTC())
	if err != nil {
		t.Fatalf("TransitionRuntimeIngressState(paused): %v", err)
	}
	if !changed || pausedIngress.Status != runtimeingress.StatusPaused {
		t.Fatalf("paused ingress state=%+v changed=%v, want paused changed", pausedIngress, changed)
	}

	if _, err := store.DB.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_source, started_at)
		VALUES (?, 'running', 'legacy', ?)
		ON CONFLICT(run_id) DO UPDATE SET status = 'running'
	`, runID, time.Now().UTC()); err != nil {
		t.Fatalf("seed sqlite run row: %v", err)
	}
	pausedRun, err := store.PauseRunControl(ctx, runtimeruncontrol.TransitionRequest{RunID: runID, Reason: "pause", ControlledBy: "operator", Now: time.Now().UTC()})
	if err != nil {
		t.Fatalf("PauseRunControl: %v", err)
	}
	if pausedRun.Status != "paused" || pausedRun.ControlStatus != "paused" {
		t.Fatalf("paused run state = %+v, want paused", pausedRun)
	}
	blocked, err := store.RunDispatchBlocked(ctx, runID)
	if err != nil {
		t.Fatalf("RunDispatchBlocked: %v", err)
	}
	if !blocked {
		t.Fatal("RunDispatchBlocked = false, want true for paused run")
	}
	runningRun, err := store.ContinueRunControl(ctx, runtimeruncontrol.TransitionRequest{RunID: runID, Reason: "continue", ControlledBy: "operator", Now: time.Now().UTC()})
	if err != nil {
		t.Fatalf("ContinueRunControl: %v", err)
	}
	if runningRun.Status != "running" {
		t.Fatalf("continued run state = %+v, want running", runningRun)
	}
	stoppedRun, err := store.StopRunControl(ctx, runtimeruncontrol.TransitionRequest{RunID: runID, Reason: "stop", ControlledBy: "operator", Now: time.Now().UTC()})
	if err != nil {
		t.Fatalf("StopRunControl: %v", err)
	}
	if stoppedRun.Status != "cancelled" || stoppedRun.ControlStatus != "stopped" {
		t.Fatalf("stopped run state = %+v, want cancelled/stopped", stoppedRun)
	}

	req := APIIdempotencyRequest{
		Method:         "mailbox.decide",
		ActorTokenID:   "token-1",
		IdempotencyKey: "idem-1",
		RequestHash:    "hash-1",
		Now:            time.Now().UTC(),
	}
	first, replayed, err := store.WithAPIIdempotency(ctx, req, func(context.Context) (APIIdempotencyCompletion, error) {
		return APIIdempotencyCompletion{ResourceID: itemID, Response: json.RawMessage(`{"ok":true}`)}, nil
	})
	if err != nil {
		t.Fatalf("WithAPIIdempotency first: %v", err)
	}
	if replayed || first.ResourceID != itemID {
		t.Fatalf("first idempotency completion=%+v replayed=%v, want new item", first, replayed)
	}
	second, replayed, err := store.WithAPIIdempotency(ctx, req, func(context.Context) (APIIdempotencyCompletion, error) {
		return APIIdempotencyCompletion{ResourceID: "wrong", Response: json.RawMessage(`{"ok":false}`)}, nil
	})
	if err != nil {
		t.Fatalf("WithAPIIdempotency replay: %v", err)
	}
	if !replayed || second.ResourceID != itemID {
		t.Fatalf("second idempotency completion=%+v replayed=%v, want replayed item", second, replayed)
	}
	req.RequestHash = "hash-2"
	_, _, err = store.WithAPIIdempotency(ctx, req, func(context.Context) (APIIdempotencyCompletion, error) {
		return APIIdempotencyCompletion{ResourceID: "wrong", Response: json.RawMessage(`{"ok":false}`)}, nil
	})
	if !errors.Is(err, ErrAPIIdempotencyConflict) {
		t.Fatalf("idempotency conflict err = %v, want ErrAPIIdempotencyConflict", err)
	}
}

func TestSQLiteRuntimeStore_RunControlStopAbandonsPendingWork(t *testing.T) {
	ctx := testAuthorActivityContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	eventID := uuid.NewString()
	now := time.Now().UTC()
	if _, err := store.DB.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_source, started_at)
		VALUES (?, 'running', 'ephemeral', ?)
	`, runID, now); err != nil {
		t.Fatalf("seed sqlite run: %v", err)
	}
	event := eventtest.PersistedProjection(
		eventID, events.EventType("custom.stop"), "test", "", json.RawMessage(`{}`), 0,
		runID, "", events.EventEnvelope{}, now,
	)
	routes := []events.DeliveryRoute{
		{SubscriberType: "agent", SubscriberID: "agent-pending"},
		{SubscriberType: "node", SubscriberID: "node-pending"},
	}
	if err := commitSemanticEventFixtureWithRoutes(ctx, store, event, routes); err != nil {
		t.Fatalf("seed sqlite event: %v", err)
	}

	state, err := store.StopRunControl(ctx, runtimeruncontrol.TransitionRequest{
		RunID:        runID,
		Reason:       "test",
		ControlledBy: "test",
		Now:          now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("StopRunControl: %v", err)
	}
	if state.Status != "cancelled" || state.ControlStatus != "stopped" || state.AbandonedDeliveries != 2 {
		t.Fatalf("stop state = %+v, want cancelled/stopped/2", state)
	}

	var deliveryStatus, reasonCode, activeSession string
	var stoppedFailure []byte
	if err := store.DB.QueryRowContext(ctx, `
		SELECT status, COALESCE(reason_code, ''), failure, COALESCE(CAST(current_attempt_version AS TEXT), '')
		FROM event_deliveries
		WHERE event_id = ?
		  AND subscriber_id = 'agent-pending'
	`, eventID).Scan(&deliveryStatus, &reasonCode, &stoppedFailure, &activeSession); err != nil {
		t.Fatalf("load stopped sqlite delivery: %v", err)
	}
	if deliveryStatus != "dead_letter" || reasonCode != "run_stopped" || len(stoppedFailure) != 0 || activeSession != "" {
		t.Fatalf("stopped sqlite delivery = %s/%s failure=%s active=%q, want dead_letter/run_stopped/no failure/no active session", deliveryStatus, reasonCode, stoppedFailure, activeSession)
	}
	if err := store.DB.QueryRowContext(ctx, `
		SELECT status, COALESCE(reason_code, ''), failure, COALESCE(CAST(current_attempt_version AS TEXT), '')
		FROM event_deliveries
		WHERE event_id = ?
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'node-pending'
	`, eventID).Scan(&deliveryStatus, &reasonCode, &stoppedFailure, &activeSession); err != nil {
		t.Fatalf("load stopped sqlite node delivery: %v", err)
	}
	if deliveryStatus != "dead_letter" || reasonCode != "run_stopped" || len(stoppedFailure) != 0 || activeSession != "" {
		t.Fatalf("stopped sqlite node delivery = %s/%s failure=%s active=%q, want dead_letter/run_stopped/no failure/no active session", deliveryStatus, reasonCode, stoppedFailure, activeSession)
	}

	var agentOutcome, agentReason string
	if err := store.DB.QueryRowContext(ctx, `
		SELECT o.outcome, COALESCE(o.reason_code, '')
		FROM event_delivery_outcomes o
		JOIN event_deliveries d ON d.delivery_id = o.delivery_id
		WHERE d.event_id = ?
		  AND d.subscriber_type = 'agent'
		  AND d.subscriber_id = 'agent-pending'
	`, eventID).Scan(&agentOutcome, &agentReason); err != nil {
		t.Fatalf("load stopped sqlite agent receipt: %v", err)
	}
	if agentOutcome != "terminalized" || agentReason != "run_stopped" {
		t.Fatalf("stopped sqlite agent outcome = %s/%s, want terminalized/run_stopped", agentOutcome, agentReason)
	}
	var nodeOutcome, nodeReason string
	if err := store.DB.QueryRowContext(ctx, `
		SELECT o.outcome, COALESCE(o.reason_code, '')
		FROM event_delivery_outcomes o
		JOIN event_deliveries d ON d.delivery_id = o.delivery_id
		WHERE d.event_id = ?
		  AND d.subscriber_type = 'node'
		  AND d.subscriber_id = 'node-pending'
	`, eventID).Scan(&nodeOutcome, &nodeReason); err != nil {
		t.Fatalf("load stopped sqlite node receipt: %v", err)
	}
	if nodeOutcome != "terminalized" || nodeReason != "run_stopped" {
		t.Fatalf("stopped sqlite node outcome = %s/%s, want terminalized/run_stopped", nodeOutcome, nodeReason)
	}

	var pipelineOutcome string
	if err := store.DB.QueryRowContext(ctx, `
		SELECT outcome
		FROM event_receipts
		WHERE event_id = ?
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, eventID).Scan(&pipelineOutcome); err != nil {
		t.Fatalf("load stopped sqlite pipeline receipt: %v", err)
	}
	if pipelineOutcome != "dead_letter" {
		t.Fatalf("stopped sqlite pipeline receipt = %s, want dead_letter", pipelineOutcome)
	}
}

func TestSQLiteRuntimeStoreUpsertAgentConsumesActivePipelineTransaction(t *testing.T) {
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), uuid.NewString())
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)

	tx, err := store.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin sqlite tx: %v", err)
	}
	committed := false
	t.Cleanup(func() {
		if !committed {
			_ = tx.Rollback()
		}
	})

	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES (?, 'running', ?)
	`, runtimecorrelation.RunIDFromContext(ctx), now); err != nil {
		t.Fatalf("seed active sqlite write transaction: %v", err)
	}

	txctx := runtimepipeline.WithPipelineSQLTxContext(ctx, tx)
	if err := store.UpsertAgent(txctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:            "agent-in-pipeline-tx",
			Role:          "worker",
			FlowID:        "global",
			Model:         "regular",
			LLMBackend:    "anthropic",
			ExecutionMode: "live",
			Memory:        agentmemory.PlatformDefault(),
			Config:        json.RawMessage(`{"system_prompt":"tx-owned agent","tools":[]}`),
		},
		Status:    "active",
		StartedAt: now,
	}); err != nil {
		t.Fatalf("UpsertAgent with active pipeline transaction: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit sqlite tx: %v", err)
	}
	committed = true

	agents, err := store.LoadAgents(ctx)
	if err != nil {
		t.Fatalf("LoadAgents: %v", err)
	}
	if len(agents) != 1 || agents[0].Config.ID != "agent-in-pipeline-tx" {
		t.Fatalf("agents = %#v, want agent-in-pipeline-tx", agents)
	}
}

func TestSQLiteDynamicFlowActivationRequiredAgentsUsePipelineTransaction(t *testing.T) {
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(storeTestWorkContext(t, testAuthorActivityContext()), runID)
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	if _, err := sqliteStore.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES (?, 'running')`, runID); err != nil {
		t.Fatalf("seed activation run: %v", err)
	}
	workflowStore := runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(sqliteStore.DB, sqliteStore)
	bus := &sqliteFlowActivationBus{}
	manager := ownStoreTestAgentManager(t, runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		WorkflowInstances: workflowStore,
		LLMBackend:        "anthropic",
		LifecycleStore:    sqliteStore,
		WorkOwner:         storeTestWorkOwner(t),
	}, sqliteStore))
	bundle := sqliteFlowActivationBundle()

	req := sqliteFlowActivationRequest(bundle, "review", "inst-1", "parent-ent", "review/inst-1")
	if err := workflowStore.RunPipelineMutation(ctx, func(txctx context.Context) error {
		return manager.ActivateFlowInstance(txctx, req)
	}); err != nil {
		t.Fatalf("ActivateFlowInstance inside sqlite pipeline transaction: %v", err)
	}

	loaded, ok, err := workflowStore.Load(ctx, "review/inst-1")
	if err != nil {
		t.Fatalf("Load workflow instance: %v", err)
	}
	if !ok || strings.TrimSpace(loaded.StorageRef) != "review/inst-1" {
		t.Fatalf("workflow instance loaded=%v value=%+v, want review/inst-1", ok, loaded)
	}
	agents, err := sqliteStore.LoadAgents(ctx)
	if err != nil {
		t.Fatalf("LoadAgents: %v", err)
	}
	assertSQLiteActivatedAgentIDs(t, agents, "reviewer-inst-1")
	assertSQLiteAddedRoutes(t, bus, "review/inst-1")
	assertSQLiteRouteMaterializationVars(t, bus, "review/inst-1", map[string]string{
		"flow_instance_path": "review/inst-1",
		"flow_scope_key":     "review",
		"instance_id":        "inst-1",
		"template_id":        "review",
	})
}

func TestSQLiteDynamicFlowActivationConcurrentFanOutChildrenPersist(t *testing.T) {
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(storeTestWorkContext(t, testAuthorActivityContext()), runID)
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	if _, err := sqliteStore.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES (?, 'running')`, runID); err != nil {
		t.Fatalf("seed fan-out run: %v", err)
	}
	workflowStore := runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(sqliteStore.DB, sqliteStore)
	bus := &sqliteFlowActivationBus{}
	manager := ownStoreTestAgentManager(t, runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		WorkflowInstances: workflowStore,
		LLMBackend:        "anthropic",
		LifecycleStore:    sqliteStore,
		WorkOwner:         storeTestWorkOwner(t),
	}, sqliteStore))
	bundle := sqliteFlowActivationBundle()

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, instanceID := range []string{"component-a", "component-b"} {
		instanceID := instanceID
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			req := sqliteFlowActivationRequest(bundle, "review", instanceID, "parent-ent", "review/"+instanceID)
			errs <- workflowStore.RunPipelineMutation(ctx, func(txctx context.Context) error {
				return manager.ActivateFlowInstance(txctx, req)
			})
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent fan-out activation: %v", err)
		}
	}

	for _, storageRef := range []string{"review/component-a", "review/component-b"} {
		loaded, ok, err := workflowStore.Load(ctx, storageRef)
		if err != nil {
			t.Fatalf("Load workflow instance %s: %v", storageRef, err)
		}
		if !ok || strings.TrimSpace(loaded.StorageRef) != storageRef {
			t.Fatalf("workflow instance %s loaded=%v value=%+v", storageRef, ok, loaded)
		}
	}
	agents, err := sqliteStore.LoadAgents(ctx)
	if err != nil {
		t.Fatalf("LoadAgents: %v", err)
	}
	assertSQLiteActivatedAgentIDs(t, agents, "reviewer-component-a", "reviewer-component-b")
	assertSQLiteAddedRoutes(t, bus, "review/component-a", "review/component-b")
	assertSQLiteRouteMaterializationVars(t, bus, "review/component-a", map[string]string{
		"flow_instance_path": "review/component-a",
		"flow_scope_key":     "review",
		"instance_id":        "component-a",
		"template_id":        "review",
	})
	assertSQLiteRouteMaterializationVars(t, bus, "review/component-b", map[string]string{
		"flow_instance_path": "review/component-b",
		"flow_scope_key":     "review",
		"instance_id":        "component-b",
		"template_id":        "review",
	})
	for _, entry := range bus.runtimeLogEntries() {
		if entry.Level == "error" || entry.Failure != nil {
			t.Fatalf("runtime log = %#v, want no activation dead-letter/runtime errors", entry)
		}
	}
}

type sqliteFlowActivationBus struct {
	mu            sync.Mutex
	runtimeLog    []runtimepipeline.RuntimeLogEntry
	routeRequests []runtimebus.FlowInstanceRouteMaterializationRequest
	published     []events.Event
}

func (b *sqliteFlowActivationBus) Publish(_ context.Context, evt events.Event) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.published = append(b.published, evt)
	return nil
}

func (*sqliteFlowActivationBus) PublishDirect(context.Context, events.Event, []string) error {
	return nil
}

func (*sqliteFlowActivationBus) PublishPersistedRecipients(context.Context, events.Event, []string) error {
	return nil
}

func (*sqliteFlowActivationBus) Subscribe(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}

func (*sqliteFlowActivationBus) Unsubscribe(string) {}

func (*sqliteFlowActivationBus) Store() runtimebus.EventStore { return nil }

func (*sqliteFlowActivationBus) ResetInMemoryState() error { return nil }

func (b *sqliteFlowActivationBus) LogRuntime(_ context.Context, entry runtimepipeline.RuntimeLogEntry) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.runtimeLog = append(b.runtimeLog, entry)
	return nil
}

func (b *sqliteFlowActivationBus) AddFlowInstanceRoute(req runtimebus.FlowInstanceRouteMaterializationRequest) error {
	return b.AddFlowInstanceRouteContext(context.Background(), req)
}

func (b *sqliteFlowActivationBus) AddFlowInstanceRouteContext(_ context.Context, req runtimebus.FlowInstanceRouteMaterializationRequest) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.routeRequests = append(b.routeRequests, req.Normalized())
	return nil
}

func (b *sqliteFlowActivationBus) runtimeLogEntries() []runtimepipeline.RuntimeLogEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]runtimepipeline.RuntimeLogEntry, len(b.runtimeLog))
	copy(out, b.runtimeLog)
	return out
}

func (b *sqliteFlowActivationBus) routePaths() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, 0, len(b.routeRequests))
	for _, req := range b.routeRequests {
		out = append(out, strings.TrimSpace(req.Identity.InstancePath))
	}
	return out
}

func (b *sqliteFlowActivationBus) materializationRequests() []runtimebus.FlowInstanceRouteMaterializationRequest {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]runtimebus.FlowInstanceRouteMaterializationRequest, len(b.routeRequests))
	copy(out, b.routeRequests)
	return out
}

func sqliteFlowActivationBundle() *runtimecontracts.WorkflowContractBundle {
	reviewFlow := &runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "review"},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"reviewer": {
				ID:            "reviewer-{instance_id}",
				Type:          "generic",
				Role:          "reviewer",
				Model:         "regular",
				Subscriptions: []string{"task.started"},
			},
		},
	}
	return &runtimecontracts.WorkflowContractBundle{
		FlowTree: runtimecontracts.FlowTree{
			Root: &runtimecontracts.FlowContractView{
				Children: []runtimecontracts.FlowContractView{*reviewFlow},
			},
			ByID: map[string]*runtimecontracts.FlowContractView{
				"review": reviewFlow,
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"review": {
				Mode: "template",
				Pins: runtimecontracts.FlowPins{
					Inputs: runtimecontracts.FlowInputPins{Events: []string{"task.started"}},
				},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{Version: "v-test"},
	}
}

func sqliteFlowActivationRequest(bundle *runtimecontracts.WorkflowContractBundle, templateID, instanceID, parentEntityID, flowPath string) runtimepipeline.FlowInstanceActivationRequest {
	instance := runtimeflowidentity.Stored(
		semanticview.Wrap(bundle),
		templateID,
		flowPath,
		instanceID,
		runtimepipeline.FlowInstanceEntityID(flowPath),
		parentEntityID,
	)
	return runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: semanticview.Wrap(bundle),
		Instance:       instance,
	}
}

func assertSQLiteActivatedAgentIDs(t *testing.T, agents []runtimemanager.PersistedAgent, wantIDs ...string) {
	t.Helper()
	got := map[string]struct{}{}
	for _, rec := range agents {
		got[strings.TrimSpace(rec.Config.ID)] = struct{}{}
	}
	for _, want := range wantIDs {
		if _, ok := got[want]; !ok {
			t.Fatalf("activated agent ids = %#v, missing %q", got, want)
		}
	}
}

func assertSQLiteAddedRoutes(t *testing.T, bus *sqliteFlowActivationBus, wantPaths ...string) {
	t.Helper()
	got := map[string]struct{}{}
	for _, path := range bus.routePaths() {
		got[strings.TrimSpace(path)] = struct{}{}
	}
	for _, want := range wantPaths {
		if _, ok := got[want]; !ok {
			t.Fatalf("added route paths = %#v, missing %q", got, want)
		}
	}
}

func assertSQLiteRouteMaterializationVars(t *testing.T, bus *sqliteFlowActivationBus, wantPath string, wantVars map[string]string) {
	t.Helper()
	for _, req := range bus.materializationRequests() {
		if strings.TrimSpace(req.Identity.InstancePath) != wantPath {
			continue
		}
		for key, want := range wantVars {
			if got := strings.TrimSpace(req.ActivationVariables[key]); got != want {
				t.Fatalf("route materialization vars for %s key %s = %q, want %q; all vars=%#v", wantPath, key, got, want, req.ActivationVariables)
			}
		}
		return
	}
	t.Fatalf("route materialization request for %s not found; got %#v", wantPath, bus.materializationRequests())
}

func TestSQLiteRuntimeStoreAPIIdempotencyAllowsNestedEventBusPublish(t *testing.T) {
	ctx := testAuthorActivityContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	bus, err := newStoreTestEventBus(t, store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}

	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := "11111111-1111-1111-1111-111111111111"
	req := APIIdempotencyRequest{
		Method:         "event.publish",
		ActorTokenID:   "token-1",
		IdempotencyKey: "idem-nested-publish",
		RequestHash:    "hash-nested-publish",
		Now:            time.Now().UTC(),
	}
	publishCalls := 0
	publish := func(ctx context.Context) (APIIdempotencyCompletion, error) {
		publishCalls++
		if err := bus.Publish(ctx, eventtest.RunCreatingRootIngress(
			eventID,
			events.EventType("item.received"),
			"api.v1",
			"",
			json.RawMessage(`{"entity_id":"11111111-1111-1111-1111-111111111111","topic":"medicine"}`),
			0,
			runID,
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
			time.Now().UTC(),
		)); err != nil {
			return APIIdempotencyCompletion{}, err
		}
		response, err := json.Marshal(map[string]string{"event_id": eventID, "run_id": runID})
		if err != nil {
			return APIIdempotencyCompletion{}, err
		}
		return APIIdempotencyCompletion{ResourceID: eventID, Response: response}, nil
	}

	first, replayed, err := store.WithAPIIdempotency(ctx, req, publish)
	if err != nil {
		t.Fatalf("WithAPIIdempotency nested publish: %v", err)
	}
	if replayed || first.ResourceID != eventID || publishCalls != 1 {
		t.Fatalf("first completion=%+v replayed=%v calls=%d, want new event", first, replayed, publishCalls)
	}
	assertSQLiteRuntimeCount(t, store, `SELECT COUNT(*) FROM runs WHERE run_id = ?`, 1, runID)
	assertSQLiteRuntimeCount(t, store, `SELECT COUNT(*) FROM events WHERE event_id = ?`, 1, eventID)
	assertSQLiteRuntimeCount(t, store, `SELECT COUNT(*) FROM api_idempotency WHERE method = ? AND idempotency_key = ?`, 1, req.Method, req.IdempotencyKey)

	second, replayed, err := store.WithAPIIdempotency(ctx, req, func(context.Context) (APIIdempotencyCompletion, error) {
		return APIIdempotencyCompletion{}, errors.New("replay executed callback")
	})
	if err != nil {
		t.Fatalf("WithAPIIdempotency replay: %v", err)
	}
	if !replayed || second.ResourceID != eventID || string(second.Response) != string(first.Response) || publishCalls != 1 {
		t.Fatalf("replay completion=%+v replayed=%v calls=%d, want stored event", second, replayed, publishCalls)
	}

	req.RequestHash = "hash-nested-publish-conflict"
	_, _, err = store.WithAPIIdempotency(ctx, req, func(context.Context) (APIIdempotencyCompletion, error) {
		return APIIdempotencyCompletion{}, errors.New("conflict executed callback")
	})
	if !errors.Is(err, ErrAPIIdempotencyConflict) {
		t.Fatalf("conflict err = %v, want ErrAPIIdempotencyConflict", err)
	}
	assertSQLiteRuntimeCount(t, store, `SELECT COUNT(*) FROM events WHERE event_id = ?`, 1, eventID)
}

func TestSQLiteRuntimeStoreAPIIdempotencyFailedNestedPublishLeavesNoCompletionOrRows(t *testing.T) {
	ctx := testAuthorActivityContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	store.SetEventPayloadValidator(func(_ context.Context, eventType string, _ []byte) error {
		if eventType == "item.failed" {
			return errors.New("schema violation")
		}
		return nil
	})
	bus, err := newStoreTestEventBus(t, store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}

	runID := uuid.NewString()
	eventID := uuid.NewString()
	req := APIIdempotencyRequest{
		Method:         "event.publish",
		ActorTokenID:   "token-1",
		IdempotencyKey: "idem-failed-publish",
		RequestHash:    "hash-failed-publish",
		Now:            time.Now().UTC(),
	}
	completion, replayed, err := store.WithAPIIdempotency(ctx, req, func(ctx context.Context) (APIIdempotencyCompletion, error) {
		err := bus.Publish(ctx, eventtest.RunCreatingRootIngress(
			eventID,
			events.EventType("item.failed"),
			"api.v1",
			"",
			json.RawMessage(`{"entity_id":"22222222-2222-2222-2222-222222222222"}`),
			0,
			runID,
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "22222222-2222-2222-2222-222222222222"),
			time.Now().UTC(),
		))
		if err != nil {
			return APIIdempotencyCompletion{}, err
		}
		return APIIdempotencyCompletion{ResourceID: eventID, Response: json.RawMessage(`{"ok":true}`)}, nil
	})
	if err == nil {
		t.Fatal("WithAPIIdempotency failed publish err = nil")
	}
	if replayed || completion.ResourceID != "" || len(completion.Response) != 0 {
		t.Fatalf("failed completion=%+v replayed=%v, want no completion", completion, replayed)
	}
	assertSQLiteRuntimeCount(t, store, `SELECT COUNT(*) FROM api_idempotency WHERE method = ? AND idempotency_key = ?`, 0, req.Method, req.IdempotencyKey)
	assertSQLiteRuntimeCount(t, store, `SELECT COUNT(*) FROM runs WHERE run_id = ?`, 0, runID)
	assertSQLiteRuntimeCount(t, store, `SELECT COUNT(*) FROM events WHERE event_id = ?`, 0, eventID)
}

func TestSQLiteRuntimeStoreAPIIdempotencySerializesAcrossSamePathHandles(t *testing.T) {
	ctx := testAuthorActivityContext()
	dbPath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
	storeA := newBootstrappedSQLiteRuntimeStoreForPath(t, dbPath)
	storeB := newBootstrappedSQLiteRuntimeStoreForPath(t, dbPath)
	req := APIIdempotencyRequest{
		Method:         "event.publish",
		ActorTokenID:   "token-1",
		IdempotencyKey: "idem-shared-path",
		RequestHash:    "hash-shared-path",
		Now:            time.Now().UTC(),
	}

	type callResult struct {
		completion APIIdempotencyCompletion
		replayed   bool
		err        error
	}
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondExecuted := make(chan struct{}, 1)
	firstDone := make(chan callResult, 1)
	secondDone := make(chan callResult, 1)
	var callbackCalls atomic.Int32

	go func() {
		completion, replayed, err := storeA.WithAPIIdempotency(ctx, req, func(context.Context) (APIIdempotencyCompletion, error) {
			if calls := callbackCalls.Add(1); calls != 1 {
				secondExecuted <- struct{}{}
			}
			close(firstStarted)
			<-releaseFirst
			return APIIdempotencyCompletion{ResourceID: "resource-1", Response: json.RawMessage(`{"ok":true}`)}, nil
		})
		firstDone <- callResult{completion: completion, replayed: replayed, err: err}
	}()

	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first idempotency callback did not start")
	}

	go func() {
		completion, replayed, err := storeB.WithAPIIdempotency(ctx, req, func(context.Context) (APIIdempotencyCompletion, error) {
			callbackCalls.Add(1)
			secondExecuted <- struct{}{}
			return APIIdempotencyCompletion{ResourceID: "resource-2", Response: json.RawMessage(`{"ok":false}`)}, nil
		})
		secondDone <- callResult{completion: completion, replayed: replayed, err: err}
	}()

	select {
	case <-secondExecuted:
		close(releaseFirst)
		first := <-firstDone
		second := <-secondDone
		t.Fatalf("second handle executed callback before first completion; first=%+v second=%+v calls=%d", first, second, callbackCalls.Load())
	case <-time.After(250 * time.Millisecond):
	}
	close(releaseFirst)

	var first callResult
	select {
	case first = <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first idempotency call did not finish")
	}
	if first.err != nil || first.replayed || first.completion.ResourceID != "resource-1" {
		t.Fatalf("first idempotency result=%+v, want new resource-1", first)
	}
	var second callResult
	select {
	case second = <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("second idempotency call did not finish")
	}
	if second.err != nil || !second.replayed || second.completion.ResourceID != "resource-1" || string(second.completion.Response) != `{"ok":true}` {
		t.Fatalf("second idempotency result=%+v, want replayed resource-1", second)
	}
	if calls := callbackCalls.Load(); calls != 1 {
		t.Fatalf("callback calls = %d, want 1", calls)
	}
	assertSQLiteRuntimeCount(t, storeA, `SELECT COUNT(*) FROM api_idempotency WHERE method = ? AND idempotency_key = ?`, 1, req.Method, req.IdempotencyKey)
}

func TestSQLiteRuntimeStoreRunEventTransactionEnsuresFreshRunRow(t *testing.T) {
	ctx := testAuthorActivityContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)

	runID := uuid.NewString()
	eventID := uuid.NewString()
	if err := store.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		return commitSemanticEventFixtureTx(txctx, store, tx, eventtest.RunCreatingRootIngress(
			eventID,
			events.EventType("item.received"),
			"api.v1",
			"",
			json.RawMessage(`{"entity_id":"33333333-3333-3333-3333-333333333333"}`),
			0,
			runID,
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "33333333-3333-3333-3333-333333333333"),
			time.Now().UTC(),
		))
	}); err != nil {
		t.Fatalf("RunEventTransaction: %v", err)
	}

	assertSQLiteRuntimeCount(t, store, `SELECT COUNT(*) FROM runs WHERE run_id = ?`, 1, runID)
	assertSQLiteRuntimeCount(t, store, `SELECT COUNT(*) FROM events WHERE event_id = ?`, 1, eventID)
}

func TestSQLiteRuntimeStoreRuntimeIngressReadDuringPublishDoesNotReenterWrite(t *testing.T) {
	ctx := testAuthorActivityContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	if _, err := store.DB.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES (?, 'running')
	`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	bus, err := newStoreTestEventBus(t, store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	controller := runtimeingress.NewController(store, bus, runtimeingress.Options{})
	if err := controller.SyncState(ctx); err != nil {
		t.Fatalf("SyncState: %v", err)
	}
	bus.SetRuntimeIngressDispatchGate(controller)

	eventID := uuid.NewString()
	err = bus.Publish(ctx, eventtest.RunCreatingRootIngress(
		eventID,
		events.EventType("item.received"),
		"api.v1",
		"",
		[]byte(`{"entity_id":"11111111-1111-1111-1111-111111111111"}`),
		0,
		runID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "11111111-1111-1111-1111-111111111111"),
		time.Now().UTC(),
	))
	if err != nil {
		t.Fatalf("Publish with runtime ingress gate: %v", err)
	}
	var count int
	if err := store.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id = ?`, eventID).Scan(&count); err != nil {
		t.Fatalf("count event: %v", err)
	}
	if count != 1 {
		t.Fatalf("event rows = %d, want 1", count)
	}
}

func assertSQLiteRuntimeCount(t *testing.T, store *SQLiteRuntimeStore, query string, want int, args ...any) {
	t.Helper()
	var count int
	if err := store.DB.QueryRowContext(testAuthorActivityContext(), query, args...).Scan(&count); err != nil {
		t.Fatalf("count sqlite runtime rows: %v", err)
	}
	if count != want {
		t.Fatalf("sqlite count for %q = %d, want %d", query, count, want)
	}
}

func TestSQLiteRuntimeStorePipelineWorkflowInstanceOwner(t *testing.T) {
	ctx := testAuthorActivityContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	if _, err := store.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES (?, 'running')`, runID); err != nil {
		t.Fatalf("seed workflow instance run: %v", err)
	}
	owner := runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(store.DB, store)
	entityID := runtimepipeline.FlowInstanceEntityID("root/acme")
	if err := owner.Create(ctx, runtimepipeline.WorkflowInstance{
		InstanceID:      "acme",
		WorkflowName:    "root",
		WorkflowVersion: "v1",
		CurrentState:    "collecting",
		EnteredStageAt:  time.Now().UTC(),
		Metadata: map[string]any{
			"flow_path":   "root/acme",
			"slug":        "acme",
			"name":        "Acme",
			"entity_type": "company",
			"score":       float64(7),
		},
		StateBuckets: map[string]any{"evidence": []any{"seed"}},
	}); err != nil {
		t.Fatalf("Create workflow instance: %v", err)
	}
	if err := owner.Mutate(ctx, "root/acme", func(instance *runtimepipeline.WorkflowInstance) {
		instance.CurrentState = "qualified"
		instance.Metadata["score"] = float64(9)
	}); err != nil {
		t.Fatalf("Mutate workflow instance: %v", err)
	}
	loaded, ok, err := owner.Load(ctx, "root/acme")
	if err != nil {
		t.Fatalf("Load workflow instance: %v", err)
	}
	if !ok || loaded.CurrentState != "qualified" || loaded.Metadata["slug"] != "acme" {
		t.Fatalf("loaded workflow instance = %#v, want qualified acme", loaded)
	}
	selected, err := owner.SelectActiveByFields(ctx, "root", []runtimepipeline.WorkflowInstanceFieldSelector{{Field: "score", Value: float64(9)}}, []string{"terminal"})
	if err != nil {
		t.Fatalf("SelectActiveByFields: %v", err)
	}
	if len(selected) != 1 || selected[0].StorageRef != "root/acme" {
		t.Fatalf("selected workflow instances = %#v, want root/acme", selected)
	}
	var mutationCount int
	if err := store.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_mutations WHERE run_id = ? AND entity_id = ?`, runID, entityID).Scan(&mutationCount); err != nil {
		t.Fatalf("count sqlite entity mutations: %v", err)
	}
	if mutationCount == 0 {
		t.Fatal("sqlite workflow instance owner wrote no entity mutation rows")
	}
}

func TestSQLiteRuntimeStoreSessionStartupConversationAndTraceVisibility(t *testing.T) {
	ctx := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	now := time.Now().UTC()
	store.SetNowFnForTest(func() time.Time { return now })
	runID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	if _, err := store.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES (?, 'running', ?)`, runID, now); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	if err := store.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:            "agent-1",
			Role:          "operator",
			FlowID:        "global",
			Model:         "regular",
			LLMBackend:    "anthropic",
			ExecutionMode: "live",
			Memory:        agentmemory.Authored(true),
			FlowPath:      "global",
			Config:        json.RawMessage(`{"system_prompt":"test","tools":[]}`),
		},
		Status:    "active",
		StartedAt: now,
	}); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}

	startupLease, err := store.AcquireRuntimeStartupOwnership(ctx, testStartupAcquireRequest("runtime-1"))
	if err != nil {
		t.Fatalf("AcquireRuntimeStartupOwnership first: %v", err)
	}
	if _, err := store.AcquireRuntimeStartupOwnership(ctx, testStartupAcquireRequest("runtime-2")); err == nil {
		t.Fatal("AcquireRuntimeStartupOwnership second unexpectedly succeeded")
	}
	if err := startupLease.Release(ctx); err != nil {
		t.Fatalf("release startup lease: %v", err)
	}
	successorStartupLease, err := store.AcquireRuntimeStartupOwnership(ctx, testStartupAcquireRequest("runtime-2"))
	if err != nil {
		t.Fatalf("AcquireRuntimeStartupOwnership after release: %v", err)
	}
	if err := successorStartupLease.Release(ctx); err != nil {
		t.Fatalf("release successor startup lease: %v", err)
	}

	identity := agentmemory.Identity{RunID: runID, AgentID: "agent-1", FlowInstance: "global"}
	lease, err := store.Acquire(ctx, identity, "owner-1")
	if err != nil {
		t.Fatalf("Acquire session: %v", err)
	}
	if lease.Identity != identity {
		t.Fatalf("session identity = %+v, want %+v", lease.Identity, identity)
	}
	if _, err := store.Acquire(ctx, identity, "owner-2"); !errors.Is(err, runtimesessions.ErrSessionLeased) {
		t.Fatalf("competing Acquire error = %v, want ErrSessionLeased", err)
	}
	if err := store.AdoptSessionID(ctx, identity, "owner-1", "provider-session-1"); err != nil {
		t.Fatalf("AdoptSessionID: %v", err)
	}
	if err := store.IncrementTurn(ctx, identity, lease.SessionID); err != nil {
		t.Fatalf("IncrementTurn: %v", err)
	}
	if err := store.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID: lease.SessionID,
		AgentID:   identity.AgentID,
		Identity:  identity,
		Memory:    agentmemory.Authored(true),
		Messages:  []runtimellm.Message{{Role: "user", Content: "hello"}},
		Summary:   "greeting",
		TurnCount: 1,
		Status:    "active",
	}); err != nil {
		t.Fatalf("UpsertConversation: %v", err)
	}
	conversation, ok, err := store.LoadActiveConversation(ctx, identity)
	if err != nil {
		t.Fatalf("LoadActiveConversation: %v", err)
	}
	if !ok || conversation.Summary != "greeting" || len(conversation.Messages) != 1 {
		t.Fatalf("conversation = %+v ok=%v, want persisted greeting", conversation, ok)
	}

	eventID := uuid.NewString()
	event := eventtest.PersistedProjection(eventID,

		events.EventType("trace.visible"),
		"agent-1", "", json.RawMessage(`{"trace":true}`), 0, runID, "", events.EventEnvelope{}, now)

	if err := commitSemanticEventFixtureWithAgents(ctx, store, event, []string{"agent-1"}); err != nil {
		t.Fatalf("PersistEventWithDeliveries trace event: %v", err)
	}
	route := events.DeliveryRoute{SubscriberType: string(runtimedelivery.SubscriberAgent), SubscriberID: "agent-1"}
	claimed, err := store.ClaimAgentDelivery(ctx, event, route)
	if err != nil {
		t.Fatalf("ClaimAgentDelivery trace event: %v", err)
	}
	if _, err := store.BindAgentSession(ctx, claimed.Claim, lease.SessionID); err != nil {
		t.Fatalf("BindAgentSession trace event: %v", err)
	}
	if err := store.AppendAgentTurn(runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeLive), managedAgentTurnRecordForTest(t, runtimellm.AgentTurnRecord{
		AgentID:          "agent-1",
		Memory:           agentmemory.Authored(true),
		SessionID:        lease.SessionID,
		RunID:            runID,
		FlowInstance:     identity.FlowInstance,
		TriggerEventID:   eventID,
		TriggerEventType: "trace.visible",
		RequestPayload:   []byte(`{"prompt":"hello"}`),
		ResponseRaw:      []byte(`{"content":"ok"}`),
		ParseOK:          true,
	})); err != nil {
		t.Fatalf("AppendAgentTurn: %v", err)
	}
	trace, _, err := store.LoadRunDebugTracePage(ctx, runID, RunDebugTraceQueryOptions{Limit: 10})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage: %v", err)
	}
	if len(trace) != 1 || trace[0].EventID != eventID || trace[0].SessionID != lease.SessionID || trace[0].TurnTriggerEventID != eventID {
		t.Fatalf("trace = %#v, want event/session/turn visibility", trace)
	}
	eventsPage, err := store.ListOperatorEvents(ctx, OperatorEventListOptions{Filter: OperatorEventListFilter{RunID: runID}, Limit: 10})
	if err != nil {
		t.Fatalf("ListOperatorEvents: %v", err)
	}
	if len(eventsPage.Events) != 1 || eventsPage.Events[0].EventID != eventID || !operatorDeliveriesContain(eventsPage.Events[0].Deliveries, "agent", "agent-1") {
		t.Fatalf("events page = %#v, want event with delivery", eventsPage)
	}

	logID := uuid.NewString()
	if err := commitDiagnosticRuntimeLogFixture(ctx, store, eventtest.DiagnosticDirect(logID,

		events.EventTypePlatformRuntimeLog,
		"runtime", "", json.RawMessage(`{"log_level":"warn","message":"runtime warning","details":{"component":"scheduler","action":"session_warning","session_id":"`+lease.SessionID+`"}}`), 0, runID, "", events.EventEnvelope{}, now.Add(time.Second))); err != nil {
		t.Fatalf("AppendEvent runtime log: %v", err)
	}
	logs, err := store.ListOperatorRuntimeLogs(ctx, OperatorRuntimeLogListOptions{RunID: runID, Level: "warn", Component: "scheduler", Limit: 10})
	if err != nil {
		t.Fatalf("ListOperatorRuntimeLogs: %v", err)
	}
	if len(logs.Logs) != 1 || logs.Logs[0].LogID != logID || logs.Logs[0].SessionID != lease.SessionID {
		t.Fatalf("runtime logs = %#v, want persisted runtime log", logs)
	}
	if err := store.Release(ctx, lease); err != nil {
		t.Fatalf("Release session: %v", err)
	}
}

func TestSQLiteRuntimeStore_StatelessAuditUsesExplicitMemoryPlan(t *testing.T) {
	ctx := testAuthorActivityContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	sessionID := uuid.NewString()
	if _, err := store.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES (?, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	if err := store.AppendAgentTurn(runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeLive), managedAgentTurnRecordForTest(t, runtimellm.AgentTurnRecord{
		AgentID:        "task-agent",
		Memory:         agentmemory.PlatformDefault(),
		SessionID:      sessionID,
		RunID:          runID,
		RequestPayload: []byte(`{"kind":"task"}`),
		ResponseRaw:    []byte(`{"ok":true}`),
		ParseOK:        true,
		Latency:        5 * time.Millisecond,
	})); err != nil {
		t.Fatalf("AppendAgentTurn(stateless): %v", err)
	}

	var count int
	var entityID, flowInstance, conversation, persistedRunID, status, memorySource string
	var memoryEnabled bool
	if err := store.DB.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(MAX(entity_id), ''), COALESCE(MAX(flow_instance), ''),
		       COALESCE(MAX(conversation), ''), COALESCE(MAX(run_id), ''), COALESCE(MAX(status), ''),
		       MAX(memory_enabled), COALESCE(MAX(memory_source), '')
		FROM agent_conversation_audits
		WHERE session_id = ?
	`, sessionID).Scan(&count, &entityID, &flowInstance, &conversation, &persistedRunID, &status, &memoryEnabled, &memorySource); err != nil {
		t.Fatalf("read stateless audit row: %v", err)
	}
	if count != 1 {
		t.Fatalf("audit row count = %d, want 1", count)
	}
	if entityID != "" || flowInstance != "" || memoryEnabled || memorySource != "platform_default" {
		t.Fatalf("audit identity entity_id=%q flow_instance=%q memory=%v source=%q", entityID, flowInstance, memoryEnabled, memorySource)
	}
	if persistedRunID != runID || status != "active" {
		t.Fatalf("audit run/status = %q/%q, want %q/active", persistedRunID, status, runID)
	}
	if conversation != "[]" {
		t.Fatalf("conversation = %s, want empty stateless audit snapshot", conversation)
	}

	var turns int
	if err := store.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM agent_turns
		WHERE session_id = ? AND memory_enabled = 0
	`, sessionID).Scan(&turns); err != nil {
		t.Fatalf("count stateless turns: %v", err)
	}
	if turns != 1 {
		t.Fatalf("task turn count = %d, want 1", turns)
	}
}

func TestSQLiteRuntimeStore_StatelessAuditPersistsEntityMetadata(t *testing.T) {
	ctx := testAuthorActivityContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	sessionID := uuid.NewString()
	entityID := uuid.NewString()
	if _, err := store.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES (?, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	if err := store.AppendAgentTurn(runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeLive), managedAgentTurnRecordForTest(t, runtimellm.AgentTurnRecord{
		AgentID:        "task-agent",
		Memory:         agentmemory.Authored(false),
		SessionID:      sessionID,
		RunID:          runID,
		EntityID:       entityID,
		RequestPayload: []byte(`{"kind":"task"}`),
		ResponseRaw:    []byte(`{"ok":true}`),
		ParseOK:        true,
		Latency:        5 * time.Millisecond,
	})); err != nil {
		t.Fatalf("AppendAgentTurn(stateless entity): %v", err)
	}

	var count int
	var gotEntityID, flowInstance, conversation, memorySource string
	var memoryEnabled bool
	if err := store.DB.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(MAX(entity_id), ''), COALESCE(MAX(flow_instance), ''),
		       COALESCE(MAX(conversation), ''), MAX(memory_enabled), COALESCE(MAX(memory_source), '')
		FROM agent_conversation_audits
		WHERE session_id = ?
	`, sessionID).Scan(&count, &gotEntityID, &flowInstance, &conversation, &memoryEnabled, &memorySource); err != nil {
		t.Fatalf("read entity stateless audit row: %v", err)
	}
	if count != 1 {
		t.Fatalf("audit row count = %d, want 1", count)
	}
	if gotEntityID != entityID || flowInstance != "" || memoryEnabled || memorySource != "authored" {
		t.Fatalf("audit entity=%q flow_instance=%q memory=%v source=%q", gotEntityID, flowInstance, memoryEnabled, memorySource)
	}
	if conversation != "[]" {
		t.Fatalf("conversation = %s, want empty stateless audit snapshot", conversation)
	}

	var linkedTurns int
	if err := store.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM agent_turns
		WHERE session_id = ? AND memory_enabled = 0 AND entity_id = ?
	`, sessionID, entityID).Scan(&linkedTurns); err != nil {
		t.Fatalf("count linked stateless turns: %v", err)
	}
	if linkedTurns != 1 {
		t.Fatalf("linked task turn count = %d, want 1", linkedTurns)
	}
}

func TestSQLiteRuntimeStore_StatelessAuditPersistsFlowInstanceMetadata(t *testing.T) {
	ctx := testAuthorActivityContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	sessionID := uuid.NewString()
	flowInstance := "review/inst-1"
	if _, err := store.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES (?, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	if err := store.AppendAgentTurn(runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeLive), managedAgentTurnRecordForTest(t, runtimellm.AgentTurnRecord{
		AgentID:        "task-agent",
		Memory:         agentmemory.PlatformDefault(),
		SessionID:      sessionID,
		RunID:          runID,
		FlowInstance:   flowInstance,
		RequestPayload: []byte(`{"kind":"task"}`),
		ResponseRaw:    []byte(`{"ok":true}`),
		ParseOK:        true,
		Latency:        5 * time.Millisecond,
	})); err != nil {
		t.Fatalf("AppendAgentTurn(stateless flow): %v", err)
	}

	var count int
	var entityID, gotFlowInstance, conversation, memorySource string
	var memoryEnabled bool
	if err := store.DB.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(MAX(entity_id), ''), COALESCE(MAX(flow_instance), ''),
		       COALESCE(MAX(conversation), ''), MAX(memory_enabled), COALESCE(MAX(memory_source), '')
		FROM agent_conversation_audits
		WHERE session_id = ?
	`, sessionID).Scan(&count, &entityID, &gotFlowInstance, &conversation, &memoryEnabled, &memorySource); err != nil {
		t.Fatalf("read flow stateless audit row: %v", err)
	}
	if count != 1 {
		t.Fatalf("audit row count = %d, want 1", count)
	}
	if gotFlowInstance != flowInstance || entityID != "" || memoryEnabled || memorySource != "platform_default" {
		t.Fatalf("audit entity=%q flow_instance=%q memory=%v source=%q", entityID, gotFlowInstance, memoryEnabled, memorySource)
	}
	if conversation != "[]" {
		t.Fatalf("conversation = %s, want empty stateless audit snapshot", conversation)
	}

	var linkedTurns int
	if err := store.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM agent_turns
		WHERE session_id = ? AND memory_enabled = 0
	`, sessionID).Scan(&linkedTurns); err != nil {
		t.Fatalf("count linked stateless turns: %v", err)
	}
	if linkedTurns != 1 {
		t.Fatalf("linked task turn count = %d, want 1", linkedTurns)
	}
}

func TestSQLiteRuntimeStoreLifecycleTerminationCleansMutableRuntimeState(t *testing.T) {
	ctx := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	now := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	store.SetNowFnForTest(func() time.Time { return now })
	runID := uuid.NewString()
	if _, err := store.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES (?, 'running', ?)`, runID, now); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	if err := store.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:            "agent-cleanup-1",
			Role:          "operator",
			FlowID:        "global",
			Model:         "regular",
			LLMBackend:    "anthropic",
			ExecutionMode: "live",
			Memory:        agentmemory.Authored(true),
			FlowPath:      "global",
			Config:        json.RawMessage(`{"system_prompt":"test","tools":[]}`),
		},
		Status:    "active",
		StartedAt: now,
	}); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}
	identity := agentmemory.Identity{RunID: runID, AgentID: "agent-cleanup-1", FlowInstance: "global"}
	lease, err := store.Acquire(ctx, identity, "owner-1")
	if err != nil {
		t.Fatalf("Acquire session: %v", err)
	}
	if err := store.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID: lease.SessionID,
		AgentID:   identity.AgentID,
		Identity:  identity,
		Memory:    agentmemory.Authored(true),
		Messages:  []runtimellm.Message{{Role: "user", Content: "hello"}},
		Summary:   "session",
		TurnCount: 1,
		Status:    "active",
	}); err != nil {
		t.Fatalf("UpsertConversation(memory): %v", err)
	}
	if err := appendManagedAgentTurnForTest(t, runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeLive), store, runtimellm.AgentTurnRecord{
		SessionID:      uuid.NewString(),
		AgentID:        identity.AgentID,
		RunID:          identity.RunID,
		FlowInstance:   identity.FlowInstance,
		Memory:         agentmemory.PlatformDefault(),
		RequestPayload: []byte(`{"kind":"stateless"}`),
		ResponseRaw:    []byte(`{"ok":true}`),
		ParseOK:        true,
	}); err != nil {
		t.Fatalf("seed stateless audit: %v", err)
	}

	if _, err := store.CommitAgentLifecycleTransition(ctx, runtimemanager.AgentLifecycleTransition{
		OperationID: uuid.NewString(), OperationKind: "teardown", RequestHash: "sqlite-test-terminate-agent-cleanup-1",
		AgentID: "agent-cleanup-1", Trigger: "test", ExpectedPhase: runtimemanager.AgentLifecycleRegistered,
		TargetEpoch: 1, TargetGeneration: 1, TargetPhase: runtimemanager.AgentLifecycleTerminated,
		ConfigRevision: "test", RunMode: runtimemanager.AgentRunModeStopped,
		Subordinate: runtimesessions.LifecycleMutationPlan{
			Action: runtimesessions.LifecycleMutationTerminateCurrentSet, TerminationReason: runtimesessions.TerminationReasonCancelled,
		},
		Now: now,
	}); err != nil {
		t.Fatalf("terminate through lifecycle authority: %v", err)
	}

	var (
		agentStatus      string
		sessionStatus    string
		sessionReason    string
		terminatedRaw    any
		auditStatus      string
		activeAuditCount int
	)
	if err := store.DB.QueryRowContext(ctx, `SELECT status FROM agents WHERE agent_id = ?`, "agent-cleanup-1").Scan(&agentStatus); err != nil {
		t.Fatalf("read agent status: %v", err)
	}
	if err := store.DB.QueryRowContext(ctx, `
		SELECT status, COALESCE(termination_reason, ''), terminated_at
		FROM agent_sessions
		WHERE agent_id = ?
	`, "agent-cleanup-1").Scan(&sessionStatus, &sessionReason, &terminatedRaw); err != nil {
		t.Fatalf("read session status: %v", err)
	}
	if err := store.DB.QueryRowContext(ctx, `
		SELECT status
		FROM agent_conversation_audits
		WHERE agent_id = ?
		ORDER BY created_at DESC
		LIMIT 1
	`, "agent-cleanup-1").Scan(&auditStatus); err != nil {
		t.Fatalf("read audit status: %v", err)
	}
	if err := store.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM agent_conversation_audits
		WHERE agent_id = ?
		  AND status = 'active'
	`, "agent-cleanup-1").Scan(&activeAuditCount); err != nil {
		t.Fatalf("count active audits: %v", err)
	}
	terminatedAt, ok, err := sqliteTimeValue(terminatedRaw)
	if err != nil {
		t.Fatalf("scan terminated_at: %v", err)
	}
	if agentStatus != "terminated" {
		t.Fatalf("agent status = %q, want terminated", agentStatus)
	}
	if sessionStatus != "terminated" {
		t.Fatalf("session status = %q, want terminated", sessionStatus)
	}
	if sessionReason != "cancelled" {
		t.Fatalf("session termination_reason = %q, want cancelled", sessionReason)
	}
	if !ok || terminatedAt.IsZero() {
		t.Fatalf("session terminated_at = %v ok=%v, want non-zero", terminatedAt, ok)
	}
	if auditStatus != "active" || activeAuditCount != 1 {
		t.Fatalf("audit status = %q active_count=%d, want immutable active evidence", auditStatus, activeAuditCount)
	}
}

func operatorDeliveriesContain(deliveries []OperatorEventDelivery, subscriberType, subscriberID string) bool {
	for _, delivery := range deliveries {
		if delivery.SubscriberType == subscriberType && delivery.SubscriberID == subscriberID {
			return true
		}
	}
	return false
}

func TestSQLiteRuntimeStoreClaimScheduleRequiresActiveRow(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)
	seedSQLiteScheduleRun(t, store, ctx, runID)
	schedule := runtimepipeline.Schedule{
		RunID:     runID,
		AgentID:   "agent-1",
		EventType: "timer.fired",
		Mode:      "once",
		At:        time.Now().UTC().Add(time.Hour),
		TaskID:    "task-claim",
		Payload:   json.RawMessage(`{"__schedule_task_id":"task-claim"}`),
	}

	if err := store.UpsertSchedule(ctx, schedule); err != nil {
		t.Fatalf("UpsertSchedule: %v", err)
	}
	claimed, err := store.ClaimSchedule(ctx, schedule)
	if err != nil {
		t.Fatalf("ClaimSchedule active: %v", err)
	}
	if !claimed {
		t.Fatal("ClaimSchedule active = false, want true")
	}
	if err := store.CancelScheduleExact(ctx, schedule); err != nil {
		t.Fatalf("CancelScheduleExact: %v", err)
	}
	claimed, err = store.ClaimSchedule(ctx, schedule)
	if err != nil {
		t.Fatalf("ClaimSchedule cancelled: %v", err)
	}
	if claimed {
		t.Fatal("ClaimSchedule cancelled = true, want false")
	}
	if err := store.UpsertSchedule(ctx, schedule); err != nil {
		t.Fatalf("UpsertSchedule after cancel: %v", err)
	}
	if err := store.MarkScheduleFiredExact(ctx, schedule); err != nil {
		t.Fatalf("MarkScheduleFiredExact: %v", err)
	}
	claimed, err = store.ClaimSchedule(ctx, schedule)
	if err != nil {
		t.Fatalf("ClaimSchedule fired: %v", err)
	}
	if claimed {
		t.Fatal("ClaimSchedule fired = true, want false")
	}
}

func TestSQLiteRuntimeStoreScheduleUsesPipelineTransactionForCommitVisibility(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)
	seedSQLiteScheduleRun(t, store, ctx, runID)
	schedule := sqliteScheduleTransactionTestSchedule(runID, "task-commit")

	tx, err := store.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx(upsert): %v", err)
	}
	txctx := runtimepipeline.WithPipelineSQLTxContext(ctx, tx)
	if err := store.UpsertSchedule(txctx, schedule); err != nil {
		t.Fatalf("UpsertSchedule(tx): %v", err)
	}
	assertSQLiteScheduleClaimed(t, store, txctx, schedule, true)
	assertSQLiteActiveScheduleCount(t, store, txctx, 1)
	assertSQLiteScheduleClaimed(t, store, ctx, schedule, false)
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit(upsert): %v", err)
	}
	assertSQLiteScheduleClaimed(t, store, ctx, schedule, true)

	tx, err = store.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx(cancel): %v", err)
	}
	txctx = runtimepipeline.WithPipelineSQLTxContext(ctx, tx)
	if err := store.CancelScheduleExact(txctx, schedule); err != nil {
		t.Fatalf("CancelScheduleExact(tx): %v", err)
	}
	assertSQLiteScheduleClaimed(t, store, txctx, schedule, false)
	assertSQLiteScheduleClaimed(t, store, ctx, schedule, true)
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit(cancel): %v", err)
	}
	assertSQLiteScheduleClaimed(t, store, ctx, schedule, false)

	if err := store.UpsertSchedule(ctx, schedule); err != nil {
		t.Fatalf("UpsertSchedule(before complete): %v", err)
	}
	tx, err = store.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx(complete): %v", err)
	}
	txctx = runtimepipeline.WithPipelineSQLTxContext(ctx, tx)
	if err := store.CompleteScheduleFireExact(txctx, schedule); err != nil {
		t.Fatalf("CompleteScheduleFireExact(tx): %v", err)
	}
	assertSQLiteScheduleClaimed(t, store, txctx, schedule, false)
	assertSQLiteScheduleClaimed(t, store, ctx, schedule, true)
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit(complete): %v", err)
	}
	assertSQLiteScheduleClaimed(t, store, ctx, schedule, false)
}

func TestSQLiteRuntimeStoreScheduleUsesPipelineTransactionForRollbackVisibility(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)
	seedSQLiteScheduleRun(t, store, ctx, runID)
	schedule := sqliteScheduleTransactionTestSchedule(runID, "task-rollback")

	tx, err := store.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx(upsert rollback): %v", err)
	}
	txctx := runtimepipeline.WithPipelineSQLTxContext(ctx, tx)
	if err := store.UpsertSchedule(txctx, schedule); err != nil {
		t.Fatalf("UpsertSchedule(tx): %v", err)
	}
	assertSQLiteScheduleClaimed(t, store, txctx, schedule, true)
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback(upsert): %v", err)
	}
	assertSQLiteScheduleClaimed(t, store, ctx, schedule, false)

	if err := store.UpsertSchedule(ctx, schedule); err != nil {
		t.Fatalf("UpsertSchedule(seed active): %v", err)
	}
	tx, err = store.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx(cancel rollback): %v", err)
	}
	txctx = runtimepipeline.WithPipelineSQLTxContext(ctx, tx)
	if err := store.CancelScheduleExact(txctx, schedule); err != nil {
		t.Fatalf("CancelScheduleExact(tx): %v", err)
	}
	assertSQLiteScheduleClaimed(t, store, txctx, schedule, false)
	assertSQLiteScheduleClaimed(t, store, ctx, schedule, true)
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback(cancel): %v", err)
	}
	assertSQLiteScheduleClaimed(t, store, ctx, schedule, true)
}

func seedSQLiteScheduleRun(t *testing.T, store *SQLiteRuntimeStore, ctx context.Context, runID string) {
	t.Helper()
	if _, err := store.DB.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_source, started_at)
		VALUES (?, 'running', 'legacy', ?)
	`, runID, time.Now().UTC()); err != nil {
		t.Fatalf("seed sqlite run row: %v", err)
	}
}

func sqliteScheduleTransactionTestSchedule(runID, taskID string) runtimepipeline.Schedule {
	return runtimepipeline.Schedule{
		RunID:     runID,
		AgentID:   "agent-1",
		EventType: "timer.fired",
		Mode:      "once",
		At:        time.Now().UTC().Add(time.Hour),
		TaskID:    taskID,
		Payload:   json.RawMessage(`{"__schedule_task_id":"` + taskID + `"}`),
	}
}

func assertSQLiteScheduleClaimed(t *testing.T, store *SQLiteRuntimeStore, ctx context.Context, schedule runtimepipeline.Schedule, want bool) {
	t.Helper()
	claimed, err := store.ClaimSchedule(ctx, schedule)
	if err != nil {
		t.Fatalf("ClaimSchedule: %v", err)
	}
	if claimed != want {
		t.Fatalf("ClaimSchedule = %v, want %v", claimed, want)
	}
}

func assertSQLiteActiveScheduleCount(t *testing.T, store *SQLiteRuntimeStore, ctx context.Context, want int) {
	t.Helper()
	active, err := store.LoadActiveSchedules(ctx)
	if err != nil {
		t.Fatalf("LoadActiveSchedules: %v", err)
	}
	if len(active) != want {
		t.Fatalf("active schedule count = %d, want %d: %#v", len(active), want, active)
	}
}

func newBootstrappedSQLiteRuntimeStoreForTest(t *testing.T) *SQLiteRuntimeStore {
	t.Helper()
	return newBootstrappedSQLiteRuntimeStoreForPath(t, filepath.Join(t.TempDir(), ".swarm", "dev.db"))
}

func newBootstrappedSQLiteRuntimeStoreForPath(t *testing.T, dbPath string) *SQLiteRuntimeStore {
	t.Helper()
	spec := loadPlatformSpecForSQLiteSchemaTest(t)
	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs: %v", err)
	}
	store, err := NewSQLiteRuntimeStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteRuntimeStore: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite runtime store: %v", err)
		}
	})
	if err := store.BootstrapSchema(testAuthorActivityContext(), SchemaBootstrapRequest{
		PlatformPlans: plans,
		Origin: RuntimeStoreOrigin{
			SwarmVersion:    "sqlite-runtime-test",
			PlatformVersion: spec.Platform.Version,
			CreatedAt:       time.Now().UTC(),
		},
	}); err != nil {
		t.Fatalf("BootstrapSchema: %v", err)
	}
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("sqlite runtime store did not create file-backed db at %s: %v", dbPath, err)
	}
	registerTestAuthorActivityCatalog(t, store)
	return store
}
