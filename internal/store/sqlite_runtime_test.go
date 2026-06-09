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
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/google/uuid"
)

func TestSQLiteRuntimeStoreSelectedCoreContracts(t *testing.T) {
	ctx := context.Background()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)

	evtID := uuid.NewString()
	evt := events.NewProjectionEvent(evtID,

		events.EventType("test.started"),
		"agent-1", "", json.RawMessage(`{"ok":true}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())

	if err := store.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	if err := store.InsertEventDeliveries(ctx, evtID, []string{"agent-1"}); err != nil {
		t.Fatalf("InsertEventDeliveries: %v", err)
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
			ID:               "agent-1",
			Role:             "operator",
			Mode:             "global",
			Model:            "regular",
			LLMBackend:       "anthropic",
			ConversationMode: "task",
			Config:           json.RawMessage(`{"system_prompt":"You are an operator.","tools":[]}`),
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
	if err := store.EnsureEntitySchema(ctx, entityID); err != nil {
		t.Fatalf("EnsureEntitySchema: %v", err)
	}

	itemID, err := store.InsertMailboxItem(ctx, runtimetools.MailboxItem{
		EventID:   evtID,
		EntityID:  entityID,
		FromAgent: "agent-1",
		Type:      "human_task",
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
	if err := store.DecideMailboxItem(ctx, itemID, "decided", "approved", "ok"); err != nil {
		t.Fatalf("DecideMailboxItem: %v", err)
	}
	item, err := store.GetMailboxItem(ctx, itemID)
	if err != nil {
		t.Fatalf("GetMailboxItem: %v", err)
	}
	if item.Status != "decided" || item.Decision != "approved" {
		t.Fatalf("mailbox item status=%q decision=%q, want decided/approved", item.Status, item.Decision)
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

func TestSQLiteRuntimeStoreUpsertAgentConsumesActivePipelineTransaction(t *testing.T) {
	ctx := runtimecorrelation.WithRunID(context.Background(), uuid.NewString())
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
			ID:               "agent-in-pipeline-tx",
			Role:             "worker",
			Mode:             "global",
			Model:            "regular",
			LLMBackend:       "anthropic",
			ConversationMode: "task",
			Config:           json.RawMessage(`{"system_prompt":"tx-owned agent","tools":[]}`),
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
	ctx := runtimecorrelation.WithRunID(context.Background(), uuid.NewString())
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	workflowStore := runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(sqliteStore.DB, sqliteStore)
	bus := &sqliteFlowActivationBus{}
	manager := runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		WorkflowInstances: workflowStore,
		LLMBackend:        "anthropic",
	}, sqliteStore)
	bundle := sqliteFlowActivationBundle()

	req := sqliteFlowActivationRequest(bundle, "review", "inst-1", "parent-ent", "review/inst-1")
	if err := workflowStore.RunInPipelineTransaction(ctx, func(txctx context.Context, _ *sql.Tx) error {
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
	ctx := runtimecorrelation.WithRunID(context.Background(), uuid.NewString())
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	workflowStore := runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(sqliteStore.DB, sqliteStore)
	bus := &sqliteFlowActivationBus{}
	manager := runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		WorkflowInstances: workflowStore,
		LLMBackend:        "anthropic",
	}, sqliteStore)
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
			errs <- workflowStore.RunInPipelineTransaction(ctx, func(txctx context.Context, _ *sql.Tx) error {
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
	if logs := bus.runtimeLogEntries(); len(logs) != 0 {
		t.Fatalf("runtime logs = %#v, want no activation dead-letter/runtime errors", logs)
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
	ctx := context.Background()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	bus, err := runtimebus.NewEventBus(store)
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
		if err := bus.Publish(ctx, (events.NewProjectionEvent(eventID,

			events.EventType("item.received"),
			"api.v1", "", json.RawMessage(`{"entity_id":"11111111-1111-1111-1111-111111111111","topic":"medicine"}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())).WithEntityID(entityID)); err != nil {
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
	ctx := context.Background()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	store.SetEventPayloadValidator(func(eventType string, _ []byte) error {
		if eventType == "item.failed" {
			return errors.New("schema violation")
		}
		return nil
	})
	bus, err := runtimebus.NewEventBus(store)
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
		err := bus.Publish(ctx, (events.NewProjectionEvent(eventID,

			events.EventType("item.failed"),
			"api.v1", "", json.RawMessage(`{"entity_id":"22222222-2222-2222-2222-222222222222"}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())).WithEntityID("22222222-2222-2222-2222-222222222222"))
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
	ctx := context.Background()
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

func TestSQLiteRuntimeStoreAppendEventTxEnsuresFreshRunRow(t *testing.T) {
	ctx := context.Background()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	tx, err := store.BeginEventTx(ctx)
	if err != nil {
		t.Fatalf("BeginEventTx: %v", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	runID := uuid.NewString()
	eventID := uuid.NewString()
	if err := store.AppendEventTx(ctx, tx, (events.NewProjectionEvent(eventID,

		events.EventType("item.received"),
		"api.v1", "", json.RawMessage(`{"entity_id":"33333333-3333-3333-3333-333333333333"}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())).WithEntityID("33333333-3333-3333-3333-333333333333")); err != nil {
		t.Fatalf("AppendEventTx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit event tx: %v", err)
	}
	committed = true

	assertSQLiteRuntimeCount(t, store, `SELECT COUNT(*) FROM runs WHERE run_id = ?`, 1, runID)
	assertSQLiteRuntimeCount(t, store, `SELECT COUNT(*) FROM events WHERE event_id = ?`, 1, eventID)
}

func TestSQLiteRuntimeStoreRuntimeIngressReadDuringPublishTxDoesNotReenterWrite(t *testing.T) {
	ctx := context.Background()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	if _, err := store.DB.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES (?, 'running')
	`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	bus, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	controller := runtimeingress.NewController(store, bus, runtimeingress.Options{})
	if err := controller.SyncState(ctx); err != nil {
		t.Fatalf("SyncState: %v", err)
	}
	bus.SetRuntimeIngressDispatchGate(controller)

	eventID := uuid.NewString()
	err = bus.Publish(ctx, (events.NewProjectionEvent(eventID,

		events.EventType("item.received"),
		"api.v1", "", []byte(`{"entity_id":"11111111-1111-1111-1111-111111111111"}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())).WithEntityID("11111111-1111-1111-1111-111111111111"))
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
	if err := store.DB.QueryRowContext(context.Background(), query, args...).Scan(&count); err != nil {
		t.Fatalf("count sqlite runtime rows: %v", err)
	}
	if count != want {
		t.Fatalf("sqlite count for %q = %d, want %d", query, count, want)
	}
}

func TestSQLiteRuntimeStorePipelineWorkflowInstanceOwner(t *testing.T) {
	ctx := context.Background()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)
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

func TestSQLiteRuntimeStoreSystemNodeReceiptOwnerSettlesDelivery(t *testing.T) {
	ctx := context.Background()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	eventID := uuid.NewString()
	evt := events.NewProjectionEvent(eventID,

		events.EventType("company.scanned"),
		"agent-1", "", json.RawMessage(`{"ok":true}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())

	if err := store.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	if err := store.InsertEventDeliveryRoutes(ctx, eventID, []events.DeliveryRoute{{SubscriberType: "node", SubscriberID: "background-node"}}); err != nil {
		t.Fatalf("InsertEventDeliveryRoutes: %v", err)
	}
	owner := runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(store.DB, store)
	if processed, err := owner.SystemNodeProcessed(ctx, "background-node", eventID); err != nil || processed {
		t.Fatalf("SystemNodeProcessed before mark = %v err=%v, want false nil", processed, err)
	}
	if err := owner.MarkSystemNodeDeliveryInProgress(ctx, "background-node", eventID, runtimepipeline.DefaultSystemNodeRetryLimit); err != nil {
		t.Fatalf("MarkSystemNodeDeliveryInProgress: %v", err)
	}
	var inProgressStatus, inProgressReason string
	if err := store.DB.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), COALESCE(reason_code, '')
		FROM event_deliveries
		WHERE event_id = ? AND subscriber_type = 'node' AND subscriber_id = 'background-node'
	`, eventID).Scan(&inProgressStatus, &inProgressReason); err != nil {
		t.Fatalf("load sqlite node delivery after start: %v", err)
	}
	if inProgressStatus != "in_progress" || inProgressReason != "node_processing" {
		t.Fatalf("node delivery after start = %s/%s, want in_progress/node_processing", inProgressStatus, inProgressReason)
	}
	if err := owner.MarkSystemNodeProcessedAndSettleDelivery(ctx, "background-node", eventID, `{"idempotency_key":"test"}`); err != nil {
		t.Fatalf("MarkSystemNodeProcessedAndSettleDelivery: %v", err)
	}
	if processed, err := owner.SystemNodeProcessed(ctx, "background-node", eventID); err != nil || !processed {
		t.Fatalf("SystemNodeProcessed after mark = %v err=%v, want true nil", processed, err)
	}
	var deliveryStatus, receiptOutcome string
	if err := store.DB.QueryRowContext(ctx, `
		SELECT status
		FROM event_deliveries
		WHERE event_id = ? AND subscriber_type = 'node' AND subscriber_id = 'background-node'
	`, eventID).Scan(&deliveryStatus); err != nil {
		t.Fatalf("load sqlite node delivery: %v", err)
	}
	if deliveryStatus != "delivered" {
		t.Fatalf("node delivery status = %q, want delivered", deliveryStatus)
	}
	if err := store.DB.QueryRowContext(ctx, `
		SELECT outcome
		FROM event_receipts
		WHERE event_id = ? AND subscriber_type = 'node' AND subscriber_id = 'background-node'
	`, eventID).Scan(&receiptOutcome); err != nil {
		t.Fatalf("load sqlite node receipt: %v", err)
	}
	if receiptOutcome != "no_op" {
		t.Fatalf("node receipt outcome = %q, want no_op", receiptOutcome)
	}
}

func TestSQLiteRuntimeStoreSystemNodeReceiptOwnerDeadLettersDelivery(t *testing.T) {
	ctx := context.Background()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	eventID := uuid.NewString()
	evt := events.NewProjectionEvent(eventID,
		events.EventType("company.scanned"),
		"agent-1", "", json.RawMessage(`{"ok":true}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())

	if err := store.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	if err := store.InsertEventDeliveryRoutes(ctx, eventID, []events.DeliveryRoute{{SubscriberType: "node", SubscriberID: "background-node"}}); err != nil {
		t.Fatalf("InsertEventDeliveryRoutes: %v", err)
	}
	owner := runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(store.DB, store)
	if err := owner.MarkSystemNodeDeliveryInProgress(ctx, "background-node", eventID, runtimepipeline.DefaultSystemNodeRetryLimit); err != nil {
		t.Fatalf("MarkSystemNodeDeliveryInProgress: %v", err)
	}
	if err := owner.MarkSystemNodeDeliveryDeadLetter(ctx, "background-node", eventID, "retry_exhausted", "boom", 2, `{"idempotency_key":"test"}`); err != nil {
		t.Fatalf("MarkSystemNodeDeliveryDeadLetter: %v", err)
	}
	if processed, err := owner.SystemNodeProcessed(ctx, "background-node", eventID); err != nil || !processed {
		t.Fatalf("SystemNodeProcessed after dead-letter = %v err=%v, want true nil", processed, err)
	}
	var deliveryStatus, deliveryReason, deliveryError, receiptOutcome string
	var retryCount int
	if err := store.DB.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), COALESCE(reason_code, ''), COALESCE(last_error, ''), COALESCE(retry_count, 0)
		FROM event_deliveries
		WHERE event_id = ? AND subscriber_type = 'node' AND subscriber_id = 'background-node'
	`, eventID).Scan(&deliveryStatus, &deliveryReason, &deliveryError, &retryCount); err != nil {
		t.Fatalf("load sqlite node delivery: %v", err)
	}
	if deliveryStatus != "dead_letter" || deliveryReason != "retry_exhausted" || deliveryError != "boom" || retryCount != 2 {
		t.Fatalf("sqlite node delivery = %s/%s retry=%d err=%q, want dead_letter/retry_exhausted retry=2 err=boom", deliveryStatus, deliveryReason, retryCount, deliveryError)
	}
	if err := store.DB.QueryRowContext(ctx, `
		SELECT COALESCE(outcome, '')
		FROM event_receipts
		WHERE event_id = ? AND subscriber_type = 'node' AND subscriber_id = 'background-node'
	`, eventID).Scan(&receiptOutcome); err != nil {
		t.Fatalf("load sqlite node receipt: %v", err)
	}
	if receiptOutcome != "dead_letter" {
		t.Fatalf("node receipt outcome = %q, want dead_letter", receiptOutcome)
	}
}

func TestSQLiteRuntimeStoreSystemNodeReceiptOwnerFailsWithoutDeliveryAuthority(t *testing.T) {
	ctx := context.Background()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	eventID := uuid.NewString()
	evt := events.NewProjectionEvent(eventID,

		events.EventType("company.scanned"),
		"agent-1", "", json.RawMessage(`{"ok":true}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())

	if err := store.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	owner := runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(store.DB, store)
	err := owner.MarkSystemNodeProcessedAndSettleDelivery(ctx, "background-node", eventID, `{"idempotency_key":"test"}`)
	if !errors.Is(err, runtimepipeline.ErrSystemNodeDeliveryAuthorityMissing) {
		t.Fatalf("MarkSystemNodeProcessedAndSettleDelivery error = %v, want ErrSystemNodeDeliveryAuthorityMissing", err)
	}
	var deliveries, receipts int
	if err := store.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE event_id = ? AND subscriber_type = 'node' AND subscriber_id = 'background-node'
	`, eventID).Scan(&deliveries); err != nil {
		t.Fatalf("count sqlite node deliveries: %v", err)
	}
	if deliveries != 0 {
		t.Fatalf("sqlite node deliveries = %d, want 0", deliveries)
	}
	if err := store.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE event_id = ? AND subscriber_type = 'node' AND subscriber_id = 'background-node'
	`, eventID).Scan(&receipts); err != nil {
		t.Fatalf("count sqlite node receipts: %v", err)
	}
	if receipts != 0 {
		t.Fatalf("sqlite node receipts = %d, want 0", receipts)
	}
}

func TestSQLiteRuntimeStoreSystemNodeReceiptOwnerFailsWithTerminalDeliveryAuthority(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     string
		retryCount int
	}{
		{name: "dead_letter", status: "dead_letter", retryCount: 2},
		{name: "retry_exhausted_failed", status: "failed", retryCount: runtimepipeline.DefaultSystemNodeRetryLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := newBootstrappedSQLiteRuntimeStoreForTest(t)
			runID := uuid.NewString()
			ctx = runtimecorrelation.WithRunID(ctx, runID)
			eventID := uuid.NewString()
			evt := events.NewProjectionEvent(eventID,

				events.EventType("company.scanned"),
				"agent-1", "", json.RawMessage(`{"ok":true}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())

			if err := store.AppendEvent(ctx, evt); err != nil {
				t.Fatalf("AppendEvent: %v", err)
			}
			if _, err := store.DB.ExecContext(ctx, `
				INSERT INTO event_deliveries (
					delivery_id, run_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, created_at
				) VALUES (
					?, ?, ?, 'node', 'background-node', ?, ?, 'terminal_test', ?
				)
			`, uuid.NewString(), runID, eventID, tc.status, tc.retryCount, time.Now().UTC()); err != nil {
				t.Fatalf("seed sqlite terminal node delivery: %v", err)
			}
			owner := runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(store.DB, store)
			err := owner.MarkSystemNodeProcessedAndSettleDelivery(ctx, "background-node", eventID, `{"idempotency_key":"test"}`)
			if !errors.Is(err, runtimepipeline.ErrSystemNodeDeliveryAuthorityMissing) {
				t.Fatalf("MarkSystemNodeProcessedAndSettleDelivery error = %v, want ErrSystemNodeDeliveryAuthorityMissing", err)
			}
			var status string
			var retryCount int
			if err := store.DB.QueryRowContext(ctx, `
				SELECT COALESCE(status, ''), COALESCE(retry_count, 0)
				FROM event_deliveries
				WHERE event_id = ? AND subscriber_type = 'node' AND subscriber_id = 'background-node'
			`, eventID).Scan(&status, &retryCount); err != nil {
				t.Fatalf("load sqlite terminal delivery: %v", err)
			}
			if status != tc.status || retryCount != tc.retryCount {
				t.Fatalf("sqlite terminal delivery = %s/%d, want %s/%d", status, retryCount, tc.status, tc.retryCount)
			}
			var receipts int
			if err := store.DB.QueryRowContext(ctx, `
				SELECT COUNT(*)
				FROM event_receipts
				WHERE event_id = ? AND subscriber_type = 'node' AND subscriber_id = 'background-node'
			`, eventID).Scan(&receipts); err != nil {
				t.Fatalf("count sqlite node receipts: %v", err)
			}
			if receipts != 0 {
				t.Fatalf("sqlite node receipts = %d, want 0", receipts)
			}
		})
	}
}

func TestSQLiteRuntimeStoreDeliveryReplayAndReceiptSemantics(t *testing.T) {
	ctx := context.Background()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	now := time.Now().UTC()
	store.SetNowFnForTest(func() time.Time { return now })

	eventID := uuid.NewString()
	evt := events.NewProjectionEvent(eventID,

		events.EventType("test.delivery_requested"),
		"runtime", "", json.RawMessage(`{"delivery":true}`), 0, runID, "", events.EventEnvelope{}, now)

	if err := store.PersistEventWithDeliveriesAndScope(ctx, evt, []string{"agent-1"}, runtimereplayclaim.CommittedReplayScopeSubscribed); err != nil {
		t.Fatalf("PersistEventWithDeliveriesAndScope: %v", err)
	}

	scope, err := store.LoadCommittedReplayScope(ctx, eventID)
	if err != nil {
		t.Fatalf("LoadCommittedReplayScope: %v", err)
	}
	if scope != runtimereplayclaim.CommittedReplayScopeSubscribed {
		t.Fatalf("committed replay scope = %q, want subscribed", scope)
	}

	pending, err := store.ListPendingEventsForAgent(ctx, "agent-1", now.Add(-time.Minute), 10)
	if err != nil {
		t.Fatalf("ListPendingEventsForAgent: %v", err)
	}
	if len(pending) != 1 || pending[0].ID() != eventID {
		t.Fatalf("pending events = %#v, want %s", pending, eventID)
	}
	if err := store.MarkEventDeliveryInProgress(ctx, eventID, "agent-1", uuid.NewString()); err != nil {
		t.Fatalf("MarkEventDeliveryInProgress: %v", err)
	}
	if err := store.UpsertEventReceipt(ctx, eventID, "agent-1", runtimemanager.ReceiptStatusError, "boom"); err != nil {
		t.Fatalf("UpsertEventReceipt retryable error: %v", err)
	}
	receipt, ok, err := store.GetEventReceipt(ctx, eventID, "agent-1")
	if err != nil {
		t.Fatalf("GetEventReceipt retryable error: %v", err)
	}
	if !ok || receipt.Status != runtimemanager.ReceiptStatusError || receipt.RetryCount != 1 || receipt.Error != "boom" {
		t.Fatalf("retryable receipt = %+v ok=%v, want error retry_count=1 boom", receipt, ok)
	}
	if err := store.UpsertEventReceipt(ctx, eventID, "agent-1", runtimemanager.ReceiptStatusProcessed, ""); err != nil {
		t.Fatalf("UpsertEventReceipt: %v", err)
	}
	receipt, ok, err = store.GetEventReceipt(ctx, eventID, "agent-1")
	if err != nil {
		t.Fatalf("GetEventReceipt: %v", err)
	}
	if !ok || receipt.Status != runtimemanager.ReceiptStatusProcessed {
		t.Fatalf("receipt = %+v ok=%v, want processed", receipt, ok)
	}
	pending, err = store.ListPendingEventsForAgent(ctx, "agent-1", now.Add(-time.Minute), 10)
	if err != nil {
		t.Fatalf("ListPendingEventsForAgent after receipt: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending events after receipt = %#v, want none", pending)
	}

	missing, err := store.ListEventsMissingPipelineReceiptForRun(ctx, runID, now.Add(-time.Minute), 10)
	if err != nil {
		t.Fatalf("ListEventsMissingPipelineReceiptForRun: %v", err)
	}
	if len(missing) != 1 || missing[0].Event.ID() != eventID {
		t.Fatalf("missing pipeline receipts = %#v, want %s", missing, eventID)
	}
	lease, claimed, err := store.ClaimPipelineReplay(ctx, eventID)
	if err != nil {
		t.Fatalf("ClaimPipelineReplay: %v", err)
	}
	if !claimed || lease == nil {
		t.Fatalf("ClaimPipelineReplay claimed=%v lease=%#v, want claim", claimed, lease)
	}
	if _, claimedAgain, err := store.ClaimPipelineReplay(ctx, eventID); err != nil || claimedAgain {
		t.Fatalf("second ClaimPipelineReplay claimed=%v err=%v, want busy/no claim", claimedAgain, err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release replay claim: %v", err)
	}
	if err := store.UpsertPipelineReceipt(ctx, eventID, "processed", ""); err != nil {
		t.Fatalf("UpsertPipelineReceipt: %v", err)
	}
	missing, err = store.ListEventsMissingPipelineReceiptForRun(ctx, runID, now.Add(-time.Minute), 10)
	if err != nil {
		t.Fatalf("ListEventsMissingPipelineReceiptForRun after receipt: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("missing pipeline receipts after receipt = %#v, want none", missing)
	}

	subSelfID := uuid.NewString()
	subOtherID := uuid.NewString()
	subNoDeliveryID := uuid.NewString()
	subEvt := func(id string, offset time.Duration) events.Event {
		return events.NewProjectionEvent(id,

			events.EventType("subscription.visible"),
			"runtime", "", json.RawMessage(`{"subscription":true}`), 0, runID, "", events.EventEnvelope{}, now.Add(offset))

	}
	if err := store.AppendEvent(ctx, subEvt(subSelfID, time.Second)); err != nil {
		t.Fatalf("AppendEvent subscription self: %v", err)
	}
	if err := store.InsertEventDeliveries(ctx, subSelfID, []string{"agent-2"}); err != nil {
		t.Fatalf("InsertEventDeliveries subscription self: %v", err)
	}
	if err := store.AppendEvent(ctx, subEvt(subOtherID, 2*time.Second)); err != nil {
		t.Fatalf("AppendEvent subscription other: %v", err)
	}
	if err := store.InsertEventDeliveries(ctx, subOtherID, []string{"agent-1"}); err != nil {
		t.Fatalf("InsertEventDeliveries subscription other: %v", err)
	}
	if err := store.AppendEvent(ctx, subEvt(subNoDeliveryID, 3*time.Second)); err != nil {
		t.Fatalf("AppendEvent subscription no delivery: %v", err)
	}
	subscribed, err := store.ListPendingSubscribedEvents(ctx, "agent-2", []events.EventType{"subscription.*"}, now.Add(-time.Minute), 10)
	if err != nil {
		t.Fatalf("ListPendingSubscribedEvents: %v", err)
	}
	if len(subscribed) != 1 || subscribed[0].ID() != subSelfID {
		t.Fatalf("subscribed pending events = %#v, want only direct self %s", subscribed, subSelfID)
	}
}

func TestSQLiteRuntimeStoreSessionStartupConversationAndTraceVisibility(t *testing.T) {
	ctx := context.Background()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	now := time.Now().UTC()
	store.SetNowFnForTest(func() time.Time { return now })

	if err := store.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:                    "agent-1",
			Role:                  "operator",
			Mode:                  "global",
			Model:                 "regular",
			LLMBackend:            "anthropic",
			ConversationMode:      "session",
			SessionScope:          "global",
			SessionScopeAuthority: runtimeactors.SessionScopeAuthorityPlatformInternal,
			Config:                json.RawMessage(`{"system_prompt":"test","tools":[]}`),
		},
		Status:    "active",
		StartedAt: now,
	}); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}

	startupLease, err := store.AcquireRuntimeStartupOwnership(ctx, "runtime-1")
	if err != nil {
		t.Fatalf("AcquireRuntimeStartupOwnership first: %v", err)
	}
	if _, err := store.AcquireRuntimeStartupOwnership(ctx, "runtime-2"); err == nil {
		t.Fatal("AcquireRuntimeStartupOwnership second unexpectedly succeeded")
	}
	if err := startupLease.Release(ctx); err != nil {
		t.Fatalf("release startup lease: %v", err)
	}
	successorStartupLease, err := store.AcquireRuntimeStartupOwnership(ctx, "runtime-2")
	if err != nil {
		t.Fatalf("AcquireRuntimeStartupOwnership after release: %v", err)
	}
	if err := successorStartupLease.Release(ctx); err != nil {
		t.Fatalf("release successor startup lease: %v", err)
	}

	lease, err := store.Acquire(ctx, "agent-1", runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, "owner-1", "")
	if err != nil {
		t.Fatalf("Acquire session: %v", err)
	}
	if lease.ScopeKey != "global" {
		t.Fatalf("session scope key = %q, want global", lease.ScopeKey)
	}
	if _, err := store.Acquire(ctx, "agent-1", runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, "owner-2", ""); !errors.Is(err, runtimesessions.ErrSessionLeased) {
		t.Fatalf("competing Acquire error = %v, want ErrSessionLeased", err)
	}
	if err := store.AdoptSessionID(ctx, "agent-1", runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, "owner-1", "provider-session-1", "global"); err != nil {
		t.Fatalf("AdoptSessionID: %v", err)
	}
	if err := store.IncrementTurn(ctx, "agent-1", runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, lease.SessionID, "global"); err != nil {
		t.Fatalf("IncrementTurn: %v", err)
	}
	if err := store.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID:    lease.SessionID,
		AgentID:      "agent-1",
		SessionScope: "global",
		ScopeKey:     "global",
		Mode:         "session",
		Messages:     []runtimellm.Message{{Role: "user", Content: "hello"}},
		Summary:      "greeting",
		TurnCount:    1,
		Status:       "active",
	}); err != nil {
		t.Fatalf("UpsertConversation: %v", err)
	}
	conversation, ok, err := store.LoadActiveConversation(ctx, "agent-1", "session", "global", "global")
	if err != nil {
		t.Fatalf("LoadActiveConversation: %v", err)
	}
	if !ok || conversation.Summary != "greeting" || len(conversation.Messages) != 1 {
		t.Fatalf("conversation = %+v ok=%v, want persisted greeting", conversation, ok)
	}

	runID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	eventID := uuid.NewString()
	if err := store.PersistEventWithDeliveries(ctx, events.NewProjectionEvent(eventID,

		events.EventType("trace.visible"),
		"agent-1", "", json.RawMessage(`{"trace":true}`), 0, runID, "", events.EventEnvelope{}, now), []string{"agent-1"}); err != nil {
		t.Fatalf("PersistEventWithDeliveries trace event: %v", err)
	}
	if err := store.MarkEventDeliveryInProgress(ctx, eventID, "agent-1", lease.SessionID); err != nil {
		t.Fatalf("MarkEventDeliveryInProgress trace event: %v", err)
	}
	if err := store.AppendAgentTurn(ctx, runtimellm.AgentTurnRecord{
		AgentID:          "agent-1",
		RuntimeMode:      "session",
		SessionID:        lease.SessionID,
		ScopeKey:         "global",
		RunID:            runID,
		TriggerEventID:   eventID,
		TriggerEventType: "trace.visible",
		RequestPayload:   []byte(`{"prompt":"hello"}`),
		ResponseRaw:      []byte(`{"content":"ok"}`),
		ParseOK:          true,
	}); err != nil {
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
	if err := store.AppendEvent(ctx, events.NewProjectionEvent(logID,

		events.EventType("platform.runtime_log"),
		"runtime", "", json.RawMessage(`{"log_level":"warn","message":"runtime warning","details":{"component":"scheduler","action":"session_warning","session_id":"`+lease.SessionID+`"}}`), 0, runID, "", events.EventEnvelope{}, now.Add(time.Second)),
	); err != nil {
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

func TestSQLiteRuntimeStoreMarkAgentTerminatedCleansRuntimeState(t *testing.T) {
	ctx := context.Background()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	now := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	store.SetNowFnForTest(func() time.Time { return now })

	if err := store.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:                    "agent-cleanup-1",
			Role:                  "operator",
			Mode:                  "global",
			Model:                 "regular",
			LLMBackend:            "anthropic",
			ConversationMode:      "session",
			SessionScope:          "global",
			SessionScopeAuthority: runtimeactors.SessionScopeAuthorityPlatformInternal,
			Config:                json.RawMessage(`{"system_prompt":"test","tools":[]}`),
		},
		Status:    "active",
		StartedAt: now,
	}); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}
	lease, err := store.Acquire(ctx, "agent-cleanup-1", runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, "owner-1", "")
	if err != nil {
		t.Fatalf("Acquire session: %v", err)
	}
	if err := store.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID:    lease.SessionID,
		AgentID:      "agent-cleanup-1",
		SessionScope: "global",
		ScopeKey:     "global",
		Mode:         "session",
		Messages:     []runtimellm.Message{{Role: "user", Content: "hello"}},
		Summary:      "session",
		TurnCount:    1,
		Status:       "active",
	}); err != nil {
		t.Fatalf("UpsertConversation(session): %v", err)
	}
	if _, err := store.DB.ExecContext(ctx, `
		INSERT INTO agent_conversation_audits (
			session_id, agent_id, scope_key, scope, conversation, turn_count,
			runtime_mode, runtime_state, status, created_at, updated_at
		)
		VALUES (?, ?, ?, ?, '[]', 0, 'task', '{}', 'active', ?, ?)
	`, uuid.NewString(), "agent-cleanup-1", "global", "global", now, now); err != nil {
		t.Fatalf("seed task audit: %v", err)
	}

	if err := store.MarkAgentTerminated(ctx, "agent-cleanup-1"); err != nil {
		t.Fatalf("MarkAgentTerminated: %v", err)
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
	if auditStatus != "terminated" || activeAuditCount != 0 {
		t.Fatalf("audit status = %q active_count=%d, want terminated and no active audits", auditStatus, activeAuditCount)
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

func TestSQLiteRuntimeStoreV1MailboxAPISelectedOwner(t *testing.T) {
	ctx := context.Background()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	eventID := uuid.NewString()
	if err := store.AppendEvent(ctx, events.NewProjectionEvent(eventID,

		events.EventType("mailbox.requested"),
		"agent-1", "", json.RawMessage(`{"request":true}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC()),
	); err != nil {
		t.Fatalf("AppendEvent source: %v", err)
	}
	entityID := uuid.NewString()
	itemID, err := store.InsertMailboxItem(ctx, runtimetools.MailboxItem{
		EventID:   eventID,
		EntityID:  entityID,
		FromAgent: "agent-1",
		Type:      "approval",
		Priority:  "critical",
		Status:    "pending",
		Summary:   "approve test",
		Context:   json.RawMessage(`{"thing":"test"}`),
	})
	if err != nil {
		t.Fatalf("InsertMailboxItem: %v", err)
	}

	items, nextCursor, err := store.ListV1MailboxItems(ctx, MailboxV1ListOptions{Status: "pending", Limit: 10})
	if err != nil {
		t.Fatalf("ListV1MailboxItems: %v", err)
	}
	if nextCursor != "" || len(items) != 1 {
		t.Fatalf("ListV1MailboxItems len=%d next=%q, want one item no cursor", len(items), nextCursor)
	}
	if items[0].MailboxID != itemID || items[0].SourceRunID != runID || items[0].Status != "pending" {
		t.Fatalf("listed item = %+v, want pending item for run %s", items[0], runID)
	}
	detail, err := store.GetV1MailboxItem(ctx, itemID)
	if err != nil {
		t.Fatalf("GetV1MailboxItem: %v", err)
	}
	if detail.Item.MailboxID != itemID || detail.Payload["thing"] != "test" {
		t.Fatalf("detail = %+v, want item payload", detail)
	}

	now := time.Now().UTC()
	req := MailboxV1DecisionRequest{
		MailboxID:                     itemID,
		Action:                        "approved",
		ActorTokenID:                  "token-1",
		DecisionPayload:               json.RawMessage(`{"approved":true}`),
		Now:                           now,
		ApprovalEventType:             "mailbox.item_decided",
		ApprovalEventSubscribers:      []string{"agent-2"},
		ApprovalEventSubscriberSource: "test",
		Idempotency: &APIIdempotencyRequest{
			Method:         "mailbox.approve",
			ActorTokenID:   "token-1",
			IdempotencyKey: "idem-mailbox",
			RequestHash:    "hash-1",
			Now:            now,
		},
	}
	outcome, err := store.DecideV1MailboxItem(ctx, req)
	if err != nil {
		t.Fatalf("DecideV1MailboxItem approve: %v", err)
	}
	if !outcome.Result.OK || outcome.Result.Status != "decided" || outcome.Result.DownstreamEventName != "mailbox.item_decided" {
		t.Fatalf("approval outcome = %+v, want decided downstream event", outcome.Result)
	}
	var eventName string
	if err := store.DB.QueryRowContext(ctx, `SELECT event_name FROM events WHERE event_id = ?`, outcome.Result.DownstreamEventID).Scan(&eventName); err != nil {
		t.Fatalf("load downstream event: %v", err)
	}
	if eventName != "mailbox.item_decided" {
		t.Fatalf("downstream event_name = %q, want mailbox.item_decided", eventName)
	}
	decided, err := store.GetV1MailboxItem(ctx, itemID)
	if err != nil {
		t.Fatalf("GetV1MailboxItem decided: %v", err)
	}
	if decided.Item.Status != "decided" || decided.Item.Decision != "approved" {
		t.Fatalf("decided item = %+v, want approved decision", decided.Item)
	}
	replayed, err := store.DecideV1MailboxItem(ctx, req)
	if err != nil {
		t.Fatalf("DecideV1MailboxItem replay: %v", err)
	}
	if !replayed.Replayed || replayed.Result.DownstreamEventID != outcome.Result.DownstreamEventID {
		t.Fatalf("replayed outcome = %+v, want idempotent replay of %s", replayed, outcome.Result.DownstreamEventID)
	}
	req.Idempotency.RequestHash = "hash-2"
	_, err = store.DecideV1MailboxItem(ctx, req)
	if !errors.Is(err, ErrAPIIdempotencyConflict) {
		t.Fatalf("DecideV1MailboxItem conflict error = %v, want ErrAPIIdempotencyConflict", err)
	}
}

func TestSQLiteRuntimeStoreClaimScheduleRequiresActiveRow(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
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
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
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
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
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
	if err := store.EnsureSchemaTables(context.Background(), plans); err != nil {
		t.Fatalf("EnsureSchemaTables: %v", err)
	}
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("sqlite runtime store did not create file-backed db at %s: %v", dbPath, err)
	}
	return store
}
