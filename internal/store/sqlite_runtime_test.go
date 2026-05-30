package store

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimeactors "swarm/internal/runtime/core/actors"
	runtimecorrelation "swarm/internal/runtime/correlation"
	runtimeingress "swarm/internal/runtime/ingress"
	runtimellm "swarm/internal/runtime/llm"
	runtimemanager "swarm/internal/runtime/manager"
	runtimepipeline "swarm/internal/runtime/pipeline"
	runtimereplayclaim "swarm/internal/runtime/replayclaim"
	runtimeruncontrol "swarm/internal/runtime/runcontrol"
	runtimesessions "swarm/internal/runtime/sessions"
	runtimetools "swarm/internal/runtime/tools"
)

func TestSQLiteRuntimeStoreSelectedCoreContracts(t *testing.T) {
	ctx := context.Background()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)

	evtID := uuid.NewString()
	evt := events.Event{
		ID:          evtID,
		RunID:       runID,
		Type:        events.EventType("test.started"),
		SourceAgent: "agent-1",
		Payload:     json.RawMessage(`{"ok":true}`),
		CreatedAt:   time.Now().UTC(),
	}
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
			ModelTier:        "generic",
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

func TestSQLiteRuntimeStoreDeliveryReplayAndReceiptSemantics(t *testing.T) {
	ctx := context.Background()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	now := time.Now().UTC()
	store.SetNowFnForTest(func() time.Time { return now })

	eventID := uuid.NewString()
	evt := events.Event{
		ID:          eventID,
		RunID:       runID,
		Type:        events.EventType("test.delivery_requested"),
		SourceAgent: "runtime",
		Payload:     json.RawMessage(`{"delivery":true}`),
		CreatedAt:   now,
	}
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
	if len(pending) != 1 || pending[0].ID != eventID {
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
	if len(missing) != 1 || missing[0].Event.ID != eventID {
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
		return events.Event{
			ID:          id,
			RunID:       runID,
			Type:        events.EventType("subscription.visible"),
			SourceAgent: "runtime",
			Payload:     json.RawMessage(`{"subscription":true}`),
			CreatedAt:   now.Add(offset),
		}
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
	if len(subscribed) != 1 || subscribed[0].ID != subSelfID {
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
			ModelTier:             "generic",
			LLMBackend:            "api",
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
	if err := store.PersistEventWithDeliveries(ctx, events.Event{
		ID:          eventID,
		RunID:       runID,
		Type:        events.EventType("trace.visible"),
		SourceAgent: "agent-1",
		Payload:     json.RawMessage(`{"trace":true}`),
		CreatedAt:   now,
	}, []string{"agent-1"}); err != nil {
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
	if err := store.AppendEvent(ctx, events.Event{
		ID:          logID,
		RunID:       runID,
		Type:        events.EventType("platform.runtime_log"),
		SourceAgent: "runtime",
		Payload:     json.RawMessage(`{"log_level":"warn","message":"runtime warning","details":{"component":"scheduler","session_id":"` + lease.SessionID + `"}}`),
		CreatedAt:   now.Add(time.Second),
	}); err != nil {
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
	if err := store.AppendEvent(ctx, events.Event{
		ID:          eventID,
		RunID:       runID,
		Type:        events.EventType("mailbox.requested"),
		SourceAgent: "agent-1",
		Payload:     json.RawMessage(`{"request":true}`),
		CreatedAt:   time.Now().UTC(),
	}); err != nil {
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
	if _, err := store.DB.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_source, started_at)
		VALUES (?, 'running', 'legacy', ?)
	`, runID, time.Now().UTC()); err != nil {
		t.Fatalf("seed sqlite run row: %v", err)
	}
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

func newBootstrappedSQLiteRuntimeStoreForTest(t *testing.T) *SQLiteRuntimeStore {
	t.Helper()
	spec := loadPlatformSpecForSQLiteSchemaTest(t)
	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs: %v", err)
	}
	dbPath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
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
