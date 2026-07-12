package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type replyContextStoreTestSurface interface {
	runtimereplycontext.Store
	InsertEventDeliveryRoutes(context.Context, string, []events.DeliveryRoute) error
	ListEventDeliveryRoutes(context.Context, string) ([]events.DeliveryRoute, error)
}

type replyContinuationStoreTestSurface interface {
	replyContextStoreTestSurface
	runtimetools.MailboxPersistence
	runtimetools.HumanTaskPersistence
	MaterializeMailboxWrite(context.Context, runtimepipeline.MailboxWriteMaterialization) error
	UpsertSchedule(context.Context, runtimepipeline.Schedule) error
	LoadActiveSchedules(context.Context) ([]runtimepipeline.Schedule, error)
}

func TestReplyContinuationRows_BackendParityNoticesAndHumanTaskRestoreContext(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T) (replyContextStoreTestSurface, func(context.Context, string, ...string) error)
	}{
		{name: "postgres", setup: setupPostgresReplyContextStoreTest},
		{name: "sqlite", setup: setupSQLiteReplyContextStoreTest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, seed := tc.setup(t)
			store, ok := base.(replyContinuationStoreTestSurface)
			if !ok {
				t.Fatalf("%s store lacks reply continuation surface", tc.name)
			}
			ctx := context.Background()
			runID := uuid.NewString()
			requestEventID := uuid.NewString()
			if err := seed(ctx, runID, requestEventID); err != nil {
				t.Fatalf("seed continuation source: %v", err)
			}
			now := time.Now().UTC()
			record := runtimereplycontext.Record{
				RunID:                runID,
				RequestEventID:       requestEventID,
				RequesterFlowID:      "requester",
				RequestOutputPin:     "provider_requested",
				ReplyInputPin:        "provider_replied",
				ProviderFlowID:       "provider",
				ProviderInputPin:     "provider_requested",
				ProviderOutputPin:    "provider_replied",
				Origin:               events.RouteIdentity{FlowID: "requester", FlowInstance: "requester/account-a", EntityID: uuid.NewString()},
				RequestCorrelationID: requestEventID,
				State:                runtimereplycontext.StateOpen,
				CreatedAt:            now,
				UpdatedAt:            now,
			}
			record.ID = runtimereplycontext.DeterministicID(record.RequestEventID, record.RequesterFlowID, record.RequestOutputPin, record.ReplyInputPin, record.ProviderFlowID, record.Origin)
			if err := store.CreateReplyContext(ctx, record); err != nil {
				t.Fatalf("CreateReplyContext: %v", err)
			}
			deliveryContext := events.DeliveryContext{Reply: &events.ReplyContextRef{ID: record.ID}}

			systemMailboxID := uuid.NewString()
			if err := store.MaterializeMailboxWrite(ctx, runtimepipeline.MailboxWriteMaterialization{
				ItemID:         systemMailboxID,
				Scope:          "global",
				ItemType:       "approval",
				SourceEventID:  requestEventID,
				FromAgent:      "system_node:provider-node",
				Severity:       "normal",
				Summary:        "approve provider result",
				Payload:        []byte(`{"kind":"system"}`),
				ReplyContextID: record.ID,
			}); err != nil {
				t.Fatalf("MaterializeMailboxWrite: %v", err)
			}
			item, err := store.GetMailboxItem(ctx, systemMailboxID)
			if err != nil || item.ReplyContextID != record.ID {
				t.Fatalf("system mailbox readback = %#v err=%v", item, err)
			}

			agentMailboxID, err := store.InsertMailboxItem(events.WithDeliveryContext(ctx, deliveryContext), runtimetools.MailboxItem{
				EventID:   requestEventID,
				FromAgent: "provider-agent",
				Type:      "approval",
				Priority:  "normal",
				Status:    "pending",
				Summary:   "agent approval",
				Context:   []byte(`{"kind":"agent"}`),
			})
			if err != nil {
				t.Fatalf("InsertMailboxItem: %v", err)
			}
			item, err = store.GetMailboxItem(ctx, agentMailboxID)
			if err != nil || item.ReplyContextID != record.ID {
				t.Fatalf("agent mailbox readback = %#v err=%v", item, err)
			}

			schedule := runtimepipeline.Schedule{
				Context:   deliveryContext,
				RunID:     runID,
				AgentID:   "provider-agent",
				EventType: "provider.resume",
				Mode:      "once",
				At:        now.Add(10 * time.Minute),
				TaskID:    "reply-resume",
				Payload:   []byte(`{"resume":true}`),
			}
			if err := store.UpsertSchedule(ctx, schedule); err != nil {
				t.Fatalf("UpsertSchedule: %v", err)
			}
			schedules, err := store.LoadActiveSchedules(ctx)
			if err != nil {
				t.Fatalf("LoadActiveSchedules: %v", err)
			}
			foundSchedule := false
			for _, loaded := range schedules {
				if loaded.TaskID == schedule.TaskID {
					foundSchedule = loaded.Context.ReplyContextID() == record.ID
				}
			}
			if !foundSchedule {
				t.Fatalf("one-shot schedule did not restore reply context: %#v", schedules)
			}
			recurring := schedule
			recurring.TaskID = "reply-recurring"
			recurring.Mode = "cron"
			recurring.Cron = "@every 1h"
			if err := store.UpsertSchedule(ctx, recurring); err == nil {
				t.Fatal("recurring schedule with open reply context unexpectedly accepted")
			}

			humanTaskID, err := store.CreateHumanTask(ctx, runtimetools.HumanTaskCreateRecord{
				ActorID:       "provider-agent",
				FlowInstance:  "provider",
				Category:      "approval",
				Description:   "approve provider response",
				ExpectedValue: "approved",
				Priority:      "normal",
				Deadline:      now.Add(2 * time.Hour),
				SourceEventID: requestEventID,
				Context:       deliveryContext,
			})
			if err != nil {
				t.Fatalf("CreateHumanTask: %v", err)
			}
			item, err = store.GetMailboxItem(ctx, humanTaskID)
			if err != nil || item.ReplyContextID != record.ID || item.FlowInstance != "provider" {
				t.Fatalf("human task readback = %#v err=%v", item, err)
			}
			var humanEvents []events.Event
			humanPublisher := func(publishCtx context.Context, evt events.Event) error {
				if _, ok := runtimepipeline.PipelineSQLTxFromContext(publishCtx); !ok {
					t.Fatal("human outcome publish escaped row transaction")
				}
				if got := events.DeliveryContextFromContext(publishCtx).ReplyContextID(); got != record.ID {
					t.Fatalf("human outcome context = %q, want %q", got, record.ID)
				}
				humanEvents = append(humanEvents, evt)
				return nil
			}
			if err := store.DecideHumanTask(ctx, runtimetools.HumanTaskDecisionRecord{
				TaskID:               humanTaskID,
				Status:               "deferred",
				ActorID:              "operator",
				Reason:               "later",
				RequeueDate:          now.Add(30 * time.Minute).Format(time.RFC3339),
				DecidedAt:            now,
				DecisionEventPublish: humanPublisher,
			}); err != nil {
				t.Fatalf("defer human task: %v", err)
			}
			item, err = store.GetMailboxItem(ctx, humanTaskID)
			if err != nil || item.Status != "pending" || item.ReplyContextID != record.ID || len(humanEvents) != 1 || humanEvents[0].Type() != "human_task.deferred" {
				t.Fatalf("deferred human task row=%#v events=%#v err=%v", item, humanEvents, err)
			}
			if err := store.DecideHumanTask(ctx, runtimetools.HumanTaskDecisionRecord{
				TaskID:               humanTaskID,
				Status:               "approved",
				ActorID:              "operator",
				Reason:               "approved",
				DecidedAt:            now.Add(time.Minute),
				DecisionEventPublish: humanPublisher,
			}); err != nil {
				t.Fatalf("approve human task: %v", err)
			}
			item, err = store.GetMailboxItem(ctx, humanTaskID)
			if err != nil || item.Status != "decided" || item.ReplyContextID != record.ID || len(humanEvents) != 2 || humanEvents[1].Type() != "human_task.approved" {
				t.Fatalf("approved human task row=%#v events=%#v err=%v", item, humanEvents, err)
			}
			loaded, err := store.LoadReplyContext(ctx, record.ID)
			if err != nil || loaded.State != runtimereplycontext.StateOpen {
				t.Fatalf("mailbox/human decisions consumed terminal claim: %#v err=%v", loaded, err)
			}
		})
	}
}

func TestReplyContextStore_BackendParityAtomicClaimAndDeliveryReadback(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T) (replyContextStoreTestSurface, func(context.Context, string, ...string) error)
	}{
		{name: "postgres", setup: setupPostgresReplyContextStoreTest},
		{name: "sqlite", setup: setupSQLiteReplyContextStoreTest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, seed := tc.setup(t)
			ctx := context.Background()
			runID := uuid.NewString()
			requestEventID := uuid.NewString()
			collisionEventID := uuid.NewString()
			replyIDs := []string{uuid.NewString(), uuid.NewString()}
			if err := seed(ctx, runID, append([]string{requestEventID, collisionEventID}, replyIDs...)...); err != nil {
				t.Fatalf("seed reply context source: %v", err)
			}
			now := time.Now().UTC()
			record := runtimereplycontext.Record{
				RunID:                runID,
				RequestEventID:       requestEventID,
				RequesterFlowID:      "requester",
				RequestOutputPin:     "provider_requested",
				ReplyInputPin:        "provider_replied",
				ProviderFlowID:       "provider",
				ProviderInputPin:     "provider_requested",
				ProviderOutputPin:    "provider_replied",
				Origin:               events.RouteIdentity{FlowID: "requester", FlowInstance: "requester/account-a", EntityID: uuid.NewString()},
				RequestCorrelationID: "same-authored-key",
				CorrelationKey:       "provider_request_id",
				State:                runtimereplycontext.StateOpen,
				CreatedAt:            now,
				UpdatedAt:            now,
			}
			record.ID = runtimereplycontext.DeterministicID(record.RequestEventID, record.RequesterFlowID, record.RequestOutputPin, record.ReplyInputPin, record.ProviderFlowID, record.Origin)
			if err := store.CreateReplyContext(ctx, record); err != nil {
				t.Fatalf("CreateReplyContext: %v", err)
			}
			if err := store.CreateReplyContext(ctx, record); err != nil {
				t.Fatalf("idempotent CreateReplyContext: %v", err)
			}
			collision := record
			collision.RequestEventID = collisionEventID
			collision.ID = runtimereplycontext.DeterministicID(collision.RequestEventID, collision.RequesterFlowID, collision.RequestOutputPin, collision.ReplyInputPin, collision.ProviderFlowID, collision.Origin)
			if err := store.CreateReplyContext(ctx, collision); err == nil {
				t.Fatal("same-origin in-flight correlation collision unexpectedly accepted")
			}
			loaded, err := store.LoadReplyContext(ctx, record.ID)
			if err != nil {
				t.Fatalf("LoadReplyContext: %v", err)
			}
			if loaded.ID != record.ID || loaded.Origin != record.Origin.Normalized() || loaded.RequestCorrelationID != record.RequestCorrelationID {
				t.Fatalf("loaded reply context = %#v, want %#v", loaded, record)
			}

			type result struct {
				eventID string
				outcome runtimereplycontext.ClaimOutcome
				err     error
			}
			results := make(chan result, len(replyIDs))
			var wg sync.WaitGroup
			for _, replyID := range replyIDs {
				wg.Add(1)
				go func(eventID string) {
					defer wg.Done()
					_, outcome, err := store.ClaimReplyContext(ctx, record.ID, eventID)
					results <- result{eventID: eventID, outcome: outcome, err: err}
				}(replyID)
			}
			wg.Wait()
			close(results)
			acceptedID := ""
			outcomes := map[runtimereplycontext.ClaimOutcome]int{}
			for got := range results {
				if got.err != nil {
					t.Fatalf("ClaimReplyContext(%s): %v", got.eventID, got.err)
				}
				outcomes[got.outcome]++
				if got.outcome == runtimereplycontext.ClaimAccepted {
					acceptedID = got.eventID
				}
			}
			if outcomes[runtimereplycontext.ClaimAccepted] != 1 || outcomes[runtimereplycontext.ClaimTerminal] != 1 {
				t.Fatalf("claim outcomes = %#v, want one accepted and one terminal", outcomes)
			}
			claimed, outcome, err := store.ClaimReplyContext(ctx, record.ID, acceptedID)
			if err != nil || outcome != runtimereplycontext.ClaimIdempotent || claimed.AcceptedReplyEventID != acceptedID {
				t.Fatalf("accepted replay = record:%#v outcome:%q err:%v", claimed, outcome, err)
			}

			deliveryContext := events.DeliveryContext{Reply: &events.ReplyContextRef{ID: record.ID}}
			routes := []events.DeliveryRoute{{
				SubscriberType: "node",
				SubscriberID:   "provider-node",
				Target:         events.RouteIdentity{FlowID: "provider", FlowInstance: "provider", EntityID: uuid.NewString()},
				Context:        deliveryContext,
			}}
			if err := store.InsertEventDeliveryRoutes(ctx, requestEventID, routes); err != nil {
				t.Fatalf("InsertEventDeliveryRoutes: %v", err)
			}
			gotRoutes, err := store.ListEventDeliveryRoutes(ctx, requestEventID)
			if err != nil {
				t.Fatalf("ListEventDeliveryRoutes: %v", err)
			}
			if len(gotRoutes) != 1 || gotRoutes[0].Context.ReplyContextID() != record.ID {
				t.Fatalf("delivery context readback = %#v", gotRoutes)
			}
		})
	}
}

func setupPostgresReplyContextStoreTest(t *testing.T) (replyContextStoreTestSurface, func(context.Context, string, ...string) error) {
	t.Helper()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	store := &PostgresStore{DB: db}
	return store, func(ctx context.Context, runID string, eventIDs ...string) error {
		if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', now())`, runID); err != nil {
			return err
		}
		for i, eventID := range eventIDs {
			eventName := "provider.replied"
			if i == 0 {
				eventName = "provider.requested"
			}
			if _, err := db.ExecContext(ctx, `
				INSERT INTO events (run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at)
				VALUES ($1::uuid, $2::uuid, $3, 'global', '{}'::jsonb, 'test', 'platform', now())
			`, runID, eventID, eventName); err != nil {
				return err
			}
		}
		return nil
	}
}

func setupSQLiteReplyContextStoreTest(t *testing.T) (replyContextStoreTestSurface, func(context.Context, string, ...string) error) {
	t.Helper()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	return store, func(ctx context.Context, runID string, eventIDs ...string) error {
		if _, err := store.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES (?, 'running', ?)`, runID, time.Now().UTC()); err != nil {
			return err
		}
		for i, eventID := range eventIDs {
			eventName := "provider.replied"
			if i == 0 {
				eventName = "provider.requested"
			}
			if _, err := store.DB.ExecContext(ctx, `
				INSERT INTO events (run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at)
				VALUES (?, ?, ?, 'global', '{}', 'test', 'platform', ?)
			`, runID, eventID, eventName, time.Now().UTC()); err != nil {
				return fmt.Errorf("insert sqlite event: %w", err)
			}
		}
		return nil
	}
}
