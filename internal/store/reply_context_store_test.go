package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
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
	MaterializeMailboxWrite(context.Context, runtimepipeline.MailboxWriteMaterialization) error
	UpsertSchedule(context.Context, runtimepipeline.Schedule) error
	LoadActiveSchedules(context.Context) ([]runtimepipeline.Schedule, error)
}

func TestReplyContinuationRows_BackendParityNoticesAndSchedulesRestoreContext(t *testing.T) {
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
			ctx := testAuthorActivityContext()
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

			loaded, err := store.LoadReplyContext(ctx, record.ID)
			if err != nil || loaded.State != runtimereplycontext.StateOpen {
				t.Fatalf("notice/schedule continuations consumed terminal claim: %#v err=%v", loaded, err)
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
			ctx := testAuthorActivityContext()
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

func TestReplyContextStore_ForkedSourceRejectsCreateAndClaimWithoutDestroyingLineage(t *testing.T) {
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
			replyEventID := uuid.NewString()
			secondRequestEventID := uuid.NewString()
			if err := seed(ctx, runID, requestEventID, replyEventID, secondRequestEventID); err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			record := replyContextFreezeTestRecord(requestEventID, runID, "first", now)
			if err := store.CreateReplyContext(ctx, record); err != nil {
				t.Fatal(err)
			}
			freezeReplyContextTestRun(t, ctx, store, runID, now.Add(time.Second))

			second := replyContextFreezeTestRecord(secondRequestEventID, runID, "second", now.Add(2*time.Second))
			if err := store.CreateReplyContext(ctx, second); !errors.Is(err, storerunlifecycle.ErrRunNotActive) {
				t.Fatalf("post-freeze create error = %v", err)
			}
			if _, _, err := store.ClaimReplyContext(ctx, record.ID, replyEventID); !errors.Is(err, storerunlifecycle.ErrRunNotActive) {
				t.Fatalf("post-freeze claim error = %v", err)
			}
			preserved, err := store.LoadReplyContext(ctx, record.ID)
			if err != nil || preserved.State != runtimereplycontext.StateOpen || preserved.AcceptedReplyEventID != "" {
				t.Fatalf("preserved reply context = %#v, %v", preserved, err)
			}
			if _, err := store.LoadReplyContext(ctx, second.ID); !errors.Is(err, runtimereplycontext.ErrNotFound) {
				t.Fatalf("rejected create left row: %v", err)
			}
		})
	}
}

func TestReplyContextStore_ForkFreezeSerializesBothCreateAndClaimCommitOrders(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T) (replyContextStoreTestSurface, func(context.Context, string, ...string) error)
	}{
		{name: "postgres", setup: setupPostgresReplyContextStoreTest},
		{name: "sqlite", setup: setupSQLiteReplyContextStoreTest},
	} {
		for _, operation := range []string{"create", "claim"} {
			for _, winner := range []string{"operation", "freeze"} {
				t.Run(tc.name+"/"+operation+"_commits_first_"+winner, func(t *testing.T) {
					store, seed := tc.setup(t)
					ctx := context.Background()
					runID := uuid.NewString()
					requestEventID := uuid.NewString()
					replyEventID := uuid.NewString()
					if err := seed(ctx, runID, requestEventID, replyEventID); err != nil {
						t.Fatal(err)
					}
					now := time.Now().UTC()
					record := replyContextFreezeTestRecord(requestEventID, runID, operation, now)
					if operation == "claim" {
						if err := store.CreateReplyContext(ctx, record); err != nil {
							t.Fatal(err)
						}
					}

					if winner == "freeze" {
						freezeReplyContextTestRun(t, ctx, store, runID, now.Add(time.Second))
					}
					var operationErr error
					if operation == "create" {
						operationErr = store.CreateReplyContext(ctx, record)
					} else {
						_, _, operationErr = store.ClaimReplyContext(ctx, record.ID, replyEventID)
					}
					if winner == "freeze" {
						if !errors.Is(operationErr, storerunlifecycle.ErrRunNotActive) {
							t.Fatalf("freeze-first %s error = %v", operation, operationErr)
						}
					} else {
						if operationErr != nil {
							t.Fatalf("operation-first %s: %v", operation, operationErr)
						}
						freezeReplyContextTestRun(t, ctx, store, runID, now.Add(time.Second))
					}

					loaded, err := store.LoadReplyContext(ctx, record.ID)
					if operation == "create" {
						if winner == "operation" && (err != nil || loaded.State != runtimereplycontext.StateOpen) {
							t.Fatalf("operation-first create = %#v, %v", loaded, err)
						}
						if winner == "freeze" && !errors.Is(err, runtimereplycontext.ErrNotFound) {
							t.Fatalf("freeze-first create row error = %v", err)
						}
					} else if err != nil || (winner == "operation" && loaded.AcceptedReplyEventID != replyEventID) || (winner == "freeze" && loaded.AcceptedReplyEventID != "") {
						t.Fatalf("%s-first claim = %#v, %v", winner, loaded, err)
					}
				})
			}
		}
	}
}

func replyContextFreezeTestRecord(requestEventID, runID, suffix string, now time.Time) runtimereplycontext.Record {
	record := runtimereplycontext.Record{
		RunID: runID, RequestEventID: requestEventID,
		RequesterFlowID: "requester-" + suffix, RequestOutputPin: "provider_requested", ReplyInputPin: "provider_replied",
		ProviderFlowID: "provider", ProviderInputPin: "provider_requested", ProviderOutputPin: "provider_replied",
		Origin:               events.RouteIdentity{FlowID: "requester", FlowInstance: "requester/" + suffix, EntityID: uuid.NewString()},
		RequestCorrelationID: requestEventID, State: runtimereplycontext.StateOpen, CreatedAt: now, UpdatedAt: now,
	}
	record.ID = runtimereplycontext.DeterministicID(record.RequestEventID, record.RequesterFlowID, record.RequestOutputPin, record.ReplyInputPin, record.ProviderFlowID, record.Origin)
	return record
}

func freezeReplyContextTestRun(t *testing.T, ctx context.Context, store replyContextStoreTestSurface, runID string, now time.Time) {
	t.Helper()
	forkRunID := uuid.NewString()
	switch backend := store.(type) {
	case *PostgresStore:
		if _, err := backend.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status, forked_from_run_id, forked_from_event_id, started_at) VALUES ($1::uuid, 'paused', $2::uuid, $3::uuid, $4)`, forkRunID, runID, uuid.NewString(), now); err != nil {
			t.Fatal(err)
		}
		lineage := runForkActivationLineage{SourceRunID: runID, ForkRunID: forkRunID, ForkEventID: uuid.NewString(), ForkEventName: "reply.freeze", ForkEventTime: now, ForkStatus: "paused", SourceRunStatus: "running"}
		if err := commitRunForkSourceFreezeForTest(ctx, backend.DB, lineage, now, true); err != nil {
			t.Fatal(err)
		}
	case *SQLiteRuntimeStore:
		tx, err := backend.DB.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		if _, err := tx.ExecContext(ctx, `INSERT INTO runs (run_id, status, forked_from_run_id, forked_from_event_id, started_at) VALUES (?, 'paused', ?, ?, ?)`, forkRunID, runID, uuid.NewString(), now); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE runs SET status = 'forked', ended_at = ?, continued_as_run_id = ? WHERE run_id = ? AND status IN ('running', 'paused')`, now, forkRunID, runID); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE runs SET status = 'running' WHERE run_id = ? AND status = 'paused'`, forkRunID); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unsupported reply-context test store %T", store)
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
				INSERT INTO events (execution_mode, run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at)
				VALUES ('live', $1::uuid, $2::uuid, $3, 'global', '{}'::jsonb, 'test', 'platform', now())
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
				INSERT INTO events (execution_mode, run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at)
				VALUES ('live', ?, ?, ?, 'global', '{}', 'test', 'platform', ?)
			`, runID, eventID, eventName, time.Now().UTC()); err != nil {
				return fmt.Errorf("insert sqlite event: %w", err)
			}
		}
		return nil
	}
}
