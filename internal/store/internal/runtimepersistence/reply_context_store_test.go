package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"

	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"

	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type replyContextStoreTestSurface interface {
	runtimereplycontext.Store
}

func replyContextFreezeTestRecord(requestEventID, runID, suffix string, now time.Time) runtimereplycontext.Record {
	record := runtimereplycontext.Record{
		RunID: runID, RequestEventID: requestEventID,
		SourceRunID: runID, SourceRequestEventID: requestEventID,
		RequesterFlowID: "requester-" + suffix, RequestOutputPin: "provider_requested", ReplyInputPin: "provider_replied",
		ProviderFlowID: "provider", ProviderInputPin: "provider_requested", ProviderOutputPin: "provider_replied",
		Origin:               events.RouteIdentity{FlowID: "requester", FlowInstance: "requester/" + suffix, EntityID: uuid.NewString()},
		RequestCorrelationID: requestEventID, State: runtimereplycontext.StateOpen, CreatedAt: now, UpdatedAt: now,
	}
	record.ID = runtimereplycontext.DeterministicID(record.RequestEventID, record.RequesterFlowID, record.RequestOutputPin, record.ReplyInputPin, record.ProviderFlowID, record.Origin)
	return record
}

type replyContinuationStoreTestSurface interface {
	replyContextStoreTestSurface
	runtimetools.MailboxPersistence
	runtimegenericschedule.Store
}

func TestReplyContinuationRows_BackendParityNoticesAndSchedulesRestoreContext(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T) (replyContextStoreTestSurface, func(context.Context, string, ...string) error)
	}{
		{name: "postgres", setup: setupPostgresReplyContextStoreTest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, seed := tc.setup(t)
			store, ok := base.(replyContinuationStoreTestSurface)
			if !ok {
				t.Fatalf("%s store lacks reply continuation surface", tc.name)
			}
			ctx := testAuthorActivityContext()
			ctx = originalStoreFixtureContext(t, ctx, store)
			runID := uuid.NewString()
			requestEventID := uuid.NewString()
			if err := seed(ctx, runID, requestEventID); err != nil {
				t.Fatalf("seed continuation source: %v", err)
			}
			now := time.Now().UTC()
			record := runtimereplycontext.Record{
				RunID:                runID,
				RequestEventID:       requestEventID,
				SourceRunID:          runID,
				SourceRequestEventID: requestEventID,
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
			item, err := store.GetMailboxItem(ctx, agentMailboxID)
			if err != nil || item.ReplyContextID != record.ID {
				t.Fatalf("agent mailbox readback = %#v err=%v", item, err)
			}

			identity := mustTestAgentIdentityForRun(runID, "provider-agent", "provider/account-a")
			routing, err := events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{
				FlowID: "provider", FlowInstance: identity.FlowInstance(), EntityID: record.Origin.EntityID,
			})
			if err != nil {
				t.Fatalf("build reply schedule routing source: %v", err)
			}
			command := runtimegenericschedule.AdmissionCommand{
				ScheduleKey: "reply-resume", RunID: runID, EntityID: record.Origin.EntityID,
				FlowInstance: identity.FlowInstance(), OwnerKind: runtimegenericschedule.OwnerAgent,
				OwnerID: identity.AgentID(), AgentIdentity: identity, EventType: "provider.resume",
				Payload:       semanticvalue.MustObject(map[string]semanticvalue.Value{"resume": semanticvalue.Bool(true)}),
				RoutingSource: routing, ExecutionMode: executionmode.Live, ReplyContext: record.ID,
				Due: runtimegenericschedule.AbsoluteDue(now.Add(10 * time.Minute)), TaskID: "reply-resume",
			}
			commit, err := store.AdmitGenericScheduleOutcome(events.WithDeliveryContext(ctx, deliveryContext), command)
			if err != nil || !commit.Acknowledged {
				t.Fatalf("AdmitGenericSchedule: commit=%+v err=%v", commit, err)
			}
			admitted := commit.Result
			loadedSchedule, found, err := store.LoadGenericScheduleActivation(ctx, admitted.Activation.ID)
			if err != nil {
				t.Fatalf("LoadGenericScheduleActivation: %v", err)
			}
			if !found || loadedSchedule.Command.ReplyContext != record.ID {
				t.Fatalf("one-shot schedule did not restore reply context: found=%v activation=%#v", found, loadedSchedule)
			}
			recurring := command
			recurring.ScheduleKey = "reply-recurring"
			recurring.TaskID = "reply-recurring"
			recurring.Due = runtimegenericschedule.EveryDue(time.Hour)
			if _, err := store.AdmitGenericScheduleOutcome(ctx, recurring); err == nil {
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
		{name: "sqlite", setup: setupSQLiteReplyContextStoreTest},
		{name: "postgres", setup: setupPostgresReplyContextStoreTest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, seed := tc.setup(t)
			ctx := testAuthorActivityContext()
			ctx = originalStoreFixtureContext(t, ctx, store)
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
				SourceRunID:          runID,
				SourceRequestEventID: requestEventID,
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
			requireReplyContextHistory(t, ctx, store, record, runtimereplycontext.StateOpen, 1)
			if err := store.CreateReplyContext(ctx, record); err != nil {
				t.Fatalf("idempotent CreateReplyContext: %v", err)
			}
			collision := record
			collision.RequestEventID = collisionEventID
			collision.SourceRequestEventID = collisionEventID
			collision.ID = runtimereplycontext.DeterministicID(collision.RequestEventID, collision.RequesterFlowID, collision.RequestOutputPin, collision.ReplyInputPin, collision.ProviderFlowID, collision.Origin)
			if err := store.CreateReplyContext(ctx, collision); err == nil {
				t.Fatal("same-origin in-flight correlation collision unexpectedly accepted")
			}
			requireReplyContextHistory(t, ctx, store, record, runtimereplycontext.StateOpen, 1)
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
			requireReplyContextHistory(t, ctx, store, record, runtimereplycontext.StateTerminal, 2)

		})
	}
}

func requireReplyContextHistory(t *testing.T, ctx context.Context, store replyContextStoreTestSurface, record runtimereplycontext.Record, state runtimereplycontext.State, count int) {
	t.Helper()
	var db *sql.DB
	switch s := store.(type) {
	case *SQLiteRuntimeStore:
		db = s.backend.ConstructionHandle()
	case *PostgresStore:
		db = s.backend.ConstructionHandle()
	default:
		t.Fatalf("unsupported history proof store %T", store)
	}
	rows, err := db.QueryContext(ctx, `SELECT fact,present FROM run_fork_fact_revisions WHERE run_id=$1 AND family='reply_contexts' ORDER BY revision`, record.RunID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got int
	var latest struct {
		ID    string                    `json:"reply_context_id"`
		State runtimereplycontext.State `json:"state"`
	}
	for rows.Next() {
		var body []byte
		var present bool
		if err := rows.Scan(&body, &present); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(body, &latest); err != nil {
			t.Fatal(err)
		}
		if !present || latest.ID != record.ID {
			t.Fatalf("unexpected reply history: present=%t fact=%s", present, body)
		}
		got++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if got != count || latest.State != state {
		t.Fatalf("reply history count=%d state=%q, want count=%d state=%q", got, latest.State, count, state)
	}
}

func setupSQLiteReplyContextStoreTest(t *testing.T) (replyContextStoreTestSurface, func(context.Context, string, ...string) error) {
	t.Helper()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	return store, func(ctx context.Context, runID string, eventIDs ...string) error {
		requireRunFixtureForTest(t, ctx, store, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, BundleHash: authorActivityTestBundleHash})
		for i, eventID := range eventIDs {
			eventName := "provider.replied"
			if i == 0 {
				eventName = "provider.requested"
			}
			event := eventtest.PersistedProjectionForProducer(eventID, events.EventType(eventName), eventtest.Producer(events.EventProducerPlatform, "test"), "", json.RawMessage(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
			if err := commitSemanticEventFixture(ctx, store, event); err != nil {
				return err
			}
		}
		return nil
	}
}

func setupPostgresReplyContextStoreTest(t *testing.T) (replyContextStoreTestSurface, func(context.Context, string, ...string) error) {
	t.Helper()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	store := admitTestPostgresStore(t, db)
	return store, func(ctx context.Context, runID string, eventIDs ...string) error {
		requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(db)), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, BundleHash: authorActivityTestBundleHash})
		for i, eventID := range eventIDs {
			eventName := "provider.replied"
			if i == 0 {
				eventName = "provider.requested"
			}
			event := eventtest.PersistedProjectionForProducer(
				eventID, events.EventType(eventName), eventtest.Producer(events.EventProducerPlatform, "test"), "",
				json.RawMessage(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC(),
			)
			if err := commitSemanticEventFixture(ctx, store, event); err != nil {
				return err
			}
		}
		return nil
	}
}
