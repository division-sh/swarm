package runtimepersistence

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	storerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type replyContextStoreTestSurface interface {
	runtimereplycontext.Store
}

type replyContinuationStoreTestSurface interface {
	replyContextStoreTestSurface
	runtimetools.MailboxPersistence
	MaterializeMailboxWrite(context.Context, runtimepipeline.MailboxWriteMaterialization) error
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

			identity := testAgentIdentity(t, "provider-agent", "provider/account-a")
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
			admitted, err := store.AdmitGenericSchedule(events.WithDeliveryContext(ctx, deliveryContext), command)
			if err != nil {
				t.Fatalf("AdmitGenericSchedule: %v", err)
			}
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
			if _, err := store.AdmitGenericSchedule(ctx, recurring); err == nil {
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

		})
	}
}

func TestReplyContextStore_ForkedSourceRejectsCreateAndClaimWithoutDestroyingLineage(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T) (replyContextStoreTestSurface, func(context.Context, string, ...string) error)
	}{
		{name: "postgres", setup: setupPostgresReplyContextStoreTest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, seed := tc.setup(t)
			ctx := testAuthorActivityBundleSourceContext()
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
	} {
		for _, operation := range []string{"create", "claim"} {
			for _, winner := range []string{"operation", "freeze"} {
				t.Run(tc.name+"/"+operation+"_commits_first_"+winner, func(t *testing.T) {
					store, seed := tc.setup(t)
					ctx := testAuthorActivityBundleSourceContext()
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
	forkEventID := uuid.NewString()
	switch backend := store.(type) {
	case *PostgresStore:
		requireRunFixtureForTest(t, ctx, backend, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(),
			RunID: forkRunID, State: storerunlifecycle.StatePaused,
			BundleHash: authorActivityTestBundleHash, StartedAt: now,
		})
		lineage := runForkActivationLineage{
			SourceRunID: runID, ForkRunID: forkRunID, ForkEventID: forkEventID,
			ForkEventName: "reply.freeze", ForkEventTime: now, ForkStatus: "paused", SourceRunStatus: "running",
			SourceBundleHash: authorActivityTestBundleHash, ForkBundleHash: authorActivityTestBundleHash,
		}
		if err := commitRunForkSourceFreezeForTest(ctx, backend, lineage, now, true); err != nil {
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
