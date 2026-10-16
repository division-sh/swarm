package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestDeliveryOperationalWritesDoNotAdvanceForkHistoryBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			store := fixture.store.(deliveryReadProjectionStore)
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			event := eventtest.PersistedProjection(uuid.NewString(), "delivery.history", "gateway", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
			route := testEntitylessNodeDeliveryRoute("history-worker")
			if err := commitSemanticEventFixtureWithRoutes(ctx, store, event, []events.DeliveryRoute{route}); err != nil {
				t.Fatal(err)
			}
			before, err := ReadSemanticEventFixtureEvidenceForTest(ctx, fixture.store, runID, event.ID())
			if err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			var retainedFinalizations uint64
			assertNoCapture := func(phase string) {
				t.Helper()
				observed, err := ReadSemanticEventFixtureEvidenceForTest(ctx, fixture.store, runID, event.ID())
				if err != nil {
					t.Fatal(err)
				}
				if observed.RevisionCount != before.RevisionCount || probe.Snapshot().Total.Revision.Finalizations != retainedFinalizations {
					t.Errorf("%s captured operational fork evidence: revisions=%d want=%d finalizations=%d", phase, observed.RevisionCount, before.RevisionCount, probe.Snapshot().Total.Revision.Finalizations)
				}
				if observed.RunEventCount != before.RunEventCount || !observed.Record.Equal(before.Record) {
					t.Fatalf("%s changed the immutable event or physical counter", phase)
				}
				requireCompleteRunForkRevision(t, ctx, fixture, runID)
			}
			if err := runDeliveryFixtureAttempt(ctx, store, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
				adapter := sqliteDeliveryAdapter
				if backend.name == "postgres" {
					adapter = postgresDeliveryAdapter
				}
				return adapter.CommitPipelineHandoff(ctx, attempt, event.ID())
			}); err != nil {
				t.Fatal(err)
			}
			handedOff, err := ObserveDeliveryEventEvidenceForTest(ctx, fixture.store, event.ID())
			if err != nil || len(handedOff.Deliveries) != 1 || !handedOff.Deliveries[0].HandoffPresent || handedOff.Deliveries[0].HandoffAt.IsZero() {
				t.Fatalf("physical continuation handoff was not committed: evidence=%+v err=%v", handedOff, err)
			}
			assertNoCapture("handoff")
			claimed, err := claimDeliveryFixture(ctx, store, event, route)
			if err != nil || claimed.Claim.Validate() != nil || claimed.Snapshot.Status != runtimedelivery.StatusInProgress {
				t.Fatalf("real claim not acquired: claim=%+v err=%v", claimed, err)
			}
			started, err := ReadSemanticEventFixtureEvidenceForTest(ctx, fixture.store, runID, event.ID())
			if err != nil || started.RevisionCount != before.RevisionCount+1 || probe.Snapshot().Total.Revision.Finalizations != 1 {
				t.Fatalf("retained in-progress transition lacks exact history: evidence=%+v err=%v", started, err)
			}
			if started.RunEventCount != before.RunEventCount || !started.Record.Equal(before.Record) {
				t.Fatal("retained lifecycle transition changed event identity or counter")
			}
			before, retainedFinalizations = started, 1
			assertNoCapture("claim after its retained lifecycle transition")
			renewed, err := store.RenewClaim(ctx, claimed.Claim)
			if err != nil || !renewed.Acknowledged || renewed.Snapshot.ClaimVersion != claimed.Claim.Version() || !renewed.Snapshot.ClaimExpiresAt.After(claimed.Snapshot.ClaimExpiresAt) {
				t.Fatalf("physical renewal not acknowledged: renewed=%+v err=%v", renewed, err)
			}
			assertNoCapture("renewal")
			busy, err := store.ClaimDelivery(ctx, claimed.Snapshot.Authority, event, route)
			if err != nil || busy.Disposition != runtimedelivery.ClaimBusy {
				t.Fatalf("live claim exclusion changed: result=%+v err=%v", busy, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if result, err := store.RenewClaim(cancelled, claimed.Claim); !errors.Is(err, context.Canceled) || result.Acknowledged {
				t.Fatalf("cancelled renewal admitted: result=%+v err=%v", result, err)
			}
			selection, err := handlerselection.NoMatch(handlerselection.ContextRules)
			if err != nil {
				t.Fatal(err)
			}
			settled, err := store.SettleSuccess(ctx, claimed.Claim, nil, 0, selection)
			if err != nil || settled.Status != runtimedelivery.StatusDelivered || !settled.FinalSelection.Present() {
				t.Fatalf("semantic settlement lost: settled=%+v err=%v", settled, err)
			}
			outcomes, err := store.Outcomes(ctx, settled.DeliveryID)
			if err != nil || len(outcomes) != 1 {
				t.Fatalf("exact settled outcome lost: outcomes=%+v err=%v", outcomes, err)
			}
			after, err := ReadSemanticEventFixtureEvidenceForTest(ctx, fixture.store, runID, event.ID())
			if err != nil || after.RevisionCount <= before.RevisionCount {
				t.Fatalf("semantic settlement not captured: evidence=%+v err=%v", after, err)
			}
			if result, err := store.RenewClaim(ctx, claimed.Claim); !errors.Is(err, runtimedelivery.ErrConflict) || result.Acknowledged {
				t.Fatalf("settled claim renewal admitted: result=%+v err=%v", result, err)
			}
			preserved, err := ReadSemanticEventFixtureEvidenceForTest(ctx, fixture.store, runID, event.ID())
			if err != nil || !reflect.DeepEqual(preserved, after) {
				t.Fatalf("refused renewal changed settled evidence: got=%+v want=%+v err=%v", preserved, after, err)
			}
		})
		t.Run(backend.name+"/retained_lifecycle", func(t *testing.T) {
			fixture := backend.open(t)
			store := fixture.store.(completionSettlementTestStore)
			completion := newCompletionSettlementFixture(t, store, fixture.db, backend.name == "sqlite")
			proveRetainedDeliveryLifecycleAccounting(t, fixture, completion)
		})
	}
}

func proveRetainedDeliveryLifecycleAccounting(t *testing.T, fixture authorActivityReceiptFixture, completion completionSettlementFixture) {
	t.Helper()
	ctx, store := completion.context, completion.store
	event := managedCompletionTestEventWithIdentity(completion.authority, uuid.NewString(), "delivery.history")
	route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(completion.agentID), AgentIdentity: completion.authority.Normal.Identity}
	if err := commitSemanticEventFixtureWithRoutes(ctx, store, event, []events.DeliveryRoute{route}); err != nil {
		t.Fatal(err)
	}
	publishCompleteRunForkRevisionBaseline(t, ctx, fixture.db, !completion.sqlite, event.RunID())
	probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	phase := func(name string, want uint64, write func() error) {
		t.Helper()
		before := probe.Snapshot().Total.Revision
		if err := write(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		after := probe.Snapshot().Total.Revision
		if got := after.Finalizations - before.Finalizations; got != want {
			t.Fatalf("%s revision finalizations=%d, want=%d", name, got, want)
		}
		requireCompleteRunForkRevision(t, ctx, fixture, event.RunID())
		t.Logf("retained lifecycle accounting: phase=%s finalizations=%d history_sql=%d", name, want,
			after.ExecCalls+after.QueryCalls+after.QueryRowCalls-before.ExecCalls-before.QueryCalls-before.QueryRowCalls)
	}
	var current runtimedelivery.ClaimedObligation
	claim := func() error {
		var err error
		current, err = claimDeliveryFixture(ctx, store, event, route)
		return err
	}
	bind := func() error {
		bound, err := store.BindAgentSession(ctx, current.Claim, completion.sessionID)
		if err == nil && (!bound.Acknowledged || bound.Snapshot.ActiveSessionID != completion.sessionID) {
			t.Fatal("retained session binding was not acknowledged exactly")
		}
		return err
	}
	expire := func() error {
		var expiresAt time.Time
		err := runDeliveryFixtureAttempt(ctx, store, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
			adapter := postgresDeliveryAdapter
			if completion.sqlite {
				adapter = sqliteDeliveryAdapter
			}
			updated, err := adapter.RenewClaim(ctx, attempt, current.Claim, time.Millisecond)
			expiresAt = updated.ClaimExpiresAt
			return err
		})
		if err == nil {
			if wait := time.Until(expiresAt); wait > 0 {
				time.Sleep(wait)
			}
		}
		return err
	}
	phase("initial_status", 1, claim)
	phase("session_bind", 1, bind)
	phase("unchanged_session_bind", 0, bind)
	phase("lease_renewal", 0, func() error {
		renewed, err := store.RenewClaim(ctx, current.Claim)
		if err == nil && !renewed.Acknowledged {
			t.Fatal("physical renewal was not acknowledged")
		}
		return err
	})
	phase("short_lease_renewal", 0, expire)
	previous := current.Claim
	phase("reclaim_session_clear", 1, claim)
	if current.Snapshot.ActiveSessionID != "" || current.Claim.Version() != previous.Version()+1 {
		t.Fatal("reclaim did not clear the previous session under a fresh claim")
	}
	phase("stale_session_bind", 0, func() error {
		if _, err := store.BindAgentSession(ctx, previous, completion.sessionID); !errors.Is(err, runtimedelivery.ErrConflict) {
			t.Fatalf("stale session binding error=%v, want conflict", err)
		}
		return nil
	})
	phase("short_lease_renewal_without_session", 0, expire)
	phase("unchanged_reclaim", 0, claim)
	phase("retry_settlement", 1, func() error {
		failed, err := store.SettleFailure(ctx, current.Claim, runtimedelivery.Settlement{
			Disposition: runtimedelivery.FailureRetry, Failure: testRetryableFailure(), RetryBase: time.Nanosecond,
			RuleSelection: runtimedelivery.NotApplicableHandlerRuleObservation(),
		})
		if err == nil && failed.Status != runtimedelivery.StatusFailed {
			t.Fatal("retry did not retain its failed lifecycle state")
		}
		return err
	})
	phase("retry_status", 1, claim)
}
