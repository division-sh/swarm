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
	}
}
