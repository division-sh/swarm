package runtimepersistence

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestDeliveryAuthorityActivationPreservesLiveEligibilityBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		for _, state := range []runlifecycle.State{runlifecycle.StateRunning, runlifecycle.StatePaused} {
			t.Run(backend.name+"/"+string(state), func(t *testing.T) {
				fixture := backend.open(t)
				ctx := testAuthorActivityContext()
				ctx = originalStoreFixtureContext(t, ctx, fixture.store)
				runID := uuid.NewString()
				seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
				for _, deliveryState := range []deliverylifecycle.State{deliverylifecycle.StateQueued, deliverylifecycle.StateLaunching} {
					if _, err := fixture.store.(runlifecycle.OperationOwner).TransitionActiveRun(ctx, runlifecycle.ActiveTransitionRequest{RunID: runID, State: runlifecycle.StateRunning}); err != nil {
						t.Fatal(err)
					}
					event := eventtest.ExistingRunRootIngress(uuid.NewString(), "freeze.delivery", "test", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
					if err := insertCanonicalEventRecordFixture(ctx, fixture.store, event); err != nil {
						t.Fatal(err)
					}
					prior := seedDeliveryStateFixture(t, ctx, fixture.store, event, events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("live-" + string(deliveryState))}, deliveryState, nil)
					if _, err := fixture.store.(runlifecycle.OperationOwner).TransitionActiveRun(ctx, runlifecycle.ActiveTransitionRequest{RunID: runID, State: state}); err != nil {
						t.Fatal(err)
					}
					authority, err := deliverylifecycle.NewNormalExecutionAuthority(prior.Authority.SourceArtifact(), uuid.NewString(), prior.Authority.Generation()+1)
					if err != nil {
						t.Fatal(err)
					}
					probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(restore)
					if err := fixture.store.(interface {
						ActivateDeliveryAuthority(context.Context, deliverylifecycle.ExecutionAuthority) error
					}).ActivateDeliveryAuthority(ctx, authority); err != nil {
						t.Fatal(err)
					}
					counts := probe.Snapshot()
					restore()
					if counts.Active != 0 || counts.Total.WriteCommits != 1 || counts.Total.Revision.LockPhases != 1 || counts.Total.Revision.QueryCalls == 0 {
						t.Fatalf("eligible delivery activation lost its revision accounting: %+v", counts)
					}
					got, err := fixture.store.Snapshot(ctx, prior.DeliveryID)
					if err != nil || !got.Authority.Equal(authority) || got.ClaimVersion != prior.ClaimVersion || got.ActiveSessionID != prior.ActiveSessionID || got.Status != prior.Status || !reflect.DeepEqual(got.Route, prior.Route) {
						t.Fatalf("live rebind lost eligible delivery evidence: before=%+v after=%+v err=%v", prior, got, err)
					}
					if deliveryState == deliverylifecycle.StateLaunching && !got.ClaimExpiresAt.Before(prior.ClaimExpiresAt) {
						t.Fatal("live predecessor attempt was not made reclaimable")
					}
				}
			})
		}
	}
}
