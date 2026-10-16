package runtimepersistence

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestDeliveryAuthorityActivationPreservesRecordedFrozenSourceBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		for _, replacement := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/replacement=%t", backend.name, replacement), func(t *testing.T) {
				fixture := backend.open(t)
				ctx := testAuthorActivityContext()
				source, child := uuid.NewString(), uuid.NewString()
				seedAuthorActivityReceiptRun(t, fixture, ctx, source)
				seedAuthorActivityReceiptRun(t, fixture, ctx, child)
				selected := fixture.store.(agentFixtureFlowStore)
				for _, state := range []deliverylifecycle.State{deliverylifecycle.StateQueued, deliverylifecycle.StateRetrying, deliverylifecycle.StateLaunching, deliverylifecycle.StateDelivered, deliverylifecycle.StateExhausted} {
					id := "frozen-" + string(state)
					seedManagedTurnFixtureAgent(t, ctx, selected, source, id, "fixture/"+id)
				}
				ctx = admitPreparedAgentClaimContext(t, ctx, selected)
				var authority deliverylifecycle.ExecutionAuthority
				for _, state := range []deliverylifecycle.State{
					deliverylifecycle.StateQueued, deliverylifecycle.StateRetrying,
					deliverylifecycle.StateLaunching, deliverylifecycle.StateDelivered, deliverylifecycle.StateExhausted,
				} {
					event := eventtest.ExistingRunRootIngress(uuid.NewString(), "freeze.delivery", "test", "", []byte(`{}`), 0, source, events.EventEnvelope{}, time.Now().UTC())
					if err := insertCanonicalEventRecordFixture(ctx, fixture.store, event); err != nil {
						t.Fatal(err)
					}
					failure := testFailureEnvelope(failures.ClassRetryExhausted, "fixture_failure", nil)
					snapshot := seedDeliveryStateFixture(t, ctx, fixture.store, event, events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("frozen-" + string(state))}, state, &failure)
					authority = snapshot.Authority
				}
				// The lower-level lifecycle owner can record a freeze with an open
				// attempt. Public materialization refuses that live-claim setup;
				// this is a persisted-state hardening control, not lawful fork admission.
				if _, _, err := fixture.store.(runlifecycle.OperationOwner).ForkRunSource(ctx, runlifecycle.ForkSourceRequest{RunID: source, ContinuedAsRunID: child, EndedAt: time.Now().UTC()}); err != nil {
					t.Fatal(err)
				}
				if replacement {
					var err error
					authority, err = deliverylifecycle.NewNormalExecutionAuthority(authority.SourceArtifact(), uuid.NewString(), authority.Generation()+1)
					if err != nil {
						t.Fatal(err)
					}
				}
				before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
				for repeat := 0; repeat < 2; repeat++ {
					probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(restore)
					commit, err := fixture.store.(interface {
						ActivateDeliveryAuthorityOutcome(context.Context, deliverylifecycle.ExecutionAuthority) (deliverylifecycle.ActivationCommit, error)
					}).ActivateDeliveryAuthorityOutcome(ctx, authority)
					if err != nil || !commit.Acknowledged {
						t.Fatalf("activation did not acknowledge: %+v %v", commit, err)
					}
					counts := probe.Snapshot()
					restore()
					if counts.Active != 0 || counts.Total.WriteCommits != 1 || counts.Total.Revision.LockPhases != 0 || counts.Total.Revision.QueryCalls != 0 || counts.Total.Revision.QueryRowCalls != 0 || counts.Total.Revision.ExecCalls != 0 {
						t.Errorf("frozen activation declared revision work: %+v", counts)
					}
					after := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
					if !reflect.DeepEqual(before, after) {
						for table, rows := range before {
							if !reflect.DeepEqual(rows, after[table]) {
								t.Errorf("activation changed frozen history table %s (repeat %d)", table, repeat)
							}
						}
						t.FailNow()
					}
				}
			})
		}
	}
}

func TestDeliveryAuthorityActivationPreservesLiveEligibilityBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		for _, state := range []runlifecycle.State{runlifecycle.StateRunning, runlifecycle.StatePaused} {
			t.Run(backend.name+"/"+string(state), func(t *testing.T) {
				fixture := backend.open(t)
				ctx := testAuthorActivityContext()
				runID := uuid.NewString()
				seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
				selected := fixture.store.(agentFixtureFlowStore)
				for _, deliveryState := range []deliverylifecycle.State{deliverylifecycle.StateQueued, deliverylifecycle.StateLaunching} {
					id := "live-" + string(deliveryState)
					seedManagedTurnFixtureAgent(t, ctx, selected, runID, id, "fixture/"+id)
				}
				ctx = admitPreparedAgentClaimContext(t, ctx, selected)
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
