package runtimepersistence

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

func TestDeliveryAuthorityActivationKeepsAdvancedSourceLiveBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := newForkActivationFrontierFixture(t, backend, true)
			staged, request := stageForkContentionFixture(t, f, true)
			if err := f.write(f.ctx, f.store); err != nil {
				t.Fatal(err)
			}
			activation, err := f.store.(runforkexecution.SelectedContractForkLifecycle).ActivateRunForkForSelectedContractExecution(f.ctx, request)
			if err != nil || !activation.Activated || !activation.SourceAdvancedAfterFork || activation.SourceFrozen || activation.BranchDivergence == nil {
				t.Fatalf("expected independently live advanced source: %+v %v", activation, err)
			}
			if err := f.store.(runlifecycle.OperationOwner).RequireActiveRun(f.ctx, f.runID); err != nil {
				t.Fatal(err)
			}
			store := f.store.(authorActivityReceiptStore)
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "freeze.delivery", "test", "", []byte(`{}`), 0, f.runID, events.EventEnvelope{}, time.Now().UTC())
			if err := insertCanonicalEventRecordFixture(f.ctx, store, event); err != nil {
				t.Fatal(err)
			}
			prior := seedDeliveryStateFixture(t, f.ctx, store, event, events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("advanced-source")}, deliverylifecycle.StateQueued, nil)
			authority, err := deliverylifecycle.NewNormalExecutionAuthority(prior.Authority.SourceArtifact(), uuid.NewString(), prior.Authority.Generation()+1)
			if err != nil {
				t.Fatal(err)
			}
			before := snapshotForkHistoricalExecutionTables(t, f.db, backend.name == "postgres")
			if err := f.store.(interface {
				ActivateDeliveryAuthority(context.Context, deliverylifecycle.ExecutionAuthority) error
			}).ActivateDeliveryAuthority(f.ctx, authority); err != nil {
				t.Fatal(err)
			}
			got, err := store.Snapshot(f.ctx, prior.DeliveryID)
			if err != nil || !got.Authority.Equal(authority) || got.Status != prior.Status || got.ClaimVersion != prior.ClaimVersion || !reflect.DeepEqual(got.Route, prior.Route) {
				t.Fatalf("advanced source incorrectly excluded: %+v %v", got, err)
			}
			after := snapshotForkHistoricalExecutionTables(t, f.db, backend.name == "postgres")
			if !reflect.DeepEqual(forkContentionRowsForRun(t, before, staged.ForkRunID), forkContentionRowsForRun(t, after, staged.ForkRunID)) {
				t.Fatal("normal source activation rewrote the independently selected child")
			}
		})
	}
}

func TestDeliveryAuthorityActivationSerializesWithFreezeBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		for _, first := range []string{"freeze", "authority"} {
			t.Run(backend.name+"/"+first, func(t *testing.T) {
				fixture := backend.open(t)
				ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 25*time.Second)
				defer cancel()
				source, child := uuid.NewString(), uuid.NewString()
				seedAuthorActivityReceiptRun(t, fixture, ctx, source)
				seedAuthorActivityReceiptRun(t, fixture, ctx, child)
				event := eventtest.ExistingRunRootIngress(uuid.NewString(), "freeze.delivery", "test", "", []byte(`{}`), 0, source, events.EventEnvelope{}, time.Now().UTC())
				if err := insertCanonicalEventRecordFixture(ctx, fixture.store, event); err != nil {
					t.Fatal(err)
				}
				// As in the persisted-state matrix, this lower-level freeze control
				// is not a claim that public fork admission permits a live attempt.
				prior := seedDeliveryStateFixture(t, ctx, fixture.store, event, events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("ordered-source")}, deliverylifecycle.StateLaunching, nil)
				authority, err := deliverylifecycle.NewNormalExecutionAuthority(prior.Authority.SourceArtifact(), uuid.NewString(), prior.Authority.Generation()+1)
				if err != nil {
					t.Fatal(err)
				}
				barrier := newForkContentionBarrier(t, backend.name, false)
				firstStore, secondStore := fixture.store, fixture.store
				if sqlite, ok := fixture.store.(*SQLiteRuntimeStore); ok {
					firstStore = newBootstrappedSQLiteRuntimeStoreForPath(t, sqlite.Path())
					secondStore = barrier.observeSQLiteStore(t, sqlite.Path())
				}
				// Both operations reach this real write only after owning the
				// existing native ordering fence. The contender must actually
				// wait on that fence (PG) or roll back BUSY (SQLite), not just start.
				point := "writer"
				if first == "freeze" {
					point = "source_freeze"
				}
				barrier.install(t, fixture.db, source, child, point)
				invoke := func(ctx context.Context, store authorActivityReceiptStore, operation string) forkContentionResult {
					if operation == "freeze" {
						_, _, err := store.(runlifecycle.OperationOwner).ForkRunSource(ctx, runlifecycle.ForkSourceRequest{RunID: source, ContinuedAsRunID: child, EndedAt: time.Now().UTC()})
						return forkContentionResult{err: err}
					}
					return forkContentionResult{err: store.(interface {
						ActivateDeliveryAuthority(context.Context, deliverylifecycle.ExecutionAuthority) error
					}).ActivateDeliveryAuthority(ctx, authority)}
				}
				second := "freeze"
				if first == "freeze" {
					second = "authority"
				}
				winner, loser := make(chan forkContentionResult, 1), make(chan forkContentionResult, 1)
				winnerJoined, loserJoined := make(chan struct{}), make(chan struct{})
				loserStarted := false
				t.Cleanup(func() {
					cancel()
					barrier.release()
					barrier.resume()
					for _, joined := range []<-chan struct{}{winnerJoined, loserJoined} {
						if joined == loserJoined && !loserStarted {
							continue
						}
						select {
						case <-joined:
						case <-time.After(5 * time.Second):
							t.Error("freeze/authority contender failed to join")
						}
					}
				})
				go func() { defer close(winnerJoined); winner <- invoke(ctx, firstStore, first) }()
				pid := barrier.awaitWinner(t, ctx, fixture.db, winner)
				loserCtx := context.WithValue(ctx, forkContentionContextKey{}, barrier)
				loserStarted = true
				go func() { defer close(loserJoined); loser <- invoke(loserCtx, secondStore, second) }()
				barrier.awaitContender(t, ctx, fixture.db, pid, &forkContentionGate{}, loser)
				barrier.release()
				barrier.resume()
				for _, result := range []<-chan forkContentionResult{winner, loser} {
					if got := awaitForkContentionResult(t, ctx, result); got.err != nil {
						t.Fatal(got.err)
					}
				}
				got, err := fixture.store.Snapshot(ctx, prior.DeliveryID)
				if err != nil {
					t.Fatal(err)
				}
				if first == "freeze" {
					if !reflect.DeepEqual(prior, got) {
						t.Fatalf("authority crossed the committed freeze: before=%+v after=%+v", prior, got)
					}
				} else if !got.Authority.Equal(authority) {
					t.Fatal("freeze lost the legitimately earlier authority activation")
				}
				before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
				if got := invoke(ctx, fixture.store, "authority"); got.err != nil {
					t.Fatal(got.err)
				}
				if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
					t.Fatal("post-ordering repeat mutated frozen history")
				}
			})
		}
	}
}
