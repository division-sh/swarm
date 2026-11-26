package runforkexecution

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
)

func TestSelectedContinuationRetirementSealsQuietCutBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newActivityForkFixture(t, backend)
			ctx, request, _ := q1GateComponentSource(t, fixture.owner, false, true, true)
			attachment := q1StageBranchPointComponent(t, ctx, request)
			p, container := attachment.prepared, attachment.container
			if err := p.retainedRuntime.startExecution(container.attachment.ctx, container.attachment.pipeline, container.diagnostics, false); err != nil {
				t.Fatal(err)
			}
			reconcile, err := container.selectedBarrierReconciler()
			if err != nil {
				t.Fatal(err)
			}
			controlCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			var afterQuiet atomic.Bool
			entered := make(chan context.Context, 1)
			coordinator, err := deliverycontinuation.NewSelected(p.owner.ports.busDurable.DeliveryLifecycle,
				container.attachment.deliveryAuthority, p.operation.selected, container.attachment.runtime.bus, nil,
				func(scanCtx context.Context) error {
					if afterQuiet.CompareAndSwap(true, false) {
						entered <- scanCtx
						select {
						case <-scanCtx.Done():
						case <-controlCtx.Done():
						}
					}
					return reconcile(scanCtx)
				})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = coordinator.Retire(context.Background()) })
			if err := container.attachment.runtime.bus.SetDeliveryContinuationOwner(coordinator); err != nil {
				t.Fatal(err)
			}
			if err := coordinator.Start(container.attachment.ctx); err != nil {
				t.Fatal(err)
			}
			if err := container.joinLiveSelectedWork(coordinator); err != nil {
				t.Fatalf("selected quiet cut: %v", err)
			}

			afterQuiet.Store(true)
			coordinator.Signal()
			synced := make(chan error, 1)
			go func() { synced <- coordinator.Synchronize(controlCtx) }()
			select {
			case scanCtx := <-entered:
				if scanCtx.Err() != nil {
					t.Fatalf("post-quiet scan already canceled before retirement: %v", scanCtx.Err())
				}
				retireErr := coordinator.Retire(controlCtx)
				<-synced
				if controlCtx.Err() != nil || scanCtx.Err() != context.Canceled {
					t.Fatalf("did not observe retirement cancel its admitted scan: scan=%v retire=%v bound=%v", scanCtx.Err(), retireErr, controlCtx.Err())
				}
				t.Fatalf("post-quiet scan entered; retirement canceled its exact context (retire=%v)", retireErr)
			case err := <-synced:
				if _, retired := deliverycontinuation.StaticRetirementCause(err); !retired {
					t.Fatalf("post-quiet synchronization was not refused by the retired coordinator: %v", err)
				}
			case <-controlCtx.Done():
				t.Fatal("post-quiet scan admission did not resolve")
			}
			if err := coordinator.Retire(controlCtx); err != nil {
				t.Fatalf("join selected continuation retirement: %v", err)
			}
			select {
			case <-entered:
				t.Fatal("selected reconciliation entered after the quiet cut")
			default:
			}
		})
	}
}
