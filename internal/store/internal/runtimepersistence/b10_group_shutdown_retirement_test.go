package runtimepersistence

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

// Finalized outbox callbacks must retire even if dispatch's initial committed-
// group validation fails. Group Close legitimately releases their exact claims;
// shutdown must not subsequently treat those callbacks as still-owned claims.
func TestB10GroupValidationFailureRetiresCallbacksBeforeShutdown(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, phase := range []string{"healthy", "canceled_validation"} {
			t.Run(backend+"/"+phase, func(t *testing.T) {
				f, _ := newB12CollectorFixture(t, backend)
				probe := &b12DeferredInterceptor{calls: map[string]int{}}
				f.bus.SetInterceptors(probe)
				ctx := f.ctx
				if phase == "canceled_validation" {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				err := f.bus.DispatchFanOutPublications(ctx, f.group, f.committed)
				if phase == "healthy" && err != nil {
					t.Fatal(err)
				}
				if phase == "canceled_validation" && !errors.Is(err, context.Canceled) {
					t.Fatalf("did not reach canceled initial validation: %v", err)
				}
				wantCalls := 1
				if phase == "canceled_validation" {
					wantCalls = 0
				}
				for _, event := range f.events {
					if probe.calls[event.ID()] != wantCalls {
						t.Fatalf("unexpected callback execution: %v", probe.calls)
					}
				}
				if err := f.group.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
				before := readP16PreservationSnapshot(t, f.db)
				if err := f.bus.ResetInMemoryState(); err != nil {
					t.Errorf("shutdown retained callbacks whose exact group claims already closed: %v", err)
				}
				requireP16PreservationSnapshot(t, f.db, before)
				if err := f.bus.ResetInMemoryState(); err != nil {
					t.Errorf("repeated shutdown did not finish retirement: %v", err)
				}
				if phase == "canceled_validation" {
					store := f.raw.(interface {
						PipelineObligations() pipelineobligation.Store
					}).PipelineObligations()
					for i, event := range f.events {
						purpose := pipelineobligation.PurposeRecovery
						if i == 1 {
							// The fixture's second event has a pending decision route;
							// ordinary recovery deliberately excludes that owner.
							purpose = pipelineobligation.PurposeDecisionRoute
						}
						work, err := store.ClaimEvent(f.ctx, event.ID(), purpose)
						if err != nil || work.Claim.EventID() != event.ID() {
							t.Fatalf("retirement lost durable recovery: event=%s work=%+v err=%v", event.ID(), work, err)
						}
						if err := store.Release(f.ctx, work.Claim); err != nil {
							t.Fatal(err)
						}
					}
				}
			})
		}
	}
}
