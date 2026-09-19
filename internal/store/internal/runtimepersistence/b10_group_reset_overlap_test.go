package runtimepersistence

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

// Receiver retirement is the real barrier between reset's callback snapshot
// and its releases. A concurrently retiring group must not leave reset with
// authority to release callbacks which exact-membership cleanup already took.
func TestB10GroupRetirementOverlapsResetSnapshot(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, order := range []string{"retire_before_snapshot", "retire_after_snapshot"} {
			t.Run(backend+"/"+order, func(t *testing.T) {
				f := newGroupProofFixture(t, backend, 2)
				values := stageB10RetirementGroup(t, f)
				probe := &b12DeferredInterceptor{calls: map[string]int{}}
				f.bus.SetInterceptors(probe)
				before := f.snapshot(t)
				subscription, err := f.bus.SubscribeInternal(f.ctx, "b10-reset-snapshot-barrier")
				if err != nil {
					t.Fatal(err)
				}
				subscription.MarkReady()
				completed := false
				defer func() {
					if !completed {
						_ = subscription.Complete(false)
					}
				}()
				retire := func() {
					ctx, cancel := context.WithCancel(f.ctx)
					cancel()
					if err := f.bus.DispatchFanOutPublications(ctx, f.group, values); !errors.Is(err, context.Canceled) {
						t.Fatalf("exact-membership dynamic failure = %v, want canceled", err)
					}
					if err := f.group.Close(context.Background()); err != nil {
						t.Fatal(err)
					}
				}
				if order == "retire_before_snapshot" {
					retire()
				}
				resetDone := make(chan error, 1)
				go func() { resetDone <- f.bus.ResetInMemoryState() }()
				awaitGroupProof(t, subscription.Retiring())
				if order == "retire_after_snapshot" {
					retire()
				}
				if err := subscription.Complete(false); err != nil {
					t.Fatal(err)
				}
				completed = true
				if err := receiveGroupProof(t, resetDone); err != nil {
					t.Errorf("reset released callbacks after exact group retirement: %v", err)
				}
				if len(probe.calls) != 0 {
					t.Fatalf("retirement executed callbacks: %v", probe.calls)
				}
				f.unchanged(t, before)
				if err := f.bus.ResetInMemoryState(); err != nil {
					t.Errorf("repeated reset: %v", err)
				}
				for _, event := range f.events {
					work, err := f.store().ClaimEvent(f.ctx, event.ID(), pipelineobligation.PurposeRecovery)
					if err != nil || work.Claim.EventID() != event.ID() {
						t.Fatalf("durable recovery lost for %s: work=%+v err=%v", event.ID(), work, err)
					}
					if err := f.store().Release(f.ctx, work.Claim); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}
