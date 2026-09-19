package runtimepersistence

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

func TestFanOutRegistrationCloseJoinsCommittedHandoffBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newGroupProofFixture(t, backend, 2)
			f.prepare(t)
			f.seal(t)
			f.commit(t)
			baseline := f.occurrence.ActiveCount()
			unrelated, err := f.occurrence.Begin(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = unrelated.Done() })
			workers := 1
			r, err := startupownership.RegisterFanOutServing(f.ctx, f.grant, f.occurrence, &workers)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(r.Close)
			permit, found, err := r.BeginTurn(f.ctx)
			if err != nil || !found {
				t.Fatalf("turn admission: found=%v err=%v", found, err)
			}
			t.Cleanup(permit.Done)
			committed, finishCommit := make(chan struct{}), make(chan struct{})
			var held atomic.Bool
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(finishCommit) }) }
			t.Cleanup(release)
			f.probe.set(func(phase, _ string) error {
				if phase == "after_write_commit" && held.CompareAndSwap(false, true) {
					close(committed)
					<-finishCommit
				}
				return nil
			})
			var outcome pipelineobligation.PublicationGroupOutcome
			settled := make(chan error, 1)
			go func() {
				var err error
				outcome, err = f.group.Settle(permit.Context(), f.members())
				err = errors.Join(err, f.group.Close(context.Background()))
				permit.Done()
				settled <- err
			}()
			awaitGroupProof(t, committed)
			closed := make(chan struct{})
			go func() { r.Close(); close(closed) }()
			awaitGroupProof(t, permit.Context().Done())
			secondClose := make(chan struct{})
			go func() { r.Close(); close(secondClose) }()
			select {
			case <-closed:
				t.Error("Close returned before acknowledged commit/handoff completed")
			case <-secondClose:
				t.Error("concurrent Close bypassed finite-turn join")
			case <-time.After(25 * time.Millisecond):
			}
			if next, found, err := r.BeginTurn(f.ctx); err == nil || found || next != nil {
				if next != nil {
					next.Done()
				}
				t.Errorf("closing registration admitted a turn: found=%v err=%v", found, err)
			}
			release()
			if err := receiveGroupProof(t, settled); err != nil {
				t.Fatalf("admitted postcommit handoff was canceled: %v", err)
			}
			awaitGroupProof(t, closed)
			awaitGroupProof(t, secondClose)
			if len(outcome.Results) != len(f.claims) {
				t.Fatalf("lost acknowledged group results: %+v", outcome)
			}
			for i, result := range outcome.Results {
				if result.Claim != f.claims[i] || !result.Outcome.Committed() || !result.Outcome.DeliveryHandoffCommitted() {
					t.Fatalf("lost exact committed/handoff authority: %+v", result)
				}
				assertDirectiveReceipt(t, f.db, result.Claim.EventID(), "processed", nil)
			}
			if unrelated.Context().Err() != nil || f.occurrence.ActiveCount() != baseline+1 {
				t.Fatalf("Close joined/canceled unrelated work or leaked turns: active=%d baseline=%d unrelated=%v", f.occurrence.ActiveCount(), baseline, unrelated.Context().Err())
			}
			r.Close()
		})
	}
}

func TestFanOutRegistrationCloseIsSourceLocalBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			raw, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			first := seedFanOutOwnerFixture(t, ctx, db, raw, postgres, 0, time.Now().UTC())
			other := seedFanOutOwnerFixtureWithArtifact(t, ctx, db, raw, postgres, 0, time.Now().UTC(), storeTestSourceArtifact("registration-close-other-source"))
			_, grants, occurrences, _ := newGrantedFanOutProcessForTest(t, ctx, raw, []fanOutOwnerFixture{first, other})
			workers := 1
			registrations := make([]*startupownership.FanOutServingRegistration, 2)
			for i := range registrations {
				var err error
				registrations[i], err = startupownership.RegisterFanOutServing(ctx, grants[i], occurrences[i], &workers)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(registrations[i].Close)
			}
			permit, found, err := registrations[0].BeginTurn(ctx)
			if err != nil || !found {
				t.Fatalf("turn admission: found=%v err=%v", found, err)
			}
			t.Cleanup(permit.Done)
			closed := make(chan struct{})
			go func() { registrations[1].Close(); close(closed) }()
			awaitGroupProof(t, closed)
			if err := permit.Context().Err(); err != nil {
				t.Fatalf("idle source retirement canceled another registration's turn: %v", err)
			}
			if next, found, err := registrations[1].BeginTurn(ctx); err == nil || found || next != nil {
				if next != nil {
					next.Done()
				}
				t.Fatalf("closed source regained admission: found=%v err=%v", found, err)
			}
			permit.Done()
			permit.Done()
			if occurrences[0].ActiveCount() != 1 || occurrences[1].ActiveCount() != 0 {
				t.Fatalf("wrong source leases retired: active=%d/%d", occurrences[0].ActiveCount(), occurrences[1].ActiveCount())
			}
			registrations[0].Close()
		})
	}
}
