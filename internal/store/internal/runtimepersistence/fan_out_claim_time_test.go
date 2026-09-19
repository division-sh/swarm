package runtimepersistence

import (
	"context"
	"errors"
	"testing"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func fanOutRetryFailureForTest() runtimefailures.Envelope {
	return runtimefailures.Normalize(runtimefailures.New(runtimefailures.ClassDependencyUnavailable,
		"fan_out_test_dependency_unavailable", "runtime.fan_out", "commit_chunk", nil), "runtime.fan_out", "commit_chunk")
}

func TestFanOutExpiredUnreclaimedClaimCannotReadOrCommitOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			owner, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			fixture := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 1, time.Now().UTC())
			turnTime := time.Now().UTC()
			_, claim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{
				Owner: "expired-unreclaimed", BundleHash: fixture.bundleHash, Now: turnTime, Lease: 100 * time.Millisecond,
			})
			if err != nil || !found {
				t.Fatalf("claim: found=%v err=%v", found, err)
			}
			// No successor reclaims: expiry alone must invalidate execution authority.
			if remaining := time.Until(claim.LeaseUntil.Add(10 * time.Millisecond)); remaining > 0 {
				time.Sleep(remaining)
			}
			if _, err := owner.LoadFanOutEvaluation(ctx, claim); !errors.Is(err, fanoutobligation.ErrStaleClaim) {
				t.Errorf("expired evaluation: %v", err)
			}
			if _, err := owner.CommitFanOutChunk(ctx, rejectedFanOutChunk(claim, 0, 1, turnTime)); !errors.Is(err, fanoutobligation.ErrStaleClaim) {
				t.Errorf("expired commit using original audit time: %v", err)
			}
			if err := owner.ReleaseFanOutRetryable(ctx, pipeline.FanOutRetryableRelease{
				Claim: claim, Now: turnTime, Failure: fanOutRetryFailureForTest(),
			}); !errors.Is(err, fanoutobligation.ErrStaleClaim) {
				t.Errorf("expired claim changed retry state: %v", err)
			}
			if err := owner.BlockFanOutClaim(ctx, pipeline.FanOutBlockRequest{
				Claim: claim, Now: turnTime, Failure: runtimefailures.Normalize(errors.New("invariant"), "runtime.fan_out", "test"),
			}); !errors.Is(err, fanoutobligation.ErrStaleClaim) {
				t.Errorf("expired claim blocked intent: %v", err)
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 0, 0)
			// Discarding our own expired claim is cleanup, not renewed authority.
			if err := owner.ReleaseFanOutClaim(ctx, claim); err != nil {
				t.Fatal(err)
			}
			_, successor, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{
				Owner: "successor", BundleHash: fixture.bundleHash, Now: time.Now().UTC(), Lease: time.Minute,
			})
			if err != nil || !found || successor.Generation <= claim.Generation {
				t.Fatalf("successor claim: found=%v claim=%+v err=%v", found, successor, err)
			}
			if err := owner.ReleaseFanOutClaim(ctx, claim); !errors.Is(err, fanoutobligation.ErrStaleClaim) {
				t.Fatalf("expired cleanup must not release successor: %v", err)
			}
			if _, err := owner.CommitFanOutChunk(ctx, rejectedFanOutChunk(successor, 0, 1, time.Now().UTC())); err != nil {
				t.Fatal(err)
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 1, 1)
		})
	}
}

func TestFanOutClaimLeaseUsesAdmissionRatherThanCallerClockOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			owner, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			fixture := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 1, time.Now().UTC())
			for _, offset := range []time.Duration{-time.Hour, time.Hour} {
				before := time.Now().UTC()
				_, claim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{
					Owner: "clock-proof", BundleHash: fixture.bundleHash, Now: before.Add(offset), Lease: time.Minute,
				})
				after := time.Now().UTC()
				if err != nil || !found {
					t.Fatalf("claim offset %s: found=%v err=%v", offset, found, err)
				}
				if claim.LeaseUntil.Before(before.Truncate(time.Microsecond).Add(time.Minute)) || claim.LeaseUntil.After(after.Add(time.Minute)) {
					t.Fatalf("caller time selected lease: before=%s after=%s claim=%+v", before, after, claim)
				}
				if err := owner.ReleaseFanOutClaim(ctx, claim); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestFanOutClaimExpiresWhileMutationWaitsForWriterOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 10*time.Second)
			defer cancel()
			owner, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			fixture := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 1, time.Now().UTC())
			at := time.Now().UTC()
			_, claim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{
				Owner: "waiting-writer", BundleHash: fixture.bundleHash, Now: at, Lease: 150 * time.Millisecond,
			})
			if err != nil || !found {
				t.Fatalf("claim: found=%v err=%v", found, err)
			}
			holder, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer holder.Rollback()
			if _, err := holder.ExecContext(ctx, `UPDATE fan_out_intents SET updated_at=updated_at WHERE run_id=$1`, fixture.runID); err != nil {
				t.Fatal(err)
			}
			entered, result := make(chan struct{}), make(chan error, 1)
			go func() {
				close(entered)
				_, err := owner.CommitFanOutChunk(ctx, rejectedFanOutChunk(claim, 0, 1, at))
				result <- err
			}()
			<-entered
			if remaining := time.Until(claim.LeaseUntil.Add(10 * time.Millisecond)); remaining > 0 {
				time.Sleep(remaining)
			}
			if err := holder.Commit(); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-result:
				if !errors.Is(err, fanoutobligation.ErrStaleClaim) {
					t.Fatalf("writer wait bypassed expiry: %v", err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 0, 0)
		})
	}
}
