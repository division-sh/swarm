package runtimepersistence

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestFanOutFiniteFairnessPreservesOlderSuffixAgainstNewcomersOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			owner, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			long := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 64, time.Now().UTC())
			request := pipeline.FanOutClaimRequest{Owner: "finite-fairness", BundleHash: long.bundleHash, Now: time.Now().UTC(), Lease: time.Minute}
			_, first, found, err := owner.ClaimFanOutIntent(ctx, request)
			if err != nil || !found {
				t.Fatalf("claim: found=%v err=%v", found, err)
			}
			if _, err := owner.CommitFanOutChunk(ctx, rejectedFanOutChunk(first, 0, 32, time.Now().UTC())); err != nil {
				t.Fatal(err)
			}
			var newcomers []fanOutOwnerFixture
			for n := 0; n < 3; n++ {
				newcomers = append(newcomers, seedFanOutOwnerIntent(t, ctx, db, long, 1, time.Now().UTC()))
			}
			_, next, found, err := owner.ClaimFanOutIntent(ctx, request)
			if err != nil || !found || next.Key != first.Key {
				t.Fatalf("new arrivals overtook older suffix: old=%+v next=%+v found=%v err=%v", first.Key, next.Key, found, err)
			}
			if _, err := owner.CommitFanOutChunk(ctx, rejectedFanOutChunk(next, 32, 32, time.Now().UTC())); err != nil {
				t.Fatal(err)
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, long, 64, 64)
			for _, newcomer := range newcomers {
				_, claim, found, err := owner.ClaimFanOutIntent(ctx, request)
				if err != nil || !found || claim.Key.ElementRef.SemanticPath != newcomer.semanticPath {
					t.Fatalf("creation-order newcomer: claim=%+v found=%v err=%v", claim, found, err)
				}
				if _, err := owner.CommitFanOutChunk(ctx, rejectedFanOutChunk(claim, 0, 1, time.Now().UTC())); err != nil {
					t.Fatal(err)
				}
				assertFanOutCursorAndOutcomeCount(t, ctx, db, newcomer, 1, 1)
			}
		})
	}
}
