package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
)

func TestFanOutExactEffectsOptionalBarrierBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, withBarrier := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/barrier=%v", backend, withBarrier), func(t *testing.T) {
				raw, _, db, _ := newFanOutOwnerPairForTest(t, backend)
				ctx, fixture, _ := seedDeclaredForkFanOutFixtureWithBarrier(t, backend,
					authorActivityReceiptFixture{db: db, store: raw.(authorActivityReceiptStore)}, 0, time.Now().UTC(), withBarrier)
				barriers := 0
				if withBarrier {
					barriers = 1
				}
				requireExactFanOutHistory(t, ctx, db, fixture, 1, 0, barriers)
				if withBarrier {
					advanceFanOutBarriersForTest(t, ctx, raw.(storeTestDurableEventBusStore), db, fixture.runID, time.Now().UTC())
					requireExactFanOutHistory(t, ctx, db, fixture, 1, 0, 2)
					before := countP16RunRevisions(t, db, fixture.runID)
					advanceFanOutBarriersForTest(t, ctx, raw.(storeTestDurableEventBusStore), db, fixture.runID, time.Now().UTC())
					if got := countP16RunRevisions(t, db, fixture.runID); got != before {
						t.Fatalf("unchanged barrier added a revision: before=%d after=%d", before, got)
					}
				}
			})
		}
	}
}

func TestFanOutExactEffectsChunkBlockCancellationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			raw, db, _ := newP16RaceStore(t, backend)
			fixture, ctx, group, members, owner, command := prepareP16PublicationGroup(t, raw, db, backend, 34)
			// An unrelated closed intent exists physically but is not declared by
			// these mutations. A whole-family capture would incorrectly include it.
			seedFanOutOwnerIntent(t, ctx, db, fixture, 0, time.Now().UTC())
			if _, err := owner.CommitFanOutChunk(ctx, command); err != nil {
				t.Fatal(err)
			}
			requireExactFanOutHistory(t, ctx, db, fixture, 1, 32, 0)
			out, err := group.Settle(ctx, members)
			requireB18Acknowledged(t, out, err, members)
			if err := group.Close(ctx); err != nil {
				t.Fatal(err)
			}
			beforeClaim := countP16RunRevisions(t, db, fixture.runID)
			_, claim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{
				Owner: "exact-effects-block", BundleHash: fixture.bundleHash, Candidate: &command.Claim.Key, Now: time.Now().UTC(), Lease: time.Minute,
			})
			if err != nil || !found {
				t.Fatalf("claim exact suffix: found=%v err=%v", found, err)
			}
			if got := countP16RunRevisions(t, db, fixture.runID); got != beforeClaim {
				t.Fatalf("operational claim added semantic revision: before=%d after=%d", beforeClaim, got)
			}
			failure, ok := failures.EnvelopeFromError(failures.New(failures.ClassInternalFailure, "exact_effects_block", "runtime.fan_out", "test", nil))
			if !ok {
				t.Fatal("missing typed block failure")
			}
			if err := owner.BlockFanOutClaim(ctx, pipeline.FanOutBlockRequest{Claim: claim, Now: time.Now().UTC(), Failure: failure}); err != nil {
				t.Fatal(err)
			}
			requireExactFanOutHistory(t, ctx, db, fixture, 2, 32, 0)
			if _, err := raw.StopRunControl(ctx, runcontrol.TransitionRequest{RunID: fixture.runID}); err != nil {
				t.Fatal(err)
			}
			requireExactFanOutHistory(t, ctx, db, fixture, 3, 32, 0)
			if got := countP16RunRevisions(t, db, fixture.runID); got != beforeClaim+2 {
				t.Fatalf("block and cancellation need exactly two cuts: before=%d after=%d", beforeClaim, got)
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 32, 32)
			for _, member := range members {
				assertDirectiveReceipt(t, db, member.Claim.EventID(), "processed", nil)
			}
		})
	}
}

func requireExactFanOutHistory(t *testing.T, ctx context.Context, db *sql.DB, fixture fanOutOwnerFixture, intents, outcomes, barriers int) {
	t.Helper()
	rows, err := db.QueryContext(ctx, `SELECT present,CAST(fact AS TEXT) FROM run_fork_fact_revisions WHERE run_id=$1 AND family='fan_out_obligations' ORDER BY revision,fact_key`, fixture.runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	counts := map[string]int{}
	ordinals := map[int]bool{}
	for rows.Next() {
		var present bool
		var raw []byte
		if err := rows.Scan(&present, &raw); err != nil {
			t.Fatal(err)
		}
		var fact struct {
			Kind       string `json:"fact_kind"`
			DeliveryID string `json:"triggering_delivery_id"`
			Path       string `json:"semantic_path"`
			Ordinal    int    `json:"ordinal"`
		}
		if err := json.Unmarshal(raw, &fact); err != nil {
			t.Fatal(err)
		}
		if !present || fact.DeliveryID != fixture.deliveryID || fact.Path != fixture.semanticPath {
			t.Fatalf("exact writer captured unrelated/absent fan-out fact: present=%v fact=%s", present, raw)
		}
		switch fact.Kind {
		case "intent", "outcome", "barrier":
		default:
			t.Fatalf("unexpected fan-out fact kind: %s", raw)
		}
		counts[fact.Kind]++
		if fact.Kind == "outcome" {
			if fact.Ordinal < 0 || fact.Ordinal >= outcomes || ordinals[fact.Ordinal] {
				t.Fatalf("missing/rewritten ordinal set: fact=%s", raw)
			}
			ordinals[fact.Ordinal] = true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if counts["intent"] != intents || counts["outcome"] != outcomes || counts["barrier"] != barriers || len(counts) > 3 {
		t.Fatalf("exact fan-out history=%v want intent=%d outcome=%d barrier=%d", counts, intents, outcomes, barriers)
	}
}
