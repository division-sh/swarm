package runtimepersistence

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

func TestB17NestedPublicationRequiresCommittedPredecessorBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mode := range []string{"deferred", "direct_publish", "engine_preparation"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				f := newFanOutGroupHistoryFixture(t, backend, 2)
				before := f.snapshot(t)
				// Reuse the real first-receipt SQL fault, not a replacement
				// settlement owner. Only the predecessor's receipt is rejected;
				// an incorrectly admitted nested event could otherwise commit.
				fault := &b10GroupFaultFixture{ctx: f.ctx, db: f.db, backend: backend, members: f.requests}
				remove := fault.installFaultAt(t, "receipt", 0)
				probe := &fanOutGroupHistoryNestedProbe{fixture: f, t: t, mode: mode}
				f.bus.SetInterceptors(probe)
				transactions, restore, err := InstallTransactionProbeForTest(f.raw, transactiontest.Options{})
				if err != nil {
					t.Fatal(err)
				}
				defer restore()
				err = f.bus.DispatchFanOutPublications(f.ctx, f.group, f.committed)
				requireB10NativeFault(t, backend, err)
				if probe.seenB != 1 || probe.seenChild != 0 || probe.child.ID() == "" {
					t.Fatalf("failed dependency boundary admitted nested execution: B=%d child=%d id=%s", probe.seenB, probe.seenChild, probe.child.ID())
				}
				var children int
				if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM events WHERE event_id=$1`, probe.child.ID()).Scan(&children); err != nil || children != 0 {
					t.Fatalf("nested event became durable before predecessor settlement: count=%d err=%v", children, err)
				}
				if after := f.snapshot(t); !reflect.DeepEqual(before, after) {
					t.Fatal("failed predecessor flush changed revision or fact history")
				}
				f.requireLiveReceipts(t, nil)
				receipt := transactions.Snapshot()
				segment := receipt.ByOperation[transactiontest.PipelineSettlement]
				if segment.Begun != 1 || segment.RollbackAttempts != 1 || segment.CommitAttempts != 0 || receipt.Total.WriteCommits != 0 || receipt.Active != 0 {
					t.Fatalf("failed flush was retried or acknowledged: %+v", receipt)
				}
				remove()
				if err := f.group.Close(f.ctx); err != nil {
					t.Fatal(err)
				}
				// Exact cleanup must leave each original event available to
				// canonical singleton recovery, without inventing a child event.
				owner := f.raw.(interface {
					PipelineObligations() pipelineobligation.Store
				}).PipelineObligations()
				for _, event := range f.events {
					work, err := owner.ClaimEvent(f.ctx, event.ID(), pipelineobligation.PurposeRecovery)
					if err != nil || work.Claim.EventID() != event.ID() {
						t.Fatalf("original event not reclaimable: id=%s work=%+v err=%v", event.ID(), work, err)
					}
					if err := owner.Release(f.ctx, work.Claim); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}
