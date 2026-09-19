package runtimepersistence

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
)

func TestFanOutFoldJoinRollbackAndFreshSiblingBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			owner, restarted, db, postgres := newFanOutOwnerPairForTest(t, backend)
			selected := restarted.(storeTestDurableEventBusStore)
			base := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
			first := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 0, base)
			second := seedFanOutOwnerIntent(t, ctx, db, first, 0, base)
			if first.semanticPath > second.semanticPath {
				first, second = second, first
			}
			firstHandle := seedFanOutDeliveryBarrier(t, ctx, db, first, base)
			secondHandle := seedFanOutDeliveryBarrier(t, ctx, db, second, base)
			// The first barrier closes before the second fold sees a missing fact.
			if _, err := db.ExecContext(ctx, `UPDATE fan_out_intents SET cardinality=1,cursor=1 WHERE run_id=$1 AND semantic_path=$2`, second.runID, second.semanticPath); err != nil {
				t.Fatal(err)
			}
			if err := advanceFanOutBarriersAttempt(ctx, selected, first.runID, base.Add(time.Second)); err == nil || !strings.Contains(err.Error(), "ordinal outcomes") {
				t.Fatalf("later corrupt sibling must reject transaction: %v", err)
			}
			for _, fixture := range []fanOutOwnerFixture{first, second} {
				assertFanOutBarrierState(t, ctx, db, fixture.runID, fixture.deliveryID, fixture.semanticPath, fanoutbarrier.StatusArmed, nil, "")
			}
			assertFanOutBarrierTimerCount(t, ctx, db, first.runID, 0)

			// A new call must see the repaired, still-open sibling rather than
			// reusing any facts from the aborted transaction.
			if _, err := db.ExecContext(ctx, `UPDATE fan_out_intents SET cursor=0,status='open' WHERE run_id=$1 AND semantic_path=$2`, second.runID, second.semanticPath); err != nil {
				t.Fatal(err)
			}
			advanceFanOutBarriersForTest(t, ctx, selected, db, first.runID, base.Add(2*time.Second))
			empty := fanoutbarrier.Summary{}
			assertFanOutBarrierState(t, ctx, db, first.runID, first.deliveryID, first.semanticPath, fanoutbarrier.StatusClosedPending, &empty, firstHandle.TaskID())
			assertFanOutBarrierState(t, ctx, db, second.runID, second.deliveryID, second.semanticPath, fanoutbarrier.StatusArmed, nil, "")
			assertFanOutBarrierTimerCount(t, ctx, db, first.runID, 1)

			if _, err := db.ExecContext(ctx, `UPDATE fan_out_intents SET status='canceled',blocked_reason='fold_join_test' WHERE run_id=$1 AND semantic_path=$2`, second.runID, second.semanticPath); err != nil {
				t.Fatal(err)
			}
			advanceFanOutBarriersForTest(t, ctx, selected, db, first.runID, base.Add(3*time.Second))
			canceled := fanoutbarrier.Summary{Total: 1, Canceled: 1}
			assertFanOutBarrierState(t, ctx, db, second.runID, second.deliveryID, second.semanticPath, fanoutbarrier.StatusClosedPending, &canceled, secondHandle.TaskID())
			assertFanOutBarrierTimerCount(t, ctx, db, first.runID, 2)
			advanceFanOutBarriersForTest(t, ctx, selected, db, first.runID, base.Add(4*time.Second))
			assertFanOutBarrierTimerCount(t, ctx, db, first.runID, 2)
		})
	}
}
