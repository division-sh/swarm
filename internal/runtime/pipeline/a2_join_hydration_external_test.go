package pipeline_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestA2PersistedJoinHydrationRefusesCorruptionOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(backend.name, func(t *testing.T) {
			p := newA2MapFanOutExecution(t, backend.open(t), true)
			p.publish(t, "batch.ready", map[string]any{"a": []int64{1}, "b": []int64{2}})
			p.restart(t)
			initial := p.instance(t)
			arm := exactJoinPersistedArm(t, initial)
			node := externalPipelineSourceNode(t, p.source, ".", "collector")
			bucketKey := "handler_joins:" + node.Key()
			var original []byte
			if err := p.selected.db.QueryRowContext(p.ctx,
				"SELECT accumulator FROM flow_instances WHERE run_id=$1 AND entity_id=$1", p.runID).Scan(&original); err != nil {
				t.Fatal(err)
			}
			write := func(raw []byte) {
				t.Helper()
				result, err := p.selected.db.ExecContext(p.ctx,
					"UPDATE flow_instances SET accumulator=$1 WHERE run_id=$2 AND entity_id=$2", string(raw), p.runID)
				if err != nil {
					t.Fatal(err)
				}
				if count, err := result.RowsAffected(); err != nil || count != 1 {
					t.Fatalf("hostile setup affected %d receivers: %v", count, err)
				}
			}
			footprint := func() [5]int {
				t.Helper()
				var counts [5]int
				if err := p.selected.db.QueryRowContext(p.ctx, `SELECT
					(SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1),
					(SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1),
					(SELECT COUNT(*) FROM timers WHERE run_id=$1),
					(SELECT COUNT(*) FROM events WHERE run_id=$1),
					(SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1)`, p.runID).Scan(
					&counts[0], &counts[1], &counts[2], &counts[3], &counts[4]); err != nil {
					t.Fatal(err)
				}
				return counts
			}
			for _, corruption := range []string{"foreign_node_bucket", "foreign_activation_key", "non_map_bucket", "null_bucket"} {
				t.Run(corruption, func(t *testing.T) {
					var buckets map[string]map[string]any
					if err := json.Unmarshal(original, &buckets); err != nil {
						t.Fatal(err)
					}
					key := arm.Key()
					if len(buckets[bucketKey]) == 0 {
						t.Fatalf("hostile setup lacks exact node bucket %q in %s", bucketKey, original)
					}
					switch corruption {
					case "foreign_node_bucket":
						buckets[bucketKey+"-foreign"] = buckets[bucketKey]
						delete(buckets, bucketKey)
					case "foreign_activation_key":
						joins := buckets[bucketKey]["handler_joins"].(map[string]any)
						key += "-foreign"
						joins[key] = joins[arm.Key()]
						delete(joins, arm.Key())
					case "non_map_bucket":
						buckets[bucketKey]["handler_joins"] = []any{"corrupt"}
					case "null_bucket":
						buckets[bucketKey]["handler_joins"] = nil
					}
					raw, err := json.Marshal(buckets)
					if err != nil {
						t.Fatal(err)
					}
					write(raw)
					t.Cleanup(func() { write(original) })
					before, counts := p.instance(t), footprint()
					carrier, err := engine.StateCarrierFromPersisted(before.Fields, before.Bookkeeping, before.Gates, before.StateBuckets)
					if err != nil {
						t.Fatal(err)
					}
					if arms, err := joinruntime.List(carrier.StateBuckets); err == nil || len(arms) != 0 {
						t.Fatalf("persisted catalog accepted %s: arms=%#v err=%v", corruption, arms, err)
					}
					if corruption != "foreign_node_bucket" {
						if loaded, found, err := joinruntime.Load(carrier.StateBuckets, node, key); err == nil || found {
							t.Fatalf("persisted single-arm reader accepted %s: arm=%#v found=%v err=%v", corruption, loaded, found, err)
						}
					}
					route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node),
						Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowInstance: p.runID, EntityID: p.runID})}
					if receipts, fence, err := pipeline.PrepareWorkflowJoinAdmission(p.source, p.runID, "item.ready", route, &before); err == nil || len(receipts) != 0 || fence != nil {
						t.Fatalf("publication admission accepted corrupt retained arm: receipts=%#v fence=%#v err=%v", receipts, fence, err)
					}
					if after := p.instance(t); !reflect.DeepEqual(after, before) || footprint() != counts {
						t.Fatal("corrupt hydration/admission changed receiver, publication or history")
					}
				})
			}
			if after := p.instance(t); !reflect.DeepEqual(after, initial) {
				t.Fatal("hostile controls did not restore the original retained receiver")
			}
			handoff := p.handoff(t)
			turn, err := p.pc.ServeFanOutCandidate(p.ctx, handoff.owner, handoff.key)
			if err != nil {
				t.Fatal(err)
			}
			waitForGateRecoveryQuiescence(t, p.bus, p.ctx)
			outputs := p.outputs(t)
			if len(outputs) != 2 {
				t.Fatalf("restored pump did not publish the exact source: turn=%#v outputs=%#v", turn, outputs)
			}
			for _, output := range outputs {
				waitCtx, cancel := context.WithTimeout(p.ctx, 10*time.Second)
				signal, err := p.probe.WaitForHandlerCompleted(waitCtx, output.EventID, node.Key())
				cancel()
				if err != nil || signal.Status != "completed" {
					t.Fatalf("restored ordinal delivery: status=%s err=%v", signal.Status, err)
				}
				assertExactJoinDeliveryStatus(t, p.selected, p.ctx, output.EventID, node.Key(), "delivered")
			}
			closed := exactJoinPersistedArm(t, p.instance(t))
			if !closed.JoinRef().Equal(arm.JoinRef()) || closed.Completed() != 2 || !closed.OutcomePending || closed.CloseReason != joinruntime.CloseReasonComplete {
				t.Fatalf("restored pump did not close the exact original arm: %#v; turn=%#v outputs=%#v", closed, turn, p.outputs(t))
			}
			p.restart(t)
			schedule := exactJoinPendingSchedule(t, p.selected, p.ctx, closed)
			if err := p.driver.Resume(p.ctx); err != nil {
				t.Fatal(err)
			}
			completionID := exactJoinOccurrenceEventID(t, p.selected, p.ctx, p.runID, "platform.join_complete")
			waitCtx, cancel := context.WithTimeout(p.ctx, 10*time.Second)
			signal, err := p.probe.WaitForHandlerCompleted(waitCtx, completionID, node.Key())
			cancel()
			if err != nil || signal.Status != "completed" {
				t.Fatalf("restored continuation: status=%s err=%v", signal.Status, err)
			}
			terminal := waitForExactJoinState(t, p.ctx, p.pc, flowidentity.RouteForInstancePath(p.runID), "ready")
			if after := exactJoinPersistedArm(t, terminal); !after.OutcomeFired || after.OutcomePending || !after.JoinRef().Equal(arm.JoinRef()) {
				t.Fatalf("restored exact continuation did not finish once: %#v", after)
			}
			assertExactJoinFiredSchedule(t, p.selected, p.ctx, schedule, completionID)
			if _, err := p.pc.ServeFanOutCandidate(p.ctx, handoff.owner, handoff.key); err != nil {
				t.Fatal(err)
			}
			if after := p.instance(t); !reflect.DeepEqual(after, terminal) {
				t.Fatal("completed intent retry changed the restored exact receiver")
			}
		})
	}
}
