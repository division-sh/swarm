package runtimepersistence

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/google/uuid"
)

// Intent and loop buckets are controlled decoder inputs; the receiver is
// actually constructed. This qualifies the native barrier consumer, not a
// served fan-out journey or the compiler's production of the intent.
func TestFanOutBarrierConsumesConstructedHeaderBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, shape := range []string{"fieldless", "fields"} {
			for _, cell := range []string{"current", "stale", "wrong_path", "wrong_scope", "wrong_header_flow", "missing_header", "malformed_bucket", "missing_fields"} {
				if cell == "missing_fields" && shape == "fieldless" {
					continue
				}
				t.Run(backend+"/"+shape+"/"+cell, func(t *testing.T) {
					files := map[string]string{"schema.yaml": "name: barrier-header\nstages:\n  work: {}\n"}
					if shape == "fields" {
						files["entities.yaml"] = "item:\n  label: text?\n"
					}
					f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, files, nil)
					base := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
					intent := seedFanOutOwnerFixtureWithArtifact(t, f.ctx, f.db, f.store, backend == "postgres", 0, base, f.bundle.SourceArtifact)
					ctx := correlation.WithRunID(f.ctx, intent.runID)
					req := sqliteFlowActivationRequest(f.bundle, ".", intent.runID, "", intent.runID)
					plan := constructHistoricalSourceFixture(t, ctx, f.store, req)
					loop, err := loopruntime.New(intent.runID, plan.Instance.EntityID, ".", "retry", "revision_id", intent.eventID, "work", 3, base)
					if err != nil {
						t.Fatal(err)
					}
					handle := seedFanOutDeliveryBarrierRecord(t, ctx, f.db, intent, plan.Instance.EntityID, loop.Generation(), base)
					if _, err := f.db.ExecContext(ctx, `UPDATE fan_out_obligation_barriers SET route_scope_key='.',route_instance_id=$2,route_instance_path=$2 WHERE run_id=$1`, intent.runID, req.Instance.InstancePath); err != nil {
						t.Fatal(err)
					}
					if cell == "stale" {
						if _, err := loop.Repeat("work", uuid.NewString(), base.Add(time.Second)); err != nil {
							t.Fatal(err)
						}
					}
					buckets := map[string]map[string]any{}
					if err := loopruntime.Store(buckets, loop); err != nil {
						t.Fatal(err)
					}
					raw, err := json.Marshal(buckets)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := f.db.ExecContext(ctx, `UPDATE flow_instances SET accumulator=$3 WHERE run_id=$1 AND entity_id=$2`, intent.runID, plan.Instance.EntityID, string(raw)); err != nil {
						t.Fatal(err)
					}
					// Declared fields remain valid; the obsolete lifecycle shadow
					// deliberately disagrees with the canonical header.
					if shape == "fields" {
						if _, err := f.db.ExecContext(ctx, `UPDATE entity_state SET accumulator='{}' WHERE run_id=$1 AND entity_id=$2`, intent.runID, plan.Instance.EntityID); err != nil {
							t.Fatal(err)
						}
					}
					var query string
					switch cell {
					case "wrong_path":
						query = `UPDATE fan_out_obligation_barriers SET route_instance_path='foreign/one' WHERE run_id=$1`
					case "wrong_scope":
						query = `UPDATE fan_out_obligation_barriers SET route_scope_key='foreign' WHERE run_id=$1`
					case "wrong_header_flow":
						query = `UPDATE flow_instances SET flow_template='foreign' WHERE run_id=$1`
					case "missing_header":
						query = `DELETE FROM flow_instances WHERE run_id=$1`
					case "malformed_bucket":
						query = `UPDATE flow_instances SET accumulator='[]' WHERE run_id=$1`
					case "missing_fields":
						query = `DELETE FROM entity_state WHERE run_id=$1`
					}
					if query != "" {
						if _, err := f.db.ExecContext(ctx, query, intent.runID); err != nil {
							t.Fatal(err)
						}
					}
					before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
					err = advanceFanOutBarriersAttempt(ctx, f.store.(storeTestDurableEventBusStore), intent.runID, base.Add(2*time.Second))
					if cell != "current" && cell != "stale" {
						if err == nil {
							t.Fatal("invalid constructed barrier owner was admitted")
						}
						if after := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres"); !reflect.DeepEqual(before, after) {
							t.Fatal("refused barrier owner mutated durable state")
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					if cell == "stale" {
						assertFanOutBarrierState(t, ctx, f.db, intent.runID, intent.deliveryID, intent.semanticPath, fanoutbarrier.StatusSuppressedGenerationSuperseded, nil, "")
						assertFanOutBarrierTimerCount(t, ctx, f.db, intent.runID, 0)
					} else {
						want := fanoutbarrier.Summary{Total: 0}
						assertFanOutBarrierState(t, ctx, f.db, intent.runID, intent.deliveryID, intent.semanticPath, fanoutbarrier.StatusClosedPending, &want, handle.TaskID())
						assertFanOutBarrierTimerCount(t, ctx, f.db, intent.runID, 1)
					}
					var count int
					if err := f.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_state WHERE run_id=$1 AND entity_id=$2`, intent.runID, plan.Instance.EntityID).Scan(&count); err != nil || (shape == "fieldless" && count != 0) || (shape == "fields" && count != 1) {
						t.Fatalf("barrier changed declared field presence: count=%d err=%v", count, err)
					}
				})
			}
		}
	}
}
