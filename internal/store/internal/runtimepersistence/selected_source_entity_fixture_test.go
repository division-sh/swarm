package runtimepersistence

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestSelectedForkSourceEntityFixtureUsesOriginalCoordinatorBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			opened := backend.open(t)
			ctx := testAuthorActivityContext()
			event := runForkSnapshotFixtureEvent(t, ctx, opened.store)
			fixture := SelectedSourceEntityFixture{
				RunID: event.RunID(), EventID: event.ID(), EntityID: uuid.NewString(),
				EntityType: "test_entity", CreatedAt: time.Unix(1700002210, 0).UTC(),
			}
			probe, restore, err := InstallTransactionProbeForTest(opened.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if err := SeedSelectedSourceEntityForTest(ctx, opened.store, fixture); err != nil {
				t.Fatal(err)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.WriteCommits != 1 || counts.Total.Revision.Finalizations != 0 || counts.Active != 0 {
				t.Fatalf("historical seed escaped original unrevisioned writer: %+v", counts)
			}
			snapshot, err := ReadSelectedForkSourceDomainForTest(ctx, opened.store, fixture.RunID)
			if err != nil {
				t.Fatal(err)
			}
			assertSelectedSourceEntityFixtureRow(t, snapshot["entity_state"], map[string]string{
				"run_id": fixture.RunID, "entity_id": fixture.EntityID, "flow_instance": "flow-a/1",
				"entity_type": fixture.EntityType, "name": "Selected Execution Entity", "current_state": "pending",
				"gates": "{}", "fields": "{}", "accumulator": "{}",
			}, fixture.CreatedAt, "entered_state_at", "created_at", "updated_at")
			assertSelectedSourceEntityFixtureRow(t, snapshot["entity_mutations"], map[string]string{
				"run_id": fixture.RunID, "entity_id": fixture.EntityID, "domain": "lifecycle_state", "path": "",
				"old_value": "null", "new_value": `"pending"`, "caused_by_event": fixture.EventID,
				"writer_type": "platform", "writer_id": "selected-execution-test", "handler_step": "seed",
			}, fixture.CreatedAt, "created_at")
			projection, err := ReadTrackedEntityMutationProjectionStorageForTest(ctx, opened.store, fixture.RunID, fixture.EntityID)
			if err != nil || projection.Revision != 1 {
				t.Fatalf("historical entity revision changed: %+v %v", projection, err)
			}
			recovered, err := ReadSelectedForkRecoveredStorageSnapshotForTest(ctx, opened.store, fixture.RunID)
			if err != nil || len(recovered["flow_instances"].Rows) != 0 || len(recovered["flow_instance_runtime_readiness"].Rows) != 0 {
				t.Fatalf("headerless source acquired construction or readiness: %+v %v", recovered, err)
			}
			beforeCounts := probe.Snapshot()
			executable, err := ReadSelectedForkExecutableCardinalityForTest(ctx, opened.store, fixture.RunID)
			if err != nil || !reflect.DeepEqual(executable, map[string]string{
				"run_status": "running", "events": "1", "deliveries": "0", "entity_state": "1",
				"timers": "0", "readiness": "0", "agents": "0", "persisted_delivery_routes": "0",
				"flow_instances": "0", "execution_lineage": "0", "runtime_executions": "0",
			}) {
				t.Fatalf("exact executable cardinality lost physical scope: %+v %v", executable, err)
			}
			global, err := ReadSelectedForkGlobalCardinalityForTest(ctx, opened.store)
			if err != nil || !reflect.DeepEqual(global, map[string]int{
				"agents": 0, "persisted_delivery_routes": 0, "flow_instances": 0, "flow_instance_runtime_readiness": 0, "timers": 0,
			}) {
				t.Fatalf("global cardinality added a run or eligibility filter: %+v %v", global, err)
			}
			descendants, err := ReadSelectedForkDescendantCardinalityForTest(ctx, opened.store, fixture.RunID)
			if err != nil || !reflect.DeepEqual(descendants, map[string]int{
				"runs": 0, "bindings": 0, "route recoveries": 0, "branch divergences": 0,
				"execution lineage": 0, "runtime executions": 0, "events": 0, "deliveries": 0, "timers": 0,
			}) {
				t.Fatalf("source descendants leaked source business rows: %+v %v", descendants, err)
			}
			afterCounts := probe.Snapshot()
			if afterCounts.Total.Begun != beforeCounts.Total.Begun+3 || afterCounts.Total.ReadCommits != beforeCounts.Total.ReadCommits+3 || afterCounts.Total.WriteCommits != beforeCounts.Total.WriteCommits || afterCounts.Active != 0 {
				t.Fatalf("cardinality reads bypassed or mutated the original coordinator: before=%+v after=%+v", beforeCounts, afterCounts)
			}
			restore()
			assertSelectedForkGlobalRouteWitness(t, ctx, opened.store, fixture.RunID)
		})
	}
}

func assertSelectedForkGlobalRouteWitness(t *testing.T, ctx context.Context, selected any, originalRun string) {
	t.Helper()
	// A foreign-run physical obligation must be visible globally even though
	// the original headerless source still owns no delivery or construction.
	foreign := runForkSnapshotFixtureEvent(t, ctx, selected)
	if err := commitDeliveryObligationFixture(ctx, selected.(deliveryFixtureStore), foreign, testEntitylessNodeDeliveryRoute("cardinality-witness")); err != nil {
		t.Fatal(err)
	}
	counts, err := ReadSelectedForkExecutableCardinalityForTest(ctx, selected, originalRun)
	if err != nil || counts["deliveries"] != "0" || counts["persisted_delivery_routes"] != "1" || counts["flow_instances"] != "0" || counts["readiness"] != "0" {
		t.Fatalf("global route witness acquired a run filter or fabricated construction: %+v %v", counts, err)
	}
	global, err := ReadSelectedForkGlobalCardinalityForTest(ctx, selected)
	if err != nil || global["persisted_delivery_routes"] != 1 || global["flow_instances"] != 0 || global["agents"] != 0 {
		t.Fatalf("global route witness ignored the foreign persisted obligation: %+v %v", global, err)
	}
}

func TestSelectedForkSourceEntityFixtureRefusalsAndRollbackBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			opened := backend.open(t)
			ctx := testAuthorActivityContext()
			event := runForkSnapshotFixtureEvent(t, ctx, opened.store)
			foreign := runForkSnapshotFixtureEvent(t, ctx, opened.store)
			fixture := SelectedSourceEntityFixture{
				RunID: event.RunID(), EventID: event.ID(), EntityID: uuid.NewString(),
				EntityType: "test_entity", CreatedAt: time.Unix(1700002210, 0).UTC(),
			}
			if err := SeedSelectedSourceEntityForTest(ctx, opened.store, fixture); err != nil {
				t.Fatal(err)
			}
			before, err := ReadSelectedForkSourceDomainForTest(ctx, opened.store, fixture.RunID)
			if err != nil {
				t.Fatal(err)
			}
			for _, cut := range []string{"late-entity-conflict", "foreign-event", "invalid-identity", "empty-type", "missing-time", "missing-source", "cancelled"} {
				t.Run(cut, func(t *testing.T) {
					request, callCtx := fixture, ctx
					switch cut {
					case "foreign-event":
						request.EventID = foreign.ID()
					case "invalid-identity":
						request.EntityID = "invalid"
					case "empty-type":
						request.EntityType = ""
					case "missing-time":
						request.CreatedAt = time.Time{}
					case "missing-source":
						callCtx = context.Background()
					case "cancelled":
						cancelled, cancel := context.WithCancel(ctx)
						cancel()
						callCtx = cancelled
					}
					if err := SeedSelectedSourceEntityForTest(callCtx, opened.store, request); err == nil || (cut == "cancelled" && !errors.Is(err, context.Canceled)) {
						t.Fatalf("source fixture accepted %s: %v", cut, err)
					}
					after, err := ReadSelectedForkSourceDomainForTest(ctx, opened.store, fixture.RunID)
					if err != nil || !reflect.DeepEqual(after, before) {
						t.Fatalf("%s changed history or left its first statement committed: before=%+v after=%+v err=%v", cut, before, after, err)
					}
				})
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if got, err := ReadSelectedForkExecutableCardinalityForTest(cancelled, opened.store, fixture.RunID); !errors.Is(err, context.Canceled) || got != nil {
				t.Fatalf("cancelled executable read yielded evidence: %+v %v", got, err)
			}
			if got, err := ReadSelectedForkGlobalCardinalityForTest(cancelled, opened.store); !errors.Is(err, context.Canceled) || got != nil {
				t.Fatalf("cancelled global read yielded evidence: %+v %v", got, err)
			}
			if got, err := ReadSelectedForkDescendantCardinalityForTest(cancelled, opened.store, fixture.RunID); !errors.Is(err, context.Canceled) || got != nil {
				t.Fatalf("cancelled descendant read yielded evidence: %+v %v", got, err)
			}
			if err := opened.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if err := SeedSelectedSourceEntityForTest(ctx, opened.store, fixture); err == nil {
				t.Fatal("closed native owner accepted a source seed")
			}
			if got, err := ReadSelectedForkExecutableCardinalityForTest(ctx, opened.store, fixture.RunID); err == nil || got != nil {
				t.Fatalf("closed native owner yielded executable evidence: %+v %v", got, err)
			}
			if got, err := ReadSelectedForkGlobalCardinalityForTest(ctx, opened.store); err == nil || got != nil {
				t.Fatalf("closed native owner yielded global evidence: %+v %v", got, err)
			}
			if got, err := ReadSelectedForkDescendantCardinalityForTest(ctx, opened.store, fixture.RunID); err == nil || got != nil {
				t.Fatalf("closed native owner yielded descendant evidence: %+v %v", got, err)
			}
		})
	}
}

func assertSelectedSourceEntityFixtureRow(t testing.TB, table SelectedForkStorageTableSnapshot, expected map[string]string, at time.Time, clocks ...string) {
	t.Helper()
	rows, err := DecodeSelectedForkSnapshotRowsForTest(table)
	if err != nil || len(rows) != 1 {
		t.Fatalf("historical source row count or decoding changed: %+v %v", rows, err)
	}
	for column, want := range expected {
		cell, found := rows[0][column]
		got, err := cell.Text()
		if !found || err != nil || got != want {
			t.Fatalf("source column %s = %q (%v), want %q", column, got, err, want)
		}
	}
	for _, column := range clocks {
		cell, found := rows[0][column]
		if found && cell.Type == "string" {
			got, err := cell.Text()
			if err != nil || got != at.String() {
				t.Fatalf("source SQLite clock %s = %q, want exact %s: %v", column, got, at, err)
			}
			continue
		}
		var got time.Time
		if !found || cell.Type != "time.Time" || json.Unmarshal(cell.Value, &got) != nil || !got.Equal(at) {
			t.Fatalf("source clock %s = %+v, want exact %s", column, cell, at)
		}
	}
}
