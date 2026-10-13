package runtimepersistence

import (
	"context"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/google/uuid"
)

func TestOrdinaryForkPredecessorFaultScopeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newOrdinaryForkPredecessorFixture(t, backend)
			proveOrdinaryForkReceiptFaultRefusals(t, f)
			before := forkReceiverFaultSnapshot(t, f.ctx, f.store)
			if err := CopyReceiverConstructionReceiptForTest(f.ctx, f.store, f.sourceOwner, f.owner, f.sourceEntity, f.entityID); err != nil {
				t.Fatal(err)
			}
			after := forkReceiverFaultSnapshot(t, f.ctx, f.store)
			proveOrdinaryForkFaultTableDelta(t, before, after, "workflow_instance_initial_materializations", 0)
			if reflect.DeepEqual(before["workflow_instance_initial_materializations"], after["workflow_instance_initial_materializations"]) {
				t.Fatal("correspondence fault did not replace the exact projected receipt")
			}
			if err := CopyReceiverConstructionReceiptForTest(f.ctx, f.store, f.sourceOwner, f.owner, f.sourceEntity, f.entityID); err == nil || !reflect.DeepEqual(after, forkReceiverFaultSnapshot(t, f.ctx, f.store)) {
				t.Fatalf("existing destination receipt was overwritten or partially changed: %v", err)
			}
			proveOrdinaryForkClosedFaultRefusals(t, f)
		})
	}
}

func TestOrdinaryForkAbsentCutFaultScopeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newOrdinaryForkPredecessorFixture(t, backend)
			before := forkReceiverFaultSnapshot(t, f.ctx, f.store)
			for _, revision := range []int64{0, -1, math.MaxInt64, f.cutRevision} {
				if err := CorruptReceiverConstructionHistoryCutForTest(f.ctx, f.store, f.owner, f.entityID, revision); err == nil || !reflect.DeepEqual(before, forkReceiverFaultSnapshot(t, f.ctx, f.store)) {
					t.Fatalf("invalid/existing cut %d was admitted or mutated history: %v", revision, err)
				}
			}
			if f.earlierRevision >= f.cutRevision {
				t.Fatalf("cut control is not a distinct earlier revision: %d >= %d", f.earlierRevision, f.cutRevision)
			}
			if err := CorruptReceiverConstructionHistoryCutForTest(f.ctx, f.store, f.owner, f.entityID, f.earlierRevision); err == nil || !strings.Contains(err.Error(), "refuses an existing source revision") || !reflect.DeepEqual(before, forkReceiverFaultSnapshot(t, f.ctx, f.store)) {
				t.Fatalf("a distinct existing cut was not refused by its exact owner: %v", err)
			}
			proveOrdinaryForkCutOwnerRefusals(t, f, before)
			if err := CorruptReceiverConstructionHistoryCutForTest(f.ctx, f.store, f.owner, f.entityID, 1<<62); err != nil {
				t.Fatal(err)
			}
			after := forkReceiverFaultSnapshot(t, f.ctx, f.store)
			proveOrdinaryForkOriginRevisionDelta(t, before, after, f.owner.RunID)
			if err := CorruptReceiverConstructionHistoryCutForTest(f.ctx, f.store, f.owner, f.entityID, 1<<61); err == nil || !reflect.DeepEqual(after, forkReceiverFaultSnapshot(t, f.ctx, f.store)) {
				t.Fatalf("stale original cut was re-corrupted or changed history: %v", err)
			}
		})
	}
}

func proveOrdinaryForkCutOwnerRefusals(t *testing.T, f ordinaryForkPredecessorFixture, before map[string]SelectedForkStorageTableSnapshot) {
	t.Helper()
	canceled, cancel := context.WithCancel(f.ctx)
	cancel()
	for _, cut := range []struct {
		ctx      context.Context
		selected any
		owner    flowidentity.RunScopedFlowInstance
		entity   string
	}{
		{f.ctx, f.store, f.sourceOwner, f.sourceEntity},
		{f.ctx, f.store, f.owner, uuid.NewString()},
		{f.ctx, nil, f.owner, f.entityID},
		{f.ctx, f.db, f.owner, f.entityID},
		{canceled, f.store, f.owner, f.entityID},
	} {
		if err := CorruptReceiverConstructionHistoryCutForTest(cut.ctx, cut.selected, cut.owner, cut.entity, 1<<62); err == nil || !reflect.DeepEqual(before, forkReceiverFaultSnapshot(t, f.ctx, f.store)) {
			t.Fatalf("foreign/invalid/canceled cut changed history: %v", err)
		}
	}
}

func proveOrdinaryForkOriginRevisionDelta(t *testing.T, before, after map[string]SelectedForkStorageTableSnapshot, runID string) {
	t.Helper()
	proveOrdinaryForkFaultTableDelta(t, before, after, "runs", 0)
	prior, err := DecodeSelectedForkSnapshotRowsForTest(before["runs"])
	if err != nil {
		t.Fatal(err)
	}
	current, err := DecodeSelectedForkSnapshotRowsForTest(after["runs"])
	if err != nil {
		t.Fatal(err)
	}
	changed := 0
	for index, row := range prior {
		id, err := row["run_id"].Text()
		if err != nil {
			t.Fatal(err)
		}
		for column, cell := range row {
			replacement := current[index][column]
			if id == runID && column == "forked_from_revision" {
				var cut int64
				if err := json.Unmarshal(replacement.Value, &cut); err != nil || replacement.Type != "int64" || cut != 1<<62 || reflect.DeepEqual(cell, replacement) {
					t.Fatalf("origin fault lost its exact sole delta: %+v %v", replacement, err)
				}
				changed++
			} else if !reflect.DeepEqual(cell, replacement) {
				t.Fatalf("origin fault changed unrelated run/column %s/%s", id, column)
			}
		}
	}
	if changed != 1 {
		t.Fatalf("origin fault changed %d revision coordinates, want one", changed)
	}
}

func proveOrdinaryForkReceiptFaultRefusals(t *testing.T, f ordinaryForkPredecessorFixture) {
	t.Helper()
	before := forkReceiverFaultSnapshot(t, f.ctx, f.store)
	for _, scenario := range []string{"missing_source", "foreign_source", "wrong_entity", "wrong_route", "invalid_owner", "raw_owner", "canceled"} {
		source, sourceEntity, destination, entity, selected, ctx := f.sourceOwner, f.sourceEntity, f.owner, f.entityID, any(f.store), f.ctx
		switch scenario {
		case "missing_source":
			source.RunID = uuid.NewString()
		case "foreign_source":
			source, sourceEntity = f.otherSourceOwner, f.otherSourceEntity
		case "wrong_entity":
			entity = uuid.NewString()
		case "wrong_route":
			destination.Route = flowidentity.StoredRoute("review", "ti-foreign", "review/ti-foreign")
		case "invalid_owner":
			selected = (*PostgresStore)(nil)
		case "raw_owner":
			selected = f.db
		case "canceled":
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			ctx = canceled
		}
		if err := CopyReceiverConstructionReceiptForTest(ctx, selected, source, destination, sourceEntity, entity); err == nil || !reflect.DeepEqual(before, forkReceiverFaultSnapshot(t, f.ctx, f.store)) {
			t.Fatalf("%s receipt fault mutated history or returned success: %v", scenario, err)
		}
	}
}

func proveOrdinaryForkClosedFaultRefusals(t *testing.T, f ordinaryForkPredecessorFixture) {
	t.Helper()
	if err := f.manager.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if err := f.store.(interface{ Close() error }).Close(); err != nil {
		t.Fatal(err)
	}
	if err := CopyReceiverConstructionReceiptForTest(f.ctx, f.store, f.sourceOwner, f.owner, f.sourceEntity, f.entityID); err == nil {
		t.Fatal("receipt fault accepted a closed owner")
	}
	if err := CorruptReceiverConstructionHistoryCutForTest(f.ctx, f.store, f.owner, f.entityID, 1<<62); err == nil {
		t.Fatal("history-cut fault accepted a closed owner")
	}
}

func proveOrdinaryForkFaultTableDelta(t *testing.T, before, after map[string]SelectedForkStorageTableSnapshot, table string, delta int) {
	t.Helper()
	if len(before) != len(after) || len(after[table].Rows)-len(before[table].Rows) != delta {
		t.Fatalf("fault changed wrong table cardinality: %s before=%+v after=%+v", table, before[table], after[table])
	}
	for name, rows := range before {
		if name != table && !reflect.DeepEqual(rows, after[name]) {
			t.Fatalf("fault changed unrelated table %s", name)
		}
	}
}
