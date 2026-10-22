package runforkexecution

import (
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/storetest"
)

// Old root: runtimepersistence.TestSelectedBranchPointActivationAtomicityBothStores.
// The native owner and every backend/point/status oracle are unchanged.
func TestSelectedBranchPointActivationAtomicityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, kind := range []runfork.RunForkPointKind{runfork.RunForkPointEvent, runfork.RunForkPointDeploymentRevision} {
			for _, status := range []string{"running", "cancelled"} {
				t.Run(backend+"/"+string(kind)+"/"+status, func(t *testing.T) {
					proveQ1BranchPointAtomicity(t, backend, kind, status)
				})
			}
		}
	}
}

func proveQ1BranchPointAtomicity(t *testing.T, backend string, kind runfork.RunForkPointKind, status string) {
	t.Helper()
	fixture := newActivityForkFixture(t, backend)
	owner, selected := fixture.owner, fixture.owner.ports.fork
	build := q1DeploymentComponentSource
	if kind == runfork.RunForkPointEvent {
		build = q1StateOnlyGateComponentSource
	}
	ctx, request, advance := build(t, owner)
	attachment := q1StageBranchPointComponent(t, ctx, request)
	advance()
	if status == "cancelled" {
		if _, _, err := storetest.TerminalizeRun(ctx, selected.(storetest.RunFixtureStore), runlifecycle.TerminalRequest{RunID: request.SourceRunID, State: runlifecycle.StateCancelled, EndedAt: time.Now().UTC()}); err != nil {
			t.Fatal(err)
		}
	}
	before := q1ComponentSnapshot(t, selected)
	remove := q1DivergenceInsertFault(t, ctx, selected)
	activation, err := attachment.activate()
	remove()
	if err == nil || !strings.Contains(err.Error(), "typed_receiver_owner_fault") || activation.Activated {
		t.Fatalf("divergence failure did not refuse activation: %+v err=%v", activation, err)
	}
	if !reflect.DeepEqual(before, q1ComponentSnapshot(t, selected)) {
		t.Fatal("failed divergence write partially committed source/child/author activity/operation state")
	}
	activation, err = attachment.activate()
	if err != nil || !activation.Activated || activation.SourceFrozen || activation.BranchDivergence == nil {
		t.Fatalf("branch retry did not activate: %+v err=%v", activation, err)
	}
	point := activation.BranchDivergence.ForkPoint
	if point.Kind != kind || point.Revision <= 0 || point.EventID != request.At || activation.BranchDivergence.ForkEventID != request.At || activation.SourceRunStatus != status || activation.BranchDivergence.SourceRunStatusAfterActivation != status {
		t.Fatalf("activation lost fixed typed point/source state: %+v", activation)
	}
	after := q1ComponentSnapshot(t, selected)
	if persisted := q1ComponentBranchPoint(t, after, attachment.materialized.ForkRunID); persisted != point {
		t.Fatalf("durable point differs from activation: %+v != %+v", persisted, point)
	}
	if !reflect.DeepEqual(q1ComponentRowsForRun(t, before, request.SourceRunID), q1ComponentRowsForRun(t, after, request.SourceRunID)) {
		t.Fatal("advanced source state/history was frozen, rewound or otherwise mutated")
	}
	operationRequest := attachment.prepared.forkOperation
	for repetition := 0; repetition < 2; repetition++ {
		operation, found, err := selected.LoadForkOperation(ctx, operationRequest.Actor, operationRequest.IdempotencyKey, operationRequest.TransportHash)
		if err != nil || !found || operation.Status != runfork.ForkOperationActivated || operation.ForkRunID != attachment.materialized.ForkRunID || operation.Result == nil || operation.Result.ForkPoint.Kind != point.Kind || operation.Result.ForkPoint.Revision != point.Revision || operation.Result.ForkPoint.EventID != point.EventID || operation.Result.SourceFrozen || operation.Result.SourceRunStatus != status {
			t.Fatalf("permanent completion lost exact branch point: %+v found=%t err=%v", operation, found, err)
		}
		encoded, err := json.Marshal(operation.Result)
		if err != nil {
			t.Fatal(err)
		}
		var decoded runfork.ForkOperationResult
		if err := json.Unmarshal(encoded, &decoded); err != nil || decoded.ForkPoint != operation.Result.ForkPoint {
			t.Fatalf("serialized public completion point changed: %+v err=%v", decoded, err)
		}
	}
	if _, err := attachment.activate(); err == nil {
		t.Fatal("repeated activation rewrote first completion")
	}
	if !reflect.DeepEqual(after, q1ComponentSnapshot(t, selected)) {
		t.Fatal("exact readback or rejected second activation mutated first committed evidence")
	}
	attachment.close(t)
}

func q1DivergenceInsertFault(t *testing.T, ctx context.Context, selected any) func() {
	t.Helper()
	if err := storetest.SetSelectedBranchDivergenceInsertFault(ctx, selected, true); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	remove := func() {
		once.Do(func() {
			if err := storetest.SetSelectedBranchDivergenceInsertFault(context.WithoutCancel(ctx), selected, false); err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(remove)
	return remove
}

func q1ComponentSnapshot(t *testing.T, selected any) map[string]storetest.SelectedForkStorageTableSnapshot {
	t.Helper()
	snapshot, err := storetest.ReadSelectedForkApplicationStorageSnapshot(runForkTestContext(t), selected)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func q1ComponentRowsForRun(t *testing.T, snapshot map[string]storetest.SelectedForkStorageTableSnapshot, runID string) map[string][]string {
	t.Helper()
	selected := make(map[string][]string, len(snapshot))
	for name, table := range snapshot {
		if !slices.Contains(table.Columns, "run_id") {
			continue
		}
		selected[name] = []string{}
		rows, err := storetest.DecodeSelectedForkSnapshotRows(table)
		if err != nil {
			t.Fatal(err)
		}
		for index, row := range rows {
			value, err := row["run_id"].Text()
			if err != nil {
				t.Fatal(err)
			}
			if value == runID {
				selected[name] = append(selected[name], table.Rows[index])
			}
		}
	}
	return selected
}

func q1ComponentBranchRows(t *testing.T, snapshot map[string]storetest.SelectedForkStorageTableSnapshot, child string) []map[string]storetest.SelectedForkSnapshotCell {
	t.Helper()
	table, found := snapshot["run_fork_selected_contract_branch_divergences"]
	if !found {
		t.Fatal("native branch table is absent")
	}
	rows, err := storetest.DecodeSelectedForkSnapshotRows(table)
	if err != nil {
		t.Fatal(err)
	}
	var matched []map[string]storetest.SelectedForkSnapshotCell
	for _, row := range rows {
		id, err := row["fork_run_id"].Text()
		if err != nil {
			t.Fatal(err)
		}
		if id == child {
			matched = append(matched, row)
		}
	}
	return matched
}

func q1ComponentRowsWithIdentity(t *testing.T, snapshot map[string]storetest.SelectedForkStorageTableSnapshot, table, column, identity string) []string {
	t.Helper()
	rows, err := storetest.DecodeSelectedForkSnapshotRows(snapshot[table])
	if err != nil {
		t.Fatal(err)
	}
	var matched []string
	for index, row := range rows {
		value, err := row[column].Text()
		if err != nil {
			t.Fatal(err)
		}
		if value == identity {
			matched = append(matched, snapshot[table].Rows[index])
		}
	}
	return matched
}

func q1ComponentBranchPoint(t *testing.T, snapshot map[string]storetest.SelectedForkStorageTableSnapshot, child string) runfork.RunForkPoint {
	t.Helper()
	rows := q1ComponentBranchRows(t, snapshot, child)
	if len(rows) != 1 {
		t.Fatalf("branch rows=%d, want exactly one", len(rows))
	}
	row := rows[0]
	kind, err := row["fork_point_kind"].Text()
	if err != nil {
		t.Fatal(err)
	}
	var revision int64
	if err := json.Unmarshal(row["fork_revision"].Value, &revision); err != nil {
		t.Fatal(err)
	}
	event, err := row["fork_event_id"].Text()
	if err != nil {
		t.Fatal(err)
	}
	if (string(row["fork_event_id"].Value) != "null") != (runfork.RunForkPointKind(kind) == runfork.RunForkPointEvent) {
		t.Fatal("stored event nullability differs from typed point kind")
	}
	return runfork.RunForkPoint{Kind: runfork.RunForkPointKind(kind), Revision: revision, EventID: event}
}
