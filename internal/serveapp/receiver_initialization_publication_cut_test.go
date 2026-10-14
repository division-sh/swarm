package serveapp

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/store/storetest"
)

// Only these two constructor routes use the release log as a snapshot-writer
// fence. Their earlier delivery/configuration/candidate gates remain required.
func requireReceiverInitializationHeldOrigin(t *testing.T, reader receiverProofStateReader, endpoint string, probe *lifecycleprobe.Probe, hold *exactOriginDiagnosticHold, seed servedEventPublishRPCResult, release func(), completion *receiverCompletionBoundary, bundleHash string) {
	t.Helper()
	defer release()
	ctx, cancel := context.WithTimeout(t.Context(), servedEventPublishLifecycleProbeWaitTimeout)
	defer cancel()
	select {
	case eventID := <-hold.entered:
		if eventID != seed.EventID {
			t.Fatalf("origin diagnostic hold borrowed event %s instead of %s", eventID, seed.EventID)
		}
	case <-ctx.Done():
		t.Fatal("exact seed did not reach its held diagnostic writer")
	}
	absent, cancelAbsent := context.WithCancel(t.Context())
	cancelAbsent()
	if _, err := probe.Wait(absent, lifecycleprobe.Signal{Kind: lifecycleprobe.PostCommitDispatchCompleted, EventID: seed.EventID}); err == nil {
		t.Fatal("held origin diagnostic advertised completion")
	}
	var logs operatorread.OperatorRuntimeLogListResult
	requireServedJSONRPCResult(t, endpoint, "runtime.logs", map[string]any{"run_id": seed.RunID, "component": "eventbus", "limit": 1000}, &logs)
	if logs.NextCursor != "" {
		t.Fatal("constructor origin logs exceeded the existing bounded page")
	}
	for _, log := range logs.Logs {
		if log.EventID == seed.EventID && log.Component == "eventbus" && log.Action == "published" {
			t.Fatal("exact published-log fence opened before its native diagnostic write")
		}
	}
	requireReceiverCompletionBoundary(t, ctx, completion, bundleHash, seed.RunID)
	before := requireReceiverApplicationSnapshot(t, reader)
	release()
	if err := waitExactOriginPublications(ctx, probe, seed.EventID); err != nil {
		t.Fatal(err)
	}
	waitServedEventPublishedLog(t, endpoint, seed.RunID, seed.EventID)
	after := requireReceiverApplicationSnapshot(t, reader)
	requireReceiverOriginDiagnosticRunCounter(t, before, after, seed.RunID)
	for table, rows := range before {
		// The native diagnostic append also records its R1 revision evidence
		// in the same transaction; these are origin writes, not later actors.
		switch table {
		case "runs", "events", "run_fork_revision_heads", "run_fork_revisions", "run_fork_fact_revisions":
			t.Logf("origin diagnostic native table %s rows %d -> %d", table, len(rows), len(after[table]))
			continue
		}
		if !reflect.DeepEqual(rows, after[table]) {
			t.Fatalf("non-origin writer survived the earlier constructor gates: %s", table)
		}
	}
	if len(after["events"]) != len(before["events"])+1 {
		t.Fatalf("origin diagnostic appended %d rows, want exactly one", len(after["events"])-len(before["events"]))
	}
	for _, table := range []string{"run_fork_revisions", "run_fork_fact_revisions"} {
		if len(after[table]) != len(before[table])+1 {
			t.Fatalf("origin diagnostic %s appended %d rows, want one", table, len(after[table])-len(before[table]))
		}
	}
	if len(after["run_fork_revision_heads"]) != len(before["run_fork_revision_heads"]) {
		t.Fatal("origin diagnostic changed the revision-head owner census")
	}
	retained := make(map[string]bool, len(after["events"]))
	for _, row := range after["events"] {
		retained[row] = true
	}
	for _, row := range before["events"] {
		if !retained[row] {
			t.Fatal("origin diagnostic changed a preexisting event")
		}
	}
	t.Logf("HELD ORIGIN: earlier delivery/configuration gates passed, exact seed %s log/completion remained absent, release appended exactly one diagnostic with its native revision evidence; every unrelated application table unchanged", seed.EventID)
}

func requireReceiverOriginDiagnosticRunCounter(t *testing.T, before, after map[string][]string, runID string) {
	t.Helper()
	var columns []string
	if err := json.Unmarshal([]byte(before["runs/columns"][0]), &columns); err != nil {
		t.Fatal(err)
	}
	oldRows, err := storetest.DecodeSelectedForkSnapshotRows(storetest.SelectedForkStorageTableSnapshot{Columns: columns, Rows: before["runs"]})
	if err != nil {
		t.Fatal(err)
	}
	newRows, err := storetest.DecodeSelectedForkSnapshotRows(storetest.SelectedForkStorageTableSnapshot{Columns: columns, Rows: after["runs"]})
	if err != nil || len(newRows) != len(oldRows) {
		t.Fatalf("origin diagnostic changed the run census: %v", err)
	}
	found := false
	for index, old := range oldRows {
		id, err := workspaceProofPhysicalText(old, "run_id")
		if err != nil {
			t.Fatal(err)
		}
		if id == runID {
			found = true
			var previous, current int64
			if err := json.Unmarshal(old["event_count"].Value, &previous); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(newRows[index]["event_count"].Value, &current); err != nil {
				t.Fatal(err)
			}
			if current != previous+1 {
				t.Fatalf("origin diagnostic run counter %d -> %d, want one exact append", previous, current)
			}
			old["event_count"] = newRows[index]["event_count"]
		}
		if !reflect.DeepEqual(old, newRows[index]) {
			t.Fatal("origin diagnostic changed a run field other than its exact event counter")
		}
	}
	if !found {
		t.Fatal("origin diagnostic lost the exact seed run")
	}
}
