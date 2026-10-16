package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"sort"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestSelectedForkReceiverCapabilityCompletionSnapshotBoundaryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			publication := newExactOriginPublicationProbe(t)
			completion := installReceiverCompletionBoundary(t)
			root := canonicalrouting.CopyTemplateCreateResolution(t, canonicalrouting.TemplateCreateResolutionOptions{Mint: canonicalrouting.CreateMintUUID})
			_, rt := startServedBoundaryFixture(t, backend, root)
			ctx, cancel := context.WithTimeout(t.Context(), servedEventPublishLifecycleProbeWaitTimeout)
			defer cancel()
			if _, err := completion.wait(ctx, rt.BundleHash); err != nil {
				t.Fatal(err)
			}
			release := make(chan struct{})
			releaseCompletion := sync.OnceFunc(func() { close(release) })
			defer releaseCompletion()
			completion.mu.Lock()
			completion.holdRun, completion.entered, completion.release = "next", make(chan runlifecycle.Candidate, 1), release
			completion.mu.Unlock()
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "validation.triggered", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{"candidate": "fork-capability"}, "idempotency_key": "capability-source"})
			waitServedBoundaryPublicationCompletion(t, rt, seed.RunID)
			requireWorkspaceProofReceiverPublicReadback(t, rt.Endpoint, rt.Selected.RuntimeDeps().EventStore, seed.RunID)
			if err := waitExactOriginPublications(ctx, publication, seed.EventID); err != nil {
				t.Fatal(err)
			}
			select {
			case candidate := <-completion.entered:
				if candidate.RunID != seed.RunID {
					t.Fatalf("completion hold borrowed another run: candidate=%+v seed=%+v", candidate, seed)
				}
			case <-ctx.Done():
				t.Fatal("source completion did not reach its native execution boundary")
			}
			canceled, cancelWait := context.WithCancel(ctx)
			cancelWait()
			if _, err := completion.wait(canceled, rt.BundleHash); !errors.Is(err, context.Canceled) {
				t.Fatalf("completion boundary accepted an in-flight source candidate: %v", err)
			}
			before := requireReceiverApplicationSnapshot(t, rt.ReceiverStateReader)
			var revision int64
			var columns []string
			if err := json.Unmarshal([]byte(before["runs/columns"][0]), &columns); err != nil {
				t.Fatal(err)
			}
			runColumn, dueColumn, revisionColumn := slices.Index(columns, "run_id"), slices.Index(columns, "completion_due_at"), slices.Index(columns, "completion_revision")
			if runColumn < 0 || dueColumn < 0 || revisionColumn < 0 {
				t.Fatal("source snapshot lost exact completion coordinates")
			}
			expected := make(map[string][]string, len(before))
			for table, rows := range before {
				expected[table] = rows
			}
			expected["runs"] = append([]string(nil), before["runs"]...)
			changed := 0
			for index, row := range before["runs"] {
				var values []storetest.SelectedForkSnapshotCell
				if err := json.Unmarshal([]byte(row), &values); err != nil || len(values) != len(columns) {
					t.Fatalf("invalid controlled run snapshot: %v", err)
				}
				runID, err := values[runColumn].Text()
				if err != nil {
					t.Fatal(err)
				}
				if runID != seed.RunID {
					continue
				}
				if string(values[dueColumn].Value) == "null" {
					t.Fatal("old source boundary did not retain its unexecuted completion candidate")
				}
				if err := json.Unmarshal(values[revisionColumn].Value, &revision); err != nil {
					t.Fatal(err)
				}
				values[dueColumn] = storetest.SelectedForkSnapshotCell{Column: "completion_due_at", Type: "<nil>", Value: json.RawMessage("null")}
				wire, err := json.Marshal(values)
				if err != nil {
					t.Fatal(err)
				}
				expected["runs"][index] = string(wire)
				changed++
			}
			sort.Strings(expected["runs"])
			if changed != 1 {
				t.Fatalf("controlled snapshot has %d source run rows, want one", changed)
			}
			releaseCompletion()
			receipts := requireReceiverCompletionBoundary(t, ctx, completion, rt.BundleHash, seed.RunID)
			matched := false
			for _, receipt := range receipts {
				matched = matched || receipt.candidate.RunID == seed.RunID && receipt.candidate.Revision == revision && receipt.result.Outcome == runlifecycle.OutcomeAwaitMutation
			}
			if !matched {
				t.Fatalf("source due-clearing lacks its exact committed run/revision acknowledgment: run=%s revision=%d", seed.RunID, revision)
			}
			after := requireReceiverApplicationSnapshot(t, rt.ReceiverStateReader)
			if !reflect.DeepEqual(expected, after) {
				t.Fatalf("controlled completion changed more than its exact source due: matched=%d\n%s", changed, forkReceiverCapabilityRefusalSnapshotDelta(expected, after))
			}
			t.Logf("source completion committed after old delivery/publication boundary: run=%s revision=%d outcome=await_mutation; only completion_due_at cleared", seed.RunID, revision)
			if reflect.DeepEqual(before, after) {
				t.Fatal("old capability-refusal source boundary incorrectly claimed completion conservation")
			}
		})
	}
}
