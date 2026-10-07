package serveapp

import (
	"context"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestSelectedForkPendingInputMixedCompletionBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			var selected *selectedStoreOwner
			previous := projectRuntimePersistenceForServe
			projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence { selected = owner; return previous(owner) }
			t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
			root := canonicalrouting.CopySelectedForkPendingInput(t, canonicalrouting.PendingInputMixedCompletion)
			childStarted := make(chan struct{}, 1)
			release := make(chan struct{})
			var once sync.Once
			hook := func(ctx context.Context, nodeID string, event events.Event) error {
				node, err := identity.ParseExecutableNodeKey(nodeID)
				if err != nil {
					return err
				}
				if node.FlowPath() != "child" || event.Type() != "work.first" {
					return nil
				}
				childStarted <- struct{}{}
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root, hook)
			t.Cleanup(func() { once.Do(func() { close(release) }) })
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.seeded", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "seed"})
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
			input := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.first", "run_id": seed.RunID, "payload": map[string]any{"token": "proof"}, "idempotency_key": "mixed-input"})
			select {
			case <-childStarted:
			case <-time.After(10 * time.Second):
				t.Fatal("child did not reach its actual delivery")
			}
			completedNode, err := identity.AdmitExecutableNodeDeclaration("a_finished", "controller")
			if err != nil {
				t.Fatal(err)
			}
			waitServedDeliveryOutcomeCount(t, rt.DB, rt.Backend, input.EventID, "node", completedNode.Key(), "delivered", 1)
			marker := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.marked", "run_id": seed.RunID, "payload": map[string]any{"token": "proof"}, "idempotency_key": "marker"})
			waitServedEventPublishReceiptOutcomeCount(t, rt.DB, rt.Backend, marker.EventID, "platform", "pipeline", "success", 1)
			waitMixedForkSourceCheckpoint(t, selected, rt.BundleHash, seed.RunID, input.EventID, marker.EventID, completedNode.Key())
			family, ok := selected.RunFork()
			if !ok {
				t.Fatal("missing fork owner")
			}
			ctx := servedControlProofAuthorActivityContext(t, rt)
			plan, err := family.Plan(ctx, runfork.RunForkPlanRequest{SourceRunID: seed.RunID, At: marker.EventID})
			if err != nil {
				t.Fatal(err)
			}
			completed, pending := 0, 0
			for _, item := range plan.PendingWork {
				if item.EventID != input.EventID || !item.DeliveryRoute.Recipient.IsNode() {
					continue
				}
				if item.Classification == runfork.RunForkPendingClassificationDeliveredCompleted {
					completed++
				} else {
					pending++
				}
			}
			if completed != 1 || pending != 1 {
				t.Fatalf("mixed fixed disposition completed=%d pending=%d: %+v", completed, pending, plan.PendingWork)
			}
			frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{Plan: plan, Source: semanticview.Wrap(loadWorkflowValidationBundleAt(t, root))})
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, event := range frontier.FrontierEvents {
				if event.SourceEventID != input.EventID {
					continue
				}
				found = true
				if len(event.DerivedRecipients) != 1 || event.DerivedRecipients[0].HandlerNode().FlowPath() != "child" {
					t.Fatalf("completed root was redelivered: %+v", event)
				}
			}
			if !found {
				t.Fatal("pending child disappeared from frontier")
			}
			before := snapshotForkReceiverApplication(t, rt)
			response := requestServedJSONRPC(t, rt.Endpoint, "run.fork", map[string]any{"source_run_id": seed.RunID, "fork_event_id": marker.EventID, "allow_source_freeze": true, "idempotency_key": "mixed-fork"})
			if response.Error == nil {
				t.Fatal("historical non-agent delivery replay was enabled")
			}
			_, refusal := family.Execute(ctx, runforkexecution.SelectedContractExecutionRequest{
				SourceRunID: seed.RunID, At: marker.EventID, AllowSourceFreeze: true, ExpectedBundleHash: rt.BundleHash,
				SourceLoader: runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repoRootForTest(), PlatformSpecPath: filepath.Join(repoRootForTest(), defaultPlatformSpecPath), Store: selected.SourceArtifactStore()},
				AgentRuntime: rt.ForkRuntime,
			})
			if refusal == nil || refusal.Error() != "selected-contract fork execution materialization blocked: non_agent_delivery_replay_unsupported" {
				t.Fatalf("historical replay refusal changed: %v", refusal)
			}
			after := snapshotForkReceiverApplication(t, rt)
			if !reflect.DeepEqual(before["runs"], after["runs"]) {
				t.Logf("changed runs columns=%v", before["runs/columns"])
				for _, row := range before["runs"] {
					t.Logf("runs before: %s", row)
				}
				for _, row := range after["runs"] {
					t.Logf("runs after: %s", row)
				}
			}
			for table, rows := range after {
				if !reflect.DeepEqual(before[table], rows) {
					t.Fatalf("refused historical replay changed table %s", table)
				}
			}
			once.Do(func() { close(release) })
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
			requirePendingInputStateCount(t, rt, seed.RunID, "done", 2)
			requirePendingInputStateCount(t, rt, seed.RunID, "archived", 1)
		})
	}
}

// Publication receipts precede handler handoff and completion-candidate work.
// Keep the child pending, but finish that independent work before the refusal baseline.
func waitMixedForkSourceCheckpoint(t *testing.T, selected *selectedStoreOwner, bundleHash, runID, inputID, markerID, completedNodeID string) {
	t.Helper()
	deps := selected.RuntimeDeps()
	child, err := identity.AdmitExecutableNodeDeclaration("child", "controller")
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(servedProofPollDeadline)
	var input, marker storetest.DeliveryEventEvidence
	var candidates runlifecycle.CandidatePage
	for time.Now().Before(deadline) {
		input = storetest.ObserveDeliveryEventEvidence(t, t.Context(), deps.EventStore, inputID)
		marker = storetest.ObserveDeliveryEventEvidence(t, t.Context(), deps.EventStore, markerID)
		if input.DeadLetters != 0 || len(input.Deliveries) != 2 || marker.DeadLetters != 0 || len(marker.Deliveries) != 1 {
			t.Fatalf("mixed source lost its exact deliveries: input=%+v marker=%+v", input, marker)
		}
		ready := true
		for _, row := range input.Deliveries {
			if row.RunID != runID || row.EventID != inputID || row.SubscriberType != "node" {
				t.Fatalf("mixed source delivery changed owner: %+v", row)
			}
			switch row.SubscriberID {
			case completedNodeID:
				ready = ready && row.Status == "delivered" && row.HandoffPresent
			case child.Key():
				if row.Status != "in_progress" || len(row.Attempts) != 1 || row.Attempts[0].ClosureKind != "open" {
					t.Fatalf("mixed source child did not remain held: %+v", row)
				}
			default:
				t.Fatalf("mixed source gained an unexpected recipient: %+v", row)
			}
		}
		row := marker.Deliveries[0]
		if row.RunID != runID || row.EventID != markerID || row.SubscriberType != "node" {
			t.Fatalf("mixed source marker changed owner: %+v", row)
		}
		if ready && row.Status == "delivered" && row.HandoffPresent {
			candidates, err = deps.RunLifecycleCandidates.ListCompletionCandidates(t.Context(), runlifecycle.CandidateScope{BundleHash: bundleHash}, runlifecycle.CandidateCursor{}, 128)
			if err != nil || !candidates.Exhausted {
				t.Fatalf("mixed source completion census is incomplete: candidates=%+v err=%v", candidates, err)
			}
			armed := false
			for _, candidate := range candidates.Candidates {
				armed = armed || candidate.RunID == runID
			}
			if !armed {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("mixed source did not finish handoff/completion work: input=%+v marker=%+v candidates=%+v", input, marker, candidates)
}
