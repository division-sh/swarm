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
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
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
			marker := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.marked", "run_id": seed.RunID, "payload": map[string]any{"token": "proof"}, "idempotency_key": "marker"})
			waitServedEventPublishReceiptOutcomeCount(t, rt.DB, rt.Backend, marker.EventID, "platform", "pipeline", "success", 1)
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
			for table, rows := range snapshotForkReceiverApplication(t, rt) {
				if !reflect.DeepEqual(before[table], rows) {
					t.Fatalf("refused historical replay changed table %s", table)
				}
			}
			once.Do(func() { close(release) })
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
			requirePendingInputStateCount(t, rt, seed.RunID, "done", 1)
			requirePendingInputStateCount(t, rt, seed.RunID, "archived", 1)
		})
	}
}
