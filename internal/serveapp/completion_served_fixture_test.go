package serveapp

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type completion2270ServedProof struct {
	Endpoint, Backend, BundleHash string
	Selected                      *selectedStoreOwner
	Probe                         *lifecycletest.Probe
	AuthorContext                 context.Context
}

func configureCompletion2270PostgresFixture(t *testing.T) {
	t.Helper()
	location := testutil.StartEmptyPostgresDSN(t)
	previous := buildStoresForServe
	buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
		return storeselected.OpenRuntime(ctx, storeselected.RuntimeRequest{
			Selection: selection, PostgresDSN: location, SessionLockTTL: runtimeSessionLockTTL(cfg),
		})
	}
	t.Cleanup(func() { buildStoresForServe = previous })
}

func createCompletion2270WaitingRun(t *testing.T, rt completion2270ServedProof) (string, string) {
	t.Helper()
	result := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
		"event_name": "item.received", "bundle_hash": rt.BundleHash,
		"payload":         map[string]any{"item_id": "completion-2270-" + uuid.NewString()},
		"idempotency_key": "issue-1864-" + rt.Backend + "-" + uuid.NewString(),
	})
	if !result.NewRunCreated || result.RunID == "" || result.EventID == "" {
		t.Fatalf("control seed event.publish result = %+v, want new run", result)
	}
	node := identitytest.RootNode(t, "item-handler").Key()
	rt.Probe.RequireNodePending(result.EventID, node)
	deadline := time.Now().Add(servedEventPublishLifecycleProbeWaitTimeout)
	rt.Probe.Expect(result.EventID).NodeInProgress(node).HandlerStarted(node).HandlerCompleted(node).NodeDelivered(node).Within(servedEventPublishLifecycleProbeWaitTimeout)
	remaining := time.Until(deadline)
	if remaining <= 0 {
		t.Fatal("node delivery consumed the complete lifecycle proof deadline")
	}
	rt.Probe.Expect(result.EventID).PostCommitDispatchStarted().PostCommitDispatchCompleted().Within(remaining)
	count, err := storetest.ReadServedDeliveryStatusCount(context.Background(), rt.Selected.RuntimeDeps().EventStore, result.EventID, "node", node)
	if err != nil || count != 1 {
		t.Fatalf("exact seed node delivery count=%d err=%v", count, err)
	}
	deadline = time.Now().Add(15 * time.Second)
	for {
		entities, err := rt.Selected.Entities().ListOperatorEntities(context.Background(), operatorread.OperatorEntityListOptions{RunID: result.RunID, CurrentState: "waiting"})
		if err != nil {
			t.Fatal(err)
		}
		if len(entities.Entities) > 0 && entities.Entities[0].EntityID != "" && entities.Entities[0].RunID == result.RunID {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("exact waiting entity is missing: %+v", entities)
		}
		time.Sleep(50 * time.Millisecond)
	}
	requireServedRunStatus(t, rt.Endpoint, result.RunID, "running")
	return result.RunID, result.EventID
}

func publishCompletion2270BusinessEvent(t *testing.T, rt completion2270ServedProof, runID, sourceEventID string) {
	t.Helper()
	result := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
		"event_name": "item.processed", "run_id": runID, "source_event_id": sourceEventID,
		"payload":         map[string]any{"item_id": "hold"},
		"idempotency_key": "issue-1910-" + rt.Backend + "-" + runID + "-agent-hold-completion-2270",
	})
	if result.RunID != runID || result.OperatorReferenceEventID != sourceEventID || result.NewRunCreated || result.EventID == "" {
		t.Fatalf("business event.publish lost its exact source/run: %+v", result)
	}
	assertServedEventPublishDeliveriesContainStatus(t, result.Deliveries, "agent", "load-agent", "pending", "in_progress", "delivered")
	scope, err := storetest.ReadCommittedPipelineScope(context.Background(), rt.Selected.RuntimeDeps().EventStore, result.EventID)
	if err != nil || scope != "subscribed" {
		t.Fatalf("committed business replay scope=%q err=%v", scope, err)
	}
	deadline := time.Now().Add(servedProofPollDeadline)
	for {
		event, err := rt.Selected.Observability().LoadOperatorEvent(context.Background(), result.EventID)
		if err != nil || event.RunID != runID {
			t.Fatalf("read exact business event: %+v err=%v", event, err)
		}
		matched, settled := 0, 0
		for _, delivery := range event.Deliveries {
			if delivery.SubscriberType != "agent" || delivery.SubscriberID != "load-agent" {
				continue
			}
			if delivery.Terminal && delivery.Status != "delivered" {
				t.Fatalf("business provider turn terminally refused: %+v", delivery)
			}
			if delivery.Status == "delivered" {
				matched++
			}
			attempts, err := storetest.ReadDeliveryAttemptDiagnosticRows(context.Background(), rt.Selected.RuntimeDeps().EventStore, delivery.DeliveryID)
			if err != nil {
				t.Fatal(err)
			}
			for _, attempt := range attempts {
				if int64(attempt.Version) == delivery.ClaimVersion && attempt.Closure == "settled" && attempt.Outcome == "delivered" {
					settled++
				}
			}
		}
		if matched == 1 && settled == 1 {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("exact delivered business obligation/settlement=%d/%d, want1/1", matched, settled)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
