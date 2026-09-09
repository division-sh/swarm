package cataloge2e

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

type retireAfterReceiverMaterialization struct {
	once    sync.Once
	retired chan struct{}
	retire  func()
}

func (p *retireAfterReceiverMaterialization) NotifyLifecycle(_ context.Context, signal lifecycleprobe.Signal) {
	if signal.Kind != lifecycleprobe.HandlerCompleted || signal.EventType != "receiver.seeded" || signal.Status != "completed" {
		return
	}
	p.once.Do(func() {
		p.retire()
		close(p.retired)
	})
}

func TestReceiverMaterializationPendingAgentRestartBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			h := newRuntimeHarnessForBackend(t, canonicalrouting.CopyReceiverMaterializationWithAgent(t, "renamed-observer"), backend, true)
			probe := &retireAfterReceiverMaterialization{retired: make(chan struct{}), retire: h.rt.WorkOccurrence().Retire}
			h.rt.Pipeline.SetTestLifecycleProbe(probe)
			publishErr := h.publishRuntimeEventResultForStep(catalogTriggerStep{Event: "start.seeded", Payload: map[string]any{"token": "restart"}}, 10*time.Second, true)
			select {
			case <-probe.retired:
			default:
				t.Fatalf("real materializer checkpoint not reached: %v", publishErr)
			}
			ctx := catalogRunContext(h, catalogRuntimeRunID)
			var store interface {
				bus.PreparedPublishEventReader
				deliverylifecycle.Store
			} = h.pg
			if h.sqlite != nil {
				store = h.sqlite
			}
			var eventID string
			if err := h.db.QueryRowContext(ctx, `SELECT event_id FROM events WHERE run_id=$1 AND event_name='receiver.seeded'`, catalogRuntimeRunID).Scan(&eventID); err != nil {
				t.Fatal(err)
			}
			publication, found, err := store.LoadPreparedPublishEvent(ctx, eventID)
			if err != nil || !found || len(publication.DeliveryRoutes) != 2 {
				t.Fatalf("paused receiver publication: %+v %v", publication, err)
			}
			var node, agent deliverylifecycle.Snapshot
			for _, route := range publication.DeliveryRoutes {
				id, err := deliverylifecycle.DeliveryID(eventID, route)
				if err != nil {
					t.Fatal(err)
				}
				snapshot, err := store.Snapshot(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				if route.Recipient.IsNode() {
					node = snapshot
				} else {
					agent = snapshot
				}
			}
			if node.Status != deliverylifecycle.StatusDelivered || agent.Status != deliverylifecycle.StatusPending || agent.ClaimVersion != 0 || !agent.StartedAt.IsZero() || !node.Authority.Equal(agent.Authority) {
				t.Fatalf("required restart window missing: node=%+v agent=%+v publish=%v", node, agent, publishErr)
			}
			h.llm.mu.Lock()
			beforeCalls := append([]scriptedDeliveryCall(nil), h.llm.deliveryCalls...)
			h.llm.mu.Unlock()
			if len(beforeCalls) != 0 {
				t.Fatalf("agent reached provider before the restart checkpoint: %+v", beforeCalls)
			}
			bundleHash, err := contracts.BundleHash(h.bundle)
			if err != nil {
				t.Fatal(err)
			}
			specDigest, err := catalogReplayPlatformSpecDigest(repoRootFromCatalogE2E(t))
			if err != nil {
				t.Fatal(err)
			}
			rootPublication, found, err := store.LoadPreparedPublishEvent(ctx, publication.Event.Event().ParentEventID())
			if err != nil || !found {
				t.Fatalf("original root input missing: %v", err)
			}
			rootEvent := rootPublication.Event.Event()
			transcript := &catalogExecutionTranscript{version: catalogReplayTranscriptVersion, platformSpecDigest: specDigest, bundleHash: bundleHash, runID: catalogRuntimeRunID,
				groups: []catalogTranscriptGroup{{steps: []catalogTriggerStep{{Event: string(rootEvent.Type()), Payload: map[string]any{"token": "restart"}, inputKind: catalogReplayInputRootIngress, eventID: rootEvent.ID(), createdAt: rootEvent.CreatedAt(), sourceAgent: rootEvent.SourceAgent()}}}}}
			reopened := h.reopenFromTranscript(transcript)
			recoveredCtx := catalogRunContext(reopened, catalogRuntimeRunID)
			var recovered deliverylifecycle.Store = reopened.pg
			if reopened.sqlite != nil {
				recovered = reopened.sqlite
			}
			if err := reopened.rt.Bus.PublishAndWait(recoveredCtx, publication.Event.Event()); err != nil {
				logSelectedForkRecoveryFailure(t, recoveredCtx, reopened, catalogRuntimeRunID, err)
				t.Fatalf("recover pending agent after committed materializer: %v", err)
			}
			afterNode, err := recovered.Snapshot(recoveredCtx, node.DeliveryID)
			if err != nil || !reflect.DeepEqual(node, afterNode) {
				t.Fatalf("restart rewrote historical materializer: %+v %v", afterNode, err)
			}
			afterAgent, err := recovered.Snapshot(recoveredCtx, agent.DeliveryID)
			if err != nil || afterAgent.Status != deliverylifecycle.StatusDelivered || afterAgent.ClaimVersion != 1 || afterAgent.Authority.Equal(node.Authority) || !afterAgent.Route.Materialization.Equal(agent.Route.Materialization) {
				t.Fatalf("successor did not consume exact original dependency once: %+v %v", afterAgent, err)
			}
			reopened.llm.mu.Lock()
			calls := append([]scriptedDeliveryCall(nil), reopened.llm.deliveryCalls...)
			reopened.llm.mu.Unlock()
			if len(calls) != 1 || calls[0].EventID != eventID || calls[0].TargetEntityID != node.Route.Target.Route().EntityID {
				t.Fatalf("restarted provider execution lost exact target: %+v", calls)
			}
		})
	}
}
