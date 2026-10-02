package cataloge2e

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestReceiverConstructionBeforeNodeAndAgentExecutionBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		for _, agent := range []string{"collector", "renamed-observer"} {
			t.Run(string(backend)+"/"+agent, func(t *testing.T) {
				root := canonicalrouting.CopyReceiverMaterializationWithAgent(t, agent)
				h := newRuntimeHarnessForBackend(t, root, backend, true)
				if err := h.publishRuntimeEventResultForStep(catalogTriggerStep{Event: "start.seeded", Payload: map[string]any{"token": "first"}}, 20*time.Second, true); err != nil {
					if owner, ok := h.rt.Bus.DeliveryContinuationOwner().(interface{ Synchronize(context.Context) error }); ok {
						t.Logf("durable continuation failure: %v", owner.Synchronize(context.WithoutCancel(h.ctx)))
					}
					logSelectedForkRecoveryFailure(t, context.WithoutCancel(h.ctx), h, catalogRuntimeRunID, err)
					t.Fatalf("publish ordinary seed: %v", err)
				}
				ctx := catalogRunContext(h, catalogRuntimeRunID)
				var eventID string
				if err := h.db.QueryRowContext(ctx, `SELECT event_id FROM events WHERE run_id=$1 AND event_name='receiver.seeded'`, catalogRuntimeRunID).Scan(&eventID); err != nil {
					t.Fatal(err)
				}
				var store interface {
					LoadPreparedPublishEvent(context.Context, string) (bus.PreparedPublishEvent, bool, error)
					deliverylifecycle.Store
				} = h.pg
				if h.sqlite != nil {
					store = h.sqlite
				}
				publication, found, err := store.LoadPreparedPublishEvent(ctx, eventID)
				if err != nil || !found || len(publication.DeliveryRoutes) != 2 {
					t.Fatalf("real receiver publication: %+v %v", publication, err)
				}
				var materializer, observer deliverylifecycle.Snapshot
				for _, route := range publication.DeliveryRoutes {
					id, err := deliverylifecycle.DeliveryID(eventID, route)
					if err != nil {
						t.Fatal(err)
					}
					deadline := time.Now().Add(15 * time.Second)
					var snapshot deliverylifecycle.Snapshot
					for {
						snapshot, err = store.Snapshot(ctx, id)
						if err != nil || snapshot.Status == deliverylifecycle.StatusDelivered || snapshot.Status == deliverylifecycle.StatusDeadLetter || time.Now().After(deadline) {
							break
						}
						time.Sleep(10 * time.Millisecond)
					}
					if err != nil || snapshot.Status != deliverylifecycle.StatusDelivered {
						if owner, ok := h.rt.Bus.DeliveryContinuationOwner().(interface{ Synchronize(context.Context) error }); ok {
							t.Logf("continuation: %v", owner.Synchronize(context.WithoutCancel(h.ctx)))
						}
						logSelectedForkRecoveryFailure(t, context.WithoutCancel(h.ctx), h, catalogRuntimeRunID, err)
						t.Fatalf("final receiver settlement: %+v %v", snapshot, err)
					}
					if route.Recipient.IsNode() {
						materializer = snapshot
					} else {
						observer = snapshot
					}
				}
				nodeTarget := materializer.Route.Target.Route()
				requireDeclaredAgentReceiverOwnership(t, catalogRuntimeRunID, materializer, observer)
				requireReceiverConstructedBeforeDelivery(t, h, catalogRuntimeRunID, materializer)
				requireReceiverConstructedBeforeDelivery(t, h, catalogRuntimeRunID, observer)
				var entity, agentEntity, phase string
				if err := h.db.QueryRowContext(ctx, `SELECT header.entity_id,COALESCE(CAST(agent.entity_id AS TEXT),''),agent.lifecycle_phase FROM agents agent JOIN flow_instances header ON header.run_id=agent.run_id AND header.instance_path=agent.flow_instance WHERE agent.run_id=$1 AND agent.agent_id=$2`, catalogRuntimeRunID, observer.SubscriberID).Scan(&entity, &agentEntity, &phase); err != nil {
					t.Fatal(err)
				}
				if entity != nodeTarget.EntityID || agentEntity != "" || phase != "running" {
					t.Fatalf("agent/header readiness ownership=%s/%s/%s", entity, agentEntity, phase)
				}
				if materializer.ClaimVersion != 1 || observer.ClaimVersion != 1 {
					t.Fatalf("exactly-once provider execution missing: node=%+v agent=%+v", materializer, observer)
				}
				var turns int
				h.llm.mu.Lock()
				calls := append([]scriptedDeliveryCall(nil), h.llm.deliveryCalls...)
				h.llm.mu.Unlock()
				for _, call := range calls {
					if call.RunID == catalogRuntimeRunID && call.EventID == eventID && call.AgentID == observer.SubscriberID && call.TargetEntityID == "" {
						turns++
					}
				}
				if turns != 1 {
					t.Fatalf("exact receiver provider turn count=%d calls=%+v", turns, calls)
				}
				if err := h.rt.Bus.PublishAndWait(ctx, publication.Event.Event()); err != nil {
					t.Fatalf("exact duplicate publication: %v", err)
				}
				for _, before := range []deliverylifecycle.Snapshot{materializer, observer} {
					after, err := store.Snapshot(ctx, before.DeliveryID)
					if err != nil || !reflect.DeepEqual(before, after) {
						t.Fatalf("duplicate changed delivery: %+v %v", after, err)
					}
				}
				h.llm.mu.Lock()
				duplicateCalls := append([]scriptedDeliveryCall(nil), h.llm.deliveryCalls...)
				h.llm.mu.Unlock()
				if !reflect.DeepEqual(calls, duplicateCalls) {
					t.Fatalf("duplicate publication invoked the provider: before=%+v after=%+v", calls, duplicateCalls)
				}
				public, err := catalogRunScopedOperatorEvents(h, catalogRuntimeRunID)
				if err != nil {
					t.Fatal(err)
				}
				if len(public[eventID].Deliveries) != 2 {
					t.Fatal("public readback omitted receiver obligations")
				}
				for _, delivery := range public[eventID].Deliveries {
					if delivery.Status != "delivered" || (delivery.Route.Recipient.IsAgent() && (!delivery.Route.Initialization.Equal(observer.Route.Initialization) || !events.SameDeliveryTargetOwnership(delivery.Route.Target, observer.Route.Target))) {
						t.Fatalf("public construction readback changed evidence: %+v", delivery)
					}
				}
				bundleHash, err := contracts.BundleHash(h.bundle)
				if err != nil {
					t.Fatal(err)
				}
				specDigest, err := catalogReplayPlatformSpecDigest(repoRootFromCatalogE2E(t))
				if err != nil {
					t.Fatal(err)
				}
				transcript := &catalogExecutionTranscript{version: catalogReplayTranscriptVersion, platformSpecDigest: specDigest, bundleHash: bundleHash, runID: catalogRuntimeRunID}
				rootPublication, found, err := store.LoadPreparedPublishEvent(ctx, publication.Event.Event().ParentEventID())
				if err != nil || !found {
					t.Fatalf("canonical seed root missing: %v", err)
				}
				rootEvent := rootPublication.Event.Event()
				transcript.groups = []catalogTranscriptGroup{{steps: []catalogTriggerStep{{Event: string(rootEvent.Type()), Payload: map[string]any{"token": "first"}, inputKind: catalogReplayInputRootIngress, eventID: rootEvent.ID(), createdAt: rootEvent.CreatedAt(), sourceAgent: rootEvent.SourceAgent()}}}}
				reopened := h.reopenFromTranscript(transcript)
				var recovered deliverylifecycle.Store = reopened.pg
				if reopened.sqlite != nil {
					recovered = reopened.sqlite
				}
				for _, before := range []deliverylifecycle.Snapshot{materializer, observer} {
					after, err := recovered.Snapshot(catalogRunContext(reopened, catalogRuntimeRunID), before.DeliveryID)
					if err != nil || !reflect.DeepEqual(before, after) {
						t.Fatalf("restart changed terminal dependency: %+v %v", after, err)
					}
				}
				reopenedContext := catalogRunContext(reopened, catalogRuntimeRunID)
				if err := reopened.rt.Bus.PublishAndWait(reopenedContext, publication.Event.Event()); err != nil {
					t.Fatalf("duplicate publication after restart: %v", err)
				}
				for _, before := range []deliverylifecycle.Snapshot{materializer, observer} {
					after, err := recovered.Snapshot(reopenedContext, before.DeliveryID)
					if err != nil || !reflect.DeepEqual(before, after) {
						t.Fatalf("restarted duplicate changed terminal delivery: %+v %v", after, err)
					}
				}
				reopened.llm.mu.Lock()
				recoveredCalls := append([]scriptedDeliveryCall(nil), reopened.llm.deliveryCalls...)
				reopened.llm.mu.Unlock()
				if len(recoveredCalls) != 0 {
					t.Fatalf("terminal dependency replay invoked a provider after restart: %+v", recoveredCalls)
				}
			})
		}
	}
}
