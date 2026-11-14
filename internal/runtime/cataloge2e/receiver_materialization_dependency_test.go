package cataloge2e

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
)

type heldReceiverReadiness struct {
	pipeline.WorkflowPersistenceOwner
	entered chan pipeline.DynamicFlowRuntimeActivationAttempt
	release chan struct{}
	once    sync.Once
}

func (g *heldReceiverReadiness) AdvanceFlowAttachment(ctx context.Context, attempt pipeline.DynamicFlowRuntimeActivationAttempt, previous pipeline.FlowAttachmentPhase, at time.Time) (pipeline.FlowAttachmentAdvanceResult, error) {
	if strings.HasPrefix(attempt.InstancePath(), "consumer/") && previous == pipeline.FlowAttachmentTimersArmed {
		g.once.Do(func() { g.entered <- attempt })
		select {
		case <-g.release:
		case <-ctx.Done():
			return pipeline.FlowAttachmentAdvanceResult{}, ctx.Err()
		}
	}
	return g.WorkflowPersistenceOwner.AdvanceFlowAttachment(ctx, attempt, previous, at)
}

func proveHeldReceiverReadiness(t *testing.T, backend catalogRuntimeBackend) {
	t.Helper()
	root := canonicalrouting.CopyReceiverMaterializationWithAgent(t, "collector")
	for label, edit := range map[string]func(string) string{
		"consumer/schema.yaml": func(s string) string { return "instance: token\n" + s },
		"consumer/state.yaml":  func(s string) string { return s + "\ntoken: text\n" },
		"schema.yaml": func(s string) string {
			for _, from := range []string{"work.ready, from: producer", "receiver.seeded, from: .", "receiver.closed, from: ."} {
				old := from + ", to: consumer}"
				mode := "select"
				if strings.HasPrefix(from, "receiver.seeded") {
					mode = "select-or-create"
				}
				if strings.Count(s, old) != 1 {
					t.Fatalf("held-readiness edge missing: %s", old)
				}
				s = strings.Replace(s, old, from+", to: consumer, resolution: "+mode+"}", 1)
			}
			return s
		},
		"events.yaml": func(s string) string {
			return strings.Replace(s, "receiver.closed:\n", "receiver.closed:\n  token: text\n", 1)
		},
	} {
		path := filepath.Join(root, label)
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(edit(string(body))), 0600); err != nil {
			t.Fatal(err)
		}
	}
	gate := &heldReceiverReadiness{entered: make(chan pipeline.DynamicFlowRuntimeActivationAttempt, 1), release: make(chan struct{})}
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(gate.release) }) })
	h := newRuntimeHarnessWithTerminalProvider(t, root, backend, true, nil, nil, nil, runlifecycle.LiveRunScope{}, func(deps *runtimepkg.RuntimeDeps) {
		gate.WorkflowPersistenceOwner = deps.EventStore.(pipeline.WorkflowPersistenceOwner)
		deps.WorkflowPersistence = pipeline.NewWorkflowPersistence(gate)
	})
	t.Cleanup(func() { release.Do(func() { close(gate.release) }) })
	ctx := catalogRunContext(h, catalogRuntimeRunID)
	done := make(chan error, 1)
	go func() {
		done <- h.publishRuntimeEventResultForStep(catalogTriggerStep{Event: "start.seeded", Payload: map[string]any{"token": "held"}}, catalogRuntimePublishTimeout, true)
		close(done)
	}()
	t.Cleanup(func() {
		release.Do(func() { close(gate.release) })
		select {
		case <-done:
		case <-time.After(catalogRuntimePublishTimeout):
			t.Error("held publication did not join")
		}
	})
	var attempt pipeline.DynamicFlowRuntimeActivationAttempt
	select {
	case attempt = <-gate.entered:
	case err := <-done:
		t.Fatalf("publication returned before receiver readiness cut: %v", err)
	case <-time.After(catalogRuntimePublishTimeout):
		t.Fatal("receiver did not reach native pre-ready cut")
	}
	readiness, found, err := gate.LoadDynamicFlowRuntimeReadiness(ctx, catalogRuntimeRunID, flowidentity.RouteForInstancePath(attempt.InstancePath()))
	if err != nil || !found || readiness.Phase != pipeline.FlowAttachmentTimersArmed || readiness.Plan.Identity.InstancePath != attempt.InstancePath() {
		t.Fatalf("held exact readiness: %+v found=%t err=%v", readiness, found, err)
	}
	eventID, err := h.readRunNamedEventIdentity(ctx, catalogRuntimeRunID, "receiver.seeded")
	if err != nil {
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
		t.Fatalf("held durable receiver publication: %+v %t %v", publication, found, err)
	}
	for _, route := range publication.DeliveryRoutes {
		id, err := deliverylifecycle.DeliveryID(eventID, route)
		if err != nil {
			t.Fatal(err)
		}
		pending, err := store.Snapshot(ctx, id)
		if err != nil || pending.Status != deliverylifecycle.StatusPending || pending.ClaimVersion != 0 || !pending.StartedAt.IsZero() {
			t.Fatalf("receiver ran before readiness: %+v %v", pending, err)
		}
	}
	h.llm.mu.Lock()
	calls := len(h.llm.deliveryCalls)
	h.llm.mu.Unlock()
	if calls != 0 {
		t.Fatalf("provider executed before readiness: %d", calls)
	}
	release.Do(func() { close(gate.release) })
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(catalogRuntimePublishTimeout):
		t.Fatal("released receiver publication did not finish")
	}
	for _, route := range publication.DeliveryRoutes {
		id, err := deliverylifecycle.DeliveryID(eventID, route)
		if err != nil {
			t.Fatal(err)
		}
		settled, err := store.Snapshot(ctx, id)
		if err != nil || settled.Status != deliverylifecycle.StatusDelivered || settled.ClaimVersion != 1 {
			t.Fatalf("ready receiver did not settle exactly once: %+v %v", settled, err)
		}
	}
}

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
				eventID, err := h.readRunNamedEventIdentity(ctx, catalogRuntimeRunID, "receiver.seeded")
				if err != nil {
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
				if agent == "collector" {
					t.Run("held_receiver_readiness", func(t *testing.T) { proveHeldReceiverReadiness(t, backend) })
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
