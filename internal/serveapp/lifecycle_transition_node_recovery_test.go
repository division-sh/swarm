package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

type lifecycleNodeRecoveryEntry struct {
	Claim runtimedelivery.Claim
	Event events.Event
}

type lifecycleNodeRecoveryDelivery struct {
	EventID, Route, Status string
	Attempts               []string
}

func readLifecycleNodeRecoveryDelivery(t *testing.T, rt servedControlProofRuntime, id, phase string) lifecycleNodeRecoveryDelivery {
	t.Helper()
	var out lifecycleNodeRecoveryDelivery
	if err := rt.DB.QueryRow(`SELECT event_id, CAST(delivery_target_route AS TEXT), status FROM event_deliveries WHERE delivery_id=$1`, id).Scan(&out.EventID, &out.Route, &out.Status); err != nil {
		t.Fatal(err)
	}
	rows, err := rt.DB.Query(`SELECT claim_version, COALESCE(outcome,''), COALESCE(reason_code,'') FROM event_delivery_attempts WHERE delivery_id=$1 ORDER BY claim_version`, id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var version int64
		var outcome, reason string
		if err := rows.Scan(&version, &outcome, &reason); err != nil {
			t.Fatal(err)
		}
		out.Attempts = append(out.Attempts, fmt.Sprintf("version=%d outcome=%s reason=%s", version, outcome, reason))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("NODE_RECOVERY_RECORD phase=%s delivery=%s %s", phase, id, raw)
	return out
}

func lifecycleRecoveryCarrier(t *testing.T, rt *runtimepkg.Runtime, escape bool) contracts.CompiledTransition {
	t.Helper()
	graph, ok := semanticview.WorkflowStageTopology(rt.Options.WorkflowModule.SemanticSource(), ".")
	if !ok {
		t.Fatal("live source has no root transition graph")
	}
	source, target := "loop.repeat", "drafting"
	if escape {
		source, target = "loop.escape", "escaped"
	}
	var matches []contracts.CompiledTransition
	for _, edge := range graph.Edges {
		if edge.Source != source || edge.HandlerEvent != "loop.repeat" || edge.LoopID != "revision" || edge.From != "review" || edge.To != target {
			continue
		}
		compiled, err := graph.AdmitTransition(edge.Site(), edge.From, edge.To)
		if err != nil {
			t.Fatal(err)
		}
		matches = append(matches, compiled)
	}
	if len(matches) != 1 {
		t.Fatalf("exact pre-execution carrier matches=%d", len(matches))
	}
	return matches[0]
}

func TestServedCompiledLoopNodeRecoveryReexecutionOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, escape := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cap_%t", backend, escape), func(t *testing.T) {
				opts, start := lifecycleRestartHarness(t, backend, canonicalrouting.CopyLifecycleEmitter(t, canonicalrouting.LifecycleLoopRepeatEmits))
				var runtime *runtimepkg.Runtime
				opts.TestRuntimeReadyHook = func(rt *runtimepkg.Runtime) { runtime = rt }
				var armed, recovering atomic.Bool
				var entries atomic.Int64
				observed := make(chan lifecycleNodeRecoveryEntry, 4)
				opts.TestWorkflowNodeHandlerStartHook = func(ctx context.Context, _ string, event events.Event) error {
					if !armed.Load() || event.Type() != "loop.repeat" {
						return nil
					}
					claim, ok := runtimedelivery.ClaimFromContext(ctx)
					if !ok {
						return fmt.Errorf("actual node attempt has no delivery claim")
					}
					entries.Add(1)
					observed <- lifecycleNodeRecoveryEntry{Claim: claim, Event: event}
					if recovering.Load() {
						return nil
					}
					// Interrupt a real claimed attempt before its mutation. Startup
					// must reclaim this same delivery, not republish a new event.
					<-ctx.Done()
					return ctx.Err()
				}
				first, rt := start()
				seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "node-recovery-seed"})
				entityID := requireServedEventPublishEntityState(t, rt.DB, backend, seed.RunID, "", "waiting")
				params := func(event, key string, payload map[string]any) map[string]any {
					return map[string]any{"event_name": event, "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": payload, "idempotency_key": key}
				}
				requireServedEventPublishRPCResult(t, rt.Endpoint, params("loop.start", "start", map[string]any{"seed": true}))
				requireServedEventPublishEntityState(t, rt.DB, backend, seed.RunID, entityID, "drafting")
				attempts := 1
				if escape {
					attempts = 2
				}
				var loop loopruntime.PublicActivation
				for attempt := 1; attempt <= attempts; attempt++ {
					loop = readLifecycleLoop(t, rt, seed.RunID, entityID)
					requireServedEventPublishRPCResult(t, rt.Endpoint, params("loop.admit", fmt.Sprintf("admit-%d", attempt), map[string]any{"revision_id": loop.RevisionID}))
					requireServedEventPublishEntityState(t, rt.DB, backend, seed.RunID, entityID, "review")
					if attempt == attempts {
						break
					}
					requireServedEventPublishRPCResult(t, rt.Endpoint, params("loop.repeat", "ordinary-before-cap", map[string]any{"revision_id": loop.RevisionID}))
					requireServedEventPublishEntityState(t, rt.DB, backend, seed.RunID, entityID, "drafting")
					waitServedRunDeliveryQuiescence(t, rt.DB, backend, seed.RunID)
				}
				waitServedRunDeliveryQuiescence(t, rt.DB, backend, seed.RunID)
				before := readLifecycleTransitionHistory(t, rt, seed.RunID, entityID)
				expected := lifecycleRecoveryCarrier(t, runtime, escape)
				armed.Store(true)
				published := requireServedEventPublishRPCResult(t, rt.Endpoint, params("loop.repeat", "interrupted-repeat", map[string]any{"revision_id": loop.RevisionID}))
				var old lifecycleNodeRecoveryEntry
				select {
				case old = <-observed:
				case <-time.After(15 * time.Second):
					t.Fatal("first claimed node attempt not reached")
				}
				if old.Event.ID() != published.EventID {
					t.Fatal("barrier captured another admitted event")
				}
				pending := readLifecycleNodeRecoveryDelivery(t, rt, old.Claim.DeliveryID(), "before_shutdown")
				if pending.Status != "in_progress" || pending.EventID != published.EventID || !reflect.DeepEqual(before, readLifecycleTransitionHistory(t, rt, seed.RunID, entityID)) {
					t.Fatal("pending first attempt changed state/history or identity")
				}
				if code := first.stop(); code != 0 {
					t.Fatalf("interrupted stop=%d", code)
				}
				recovering.Store(true)
				setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
				second, rt := start()
				var retry lifecycleNodeRecoveryEntry
				select {
				case retry = <-observed:
				case <-time.After(15 * time.Second):
					t.Fatal("recovery never re-entered the same node handler")
				}
				if retry.Claim.DeliveryID() != old.Claim.DeliveryID() || retry.Claim.Version() <= old.Claim.Version() || retry.Event.ID() != old.Event.ID() || retry.Event.Type() != old.Event.Type() || !bytes.Equal(retry.Event.Payload(), old.Event.Payload()) || retry.Claim.RouteIdentity() != old.Claim.RouteIdentity() {
					t.Fatal("recovery reexecuted a different event/route or failed to advance the lease fence")
				}
				if !reflect.DeepEqual(expected, lifecycleRecoveryCarrier(t, runtime, escape)) {
					t.Fatal("restart changed compiled carrier before recovered mutation")
				}
				target := "drafting"
				if escape {
					target = "escaped"
				}
				requireServedEventPublishEntityState(t, rt.DB, backend, seed.RunID, entityID, target)
				waitServedRunDeliveryQuiescence(t, rt.DB, backend, seed.RunID)
				after := readLifecycleTransitionHistory(t, rt, seed.RunID, entityID)
				if len(after) != len(before)+1 || !reflect.DeepEqual(after[:len(before)], before) {
					t.Fatal("reexecuted node lost or duplicated prior history")
				}
				last := after[len(before)]
				compiled, ok := last.Evidence.Compiled()
				if !ok || !reflect.DeepEqual(compiled, expected) || last.TriggerEventID != published.EventID {
					t.Fatalf("reexecuted node did not preserve exact compiled identity: %#v", last)
				}
				settled := readLifecycleNodeRecoveryDelivery(t, rt, old.Claim.DeliveryID(), "after_recovery")
				if settled.Status != "delivered" || settled.Route != pending.Route || len(settled.Attempts) != 2 || entries.Load() != 2 {
					t.Fatalf("real node recovery attempts=%#v entries=%d", settled, entries.Load())
				}
				requireLifecycleEventCount(t, rt, seed.RunID, "ordinary.repeated", 1)
				if escape {
					requireLifecycleEventCount(t, rt, seed.RunID, "loop.escaped", 1)
					requireLifecycleFlowEntity(t, rt, seed.RunID, "sink/", "done")
				} else {
					snapshot := lifecycleStoredSnapshot(t, rt, seed.RunID)
					refused := requireServedJSONRPCError(t, rt.Endpoint, "event.replay", map[string]any{"event_id": published.EventID, "idempotency_key": "node-only-refusal"})
					if refused.Data["code"] != "EVENT_REPLAY_NO_DELIVERY_HISTORY" || lifecycleStoredSnapshot(t, rt, seed.RunID) != snapshot {
						t.Fatal("node-only public refusal contract changed")
					}
					current := readLifecycleLoop(t, rt, seed.RunID, entityID)
					requireServedEventPublishRPCResult(t, rt.Endpoint, params("loop.admit", "final-admit", map[string]any{"revision_id": current.RevisionID}))
					requireServedEventPublishEntityState(t, rt.DB, backend, seed.RunID, entityID, "review")
					requireServedEventPublishRPCResult(t, rt.Endpoint, params("loop.close", "close", map[string]any{"revision_id": current.RevisionID}))
					requireServedEventPublishEntityState(t, rt.DB, backend, seed.RunID, entityID, "done")
				}
				waitServedRunDeliveryQuiescence(t, rt.DB, backend, seed.RunID)
				if closed := readLifecycleLoop(t, rt, seed.RunID, entityID); closed.Status != loopruntime.StatusClosed {
					t.Fatal("loop not closed")
				}
				closedState := lifecycleStoredSnapshot(t, rt, seed.RunID)
				if code := second.stop(); code != 0 {
					t.Fatalf("second stop=%d", code)
				}
				third, rt := start()
				final := readLifecycleNodeRecoveryDelivery(t, rt, old.Claim.DeliveryID(), "after_closed_recovery")
				if !reflect.DeepEqual(final, settled) || entries.Load() != 2 || lifecycleStoredSnapshot(t, rt, seed.RunID) != closedState {
					t.Fatal("terminal recovery reexecuted settled work or changed compiled history")
				}
				t.Logf("NODE_REEXECUTION event=%s delivery=%s old_claim=%d new_claim=%d compiled=%#v handler_entries=%d", published.EventID, old.Claim.DeliveryID(), old.Claim.Version(), retry.Claim.Version(), compiled, entries.Load())
				if code := third.stop(); code != 0 {
					t.Fatalf("third stop=%d", code)
				}
			})
		}
	}
}
