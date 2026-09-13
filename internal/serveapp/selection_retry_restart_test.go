package serveapp

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

type servedSelectionReceiverFault struct {
	pipeline.WorkflowPersistenceOwner
	enabled *atomic.Bool
	hits    *atomic.Int64
}

func (f servedSelectionReceiverFault) LoadWorkflowTargetPersistence(ctx context.Context, owner flowidentity.RunScopedFlowInstance, entity identity.EntityID) (pipeline.WorkflowTargetPersistenceRecord, error) {
	if _, claimed := deliverylifecycle.ClaimFromContext(ctx); claimed && f.enabled.Load() {
		f.hits.Add(1)
		return pipeline.WorkflowTargetPersistenceRecord{}, failures.Wrap(failures.ClassDependencyUnavailable, "selection_receiver_read_fault", "test", "load_receiver", nil, errors.New("controlled receiver read failure"))
	}
	return f.WorkflowPersistenceOwner.LoadWorkflowTargetPersistence(ctx, owner, entity)
}

func TestServedSelectionRetryAndRestartBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		for _, faulted := range []bool{false, true} {
			name := "no_fault"
			if faulted {
				name = "receiver_retry"
			}
			t.Run(string(backend)+"/"+name, func(t *testing.T) {
				var enabled, resumeOnConstruction atomic.Bool
				var hits atomic.Int64
				previous := projectRuntimePersistenceForServe
				projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
					p := previous(owner)
					if resumeOnConstruction.Load() {
						enabled.Store(false)
					}
					p.deps.WorkflowPersistence = pipeline.NewWorkflowPersistence(servedSelectionReceiverFault{
						WorkflowPersistenceOwner: p.deps.EventStore.(pipeline.WorkflowPersistenceOwner), enabled: &enabled, hits: &hits,
					})
					return p
				}
				t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
				rt, _, restart := newRetainedMailboxCompletionRuntime(t, backend, canonicalrouting.CopySelectionRetry(t))
				seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "seed", "bundle_hash": rt.BundleHash, "payload": map[string]any{}, "idempotency_key": "selection-seed"})
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
				enabled.Store(faulted)
				params := map[string]any{"event_name": "select", "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{}, "idempotency_key": "selection-request"}
				accepted := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
				var immutable operatorread.OperatorEventFull
				requireServedJSONRPCResult(t, rt.Endpoint, "event.get", map[string]any{"event_id": accepted.EventID}, &immutable)
				var id string
				var failedClaim int64
				if faulted {
					deadline := time.Now().Add(10 * time.Second)
					for {
						var status string
						err := rt.DB.QueryRow("SELECT delivery_id,status,claim_version FROM event_deliveries WHERE event_id=$1", accepted.EventID).Scan(&id, &status, &failedClaim)
						if err == nil && status == "failed" {
							break
						}
						if time.Now().After(deadline) {
							t.Fatalf("receiver retry never committed: status=%s err=%v\n%s", status, err, servedEventPublishDebugSummary(t, rt.DB, rt.Backend, seed.RunID))
						}
						time.Sleep(10 * time.Millisecond)
					}
					if hits.Load() == 0 {
						t.Fatal("receiver fault did not execute after a real claim")
					}
					var count int
					if err := rt.DB.QueryRow("SELECT COUNT(*) FROM event_delivery_handler_rule_selections WHERE delivery_id=$1", id).Scan(&count); err != nil || count != 0 {
						t.Fatalf("prehandler retry froze selection: %d %v", count, err)
					}
					if err := rt.DB.QueryRow("SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name IN ('selected','ack')", seed.RunID).Scan(&count); err != nil || count != 0 {
						t.Fatalf("prehandler retry published business effects: %d %v", count, err)
					}
					assertServedSelectionTrace(t, rt, accepted.EventID, seed.RunID, false)
				} else {
					waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
					assertServedSelectionTrace(t, rt, accepted.EventID, seed.RunID, true)
				}
				// Clear only the injected dependency fault while constructing the next
				// runtime, after the original lifecycle has joined. Retry timing, source,
				// persisted obligation and normal startup recovery remain untouched.
				resumeOnConstruction.Store(true)
				rt, _ = restart()
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
				assertServedSelectionTrace(t, rt, accepted.EventID, seed.RunID, true)
				if faulted {
					var claim int64
					var live int
					if err := rt.DB.QueryRow("SELECT claim_version,CASE WHEN current_attempt_version IS NULL AND next_eligible_at IS NULL THEN 0 ELSE 1 END FROM event_deliveries WHERE delivery_id=$1 AND status='delivered'", id).Scan(&claim, &live); err != nil || claim <= failedClaim || live != 0 {
						t.Fatalf("restart did not fence and settle original obligation: claim=%d before=%d live=%d err=%v", claim, failedClaim, live, err)
					}
				}
				duplicate := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
				if duplicate.EventID != accepted.EventID || duplicate.RunID != seed.RunID {
					t.Fatal("duplicate changed the admitted event/run")
				}
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
				var after operatorread.OperatorEventFull
				requireServedJSONRPCResult(t, rt.Endpoint, "event.get", map[string]any{"event_id": accepted.EventID}, &after)
				immutable.Deliveries, after.Deliveries = nil, nil
				if !reflect.DeepEqual(immutable, after) {
					t.Fatalf("retry changed immutable accepted publication: before=%+v after=%+v", immutable, after)
				}
				var count int
				if err := rt.DB.QueryRow("SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='ack'", seed.RunID).Scan(&count); err != nil || count != 1 {
					t.Fatalf("final consumer count=%d err=%v", count, err)
				}
				if err := rt.DB.QueryRow("SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE e.run_id=$1 AND e.event_name='selected' AND d.status='delivered'", seed.RunID).Scan(&count); err != nil || count != 1 {
					t.Fatalf("final consumer did not settle once: %d %v", count, err)
				}
				assertServedSelectionTrace(t, rt, accepted.EventID, seed.RunID, true)
			})
		}
	}
}

func assertServedSelectionTrace(t *testing.T, rt servedControlProofRuntime, eventID, runID string, present bool) {
	t.Helper()
	if present {
		node, err := identity.ParseExecutableNode(".", "select")
		if err != nil {
			t.Fatal(err)
		}
		requireServedTraceReadback(t, rt.Endpoint, runID, eventID, "select", node.Key())
	}
	var response struct {
		Trace []operatorread.RunDebugTraceRow `json:"trace"`
	}
	requireServedJSONRPCResult(t, rt.Endpoint, "run.trace", map[string]any{"run_id": runID, "limit": 100}, &response)
	for _, row := range response.Trace {
		if row.EventID != eventID {
			continue
		}
		fact := row.HandlerRuleSelection
		if !present && fact == nil {
			return
		}
		if present && row.DeliveryStatus == "delivered" && fact != nil && fact.Context == handlerselection.ContextRules && fact.Disposition == handlerselection.DispositionSelected && fact.DisplayLabel == "first" && fact.FlowPath == "." && fact.SemanticPath == `nodes["select"].handlers["select"].rules[0]` {
			return
		}
		t.Fatalf("incorrect public selection presence: want=%v row=%+v", present, row)
	}
	t.Fatal("public trace omitted the selection delivery")
}
