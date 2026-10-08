package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

type turnCancellationCutStore struct {
	effects.Store
	effects.TurnLifetimeStore
	effects.CanceledTurnStore
	cut       *atomic.Bool
	seen      chan effects.CanceledTurnCommand
	once      *sync.Once
	recovered chan effects.CanceledTurnCommand
}

func (s turnCancellationCutStore) CommitCanceledTurn(ctx context.Context, command effects.CanceledTurnCommand) (effects.CanceledTurnCommit, error) {
	if s.cut.Load() {
		s.once.Do(func() { s.seen <- command })
		return effects.CanceledTurnCommit{}, errors.New("injected canceled-origin commit interruption after physical cleanup")
	}
	select {
	case s.recovered <- command:
	default:
	}
	return s.CanceledTurnStore.CommitCanceledTurn(ctx, command)
}

// Real HTTP admission, Manager/provider execution, native startup and HTTP
// readback; the compiled internal mock lifecycle is not public-launcher or
// paid-provider qualification. Only the exact logical settlement port fails.
func TestServedCanceledTurnRecoveryBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			var cut atomic.Bool
			var recoverNext atomic.Bool
			cut.Store(true)
			seen := make(chan effects.CanceledTurnCommand, 1)
			recovered := make(chan effects.CanceledTurnCommand, 1)
			var once sync.Once
			var selected interface {
				deliverylifecycle.Store
				effects.CanceledTurnRecoveryStore
			}
			original := projectRuntimePersistenceForServe
			projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
				if recoverNext.Load() {
					cut.Store(false)
				}
				persistence := original(owner)
				selected = persistence.deps.EventStore.(interface {
					deliverylifecycle.Store
					effects.CanceledTurnRecoveryStore
				})
				role := persistence.deps.ManagerPersistenceRoles.LifecycleEffects
				persistence.deps.ManagerPersistenceRoles.LifecycleEffects = turnCancellationCutStore{
					Store: role, TurnLifetimeStore: role.(effects.TurnLifetimeStore), CanceledTurnStore: role.(effects.CanceledTurnStore),
					cut: &cut, seen: seen, once: &once, recovered: recovered,
				}
				return persistence
			}
			t.Cleanup(func() { projectRuntimePersistenceForServe = original })
			root := canonicalrouting.CopyTurnCancellationRecovery(t)
			name := "sqlite"
			if backend == servedparity.BackendExplicitPostgres {
				name = "postgres"
			}
			_, start := issue2564ServeHarness(t, name, root, true)
			process, rt := start()
			t.Cleanup(func() {
				if code := process.stop(); code != 0 {
					t.Errorf("retained cancellation runtime stop=%d\n%s", code, process.outputString())
				}
			})
			restart := func() {
				if code := process.stop(); code != 0 {
					t.Fatalf("retained cancellation predecessor stop=%d\n%s", code, process.outputString())
				}
				process, rt = start()
			}
			params := map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"case_id": "bounded-turn"}, "idempotency_key": "canceled-recovery"}
			accepted := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
			var command effects.CanceledTurnCommand
			wait := time.NewTimer(20 * time.Second)
			defer wait.Stop()
			poll := time.NewTicker(20 * time.Millisecond)
			defer poll.Stop()
		waitForSettlement:
			for {
				select {
				case command = <-seen:
					break waitForSettlement
				case <-poll.C:
					var trace struct {
						Trace []operatorread.RunDebugTraceRow `json:"trace"`
					}
					requireServedJSONRPCResult(t, rt.Endpoint, "run.trace", map[string]any{"run_id": accepted.RunID, "limit": 100}, &trace)
					for _, row := range trace.Trace {
						if row.DeliveryStatus == "dead_letter" {
							detail, _ := json.Marshal(row)
							t.Fatalf("execution failed before cancellation settlement: %s", detail)
						}
					}
				case <-wait.C:
					var trace struct {
						Trace []operatorread.RunDebugTraceRow `json:"trace"`
					}
					requireServedJSONRPCResult(t, rt.Endpoint, "run.trace", map[string]any{"run_id": accepted.RunID, "limit": 100}, &trace)
					t.Fatalf("real Manager did not reach canceled-origin settlement: %+v", trace)
				}
			}
			if command.Origin.Kind != effects.CompletionOriginDelivery || command.Origin.Delivery.RunID() != accepted.RunID || command.Publication == nil {
				t.Fatalf("foreground omitted exact origin/reaction: %+v", command)
			}
			before, err := selected.Snapshot(context.Background(), command.Origin.Delivery.DeliveryID())
			if err != nil || before.Status != deliverylifecycle.StatusInProgress {
				t.Fatalf("interruption falsely settled business origin: %+v %v", before, err)
			}
			pending, err := selected.ListCanceledTurnRecoveries(context.Background(), effects.NewRecoveryRequest(time.Now().UTC(), executionposture.MockOnly))
			if err != nil || len(pending) != 1 || !pending[0].Cancellation.Origin.Same(command.Origin) || pending[0].Clock == nil {
				t.Fatalf("startup lost exact physically settled pending intent: %+v %v", pending, err)
			}
			cause := pending[0].Cancellation.CauseEvent
			// Keep the cut through predecessor shutdown. Only construction of the
			// successor releases it; cleanup must not masquerade as startup proof.
			recoverNext.Store(true)
			restart()
			select {
			case recoveredCommand := <-recovered:
				plan, ok := recoveredCommand.Publication.(bus.EnginePublicationPlan)
				if !ok {
					t.Fatal("startup omitted its exact admitted reaction plan")
				}
				request := plan.PublicationCommand().Commit
				if len(request.DeliveryRoutes) != 1 || !request.DeliveryRoutes[0].Recipient.IsNode() || request.DeliveryAuthority.Validate() != nil || request.DeliveryAuthority.Kind() != deliverylifecycle.ExecutionAuthorityNormalRuntime {
					t.Fatalf("startup lost the declared consumer or exact preparation stamp: %+v", request)
				}
			default:
				t.Fatal("successor did not own canceled-turn settlement before recovery dispatch")
			}
			var trace struct {
				Trace []operatorread.RunDebugTraceRow `json:"trace"`
			}
			requireServedJSONRPCResult(t, rt.Endpoint, "run.trace", map[string]any{"run_id": accepted.RunID, "limit": 100}, &trace)
			var cancellation *operatorread.RunDebugTraceRow
			for i := range trace.Trace {
				row := &trace.Trace[i]
				if row.DeliveryID == before.DeliveryID {
					cancellation = row
				}
			}
			if cancellation == nil || cancellation.DeliveryStatus != "canceled" || cancellation.DeliveryReasonCode != "turn_timeout" || cancellation.DeliveryFailure != nil || cancellation.DeliveryRetryCount != before.RetryCount {
				t.Fatalf("public startup readback lost typed cancellation: %+v", trace)
			}
			var eventList operatorread.OperatorEventListResult
			deadline := time.Now().Add(20 * time.Second)
			for {
				requireServedJSONRPCResult(t, rt.Endpoint, "event.list", map[string]any{"filter": map[string]any{"run_id": accepted.RunID}, "limit": 100}, &eventList)
				var reactions, observations int
				for _, event := range eventList.Events {
					if event.EventID == cause && event.EventName == "work.timed_out" {
						reactions++
					}
					if event.EventName == "work.timeout_recorded" && event.SourceEventID == cause {
						observations++
					}
				}
				if reactions == 1 && observations == 1 {
					break
				}
				if reactions > 1 || observations > 1 || time.Now().After(deadline) {
					t.Fatalf("startup did not dispatch the exact atomic reaction once: %+v", eventList)
				}
				time.Sleep(10 * time.Millisecond)
			}
			var exact operatorread.OperatorEventFull
			requireServedJSONRPCResult(t, rt.Endpoint, "event.get", map[string]any{"event_id": cause}, &exact)
			restart()
			duplicate := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
			if duplicate.EventID != accepted.EventID || duplicate.RunID != accepted.RunID {
				t.Fatal("public retry changed the original ingress identity")
			}
			var after operatorread.OperatorEventFull
			requireServedJSONRPCResult(t, rt.Endpoint, "event.get", map[string]any{"event_id": cause}, &after)
			if !reflect.DeepEqual(exact, after) {
				t.Fatalf("restart changed atomic reaction evidence: before=%+v after=%+v", exact, after)
			}
			requireServedJSONRPCResult(t, rt.Endpoint, "event.list", map[string]any{"filter": map[string]any{"run_id": accepted.RunID}, "limit": 100}, &eventList)
			var observations int
			for _, event := range eventList.Events {
				if event.EventName == "work.timeout_recorded" && event.SourceEventID == cause {
					observations++
				}
			}
			if observations != 1 {
				t.Fatalf("settled canceled turn/reaction reexecuted: %+v", eventList)
			}
		})
	}
}
