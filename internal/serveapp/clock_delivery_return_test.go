package serveapp

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestServedClockPendingConsumerReturnsAndRestartsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			opts, start := clockDeploymentHarness(t, backend, canonicalrouting.CopyClockDeployment(t, true))
			reached := make(chan string, 1)
			opts.TestWorkflowNodeHandlerStartHook = func(ctx context.Context, _ string, event events.Event) error {
				if string(event.Type()) != "clock/poll.tick" {
					return nil
				}
				select {
				case reached <- event.ID():
				default:
				}
				<-ctx.Done()
				return ctx.Err()
			}
			first, served := start()
			var held string
			select {
			case held = <-reached:
			case <-time.After(10 * time.Second):
				t.Fatal("clock did not reach its genuine connected consumer")
			}
			var before operatorread.OperatorEventFull
			requireServedJSONRPCResult(t, served.Endpoint, "event.get", map[string]any{"event_id": held}, &before)
			if len(before.Deliveries) != 1 || before.Deliveries[0].Terminal || len(before.DeadLetters) != 0 {
				t.Fatalf("held accepted clock delivery=%+v", before)
			}
			clock := readServedClockHeader(t, served.Endpoint, before.RunID).ClockSchedules[0]
			statuses, err := servedTestProcessRuntime(t, first).Pipeline.ListStandingServiceStatuses(t.Context())
			if err != nil || len(statuses) != 1 || statuses[0].RunID != before.RunID {
				t.Fatalf("held clock has no exact deployment generation: statuses=%+v err=%v", statuses, err)
			}
			generation := statuses[0]
			if code := first.stop(); code != 0 {
				t.Fatalf("held clock cleanup did not join: code=%d\n%s", code, first.outputString())
			}
			var recoveredExecutions atomic.Int32
			opts.TestWorkflowNodeHandlerStartHook = func(_ context.Context, _ string, event events.Event) error {
				if event.ID() == held {
					recoveredExecutions.Add(1)
				}
				return nil
			}
			setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
			second, restarted := start()
			deadline := time.Now().Add(10 * time.Second)
			for {
				var after operatorread.OperatorEventFull
				requireServedJSONRPCResult(t, restarted.Endpoint, "event.get", map[string]any{"event_id": held}, &after)
				if after.EventID != before.EventID || after.RunID != before.RunID || !after.CreatedAt.Equal(before.CreatedAt) || after.Source != before.Source || len(after.Deliveries) != 1 || len(after.DeadLetters) != 0 {
					t.Fatalf("recovery reminted publication or delivery authority: before=%+v after=%+v", before, after)
				}
				if after.Deliveries[0].Terminal && after.Deliveries[0].Status == "delivered" {
					if after.Deliveries[0].DeliveryID != before.Deliveries[0].DeliveryID || after.Deliveries[0].Target.FlowID != "consumer" {
						t.Fatalf("pending clock recovered another carrier: %+v", after)
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("pending clock did not settle after restart: %+v", after)
				}
				time.Sleep(10 * time.Millisecond)
			}
			if current := readServedClockHeader(t, restarted.Endpoint, before.RunID).ClockSchedules[0]; current.ActivationID != clock.ActivationID || current.RunID != clock.RunID || current.FlowInstance != clock.FlowInstance || !current.InitialDueAt.Equal(clock.InitialDueAt) {
				t.Fatalf("pending delivery recovery replaced its clock: before=%+v current=%+v", clock, current)
			}
			statuses, err = servedTestProcessRuntime(t, second).Pipeline.ListStandingServiceStatuses(t.Context())
			if err != nil || len(statuses) != 1 || statuses[0].ServiceID != generation.ServiceID || statuses[0].RunID != generation.RunID || statuses[0].Generation != generation.Generation {
				t.Fatalf("recovery borrowed another deployment generation: before=%+v after=%+v err=%v", generation, statuses, err)
			}
			if recoveredExecutions.Load() != 1 {
				t.Fatalf("held delivery executed %d times after restart, want once", recoveredExecutions.Load())
			}
			if code := second.stop(); code != 0 {
				t.Fatalf("recovered clock shutdown code=%d", code)
			}
		})
	}
}

func TestServedClockProducerStopDoesNotCancelAcceptedConsumerBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			opts, start := clockDeploymentHarness(t, backend, canonicalrouting.CopyClockDeployment(t, true))
			reached := make(chan string, 1)
			release := make(chan struct{})
			cancelled := make(chan struct{}, 1)
			var executionMu sync.Mutex
			executions := map[string]int{}
			opts.TestWorkflowNodeHandlerStartHook = func(ctx context.Context, _ string, event events.Event) error {
				if string(event.Type()) != "clock/poll.tick" {
					return nil
				}
				executionMu.Lock()
				executions[event.ID()]++
				executionMu.Unlock()
				select {
				case reached <- event.ID():
				default:
				}
				select {
				case <-ctx.Done():
					select {
					case cancelled <- struct{}{}:
					default:
					}
					return ctx.Err()
				case <-release:
					return nil
				}
			}
			process, served := start()
			var held string
			select {
			case held = <-reached:
			case <-time.After(10 * time.Second):
				t.Fatal("clock did not reach its connected consumer")
			}
			var before operatorread.OperatorEventFull
			requireServedJSONRPCResult(t, served.Endpoint, "event.get", map[string]any{"event_id": held}, &before)
			if len(before.Deliveries) != 1 || before.Deliveries[0].Terminal || len(before.DeadLetters) != 0 {
				t.Fatalf("consumer was not held after durable acceptance: %+v", before)
			}
			stopCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			if err := servedTestProcessRuntime(t, process).GenericSchedules.Stop(stopCtx); err != nil {
				t.Fatalf("clock producer shutdown depends on its consumer: %v", err)
			}
			select {
			case <-cancelled:
				t.Fatal("clock producer shutdown cancelled its independently accepted receiver")
			default:
			}
			close(release)
			deadline := time.Now().Add(10 * time.Second)
			for {
				var after operatorread.OperatorEventFull
				requireServedJSONRPCResult(t, served.Endpoint, "event.get", map[string]any{"event_id": held}, &after)
				if after.EventID != before.EventID || after.RunID != before.RunID || !after.CreatedAt.Equal(before.CreatedAt) || len(after.Deliveries) != 1 || after.Deliveries[0].DeliveryID != before.Deliveries[0].DeliveryID || len(after.DeadLetters) != 0 {
					t.Fatalf("producer stop reminted accepted delivery: before=%+v after=%+v", before, after)
				}
				if after.Deliveries[0].Terminal && after.Deliveries[0].Status == "delivered" {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("accepted consumer did not settle after producer stop: %+v", after)
				}
				time.Sleep(10 * time.Millisecond)
			}
			executionMu.Lock()
			count := executions[held]
			executionMu.Unlock()
			if count != 1 {
				t.Fatalf("held route executed %d times, want exactly once", count)
			}
			if code := process.stop(); code != 0 {
				t.Fatalf("independent consumer shutdown=%d\n%s", code, process.outputString())
			}
		})
	}
}
