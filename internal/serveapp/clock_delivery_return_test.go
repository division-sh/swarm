package serveapp

import (
	"context"
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
			if code := first.stop(); code != 0 {
				t.Fatalf("held clock cleanup did not join: code=%d\n%s", code, first.outputString())
			}
			opts.TestWorkflowNodeHandlerStartHook = nil
			setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
			second, restarted := start()
			deadline := time.Now().Add(10 * time.Second)
			for {
				var after operatorread.OperatorEventFull
				requireServedJSONRPCResult(t, restarted.Endpoint, "event.get", map[string]any{"event_id": held}, &after)
				if after.EventID != before.EventID || !after.CreatedAt.Equal(before.CreatedAt) || after.Source != before.Source || len(after.Deliveries) != 1 || len(after.DeadLetters) != 0 {
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
			if current := readServedClockHeader(t, restarted.Endpoint, before.RunID).ClockSchedules[0]; current.ActivationID != clock.ActivationID || !current.InitialDueAt.Equal(clock.InitialDueAt) {
				t.Fatalf("pending delivery recovery replaced its clock: before=%+v current=%+v", clock, current)
			}
			if code := second.stop(); code != 0 {
				t.Fatalf("recovered clock shutdown code=%d", code)
			}
		})
	}
}
