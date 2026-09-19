package pipeline

import (
	"context"
	"fmt"
	"time"
)

// Direct failure-algebra tests deliberately supply a bounded fake owner. This
// entry point is not linked into production; real execution requires the shared
// process selector and its exact generation-bound candidate owner.
func (pc *PipelineCoordinator) serveFanOutTurn(ctx context.Context, now time.Time) (fanOutTurnDisposition, error) {
	if pc == nil || pc.workflowStore == nil || pc.workflowStore.fanOutObligations == nil {
		return fanOutTurnExhausted, nil
	}
	owner, ok := pc.workflowStore.fanOutObligations.(FanOutObligationOwner)
	if !ok {
		return fanOutTurnAwaitScan, fmt.Errorf("direct fan-out test requires an explicitly supplied test owner")
	}
	return pc.claimAndServeFanOutTurn(ctx, owner, nil, now)
}

type fanOutTestWake chan struct{}

func (w fanOutTestWake) Wake() {
	select {
	case w <- struct{}{}:
	default:
	}
}

// This bounded unit driver exercises the executor's refill/failure algebra with
// injected owners. Production recovery/capacity is tested through the real
// startupownership service and both-store conformance fixtures, not this driver.
func runFanOutTestDriver(ctx context.Context, pc *PipelineCoordinator, wake fanOutTestWake, interval time.Duration) {
	var ticks <-chan time.Time
	if interval > 0 {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		ticks = ticker.C
	}
	run := func() {
		result, err := pc.serveFanOutTurn(ctx, time.Now())
		if err != nil {
			pc.ReportFanOutServingError(ctx, err)
		}
		if result.refill() {
			wake.Wake()
		}
	}
	run()
	for {
		select {
		case <-ctx.Done():
			return
		case <-wake:
			run()
		case <-ticks:
			run()
		}
	}
}
