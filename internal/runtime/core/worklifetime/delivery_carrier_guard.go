package worklifetime

import (
	"context"
	"errors"
)

// DeliveryCarrierGuard keeps one exact delivery capability reachable until it
// is consumed into an attempt or returned to its durable owner. It is scoped to
// the caller's stack; there is no secondary registry or background poller.
type DeliveryCarrierGuard struct {
	ctx          context.Context
	delivery     *EventDelivery
	continuation DeliveryContinuation
	resolved     bool
	resolution   DeliveryContinuationResolution
}

func NewEventDeliveryCarrierGuard(delivery *EventDelivery) (*DeliveryCarrierGuard, error) {
	if delivery == nil {
		return nil, errors.New("local delivery carrier is required")
	}
	return &DeliveryCarrierGuard{ctx: delivery.Context(), delivery: delivery}, nil
}

func NewDeliveryContinuationGuard(ctx context.Context, continuation DeliveryContinuation) (*DeliveryCarrierGuard, error) {
	if continuation == nil || continuation.DeliveryID() == "" {
		return nil, errors.New("exact delivery continuation is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return &DeliveryCarrierGuard{ctx: ctx, continuation: continuation}, nil
}

// Consume resolves the guarded continuation once. A concurrent terminal fence
// is returned as a typed resolution and never converted into an attempt.
func (g *DeliveryCarrierGuard) Consume(report func(error)) (DeliveryContinuationResolution, error) {
	if g == nil {
		return 0, errors.New("delivery carrier guard is required")
	}
	if g.resolved {
		return g.resolution, nil
	}
	var (
		resolution DeliveryContinuationResolution
		err        error
	)
	if g.delivery != nil {
		resolution, err = g.delivery.resolveContinuation(DeliveryContinuationConsume)
	} else if g.continuation != nil {
		resolution, err = g.continuation.Resolve(context.WithoutCancel(g.ctx), DeliveryContinuationConsume)
	} else {
		return 0, errors.New("delivery carrier capability is required")
	}
	if err != nil {
		if report != nil {
			report(err)
		}
		return 0, err
	}
	if resolution != DeliveryContinuationConsumed && resolution != DeliveryContinuationTerminal {
		return 0, errors.New("delivery carrier consume returned an invalid resolution")
	}
	g.resolution = resolution
	if g.delivery == nil {
		g.resolved = true
	}
	return resolution, nil
}

// Complete returns an unconsumed continuation and settles its process-local
// carrier. Resolution is a single atomic owner transition; errors are reported
// as invariant evidence and are never retried by polling.
func (g *DeliveryCarrierGuard) Complete(report func(error)) (DeliveryContinuationResolution, error) {
	if g == nil {
		return 0, errors.New("delivery carrier guard is required")
	}
	if g.resolved {
		return g.resolution, nil
	}
	var (
		resolution DeliveryContinuationResolution
		err        error
	)
	if g.delivery != nil {
		resolution, err = g.delivery.completeCarrier()
	} else if g.continuation != nil {
		resolution, err = g.continuation.Resolve(context.WithoutCancel(g.ctx), DeliveryContinuationReturn)
	} else {
		return 0, errors.New("delivery carrier capability is required")
	}
	if err != nil {
		if report != nil {
			report(err)
		}
		return 0, err
	}
	if resolution != DeliveryContinuationUntracked && resolution != DeliveryContinuationReturned && resolution != DeliveryContinuationConsumed && resolution != DeliveryContinuationTerminal {
		return 0, errors.New("delivery carrier completion returned an invalid resolution")
	}
	g.resolution = resolution
	g.resolved = true
	return resolution, nil
}
