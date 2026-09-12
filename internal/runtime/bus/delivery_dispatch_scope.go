package bus

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

// This stack-owned scope carries the result of one election through direct
// interception and queue handoff. The coordinator remains the semantic owner.
type deliveryDispatchScope struct {
	eventID string
	entries map[string]*deliveryDispatchEntry
	closed  bool
}

type deliveryDispatchEntry struct {
	acquisition worklifetime.DeliveryAcquisition
	guard       *worklifetime.DeliveryCarrierGuard
	transferred bool
}

type deliveryDispatchScopeKey struct{}

func (eb *EventBus) beginDeliveryDispatch(ctx context.Context, evt events.Event, routes []events.DeliveryRoute) (context.Context, *deliveryDispatchScope, func() error, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	scope := &deliveryDispatchScope{eventID: evt.ID(), entries: make(map[string]*deliveryDispatchEntry)}
	closeScope := func() error {
		scope.closed = true
		var errs []error
		for _, e := range scope.entries {
			if e.guard != nil && !e.transferred {
				_, err := e.guard.Complete(nil)
				errs = append(errs, err)
			}
		}
		return errors.Join(errs...)
	}
	owner := eb.DeliveryContinuationOwner()
	if owner == nil && eb.ephemeral {
		return ctx, nil, func() error { return nil }, nil
	}
	for _, route := range events.NormalizeDeliveryRoutes(routes) {
		if route.Recipient.Empty() {
			continue
		}
		id, err := runtimedelivery.DeliveryID(evt.ID(), route)
		if err != nil {
			return ctx, nil, nil, errors.Join(err, closeScope())
		}
		if owner == nil {
			return ctx, nil, nil, errors.Join(errors.New("exact delivery continuation owner is required"), closeScope())
		}
		acquisition, err := owner.Acquire(id)
		if err != nil {
			return ctx, nil, nil, errors.Join(err, closeScope())
		}
		if err := acquisition.Validate(id); err != nil {
			return ctx, nil, nil, errors.Join(err, closeScope())
		}
		e := &deliveryDispatchEntry{acquisition: acquisition}
		if cap, acquired := acquisition.Acquired(); acquired {
			e.guard, err = worklifetime.NewDeliveryContinuationGuard(ctx, cap)
			if err != nil {
				return ctx, nil, nil, errors.Join(err, returnDeliveryContinuation(ctx, cap), closeScope())
			}
		}
		scope.entries[id] = e
	}
	return context.WithValue(ctx, deliveryDispatchScopeKey{}, scope), scope, closeScope, nil
}

func (s *deliveryDispatchScope) entry(evt events.Event, route events.DeliveryRoute) (*deliveryDispatchEntry, error) {
	id, err := runtimedelivery.DeliveryID(evt.ID(), route)
	if err != nil {
		return nil, err
	}
	if s == nil || s.closed || s.eventID != evt.ID() {
		return nil, errors.New("exact event dispatch scope is required")
	}
	e, ok := s.entries[id]
	if !ok {
		return nil, fmt.Errorf("delivery %s has no dispatch election", id)
	}
	return e, nil
}

func (eb *EventBus) borrowDeliveryDispatch(ctx context.Context, evt events.Event, routes []events.DeliveryRoute) (context.Context, *deliveryDispatchScope, func() error, error) {
	if scope, ok := deliveryDispatchFromContext(ctx, evt); ok {
		for _, route := range routes {
			if route.Recipient.Empty() {
				continue
			}
			if _, err := scope.entry(evt, route); err != nil {
				return ctx, nil, nil, err
			}
		}
		return ctx, scope, func() error { return nil }, nil
	}
	return eb.beginDeliveryDispatch(ctx, evt, routes)
}

func deliveryDispatchFromContext(ctx context.Context, evt events.Event) (*deliveryDispatchScope, bool) {
	if ctx == nil {
		return nil, false
	}
	s, ok := ctx.Value(deliveryDispatchScopeKey{}).(*deliveryDispatchScope)
	return s, ok && s != nil && s.eventID == evt.ID()
}
