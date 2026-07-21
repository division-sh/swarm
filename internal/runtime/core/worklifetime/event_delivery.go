package worklifetime

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/events"
)

// Occurrence is deliberately closed to the fixed runtime-generation types in
// this package. Consumers cannot invent a generic or string-keyed work owner.
type Occurrence interface {
	Begin(context.Context) (*Lease, error)
	BeginStanding(context.Context) (*Lease, error)
	Wait(context.Context) error
	WaitForQuiescence(context.Context) error
	NewRoute(context.Context, RouteIdentity) (*RouteOccurrence, error)
	NewEventDelivery(context.Context, events.Event) (*EventDelivery, error)
	NewRoutedEventDelivery(context.Context, events.Event, events.DeliveryRoute) (*EventDelivery, error)
	workOccurrence()
}

// InternalSubscription is one process-local subscriber generation. The bus
// retires the generation separately from its delivery channel so a snapshotted
// sender cannot publish into an orphaned queue.
type InternalSubscription interface {
	Deliveries() <-chan *EventDelivery
	Retiring() <-chan struct{}
	MarkReady()
	Complete(restart bool) error
}

// EventDelivery is the process-local EventBus carrier. It can only be minted
// by a fixed typed occurrence, so a queued event always owns lifetime before
// it escapes the producer.
type EventDelivery struct {
	event     events.Event
	route     events.DeliveryRoute
	lease     *Lease
	companion *Lease
	ctx       context.Context
	once      sync.Once
	err       error
	mu        sync.Mutex
	completed bool
	callbacks []func()
}

func newEventDelivery(ctx context.Context, event events.Event, route events.DeliveryRoute, owner Occurrence, begin func(context.Context) (*Lease, error)) (*EventDelivery, error) {
	if event.Type() == "" {
		return nil, errors.New("local delivery event is required")
	}
	lease, err := begin(ctx)
	if err != nil {
		return nil, err
	}
	return &EventDelivery{event: event, route: route, lease: lease, ctx: WithOccurrence(lease.Context(), owner)}, nil
}

func (*RuntimeOccurrence) workOccurrence()      {}
func (*StandingOccurrence) workOccurrence()     {}
func (*SelectedForkOccurrence) workOccurrence() {}
func (*ManagerWorkOccurrence) workOccurrence()  {}

func (d *EventDelivery) Context() context.Context {
	if d == nil || d.ctx == nil {
		return context.Background()
	}
	return d.ctx
}

func (d *EventDelivery) Event() events.Event {
	if d == nil {
		panic("local delivery is required")
	}
	return d.event
}

func (d *EventDelivery) HandoffRoute() events.DeliveryRoute {
	if d == nil {
		return events.DeliveryRoute{}
	}
	return d.route
}

func (d *EventDelivery) ID() string             { return d.Event().ID() }
func (d *EventDelivery) Type() events.EventType { return d.Event().Type() }
func (d *EventDelivery) RunID() string          { return d.Event().RunID() }
func (d *EventDelivery) EntityID() string       { return d.Event().EntityID() }
func (d *EventDelivery) FlowInstance() string   { return d.Event().FlowInstance() }
func (d *EventDelivery) TargetRoute() events.RouteIdentity {
	return d.Event().TargetRoute()
}
func (d *EventDelivery) TargetRoutes() []events.RouteIdentity { return d.Event().TargetRoutes() }
func (d *EventDelivery) Payload() []byte                      { return d.Event().Payload() }
func (d *EventDelivery) CreatedAt() time.Time                 { return d.Event().CreatedAt() }

func (d *EventDelivery) Complete() error {
	if d == nil {
		return errors.New("local delivery is required")
	}
	run := false
	d.once.Do(func() {
		run = true
		d.err = d.lease.Done()
		if d.companion != nil {
			d.err = errors.Join(d.err, d.companion.Done())
		}
		d.mu.Lock()
		d.completed = true
		callbacks := append([]func(){}, d.callbacks...)
		d.callbacks = nil
		d.mu.Unlock()
		for _, callback := range callbacks {
			callback()
		}
	})
	if !run && d.err == nil {
		return ErrAlreadySettled
	}
	return d.err
}

// OnComplete registers process-local accounting that must settle with this
// delivery. Registration is synchronous and never creates another task.
func (d *EventDelivery) OnComplete(callback func()) error {
	if d == nil || callback == nil {
		return errors.New("local delivery completion callback is required")
	}
	d.mu.Lock()
	if d.completed {
		d.mu.Unlock()
		callback()
		return nil
	}
	d.callbacks = append(d.callbacks, callback)
	d.mu.Unlock()
	return nil
}

func (r *RuntimeOccurrence) NewEventDelivery(ctx context.Context, event events.Event) (*EventDelivery, error) {
	if r == nil {
		return nil, errors.New("runtime occurrence is required")
	}
	return newEventDelivery(ctx, event, events.DeliveryRoute{}, r, r.Begin)
}

func (r *RuntimeOccurrence) NewRoutedEventDelivery(ctx context.Context, event events.Event, route events.DeliveryRoute) (*EventDelivery, error) {
	if r == nil {
		return nil, errors.New("runtime occurrence is required")
	}
	return newEventDelivery(ctx, event, route, r, r.Begin)
}

func (r *RouteOccurrence) NewEventDelivery(ctx context.Context, event events.Event) (*EventDelivery, error) {
	if r == nil {
		return nil, errors.New("route occurrence is required")
	}
	return r.newEventDelivery(ctx, event, events.DeliveryRoute{})
}

func (r *RouteOccurrence) NewRoutedEventDelivery(ctx context.Context, event events.Event, route events.DeliveryRoute) (*EventDelivery, error) {
	if r == nil {
		return nil, errors.New("route occurrence is required")
	}
	return r.newEventDelivery(ctx, event, route)
}

func (r *RouteOccurrence) newEventDelivery(ctx context.Context, event events.Event, route events.DeliveryRoute) (*EventDelivery, error) {
	delivery, err := newEventDelivery(ctx, event, route, r.owner, r.Begin)
	if err != nil {
		return nil, err
	}
	contextOwner, ok := OccurrenceFromContext(ctx)
	standing, ok := contextOwner.(*StandingOccurrence)
	if !ok || standing == nil || contextOwner == r.owner {
		return delivery, nil
	}
	companion, err := standing.Begin(ctx)
	if err != nil {
		_ = delivery.Complete()
		return nil, err
	}
	delivery.companion = companion
	delivery.ctx = WithOccurrence(delivery.ctx, standing)
	return delivery, nil
}

func (s *StandingOccurrence) NewEventDelivery(ctx context.Context, event events.Event) (*EventDelivery, error) {
	if s == nil {
		return nil, errors.New("standing occurrence is required")
	}
	return newEventDelivery(ctx, event, events.DeliveryRoute{}, s, s.Begin)
}

func (s *StandingOccurrence) NewRoutedEventDelivery(ctx context.Context, event events.Event, route events.DeliveryRoute) (*EventDelivery, error) {
	if s == nil {
		return nil, errors.New("standing occurrence is required")
	}
	return newEventDelivery(ctx, event, route, s, s.Begin)
}

func (s *SelectedForkOccurrence) NewEventDelivery(ctx context.Context, event events.Event) (*EventDelivery, error) {
	if s == nil {
		return nil, errors.New("selected-fork occurrence is required")
	}
	return newEventDelivery(ctx, event, events.DeliveryRoute{}, s, s.Begin)
}

func (s *SelectedForkOccurrence) NewRoutedEventDelivery(ctx context.Context, event events.Event, route events.DeliveryRoute) (*EventDelivery, error) {
	if s == nil {
		return nil, errors.New("selected-fork occurrence is required")
	}
	return newEventDelivery(ctx, event, route, s, s.Begin)
}

func (m *ManagerWorkOccurrence) NewEventDelivery(ctx context.Context, event events.Event) (*EventDelivery, error) {
	if m == nil || m.manager == nil {
		return nil, errors.New("manager work occurrence is required")
	}
	return newEventDelivery(ctx, event, events.DeliveryRoute{}, m, m.Begin)
}

func (m *ManagerWorkOccurrence) NewRoutedEventDelivery(ctx context.Context, event events.Event, route events.DeliveryRoute) (*EventDelivery, error) {
	if m == nil || m.manager == nil {
		return nil, errors.New("manager work occurrence is required")
	}
	return newEventDelivery(ctx, event, route, m, m.Begin)
}
