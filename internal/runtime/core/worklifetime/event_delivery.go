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
	standingProjection() *StandingOccurrence
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
	event                events.Event
	route                events.DeliveryRoute
	lease                *Lease
	companion            *Lease
	ctx                  context.Context
	err                  error
	mu                   sync.Mutex
	completed            bool
	callbacks            []func()
	continuation         DeliveryContinuation
	continuationResult   DeliveryContinuationResolution
	continuationSettling bool
	settling             bool
}

// DeliveryContinuation is an exact one-delivery ownership capability. Its
// implementation is delivery-specific. One closed transition resolves the
// carrier atomically as returned, consumed into an attempt, or terminal-fenced.
type DeliveryContinuation interface {
	DeliveryID() string
	Resolve(context.Context, DeliveryContinuationIntent) (DeliveryContinuationResolution, error)
}

type DeliveryContinuationIntent uint8

const (
	DeliveryContinuationReturn DeliveryContinuationIntent = iota + 1
	DeliveryContinuationConsume
)

type DeliveryContinuationResolution uint8

const (
	DeliveryContinuationUntracked DeliveryContinuationResolution = iota + 1
	DeliveryContinuationReturned
	DeliveryContinuationConsumed
	DeliveryContinuationTerminal
)

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

func (*RuntimeOccurrence) standingProjection() *StandingOccurrence { return nil }
func (s *StandingOccurrence) standingProjection() *StandingOccurrence {
	return s
}
func (*SelectedForkOccurrence) standingProjection() *StandingOccurrence { return nil }
func (m *ManagerWorkOccurrence) standingProjection() *StandingOccurrence {
	if m == nil || m.companion == nil {
		return nil
	}
	return m.companion.standingProjection()
}

// StandingProjection returns the exact standing generation represented by a
// fixed occurrence, including a Manager work projection. It does not expose
// arbitrary parentage or the Manager's raw companion.
func StandingProjection(owner Occurrence) (*StandingOccurrence, bool) {
	if owner == nil {
		return nil, false
	}
	standing := owner.standingProjection()
	return standing, standing != nil
}

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

func (d *EventDelivery) AttachContinuation(continuation DeliveryContinuation) error {
	if d == nil || continuation == nil || continuation.DeliveryID() == "" {
		return errors.New("exact delivery continuation is required")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.completed || d.continuation != nil {
		return errors.New("delivery continuation cannot be attached")
	}
	d.continuation = continuation
	return nil
}

func (d *EventDelivery) resolveContinuation(intent DeliveryContinuationIntent) (DeliveryContinuationResolution, error) {
	if d == nil {
		return 0, errors.New("local delivery is required")
	}
	if intent != DeliveryContinuationReturn && intent != DeliveryContinuationConsume {
		return 0, errors.New("delivery continuation resolution intent is invalid")
	}
	d.mu.Lock()
	if d.completed {
		d.mu.Unlock()
		return 0, errors.New("local delivery is already completed")
	}
	if d.continuationResult != 0 {
		result := d.continuationResult
		d.mu.Unlock()
		return result, nil
	}
	if d.continuation == nil {
		d.continuationResult = DeliveryContinuationUntracked
		d.mu.Unlock()
		return DeliveryContinuationUntracked, nil
	}
	if d.continuationSettling {
		d.mu.Unlock()
		return 0, errors.New("delivery continuation resolution is already in progress")
	}
	continuation := d.continuation
	d.continuationSettling = true
	d.mu.Unlock()
	result, err := continuation.Resolve(context.WithoutCancel(d.Context()), intent)
	if err == nil && result != DeliveryContinuationReturned && result != DeliveryContinuationConsumed && result != DeliveryContinuationTerminal {
		err = errors.New("delivery continuation returned an invalid resolution")
	}
	d.mu.Lock()
	d.continuationSettling = false
	if err == nil {
		d.continuationResult = result
	}
	d.mu.Unlock()
	return result, err
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
	_, err := d.completeCarrier()
	return err
}

func (d *EventDelivery) completeCarrier() (DeliveryContinuationResolution, error) {
	if d == nil {
		return 0, errors.New("local delivery is required")
	}
	d.mu.Lock()
	if d.completed {
		d.mu.Unlock()
		return 0, ErrAlreadySettled
	}
	if d.settling {
		d.mu.Unlock()
		return 0, errors.New("local delivery completion is already in progress")
	}
	d.settling = true
	d.mu.Unlock()

	result, resolveErr := d.resolveContinuation(DeliveryContinuationReturn)
	if resolveErr != nil {
		d.mu.Lock()
		d.settling = false
		d.mu.Unlock()
		return 0, resolveErr
	}

	err := d.lease.Done()
	if d.companion != nil {
		err = errors.Join(err, d.companion.Done())
	}
	d.mu.Lock()
	d.err = err
	d.completed = true
	d.settling = false
	callbacks := append([]func(){}, d.callbacks...)
	d.callbacks = nil
	d.mu.Unlock()
	for _, callback := range callbacks {
		callback()
	}
	return result, err
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
	contextOwner, ok := OccurrenceFromContext(ctx)
	if !ok || contextOwner == nil || contextOwner == r.owner {
		return newEventDelivery(ctx, event, route, r.owner, r.Begin)
	}
	companion, err := contextOwner.Begin(ctx)
	if err != nil {
		return nil, err
	}
	delivery, err := newEventDelivery(companion.Context(), event, route, r.owner, r.Begin)
	if err != nil {
		_ = companion.Done()
		return nil, err
	}
	delivery.companion = companion
	delivery.ctx = WithOccurrence(delivery.ctx, contextOwner)
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
