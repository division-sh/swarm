package bus

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimecorrelation "swarm/internal/runtime/correlation"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
)

// EventInterceptor runs deterministic coordination in the publish path.
// It may consume the inbound event and/or emit deferred events.
type EventInterceptor interface {
	Intercept(ctx context.Context, evt events.Event) (passthrough bool, deferred []events.Event, err error)
}

type PayloadValidator func(eventType string, payload []byte) error

type EventBus struct {
	mu                  sync.RWMutex
	channels            map[events.EventType]map[string]chan events.Event
	agentChans          map[string]chan events.Event
	subscriptions       map[string][]events.EventType
	routeTable          *RouteTable
	interceptors        []EventInterceptor
	interceptorProvider func() []EventInterceptor
	store               EventStore
	logger              LoggerHook
	semanticSource      semanticview.Source
	payloadValidator    PayloadValidator
	outboxSweeperActive bool
	inFlightPublishes   atomic.Int64
}

type EventBusOptions struct {
	Logger              LoggerHook
	Interceptors        []EventInterceptor
	InterceptorProvider func() []EventInterceptor
	ContractBundle      semanticview.Source
	RouteTable          *RouteTable
	PayloadValidator    PayloadValidator
}

const deliverySendTimeout = 250 * time.Millisecond

var ErrStaleRuntimeEpoch = errors.New("stale runtime epoch")

type eventDeliveryPlan struct {
	Event               events.Event
	Recipients          []string
	PersistedRecipients []string
	RoutedRecipients    []Subscriber
	SubscribedRecipients []string
	ExtraDetail         map[string]any
	ContradictionReason string
	BlockedByCycle      bool
	CycleEscalation     *events.Event
}

func NewEventBus(store EventStore) (*EventBus, error) {
	return NewEventBusWithOptions(store, EventBusOptions{})
}

func NewEventBusWithOptions(store EventStore, opts EventBusOptions) (*EventBus, error) {
	if store == nil {
		store = InMemoryEventStore{}
	}
	semanticSource := opts.ContractBundle
	if semanticSource == nil {
		semanticSource = runtimepipeline.DefaultWorkflowSemanticSourceOrNil()
	}
	filtered := make([]EventInterceptor, 0, len(opts.Interceptors))
	for _, it := range opts.Interceptors {
		if it != nil {
			filtered = append(filtered, it)
		}
	}
	routeTable := opts.RouteTable
	if routeTable == nil {
		derived, err := DeriveRouteTable(semanticSource)
		if err != nil {
			return nil, err
		}
		routeTable = derived
	}
	return &EventBus{
		channels:            make(map[events.EventType]map[string]chan events.Event),
		agentChans:          make(map[string]chan events.Event),
		subscriptions:       make(map[string][]events.EventType),
		routeTable:          routeTable,
		store:               store,
		logger:              opts.Logger,
		interceptors:        filtered,
		interceptorProvider: opts.InterceptorProvider,
		semanticSource:      semanticSource,
		payloadValidator:    opts.PayloadValidator,
	}, nil
}

func (eb *EventBus) Store() EventStore {
	if eb == nil {
		return nil
	}
	return eb.store
}

func (eb *EventBus) MarkDeliveryInProgress(ctx context.Context, agentID, sessionID string) error {
	if eb == nil || eb.store == nil {
		return nil
	}
	inbound, ok := runtimecorrelation.InboundEventFromContext(ctx)
	if !ok || strings.TrimSpace(inbound.ID) == "" || strings.TrimSpace(agentID) == "" {
		return nil
	}
	type deliveryProgressWriter interface {
		MarkEventDeliveryInProgress(ctx context.Context, eventID, agentID, sessionID string) error
	}
	writer, ok := eb.store.(deliveryProgressWriter)
	if !ok || writer == nil {
		return nil
	}
	return writer.MarkEventDeliveryInProgress(ctx, inbound.ID, agentID, sessionID)
}

func (eb *EventBus) RouteTable() *RouteTable {
	if eb == nil {
		return nil
	}
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	return eb.routeTable
}

func (eb *EventBus) AddFlowInstance(template runtimecontracts.SystemNodeContract, instancePath string) error {
	if eb == nil {
		return errors.New("event bus is required")
	}
	eb.mu.RLock()
	table := eb.routeTable
	eb.mu.RUnlock()
	if table == nil {
		return errors.New("route table is not initialized")
	}
	if err := table.AddFlowInstance(template, instancePath); err != nil {
		return err
	}
	persister, ok := eb.store.(FlowInstanceRoutePersistence)
	if !ok {
		return nil
	}
	instancePath = strings.Trim(strings.TrimSpace(instancePath), "/")
	routes := table.MaterializedRoutes(instancePath)
	if len(routes) == 0 {
		return nil
	}
	for _, route := range routes {
		if err := persister.UpsertFlowInstanceRoute(context.Background(), route); err != nil {
			_ = persister.DeleteFlowInstanceRoute(context.Background(), route.TemplateID, route.InstanceID)
			table.RemoveFlowInstance(route.TemplateID, route.InstanceID)
			return err
		}
	}
	return nil
}

func (eb *EventBus) RemoveFlowInstance(templateID, instanceID string) error {
	if eb == nil {
		return errors.New("event bus is required")
	}
	eb.mu.RLock()
	table := eb.routeTable
	eb.mu.RUnlock()
	if table == nil {
		return errors.New("route table is not initialized")
	}
	if persister, ok := eb.store.(FlowInstanceRoutePersistence); ok {
		if err := persister.DeleteFlowInstanceRoute(context.Background(), templateID, instanceID); err != nil {
			return err
		}
	}
	table.RemoveFlowInstance(templateID, instanceID)
	return nil
}

func (eb *EventBus) SetLoggerHook(logger LoggerHook) {
	if eb == nil {
		return
	}
	eb.mu.Lock()
	eb.logger = logger
	eb.mu.Unlock()
}

func (eb *EventBus) SetInterceptors(interceptors ...EventInterceptor) {
	if eb == nil {
		return
	}
	filtered := make([]EventInterceptor, 0, len(interceptors))
	for _, it := range interceptors {
		if it != nil {
			filtered = append(filtered, it)
		}
	}
	eb.mu.Lock()
	eb.interceptors = filtered
	eb.interceptorProvider = nil
	eb.mu.Unlock()
}

func (eb *EventBus) ResetInMemoryState() error {
	if eb == nil {
		return nil
	}
	eb.mu.Lock()
	defer eb.mu.Unlock()
	for _, ch := range eb.agentChans {
		close(ch)
	}
	eb.channels = make(map[events.EventType]map[string]chan events.Event)
	eb.agentChans = make(map[string]chan events.Event)
	eb.subscriptions = make(map[string][]events.EventType)
	routeTable, err := eb.deriveBootRouteTableLocked()
	if err != nil {
		return err
	}
	eb.routeTable = routeTable
	eb.inFlightPublishes.Store(0)
	return nil
}

func (eb *EventBus) WaitForQuiescence(ctx context.Context) error {
	if eb == nil {
		return nil
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if eb.inFlightPublishes.Load() == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (eb *EventBus) Subscribe(agentID string, eventTypes ...events.EventType) <-chan events.Event {
	ch := make(chan events.Event, 128)
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if existing, ok := eb.agentChans[agentID]; ok {
		ch = existing
	} else {
		eb.agentChans[agentID] = ch
	}

	for _, et := range eventTypes {
		eb.subscriptions[agentID] = AppendUniqueEventType(eb.subscriptions[agentID], et)
		if eb.channels[et] == nil {
			eb.channels[et] = make(map[string]chan events.Event)
		}
		eb.channels[et][agentID] = ch
	}
	return ch
}

func (eb *EventBus) Unsubscribe(agentID string) {
	agentID = strings.TrimSpace(agentID)
	if eb == nil || agentID == "" {
		return
	}
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if ch, ok := eb.agentChans[agentID]; ok {
		delete(eb.agentChans, agentID)
		close(ch)
	}
	delete(eb.subscriptions, agentID)
	for et := range eb.channels {
		delete(eb.channels[et], agentID)
		if len(eb.channels[et]) == 0 {
			delete(eb.channels, et)
		}
	}
}

func (eb *EventBus) deriveBootRouteTableLocked() (*RouteTable, error) {
	derived, err := DeriveRouteTable(eb.semanticSource)
	if err != nil {
		return nil, err
	}
	return derived, nil
}
