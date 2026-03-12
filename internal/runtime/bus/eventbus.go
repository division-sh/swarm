package bus

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"empireai/internal/events"
	runtimecontracts "empireai/internal/runtime/contracts"
	runtimepipeline "empireai/internal/runtime/pipeline"
	"empireai/internal/runtime/semanticview"
)

// EventInterceptor runs deterministic coordination in the publish path.
// It may consume the inbound event and/or emit deferred events.
type EventInterceptor interface {
	Intercept(ctx context.Context, evt events.Event) (passthrough bool, deferred []events.Event, err error)
}

type EventBus struct {
	mu                  sync.RWMutex
	channels            map[events.EventType]map[string]chan events.Event
	agentChans          map[string]chan events.Event
	subscriptions       map[string][]events.EventType
	routeTable          *RouteTable
	interceptors        []EventInterceptor
	interceptorProvider func() []EventInterceptor
	cycleTracker        *OpCoCycleTracker
	store               EventStore
	logger              LoggerHook
	semanticSource      semanticview.Source
}

type EventBusOptions struct {
	Logger              LoggerHook
	CycleTracker        *OpCoCycleTracker
	Interceptors        []EventInterceptor
	InterceptorProvider func() []EventInterceptor
	ContractBundle      semanticview.Source
	RouteTable          *RouteTable
}

const deliverySendTimeout = 250 * time.Millisecond

var ErrStaleRuntimeEpoch = errors.New("stale runtime epoch")

type eventDeliveryPlan struct {
	Event               events.Event
	Recipients          []string
	PersistedRecipients []string
	ExtraDetail         map[string]any
	ContradictionReason string
	BlockedByCycle      bool
	CycleEscalation     *events.Event
}

func NewEventBus(store EventStore) *EventBus {
	return NewEventBusWithOptions(store, EventBusOptions{})
}

func NewEventBusWithOptions(store EventStore, opts EventBusOptions) *EventBus {
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
			panic(err)
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
		cycleTracker:        opts.CycleTracker,
		interceptors:        filtered,
		interceptorProvider: opts.InterceptorProvider,
		semanticSource:      semanticSource,
	}
}

func (eb *EventBus) Store() EventStore {
	if eb == nil {
		return nil
	}
	return eb.store
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
	return table.AddFlowInstance(template, instancePath)
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

func (eb *EventBus) SetCycleTracker(tracker *OpCoCycleTracker) {
	if eb == nil {
		return
	}
	eb.mu.Lock()
	eb.cycleTracker = tracker
	eb.mu.Unlock()
}

func (eb *EventBus) ResetInMemoryState() {
	if eb == nil {
		return
	}
	eb.mu.Lock()
	defer eb.mu.Unlock()
	for _, ch := range eb.agentChans {
		close(ch)
	}
	eb.channels = make(map[events.EventType]map[string]chan events.Event)
	eb.agentChans = make(map[string]chan events.Event)
	eb.subscriptions = make(map[string][]events.EventType)
	eb.routeTable = eb.deriveBootRouteTableLocked()
	if eb.cycleTracker != nil {
		eb.cycleTracker.ResetAll(context.Background())
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

func (eb *EventBus) deriveBootRouteTableLocked() *RouteTable {
	derived, err := DeriveRouteTable(eb.semanticSource)
	if err != nil {
		panic(err)
	}
	return derived
}
