package bus

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"empireai/internal/events"
)

// EventInterceptor runs deterministic coordination in the publish path.
// It may consume the inbound event and/or emit deferred events.
type EventInterceptor interface {
	Intercept(ctx context.Context, evt events.Event) (passthrough bool, deferred []events.Event, err error)
}

type EventBus struct {
	mu            sync.RWMutex
	channels      map[events.EventType]map[string]chan events.Event
	agentChans    map[string]chan events.Event
	subscriptions map[string][]events.EventType
	routingTable  map[string]*RoutingTable
	interceptors  []EventInterceptor
	interceptorProvider func() []EventInterceptor
	cycleTracker  *OpCoCycleTracker
	store         EventStore
	logger        LoggerHook
}

type EventBusOptions struct {
	Logger              LoggerHook
	CycleTracker        *OpCoCycleTracker
	Interceptors        []EventInterceptor
	InterceptorProvider func() []EventInterceptor
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
	filtered := make([]EventInterceptor, 0, len(opts.Interceptors))
	for _, it := range opts.Interceptors {
		if it != nil {
			filtered = append(filtered, it)
		}
	}
	return &EventBus{
		channels:      make(map[events.EventType]map[string]chan events.Event),
		agentChans:    make(map[string]chan events.Event),
		subscriptions: make(map[string][]events.EventType),
		routingTable:  make(map[string]*RoutingTable),
		store:         store,
		logger:        opts.Logger,
		cycleTracker:  opts.CycleTracker,
		interceptors:  filtered,
		interceptorProvider: opts.InterceptorProvider,
	}
}

func (eb *EventBus) Store() EventStore {
	if eb == nil {
		return nil
	}
	return eb.store
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
	eb.routingTable = make(map[string]*RoutingTable)
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

func (eb *EventBus) SetRoutingTable(verticalID string, table *RoutingTable) error {
	if verticalID == "" || table == nil {
		return errors.New("verticalID and table are required")
	}
	eb.mu.Lock()
	defer eb.mu.Unlock()
	eb.routingTable[verticalID] = table
	return nil
}

func (eb *EventBus) GetRoutingTable(verticalID string) *RoutingTable {
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	return eb.routingTable[verticalID]
}
