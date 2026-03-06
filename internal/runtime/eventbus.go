package runtime

import (
	"context"
	"errors"
	"sync"
	"time"

	"empireai/internal/events"
	runtimebus "empireai/internal/runtime/bus"
)

type EventStore = runtimebus.EventStore
type ActiveAgentLister = runtimebus.ActiveAgentLister
type PipelineReceiptPersistence = runtimebus.PipelineReceiptPersistence
type AtomicEventPersistence = runtimebus.AtomicEventPersistence
type TransactionalEventStore = runtimebus.TransactionalEventStore

// EventInterceptor runs deterministic runtime coordination in the publish path.
// It may consume the inbound event and/or emit deferred events.
type EventInterceptor interface {
	Intercept(ctx context.Context, evt events.Event) (passthrough bool, deferred []events.Event, err error)
}

type InMemoryEventStore = runtimebus.InMemoryEventStore

type EventBus struct {
	mu            sync.RWMutex
	channels      map[events.EventType]map[string]chan events.Event
	agentChans    map[string]chan events.Event
	subscriptions map[string][]events.EventType
	routingTable  map[string]*RoutingTable
	interceptors  []EventInterceptor
	cycleTracker  *OpCoCycleTracker
	store         EventStore
	logger        *RuntimeLogger
}

const deliverySendTimeout = 250 * time.Millisecond

var ErrStaleRuntimeEpoch = errors.New("stale runtime epoch")
var factoryEventPrefixes = runtimebus.FactoryEventPrefixes

type RoutingTable = runtimebus.RoutingTable
type Route = runtimebus.Route

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
	if store == nil {
		store = runtimebus.InMemoryEventStore{}
	}
	return &EventBus{
		channels:      make(map[events.EventType]map[string]chan events.Event),
		agentChans:    make(map[string]chan events.Event),
		subscriptions: make(map[string][]events.EventType),
		routingTable:  make(map[string]*RoutingTable),
		store:         store,
	}
}

func (eb *EventBus) SetRuntimeLogger(logger *RuntimeLogger) {
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

// ResetInMemoryState clears process-local EventBus state (subscriptions,
// delivery channels, and routing tables) without touching the persistent store.
// This is used during runtime reset flows where DB state is truncated and
// agents are re-seeded from scratch.
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
