package runtime

import (
	"errors"
	"strings"

	"empireai/internal/events"
)

func (eb *EventBus) Subscribe(agentID string, eventTypes ...events.EventType) <-chan events.Event {
	ch := make(chan events.Event, 128)
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if existing, ok := eb.agentChans[agentID]; ok {
		ch = existing
	} else {
		eb.agentChans[agentID] = ch
	}

	// Track subscription patterns for wildcard delivery.
	for _, et := range eventTypes {
		eb.subscriptions[agentID] = appendUniqueEventType(eb.subscriptions[agentID], et)
	}

	for _, et := range eventTypes {
		if eb.channels[et] == nil {
			eb.channels[et] = make(map[string]chan events.Event)
		}
		eb.channels[et][agentID] = ch
	}
	return ch
}

// Unsubscribe removes an agent from all in-memory EventBus subscription maps.
// The existing channel is closed and discarded to prevent resource leaks after
// teardown/reconfigure cycles.
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
