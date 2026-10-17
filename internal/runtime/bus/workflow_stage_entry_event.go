package bus

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
)

// LoadWorkflowStageEntryEvent borrows only immutable publication facts from
// the original event owner; it neither replays nor replans the publication.
func (eb *EventBus) LoadWorkflowStageEntryEvent(ctx context.Context, originRunID, eventID string) (events.Event, error) {
	var empty events.Event
	if eb == nil || eb.durable.PreparedEvents == nil {
		return empty, fmt.Errorf("stage entry event requires its native publication reader")
	}
	if originRunID == "" || originRunID != strings.TrimSpace(originRunID) || eventID == "" || eventID != strings.TrimSpace(eventID) {
		return empty, fmt.Errorf("stage entry event requires exact run and event identity")
	}
	prepared, found, err := loadValidatedPreparedPublishEvent(ctx, eb.durable.PreparedEvents, eventID)
	if err != nil {
		return empty, err
	}
	if !found {
		return empty, fmt.Errorf("stage entry event %s is missing", eventID)
	}
	event := prepared.Event.Event()
	if event.RunID() != originRunID {
		return empty, fmt.Errorf("stage entry event belongs to another origin run")
	}
	return event, nil
}
