package runforkrevision

import "github.com/division-sh/swarm/internal/events"

const excludedEventName = string(events.EventTypePlatformRuntimeLog)

// CapturesEvent selects fork evidence from an already-validated event record.
// Runtime logs remain physical diagnostics, including their event-count delta.
func CapturesEvent(eventName string) bool {
	return eventName != excludedEventName
}
