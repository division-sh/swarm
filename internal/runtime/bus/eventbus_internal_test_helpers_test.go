package bus

import (
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"time"
)

func requireBusEvent(t testing.TB, ch <-chan events.Event, context string) events.Event {
	t.Helper()
	select {
	case evt := <-ch:
		return evt
	default:
		t.Fatalf("%s: expected queued bus event", context)
		return events.NewProjectionEvent("", events.EventType(""), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{})
	}
}
