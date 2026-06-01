package bus

import (
	"testing"

	"swarm/internal/events"
)

func requireBusEvent(t testing.TB, ch <-chan events.Event, context string) events.Event {
	t.Helper()
	select {
	case evt := <-ch:
		return evt
	default:
		t.Fatalf("%s: expected queued bus event", context)
		return events.Event{}
	}
}
