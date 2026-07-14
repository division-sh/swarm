package bus

import (
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"time"
)

func requireBusEvent(t testing.TB, ch <-chan events.Event, context string) events.Event {
	t.Helper()
	select {
	case evt := <-ch:
		return evt
	default:
		t.Fatalf("%s: expected queued bus event", context)
		return eventtest.RootIngress("", events.EventType(""), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{})
	}
}

func requireNoBusEvent(t testing.TB, ch <-chan events.Event, context string) {
	t.Helper()
	select {
	case evt := <-ch:
		t.Fatalf("%s: unexpected bus event: %#v", context, evt)
	default:
	}
}
