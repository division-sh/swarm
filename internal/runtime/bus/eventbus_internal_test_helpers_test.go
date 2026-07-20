package bus

import (
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"time"
)

func requireBusEvent(t testing.TB, ch <-chan *LocalDelivery, context string) events.Event {
	t.Helper()
	select {
	case delivery := <-ch:
		evt := delivery.Event()
		_ = delivery.Complete()
		return evt
	default:
		t.Fatalf("%s: expected queued bus event", context)
		return eventtest.RunCreatingRootIngress("", events.EventType(""), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{})
	}
}

func requireNoBusEvent(t testing.TB, ch <-chan *LocalDelivery, context string) {
	t.Helper()
	select {
	case delivery := <-ch:
		_ = delivery.Complete()
		t.Fatalf("%s: unexpected bus event: %#v", context, delivery.Event())
	default:
	}
}
