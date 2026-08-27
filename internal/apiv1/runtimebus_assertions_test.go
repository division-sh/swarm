package apiv1

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
)

const apiv1RuntimeBusAssertionTimeout = time.Second
const apiv1RuntimeBusAbsenceTimeout = 150 * time.Millisecond

func requireAPIV1RuntimeBusEvent(t *testing.T, ch <-chan *runtimebus.LocalDelivery, description string) events.Event {
	t.Helper()
	delivery := requireAPIV1RuntimeBusValue[*runtimebus.LocalDelivery](t, ch, description)
	evt := delivery.Event()
	_ = delivery.Complete()
	return evt
}

func requireAPIV1RuntimeBusEventID(
	t *testing.T,
	ch <-chan *runtimebus.LocalDelivery,
	eventID string,
	description string,
) events.Event {
	t.Helper()
	timer := time.NewTimer(apiv1RuntimeBusAssertionTimeout)
	defer timer.Stop()
	var intervening []string
	for {
		select {
		case delivery := <-ch:
			evt := delivery.Event()
			_ = delivery.Complete()
			if evt.ID() == eventID {
				return evt
			}
			intervening = append(intervening, evt.ID())
		case <-timer.C:
			t.Fatalf(
				"timed out waiting for %s event %s after intervening retries %v",
				description,
				eventID,
				intervening,
			)
		}
	}
}

func requireNoAPIV1RuntimeBusEvent(t *testing.T, ch <-chan *runtimebus.LocalDelivery, description string) {
	t.Helper()
	timer := time.NewTimer(apiv1RuntimeBusAbsenceTimeout)
	defer timer.Stop()
	select {
	case delivery := <-ch:
		_ = delivery.Complete()
		t.Fatalf("%s delivered unexpected runtimebus value: %#v", description, delivery.Event())
	case <-timer.C:
	}
}

func requireAPIV1RuntimeBusValue[T any](t *testing.T, ch <-chan T, description string) T {
	t.Helper()
	timer := time.NewTimer(apiv1RuntimeBusAssertionTimeout)
	defer timer.Stop()

	select {
	case got := <-ch:
		return got
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", description)
	}

	var zero T
	return zero
}
