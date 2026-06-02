package apiv1

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
)

const apiv1RuntimeBusAssertionTimeout = time.Second
const apiv1RuntimeBusAbsenceTimeout = 150 * time.Millisecond

func requireAPIV1RuntimeBusEvent(t *testing.T, ch <-chan events.Event, description string) events.Event {
	t.Helper()
	return requireAPIV1RuntimeBusValue[events.Event](t, ch, description)
}

func requireNoAPIV1RuntimeBusEvent(t *testing.T, ch <-chan events.Event, description string) {
	t.Helper()
	requireNoAPIV1RuntimeBusValue[events.Event](t, ch, description)
}

func requireAPIV1RuntimeBusSignal(t *testing.T, ch <-chan string, description string) string {
	t.Helper()
	return requireAPIV1RuntimeBusValue[string](t, ch, description)
}

func requireNoAPIV1RuntimeBusSignal(t *testing.T, ch <-chan string, description string) {
	t.Helper()
	requireNoAPIV1RuntimeBusValue[string](t, ch, description)
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

func requireNoAPIV1RuntimeBusValue[T any](t *testing.T, ch <-chan T, description string) {
	t.Helper()
	timer := time.NewTimer(apiv1RuntimeBusAbsenceTimeout)
	defer timer.Stop()

	select {
	case got := <-ch:
		t.Fatalf("%s delivered unexpected runtimebus value: %#v", description, got)
	case <-timer.C:
	}
}
