package bus_test

import (
	"testing"
	"time"

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

func requireNoBusEvent(t testing.TB, ch <-chan events.Event, context string) {
	t.Helper()
	select {
	case evt := <-ch:
		t.Fatalf("%s: unexpected bus event: %#v", context, evt)
	default:
	}
}

func requireBusEventTypes(t testing.TB, ch <-chan events.Event, context string, want ...events.EventType) {
	t.Helper()
	got := make(map[events.EventType]struct{}, len(want))
	for len(got) < len(want) {
		evt := requireBusEvent(t, ch, context)
		got[evt.Type] = struct{}{}
	}
	for _, eventType := range want {
		if _, ok := got[eventType]; !ok {
			t.Fatalf("%s: received event types = %#v, missing %s", context, got, eventType)
		}
	}
}

func requireSignalBefore(t testing.TB, ch <-chan struct{}, timeout time.Duration, context string) {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ch:
	case <-timer.C:
		t.Fatalf("%s: timed out after %s", context, timeout)
	}
}

func requireErrorBefore(t testing.TB, ch <-chan error, timeout time.Duration, context string) error {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-ch:
		return err
	case <-timer.C:
		t.Fatalf("%s: timed out after %s", context, timeout)
		return nil
	}
}
