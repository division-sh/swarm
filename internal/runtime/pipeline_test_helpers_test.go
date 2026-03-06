package runtime

import (
	"testing"
	"time"

	"empireai/internal/events"
)

func assertNoEventType(t *testing.T, ch <-chan events.Event, typ string, d time.Duration) {
	t.Helper()
	select {
	case evt := <-ch:
		if string(evt.Type) == typ {
			t.Fatalf("unexpected %s event: %+v", typ, evt)
		}
	case <-time.After(d):
	}
}
