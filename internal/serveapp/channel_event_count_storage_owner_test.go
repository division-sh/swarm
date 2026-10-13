package serveapp

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestChannelRunNamedEventObservationKeepsPhysicalScopeAndJoinedLifetimeBothStores(t *testing.T) {
	if _, grantsClose := reflect.TypeFor[storetest.ChannelAnchorObservation]().MethodByName("Close"); grantsClose {
		t.Fatal("borrowed physical observer grants child-owned close authority")
	}
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			var reader storetest.ChannelAnchorObservation
			var runID string
			t.Run("parent", func(t *testing.T) {
				h, bundleHash := startChannelAnchorPublicJourney(t, backend, "event-count-owner-token", false, 0, nil)
				seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{"event_name": "work.requested", "bundle_hash": bundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "count-first"})
				runID = seed.RunID
				requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{"event_name": "work.requested", "run_id": runID, "payload": map[string]any{"seed": true}, "idempotency_key": "count-second"})
				reader = channelAnchorStorageObservation(t, h)
				assertChannelRunNamedEventScope(t, reader, runID)
				cancelled, cancel := context.WithCancel(context.Background())
				cancel()
				if count, err := reader.CountRunNamedEventStorage(cancelled, runID, "work.requested"); !errors.Is(err, context.Canceled) || count != 0 {
					t.Fatalf("cancelled observer yielded provisional evidence: %d %v", count, err)
				}
			})
			if reader == nil || runID == "" {
				t.Fatal("native public observer was not reached")
			}
			if count, err := reader.CountRunNamedEventStorage(context.Background(), runID, "work.requested"); err == nil || count != 0 {
				t.Fatalf("parent cleanup left event observer live: %d %v", count, err)
			}
		})
	}
}

func assertChannelRunNamedEventScope(t *testing.T, reader storetest.ChannelAnchorObservation, runID string) {
	t.Helper()
	for _, scope := range []struct {
		run, name string
		want      int
	}{
		{runID, "work.requested", 2}, {uuid.NewString(), "work.requested", 0},
		{runID, strings.ToUpper("work.requested"), 0}, {runID, "work.requested ", 0}, {runID, "", 0},
	} {
		if count, err := reader.CountRunNamedEventStorage(context.Background(), scope.run, scope.name); err != nil || count != scope.want {
			t.Fatalf("native run/name scope %s/%q=%d/%v want%d", scope.run, scope.name, count, err, scope.want)
		}
	}
}
