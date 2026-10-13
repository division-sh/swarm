package serveapp

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestServedRunNamedEventObservationKeepsPhysicalScopeAndSnapshotFailureBothStores(t *testing.T) {
	if _, grantsClose := reflect.TypeFor[servedEventCountReader]().MethodByName("Close"); grantsClose {
		t.Fatal("borrowed scalar reader grants close authority")
	}
	if count, err := readServedEventNameCountForRun(context.Background(), nil, uuid.NewString(), "work.requested"); err == nil || count != 0 {
		t.Fatalf("missing selected owner yielded evidence: %d %v", count, err)
	}
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			h, bundle := startChannelAnchorPublicJourney(t, backend, "served-count-owner-token", false, 0, nil)
			first := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{"event_name": "work.requested", "bundle_hash": bundle, "payload": map[string]any{"seed": true}, "idempotency_key": "count-first"})
			requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{"event_name": "work.requested", "run_id": first.RunID, "payload": map[string]any{"seed": true}, "idempotency_key": "count-second"})
			driver := "sqlite"
			if backend == servedparity.BackendExplicitPostgres {
				driver = "postgres"
			}
			owner, err := storetest.OpenReleaseProcessReadOnlyInspection(driver, h.storeDSN)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			})
			assertServedRunNamedEventPhysicalScope(t, owner, first.RunID)
			cancelled, cancel := context.WithCancel(context.Background())
			cancel()
			if count, err := readServedEventNameCountForRun(cancelled, owner, first.RunID, "work.requested"); !errors.Is(err, context.Canceled) || count != 0 {
				t.Fatalf("cancelled snapshot yielded provisional evidence: %d %v", count, err)
			}
			if err := owner.Close(); err != nil {
				t.Fatal(err)
			}
			if count, err := readServedEventNameCountForRun(context.Background(), owner, first.RunID, "work.requested"); err == nil || count != 0 {
				t.Fatalf("closed snapshot yielded provisional evidence: %d %v", count, err)
			}
		})
	}
}

func assertServedRunNamedEventPhysicalScope(t *testing.T, reader servedEventCountReader, runID string) {
	t.Helper()
	for _, scope := range []struct {
		run, name string
		want      int
	}{
		{runID, "work.requested", 2}, {uuid.NewString(), "work.requested", 0},
		{runID, "WORK.REQUESTED", 0}, {runID, "work.requested ", 0}, {runID, "", 0},
	} {
		if count, err := readServedEventNameCountForRun(context.Background(), reader, scope.run, scope.name); err != nil || count != scope.want {
			t.Fatalf("physical run/name count %s/%q=%d/%v want%d", scope.run, scope.name, count, err, scope.want)
		}
	}
}
