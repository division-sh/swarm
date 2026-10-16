package serveapp

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func readServedGlobalEventNameCount(ctx context.Context, reader servedEventCountReader, eventName string) (int, error) {
	if reader == nil {
		return 0, fmt.Errorf("global event-name observation requires the original selected reader")
	}
	var count int
	err := reader.InspectSnapshot(ctx, func(ctx context.Context) error {
		var err error
		count, err = storetest.CountEventNameStorage(ctx, reader, eventName)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func TestServedGlobalEventNameCountRefusesMissingOwner(t *testing.T) {
	if count, err := readServedGlobalEventNameCount(context.Background(), nil, "platform.boot"); count != 0 || err == nil {
		t.Fatalf("missing owner count=%d err=%v", count, err)
	}
}

func TestServedGlobalEventNameObservationCountsAllRunsAndRefusesLifetimeFailureBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			h := newChannelOnboardingE2EHarness(t, backend, false)
			h.opts.SourceRoot = canonicalrouting.CopyRootIngressServedFollowUp(t)
			h.opts.AbandonActiveRuns = false
			h.start(t)
			t.Cleanup(func() { h.stop(t) })
			bundle := servedEventPublishFixtureBundleHash(t, h.opts.SourceRoot)
			first := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{"event_name": "item.received", "bundle_hash": bundle, "payload": map[string]any{"item_id": "first"}, "idempotency_key": "global-name-first"})
			second := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{"event_name": "item.received", "bundle_hash": bundle, "payload": map[string]any{"item_id": "second"}, "idempotency_key": "global-name-second"})
			if first.RunID == second.RunID {
				t.Fatal("global count control requires two distinct genuine runs")
			}
			driver := "sqlite"
			if backend == servedparity.BackendExplicitPostgres {
				driver = "postgres"
			}
			reader := openChannelAnchorStorageAtLocation(t, driver, h.storeDSN)
			for name, want := range map[string]int{"item.received": 2, "ITEM.RECEIVED": 0, "item.received ": 0, "missing": 0, "": 0} {
				if count, err := reader.CountEventNameStorage(context.Background(), name); err != nil || count != want {
					t.Fatalf("exact global name %q=%d/%v, want%d", name, count, err, want)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if count, err := reader.CountEventNameStorage(ctx, "item.received"); count != 0 || !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled observation returned evidence or lost context: %d/%v", count, err)
			}
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
			if count, err := reader.CountEventNameStorage(context.Background(), "item.received"); count != 0 || err == nil {
				t.Fatalf("closed observation retained authority: %d/%v", count, err)
			}
			requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{"event_name": "item.received", "bundle_hash": bundle, "payload": map[string]any{"item_id": "survivor"}, "idempotency_key": "global-name-survivor"})
			fresh := openChannelAnchorStorageAtLocation(t, driver, h.storeDSN)
			if count, err := fresh.CountEventNameStorage(context.Background(), "item.received"); count != 3 || err != nil {
				t.Fatalf("closing inspection retired writer or lost global history: %d/%v", count, err)
			}
		})
	}
}
