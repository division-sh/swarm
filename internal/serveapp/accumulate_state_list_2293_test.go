package serveapp

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/testutil"
)

// Invariant: an authored state-list accumulator is a writer and its collected
// list is readable through state.items.size() during real served execution.
func TestAccumulateStateListPublicBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := t.TempDir()
			for name, body := range map[string]string{
				"schema.yaml": "name: state-list-collection\nstages:\n  ready: {}\n  done: {final: true}\ninputs: [item.received]\noutputs: [item.recorded]\n",
				"state.yaml":  "items: [Item]\ncount: integer\n",
				"types.yaml":  "Item:\n  id: text\n  message: text\n",
				"events.yaml": "item.received:\n  id: text\n  message: text\nitem.recorded:\n  count: integer\n",
				"nodes.yaml": `collector:
  item.received:
    accumulate: {target: items, key: id}
    filter:
      items_from: state.items
      condition: state.items.size() >= 1
      store_as: computed.filtered
    count:
      items_from: computed.filtered
      store_as: state.count
    advances_to: done
    emit: {event: item.recorded, fields: {count: state.count}}
`,
			} {
				write2376SourceFile(t, root, name, body)
			}
			unsetStoreSelectorEnv(t)
			stubServeRuntimeWorkspaceLifecycle(t)
			opts := cliapp.ServeOptions{SourceRoot: root, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: true, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig()}
			opts.ConfigPath = writeStoreBackendRuntimeConfig(t, "sqlite", filepath.Join(t.TempDir(), "collection.sqlite"))
			if backend == "postgres" {
				opts.ConfigPath = writeChannelOnboardingPostgresRuntimeConfig(t, testutil.StartEmptyPostgresDSN(t))
			}
			var out, errOut bytes.Buffer
			if code := executeCLIFrom(context.Background(), root, []string{"verify", ".", "--portable", "--json", "--config", opts.ConfigPath}, &out, &errOut, nil); code != 0 {
				t.Fatalf("public verify rejected state-list collection: code=%d\n%s\n%s", code, &out, &errOut)
			}
			process := startServeRuntimeTestProcess(t, opts)
			process.waitForReadyLine()
			defer process.stop()
			endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
			published := requireServedEventPublishRPCResult(t, endpoint, map[string]any{
				"event_name": "item.received", "bundle_hash": servedEventPublishFixtureBundleHash(t, root),
				"payload": map[string]any{"id": "first", "message": "collected"}, "idempotency_key": "state-list-first",
			})
			var observed operatorread.OperatorEntityFull
			for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
				var listed operatorread.OperatorEntityListResult
				requireServedJSONRPCResult(t, endpoint, "entity.list", map[string]any{"run_id": published.RunID}, &listed)
				if len(listed.Entities) == 1 {
					requireServedJSONRPCResult(t, endpoint, "entity.get", map[string]any{"run_id": published.RunID, "entity_id": listed.Entities[0].EntityID}, &observed)
					if items, ok := observed.Fields["items"].([]any); ok && len(items) == 1 && observed.Fields["count"] != nil {
						item, ok := items[0].(map[string]any)
						if !ok || item["id"] != "first" || item["message"] != "collected" || observed.Fields["count"] != float64(1) {
							t.Fatalf("collection/readback differs: %#v", observed.Fields)
						}
						return
					}
				}
				time.Sleep(20 * time.Millisecond)
			}
			t.Fatalf("state-list collection did not commit: %#v", observed.Fields)
		})
	}
}
