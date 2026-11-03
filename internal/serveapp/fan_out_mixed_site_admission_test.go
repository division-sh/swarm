package serveapp

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/testutil"
)

// Invariant: native MockOnly startup refuses competing handler emit sites
// before handler execution; portable verification alone does not cover startup.
func TestOwnedMockFanOutMixedSitesRefuseBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, site := range []string{"rules", "on_complete"} {
			t.Run(backend+"/"+site, func(t *testing.T) {
				isolateCLIAPIConfigEnv(t)
				unsetStoreSelectorEnv(t)
				stubServeRuntimeWorkspaceLifecycle(t)
				root := t.TempDir()
				for name, body := range map[string]string{
					"manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
					"schema.yaml":   "stages: []\ninputs: [batch.ready]\noutputs: [item.ready, batch.done]\n",
					"events.yaml":   "batch.ready:\n  items: [text]\nitem.ready:\n  value: text\nbatch.done:\n",
					"nodes.yaml":    "scatter:\n  batch.ready:\n    fan_out:\n      items_from: payload.items\n      as: row\n      emit:\n        event: item.ready\n        fields:\n          value: row\n    " + site + ":\n      - emit: batch.done\n",
				} {
					write2376SourceFile(t, root, name, body)
				}
				opts := cliapp.ServeOptions{SourceRoot: root, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: true}
				opts.ConfigPath = writeMockAgentRuntimeConfig(t, "sqlite", filepath.Join(t.TempDir(), "runtime.db"))
				if backend == "postgres" {
					opts.ConfigPath = writeChannelOnboardingPostgresRuntimeConfig(t, testutil.StartEmptyPostgresDSN(t))
					opts.StoreMode, opts.StoreModeSet = backend, true
				}
				process := startOwnedMockLifecycleTestProcess(t, repoRootForTest(), ownedMockLifecycleRoot(t), opts)
				code, exited := process.waitForExit(serveRuntimeReadyTimeout)
				output := process.outputString()
				if !exited || code == 0 {
					t.Fatalf("mixed sites did not refuse startup: exited=%v code=%d\n%s", exited, code, output)
				}
				for _, want := range []string{"backend=" + backend, "handler.fan_out", "handler." + site, "put the fan-out on the rule", "use no rules"} {
					if !strings.Contains(output, want) {
						t.Errorf("refusal omitted %q:\n%s", want, output)
					}
				}
				if process.runtime != nil || serveOutputIsReady(output) {
					t.Fatal("mixed sites reached runtime readiness")
				}
			})
		}
	}
}
