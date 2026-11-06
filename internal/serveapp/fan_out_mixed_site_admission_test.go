package serveapp

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/tests/support/testutil"
)

// Invariant: native MockOnly startup refuses competing handler emit sites
// before handler execution; portable verification alone does not cover startup.
// A legal rule-level fan-out persists its count through the same native owner.
func TestOwnedMockFanOutMixedSitesRefuseBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, site := range []string{"rules", "on_complete", "rule_count"} {
			t.Run(backend+"/"+site, func(t *testing.T) {
				isolateCLIAPIConfigEnv(t)
				unsetStoreSelectorEnv(t)
				stubServeRuntimeWorkspaceLifecycle(t)
				root := t.TempDir()
				nodes := "scatter:\n  batch.ready:\n    fan_out:\n      items_from: payload.items\n      as: row\n      emit:\n        event: item.ready\n        fields:\n          value: row\n    " + site + ":\n      - emit: batch.done\n"
				if site == "rule_count" {
					write2376SourceFile(t, root, "state.yaml", "observed_count: integer\n")
					nodes = `scatter:
  batch.ready:
    rules:
      - fan_out:
          items_from: payload.items
          as: row
          emit:
            event: item.ready
            fields:
              value: row
        data_accumulation:
          writes:
            - target_field: observed_count
              value: fan_out.count
collector:
  item.ready: {}
`
				}
				for name, body := range map[string]string{
					"manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
					"schema.yaml":   "stages: []\ninputs: [batch.ready]\noutputs: [item.ready, batch.done]\n",
					"events.yaml":   "batch.ready:\n  items: [text]\nitem.ready:\n  value: text\nbatch.done:\n",
					"nodes.yaml":    nodes,
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
				if site == "rule_count" {
					process.waitForReadyLine()
					endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
					seed := requireServedEventPublishRPCResult(t, endpoint, map[string]any{
						"bundle_hash": servedEventPublishFixtureBundleHash(t, root), "event_name": "batch.ready",
						"payload": map[string]any{"items": []string{"first", "second", "third"}}, "idempotency_key": "r84-rule-count",
					})
					var diagnosis cliapp.DiagnosticRunDiagnosisResult
					settled := false
					for deadline := time.Now().Add(servedProofPollDeadline); time.Now().Before(deadline); {
						requireServedJSONRPCResult(t, endpoint, "run.diagnose", map[string]any{"run_id": seed.RunID}, &diagnosis)
						if len(diagnosis.FailedDeliveries) != 0 {
							t.Fatalf("rule-level count delivery failed: %+v\n%s", diagnosis, process.outputString())
						}
						if diagnosis.TestQuiescence != nil && cliapp.BoolPointerValue(diagnosis.TestQuiescence.Ready) && diagnosis.FanOut.Settled == 3 {
							settled = true
							break
						}
						time.Sleep(10 * time.Millisecond)
					}
					if !settled || diagnosis.FanOut.Intents != 1 || diagnosis.FanOut.Cardinality != 3 || diagnosis.FanOut.Committed != 3 || diagnosis.FanOut.Unsettled != 0 || diagnosis.FanOut.Owed != 0 {
						t.Fatalf("rule-level fan-out did not settle three items: %+v\n%s", diagnosis, process.outputString())
					}
					var entity operatorread.OperatorEntityFull
					requireServedJSONRPCResult(t, endpoint, "entity.get", map[string]any{"run_id": seed.RunID, "entity_id": seed.RunID}, &entity)
					if entity.Entity.RunID != seed.RunID || entity.Entity.EntityID != seed.RunID || entity.Fields["observed_count"] != float64(3) {
						t.Fatalf("post-fan-out count was not persisted: %+v", entity)
					}
					var page fanoutobligation.ListPage
					requireServedJSONRPCResult(t, endpoint, "run.fan_out.list", map[string]any{"run_id": seed.RunID}, &page)
					if err := page.Validate(fanoutobligation.ListQuery{RunID: seed.RunID}); err != nil || len(page.Intents) != 1 || page.NextCursor != "" {
						t.Fatalf("rule-level fan-out readback: %+v err=%v", page, err)
					}
					if intent := page.Intents[0]; intent.Status != fanoutobligation.StatusClosed || intent.Cardinality != 3 || intent.Cursor != 3 || intent.Owed != 0 {
						t.Fatalf("rule-level durable intent disagrees with count: %+v", intent)
					}
					if code := process.stop(); code != 0 {
						t.Fatalf("rule-level count shutdown: code=%d\n%s", code, process.outputString())
					}
					return
				}
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
