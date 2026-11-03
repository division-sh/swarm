package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/testutil/llmapi"
)

func TestContextCompactionPreservesMemoryAcrossRestartBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			unsetStoreSelectorEnv(t)
			stubServeRuntimeWorkspaceLifecycle(t)
			configureHostWorkspaceProofLifecycle(t)
			denyProviderJourneyExternalNetwork(t)
			root := canonicalrouting.CopyRootIngressServedSessionCleanup(t)
			const model = "claude-3-haiku-20240307"
			const summary = "Context checkpoint: the first fact is cedar-731. Continue the pending conversation."
			var summaries, turns atomic.Int32
			var compacted atomic.Bool
			var provider *llmapi.Server
			provider = llmapi.New(t, llmapi.Anthropic, "/v1/messages", "compaction-key", func(request llmapi.Request) (llmapi.Reply, error) {
				var body struct {
					Messages []struct {
						Content json.RawMessage `json:"content"`
					} `json:"messages"`
					Tools json.RawMessage `json:"tools"`
				}
				if err := json.Unmarshal(request.Body, &body); err != nil || len(body.Messages) == 0 {
					return llmapi.Reply{}, fmt.Errorf("compaction journey has no provider messages: %v", err)
				}
				if bytes.Contains(body.Messages[len(body.Messages)-1].Content, []byte("Context compaction:")) {
					var first struct{ Tools json.RawMessage }
					if err := json.Unmarshal(provider.Requests()[0].Body, &first); err != nil || !bytes.Equal(first.Tools, body.Tools) || !bytes.Contains(request.Body, []byte("cedar-731")) {
						return llmapi.Reply{}, fmt.Errorf("summary lost its original facts or frozen tools: %v", err)
					}
					summaries.Add(1)
					return llmapi.Reply{Model: model, Text: summary, InputTokens: 125000, OutputTokens: 20}, nil
				}
				turn := turns.Add(1)
				if turn > 1 && bytes.Contains(request.Body, []byte(summary)) && !bytes.Contains(request.Body, []byte(strings.Repeat("shadowed ", 100))) {
					compacted.Store(true)
				}
				return llmapi.Reply{ID: fmt.Sprintf("compaction-response-%d", turn), Model: model,
					Text: fmt.Sprintf("ordinary response %d", turn), InputTokens: 125000, OutputTokens: 8}, nil
			})
			redirectExternalHosts(t, map[string]string{"api.anthropic.com": provider.URL})
			probe := lifecycletest.New(t, lifecycletest.WithTimeout(servedEventPublishLifecycleProbeWaitTimeout))
			opts := cliapp.ServeOptions{SourceRoot: root, ConfigPath: writeServeRuntimeTestConfig(t), SelfCheck: true, Verbose: true,
				APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", TestLifecycleProbe: probe}
			storeConfig := fmt.Sprintf("store:\n  backend: sqlite\n  sqlite:\n    path: %s\n", filepath.Join(t.TempDir(), "store.db"))
			if backend != servedparity.BackendDefaultSQLite {
				configureCompletion2270PostgresFixture(t)
				opts.StoreMode, opts.StoreModeSet = "postgres", true
				storeConfig = ""
			}
			configuration := fmt.Sprintf("runtime:\n  recovery_on_startup: true\n%sllm:\n  backend: anthropic\n  models:\n    regular: {anthropic: %s}\n  session:\n    lock_ttl: 10s\n    rotate_on_parse_failures: 3\n", storeConfig, model)
			if err := os.WriteFile(opts.ConfigPath, []byte(withTestProviderTriggerPlatformInventory(t, configuration)), 0o600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
			credentials, err := runtimecredentials.NewFileStore(os.Getenv("SWARM_CREDENTIALS_FILE"))
			if err != nil {
				t.Fatal(err)
			}
			if err := credentials.Set(context.Background(), "ANTHROPIC_API_KEY", "compaction-key"); err != nil {
				t.Fatal(err)
			}
			process := startServeRuntimeTestProcess(t, opts)
			defer func() {
				if process != nil && process.stop() != 0 {
					t.Error("compaction journey failed to stop")
				}
			}()
			process.waitForReadyLine()
			endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
			seed := requireServedEventPublishRPCResult(t, endpoint, map[string]any{
				"event_name": "item.received", "bundle_hash": servedEventPublishFixtureBundleHash(t, root),
				"payload": map[string]any{"item_id": "compaction"}, "idempotency_key": "compaction-seed",
			})
			probe.Expect(seed.EventID).NodeDelivered(identitytest.RootNode(t, "item-handler").Key()).Within(servedEventPublishLifecycleProbeWaitTimeout)
			inputs := []string{"first fact cedar-731: " + strings.Repeat("shadowed ", 55555), "kept-tail: " + strings.Repeat("recent ", 8571), "What was the first fact?"}
			var sessionID string
			for i, input := range inputs {
				if i == 2 {
					if code := process.stop(); code != 0 {
						t.Fatalf("stop before checkpoint replay: exit=%d", code)
					}
					process = nil
					process = startServeRuntimeTestProcess(t, opts)
					process.waitForReadyLine()
					endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
				}
				requireServedEventPublishRPCResult(t, endpoint, map[string]any{
					"event_name": "item.agent_hold", "run_id": seed.RunID, "source_event_id": seed.EventID,
					"payload": map[string]any{"note": input}, "idempotency_key": fmt.Sprintf("compaction-turn-%d", i),
				})
				current := requireProviderMemoryTurns(t, endpoint, seed.RunID, i+1)
				if sessionID != "" && current != sessionID {
					t.Fatalf("compaction replaced the memory session: %s -> %s", sessionID, current)
				}
				sessionID = current
				if i > 0 && (summaries.Load() != 1 || !compacted.Load()) {
					t.Fatalf("context boundary was not summarized once and replayed: summaries=%d compacted=%t request_bytes=%d", summaries.Load(), compacted.Load(), len(provider.Requests()[0].Body))
				}
			}
			if turns.Load() != 3 || summaries.Load() != 1 {
				t.Fatalf("restart regenerated maintenance or business work: turns=%d summaries=%d", turns.Load(), summaries.Load())
			}
		})
	}
}
