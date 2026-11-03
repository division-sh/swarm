package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/testutil/llmapi"
)

func TestContextCompactionPreservesMemoryAcrossRestartBothStores(t *testing.T) {
	for _, scenario := range []struct {
		name, profile, model string
		inputs               int
	}{
		{"summary", "anthropic", "claude-3-haiku-20240307", 2},
		{"summary_overflow", "anthropic", "claude-3-haiku-20240307", 2},
		{"summary_overflow_deepseek", "openai_compatible", "deepseek-flash", 2},
		{"anthropic_native", "anthropic", "claude-opus-4-6", 7},
		{"openai_native", "openai_responses", "gpt-5.4-mini", 2},
	} {
		for _, backend := range servedparity.RequiredBackends {
			t.Run(scenario.name+"/"+string(backend), func(t *testing.T) {
				isolateCLIAPIConfigEnv(t)
				unsetStoreSelectorEnv(t)
				stubServeRuntimeWorkspaceLifecycle(t)
				configureHostWorkspaceProofLifecycle(t)
				denyProviderJourneyExternalNetwork(t)
				root := canonicalrouting.CopyRootIngressServedSessionCleanup(t)
				model := scenario.model
				const summary = "Context checkpoint: the first fact is cedar-731. Continue the pending conversation."
				var summaries, turns atomic.Int32
				var rejected atomic.Bool
				var compacted atomic.Bool
				var providerURL string
				var firstRequestLength func() int
				if strings.HasPrefix(scenario.name, "summary") {
					var provider *llmapi.Server
					protocol, providerPath := llmapi.Anthropic, "/v1/messages"
					if scenario.profile == "openai_compatible" {
						protocol, providerPath = llmapi.OpenAICompatible, "/v1/chat/completions"
					}
					provider = llmapi.New(t, protocol, providerPath, "compaction-key", func(request llmapi.Request) (llmapi.Reply, error) {
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
							return llmapi.Reply{ID: "compaction-summary-1", Model: model, Text: summary, InputTokens: 125000, OutputTokens: 20}, nil
						}
						if strings.HasPrefix(scenario.name, "summary_overflow") && turns.Load() == 1 && summaries.Load() == 0 && !rejected.Swap(true) {
							return llmapi.Reply{StatusCode: http.StatusBadRequest, ErrorBody: `{"error":{"code":"context_length_exceeded","message":"context limit exceeded"}}`}, nil
						}
						turn := turns.Add(1)
						if turn > 1 && bytes.Contains(request.Body, []byte(summary)) && !bytes.Contains(request.Body, []byte(strings.Repeat("shadowed ", 100))) {
							compacted.Store(true)
						}
						return llmapi.Reply{ID: fmt.Sprintf("compaction-response-%d", turn), Model: model,
							Text: fmt.Sprintf("ordinary response %d", turn), InputTokens: 125000, OutputTokens: 8}, nil
					})
					providerURL = provider.URL
					firstRequestLength = func() int { return len(provider.Requests()[0].Body) }
				} else {
					providerURL, firstRequestLength = nativeContextCompactionProvider(t, scenario.name, model, &summaries, &turns, &compacted)
				}
				redirectExternalHosts(t, map[string]string{"api.anthropic.com": providerURL, "api.openai.com": providerURL, "api.deepseek.com": providerURL})
				probe := lifecycletest.New(t, lifecycletest.WithTimeout(servedEventPublishLifecycleProbeWaitTimeout))
				opts := cliapp.ServeOptions{SourceRoot: root, ConfigPath: writeServeRuntimeTestConfig(t), SelfCheck: true, Verbose: true,
					APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", TestLifecycleProbe: probe}
				storeConfig := fmt.Sprintf("store:\n  backend: sqlite\n  sqlite:\n    path: %s\n", filepath.Join(t.TempDir(), "store.db"))
				if backend != servedparity.BackendDefaultSQLite {
					configureCompletion2270PostgresFixture(t)
					opts.StoreMode, opts.StoreModeSet = "postgres", true
					storeConfig = ""
				}
				configuration := fmt.Sprintf("runtime:\n  recovery_on_startup: true\n%sllm:\n  backend: %s\n  models:\n    regular: {%s: %s}\n  session:\n    lock_ttl: 10s\n    rotate_on_parse_failures: 3\n", storeConfig, scenario.profile, scenario.profile, model)
				if scenario.profile == "openai_compatible" {
					configuration += "  openai_compatible:\n    base_url: https://api.deepseek.com\n"
				}
				if err := os.WriteFile(opts.ConfigPath, []byte(withTestProviderTriggerPlatformInventory(t, configuration)), 0o600); err != nil {
					t.Fatal(err)
				}
				t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
				credentials, err := runtimecredentials.NewFileStore(os.Getenv("SWARM_CREDENTIALS_FILE"))
				if err != nil {
					t.Fatal(err)
				}
				credential := "ANTHROPIC_API_KEY"
				if scenario.profile == "openai_responses" {
					credential = "OPENAI_API_KEY"
				} else if scenario.profile == "openai_compatible" {
					credential = "OPENAI_COMPATIBLE_API_KEY"
				}
				if err := credentials.Set(context.Background(), credential, "compaction-key"); err != nil {
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
				lastEventID := ""
				defer func() {
					if !t.Failed() || process == nil || lastEventID == "" {
						return
					}
					var event operatorread.OperatorEventFull
					requireServedJSONRPCResult(t, endpoint, "event.get", map[string]any{"event_id": lastEventID}, &event)
					deliveries, _ := json.Marshal(event.Deliveries)
					t.Logf("failed compaction delivery: %s", deliveries)
					var logs any
					requireServedJSONRPCResult(t, endpoint, "runtime.logs", map[string]any{"run_id": seed.RunID, "level": "error", "limit": 5}, &logs)
					raw, _ := json.Marshal(logs)
					t.Logf("compaction error readback: %s", raw)
				}()
				inputs := []string{"first fact cedar-731: " + strings.Repeat("shadowed ", 55555)}
				if strings.HasPrefix(scenario.name, "summary_overflow") {
					inputs[0] = "first fact cedar-731"
				}
				for i := 1; i < scenario.inputs; i++ {
					repeats := 71428
					if scenario.name == "summary" {
						repeats = 8571
					}
					inputs = append(inputs, fmt.Sprintf("kept-tail-%d: ", i)+strings.Repeat("recent ", repeats))
					if strings.HasPrefix(scenario.name, "summary_overflow") {
						inputs[len(inputs)-1] = "Recall the first fact."
					}
				}
				inputs = append(inputs, "What was the first fact?")
				if scenario.name == "openai_native" {
					inputs = append(inputs, "Continue with the upgraded prompt and original fact.")
				}
				var sessionID string
				for i, input := range inputs {
					if i >= scenario.inputs {
						if code := process.stop(); code != 0 {
							t.Fatalf("stop before checkpoint replay: exit=%d", code)
						}
						process = nil
						if i > scenario.inputs {
							prompt := filepath.Join(root, "hold", "prompts", "load-agent.md")
							original, err := os.ReadFile(prompt)
							if err != nil {
								t.Fatal(err)
							}
							if err := os.WriteFile(prompt, append(original, []byte("\nUse the upgraded prompt.\n")...), 0o600); err != nil {
								t.Fatal(err)
							}
						}
						process = startServeRuntimeTestProcess(t, opts)
						process.waitForReadyLine()
						endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
					}
					published := requireServedEventPublishRPCResult(t, endpoint, map[string]any{
						"event_name": "item.agent_hold", "run_id": seed.RunID, "source_event_id": seed.EventID,
						"payload": map[string]any{"note": input}, "idempotency_key": fmt.Sprintf("compaction-turn-%d", i),
					})
					lastEventID = published.EventID
					current := requireProviderMemoryTurns(t, endpoint, seed.RunID, i+1)
					if sessionID != "" && current != sessionID {
						t.Fatalf("compaction replaced the memory session: %s -> %s", sessionID, current)
					}
					sessionID = current
					if i < scenario.inputs-1 && summaries.Load() != 0 {
						t.Fatal("compaction fired below its documented trigger")
					}
					wantSummaries := int32(1)
					if i > scenario.inputs {
						wantSummaries = 2
					}
					if i >= scenario.inputs-1 && (summaries.Load() != wantSummaries || !compacted.Load()) {
						t.Fatalf("context boundary was not summarized once and replayed: summaries=%d compacted=%t request_bytes=%d", summaries.Load(), compacted.Load(), firstRequestLength())
					}
				}
				wantSummaries := int32(1)
				if scenario.name == "openai_native" {
					wantSummaries = 2
				}
				if int(turns.Load()) != len(inputs) || summaries.Load() != wantSummaries {
					t.Fatalf("restart regenerated maintenance or business work: turns=%d summaries=%d", turns.Load(), summaries.Load())
				}
				if strings.HasPrefix(scenario.name, "summary_overflow") && !rejected.Load() {
					t.Fatal("overflow cell did not exercise a confirmed context rejection")
				}
			})
		}
	}
}

func nativeContextCompactionProvider(t *testing.T, method, model string, summaries, turns *atomic.Int32, compacted *atomic.Bool) (string, func() int) {
	t.Helper()
	const signed = `[{"type":"compaction","content":"The first fact is cedar-731.","signature":"exact-native-seal","future":{"preserve":true}}]`
	const window = `[{"type":"message","role":"user","content":[{"type":"input_text","text":"retained upstream note"}]},{"type":"compaction","id":"cmp-1","encrypted_content":"exact-opaque-state","future":{"preserve":true}},{"type":"reasoning","id":"reason-1","summary":[]}]`
	var mu sync.Mutex
	firstSize := 0
	var tools json.RawMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		var wire struct {
			Compaction *struct{ Type string } `json:"compaction"`
			Tools      json.RawMessage        `json:"tools"`
			Messages   []struct {
				Role    string
				Content json.RawMessage
			} `json:"messages"`
			Input []json.RawMessage `json:"input"`
		}
		if err := json.Unmarshal(body, &wire); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		protocol, credential, path := llmapi.Anthropic, request.Header.Get("x-api-key"), "/v1/messages"
		if method == "openai_native" {
			protocol, credential, path = llmapi.OpenAIResponses, strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer "), "/v1/responses"
		}
		if request.Method != "POST" || credential != "compaction-key" || request.URL.Path != path && request.URL.Path != path+"/compact" {
			t.Error("native compaction changed the configured route or credential")
			w.WriteHeader(400)
			return
		}
		mu.Lock()
		if firstSize == 0 {
			firstSize, tools = len(body), append(json.RawMessage(nil), wire.Tools...)
		}
		frozenTools := append(json.RawMessage(nil), tools...)
		mu.Unlock()
		maintenance := wire.Compaction != nil || request.URL.Path == path+"/compact"
		if maintenance {
			if !bytes.Contains(body, []byte("cedar-731")) {
				t.Error("native compaction lost the shadowed fact")
			}
			summaries.Add(1)
			inputTokens := (len(body) + 3) / 4
			w.Header().Set("Content-Type", "application/json")
			if method == "anthropic_native" {
				if request.Header.Get("anthropic-beta") != "compact-2026-09-04" || wire.Compaction == nil || wire.Compaction.Type != "summarize" || !bytes.Equal(frozenTools, wire.Tools) {
					t.Error("native Anthropic request lost beta or frozen tools")
				}
				fmt.Fprintf(w, `{"id":"native-1","model":%q,"role":"assistant","content":%s,"stop_reason":"compaction","usage":{"input_tokens":0,"output_tokens":0,"iterations":[{"type":"compaction","input_tokens":%d,"output_tokens":20}]}}`, model, signed, inputTokens)
			} else {
				if len(wire.Tools) != 0 {
					t.Error("standalone compact request sent unsupported tools parameter")
				}
				fmt.Fprintf(w, `{"id":"native-1","object":"response.compaction","output":%s,"usage":{"input_tokens":%d,"output_tokens":20}}`, window, inputTokens)
			}
			return
		}
		turn := turns.Add(1)
		if summaries.Load() != 0 {
			if !bytes.Equal(frozenTools, wire.Tools) || bytes.Contains(body, []byte(strings.Repeat("shadowed ", 100))) {
				t.Error("native replay lost tools or replayed the shadowed prefix")
			}
			if method == "anthropic_native" {
				if len(wire.Messages) == 0 || wire.Messages[0].Role != "assistant" || request.Header.Get("anthropic-beta") != "compact-2026-09-04" || !sameCompactionJSON(wire.Messages[0].Content, []byte(signed)) {
					t.Error("signed block changed or lost replay beta")
				}
			} else {
				if len(wire.Input) < 3 {
					t.Error("native replay pruned returned items")
				} else {
					prefix, _ := json.Marshal(wire.Input[:3])
					if !sameCompactionJSON(prefix, []byte(window)) {
						t.Error("native window lost an item, opaque content or unknown member")
					}
				}
			}
			compacted.Store(true)
		}
		if err := llmapi.WriteReply(w, protocol, llmapi.Reply{ID: fmt.Sprintf("ordinary-%d", turn), TextID: fmt.Sprintf("text-%d", turn), Model: model, Text: "ordinary response", InputTokens: 125000, OutputTokens: 8, TotalTokens: 125008}); err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(server.Close)
	return server.URL, func() int { mu.Lock(); defer mu.Unlock(); return firstSize }
}

func sameCompactionJSON(left, right []byte) bool {
	var l, r any
	if json.Unmarshal(left, &l) != nil || json.Unmarshal(right, &r) != nil {
		return false
	}
	lb, _ := json.Marshal(l)
	rb, _ := json.Marshal(r)
	return bytes.Equal(lb, rb)
}
