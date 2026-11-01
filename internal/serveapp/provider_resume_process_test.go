package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/operatorread"
	credentials "github.com/division-sh/swarm/internal/runtime/credentials"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/llmapi"
	"gopkg.in/yaml.v3"
)

const providerResumeProcessEnv = "SWARM_TEST_PROVIDER_RESUME_PROCESS"

func TestInterruptedReadOnlyProviderTurnResumesOnlyInterruptedCallBothStores(t *testing.T) {
	if target := os.Getenv(providerResumeProcessEnv); target != "" {
		redirectExternalHosts(t, map[string]string{"api.anthropic.com": target})
		configureHostWorkspaceProofLifecycle(t)
		if err := os.Chdir(os.Getenv("SWARM_TEST_PROVIDER_RESUME_ROOT")); err != nil {
			t.Fatal(err)
		}
		TestOwnedMockLifecycleProcessEntry(t)
		return
	}
	for _, profile := range []string{"openai_compatible", "openai_responses", "anthropic"} {
		for _, backend := range []string{"sqlite", "postgres"} {
			for _, exhaust := range []bool{false, true} {
				for _, memory := range []bool{true, false} {
					for _, selected := range []bool{false, true} {
						if selected && profile != "openai_compatible" {
							continue
						}
						t.Run(fmt.Sprintf("%s/%s/exhausted_%t/memory_%t/selected_%t", profile, backend, exhaust, memory, selected), func(t *testing.T) {
							isolateCLIAPIConfigEnv(t)
							unsetStoreSelectorEnv(t)
							var toolCalls atomic.Int32
							tool := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
								var input struct{ Message string }
								if r.Method != http.MethodPost || r.URL.Path != "/marker" ||
									json.NewDecoder(r.Body).Decode(&input) != nil || input.Message != "only once" {
									t.Error("authored tool received a different request")
									http.Error(w, "wrong tool request", http.StatusBadRequest)
									return
								}
								toolCalls.Add(1)
								w.Header().Set("Content-Type", "application/json")
								_ = json.NewEncoder(w).Encode(map[string]string{"receipt": "settled-marker-1"})
							}))
							t.Cleanup(tool.Close)
							source := writeProviderResumeSource(t, tool.URL+"/marker", memory)
							interrupted := make(chan llmapi.Request, 1)
							release := make(chan struct{})
							var releaseOnce sync.Once
							unblock := func() { releaseOnce.Do(func() { close(release) }) }
							retryRelease := make(chan struct{})
							var retryReleaseOnce sync.Once
							unblockRetry := func() { retryReleaseOnce.Do(func() { close(retryRelease) }) }
							path := map[string]string{"anthropic": "/v1/messages", "openai_compatible": "/v1/chat/completions", "openai_responses": "/v1/responses"}[profile]
							wantCalls, wantTools := 3, int32(1)
							if selected {
								wantCalls, wantTools = 5, 2
							}
							provider := llmapi.New(t, llmapi.Protocol(profile), path, "resume-key", func(request llmapi.Request) (llmapi.Reply, error) {
								reply := llmapi.Reply{ID: fmt.Sprintf("provider-resume-%d", request.Number), Model: "resume-model",
									TextID: fmt.Sprintf("text-%d", request.Number), InputTokens: 8, OutputTokens: 2, TotalTokens: 10}
								step := request.Number
								if selected {
									if step == 2 {
										reply.Text = "source completed"
										return reply, nil
									}
									if step > 2 {
										step -= 2
									}
								}
								switch step {
								case 1:
									reply.Tools = []llmapi.ToolCall{{ID: "marker-call", OutputID: "marker-output", Name: "record_marker", Arguments: json.RawMessage(`{"message":"only once"}`)}}
								case 2:
									if toolCalls.Load() != wantTools || !bytes.Contains(request.Body, []byte("settled-marker-1")) {
										return llmapi.Reply{}, fmt.Errorf("second provider call lacks its settled authored tool result")
									}
									interrupted <- request
									<-release
									// The predecessor no longer exists; do not report its closed
									// response socket as a failure of the parent-owned double.
									return llmapi.Reply{StatusCode: http.StatusServiceUnavailable}, nil
								case 3:
									if exhaust {
										interrupted <- request
										<-retryRelease
										return llmapi.Reply{StatusCode: http.StatusServiceUnavailable}, nil
									}
									reply.Text = "resumed exact interrupted call"
								default:
									return llmapi.Reply{}, fmt.Errorf("unexpected provider redispatch %d", request.Number)
								}
								return reply, nil
							})
							t.Cleanup(unblock)
							t.Cleanup(unblockRetry)
							root := t.TempDir()
							config := writeProviderResumeConfig(t, backend, profile, root, provider.URL)
							selection, err := llmselection.ResolveActiveBackend(profile)
							if err != nil {
								t.Fatal(err)
							}
							t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(root, "credentials.json"))
							secrets, err := credentials.NewFileStore(os.Getenv("SWARM_CREDENTIALS_FILE"))
							if err != nil {
								t.Fatal(err)
							}
							if err := secrets.Set(context.Background(), selection.Credential.EnvVar, "resume-key"); err != nil {
								t.Fatal(err)
							}
							request, err := json.Marshal(map[string]any{"ConfigPath": config, "Source": source, "Live": true,
								"Store": backend, "APIPort": 0, "Token": apiv1.DefaultLoopbackAPIToken, "ShutdownGrace": int64(5 * time.Second)})
							if err != nil {
								t.Fatal(err)
							}
							environment := []string{providerResumeProcessEnv + "=" + provider.URL,
								"SWARM_TEST_PROVIDER_RESUME_ROOT=" + root,
								"SWARM_TEST_INTERNAL_MOCK_LIFECYCLE_REQUEST=" + string(request)}
							first := startServedCrashProcess(t, "TestInterruptedReadOnlyProviderTurnResumesOnlyInterruptedCallBothStores", environment)
							endpoint := first.endpoint(t) + "/v1/rpc"
							seed := requireServedEventPublishRPCResult(t, endpoint, map[string]any{"event_name": "item.received",
								"bundle_hash": servedEventPublishFixtureBundleHash(t, source),
								"payload":     map[string]any{"item_id": "provider-resume"}, "idempotency_key": "resume-seed"})
							business := requireServedEventPublishRPCResult(t, endpoint, map[string]any{"event_name": "item.agent_hold",
								"run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"note": "one interrupted tool round"},
								"idempotency_key": "resume-original-call"})
							if selected {
								requireProviderResumeDelivery(t, endpoint, business.EventID, provider, llmapi.Request{}, &toolCalls, false, 2, 1)
								requireServedOKJSONRPC(t, endpoint, "run.pause", map[string]any{"run_id": seed.RunID, "idempotency_key": "pause-resume-source"})
								forkCtx, cancelFork := context.WithCancel(t.Context())
								forkResponse := make(chan publicationAuditHTTPResult, 1)
								forkParams := map[string]any{"source_run_id": seed.RunID, "fork_event_id": business.EventID, "idempotency_key": "resume-selected-fork"}
								go func(endpoint string) {
									forkResponse <- publicationCompletionHTTPMethod(forkCtx, endpoint, "run.fork", forkParams)
								}(endpoint)
								defer func() { cancelFork(); <-forkResponse }()
							}
							var original llmapi.Request
							select {
							case original = <-interrupted:
							case <-time.After(servedEventPublishLifecycleProbeWaitTimeout):
								var event operatorread.OperatorEventFull
								requireServedJSONRPCResult(t, endpoint, "event.get", map[string]any{"event_id": business.EventID}, &event)
								var logs operatorread.OperatorRuntimeLogListResult
								requireServedJSONRPCResult(t, endpoint, "runtime.logs", map[string]any{"run_id": seed.RunID, "limit": 100}, &logs)
								raw, _ := json.Marshal(event)
								rawLogs, _ := json.Marshal(logs)
								t.Fatalf("actual noninitial provider request not reached: provider_calls=%d settled_tool_calls=%d event=%s logs=%s\n%s", len(provider.Requests()), toolCalls.Load(), raw, rawLogs, first.output.String())
							}
							if selected {
								business = providerResumeForkInput(t, endpoint, seed.RunID)
							}
							if err := first.kill(); err != nil {
								t.Fatal(err)
							}
							unblock()
							second := startServedCrashProcess(t, "TestInterruptedReadOnlyProviderTurnResumesOnlyInterruptedCallBothStores", environment)
							t.Cleanup(func() {
								if t.Failed() {
									t.Logf("restart process:\n%s", second.output.String())
								}
							})
							endpoint = second.endpoint(t) + "/v1/rpc"
							if exhaust {
								select {
								case replacement := <-interrupted:
									if !bytes.Equal(original.Body, replacement.Body) {
										t.Fatal("replacement changed the interrupted request")
									}
								case <-time.After(servedEventPublishLifecycleProbeWaitTimeout):
									t.Fatalf("replacement provider request not reached\n%s", second.output.String())
								}
								if err := second.kill(); err != nil {
									t.Fatal(err)
								}
								unblockRetry()
								second = startServedCrashProcess(t, "TestInterruptedReadOnlyProviderTurnResumesOnlyInterruptedCallBothStores", environment)
								endpoint = second.endpoint(t) + "/v1/rpc"
							}
							requireProviderResumeDelivery(t, endpoint, business.EventID, provider, original, &toolCalls, exhaust, wantCalls, wantTools)
							if err := second.stop(); err != nil {
								t.Fatal(err)
							}
						})
					}
				}
			}
		}
	}
}

func writeProviderResumeSource(t *testing.T, toolURL string, memory bool) string {
	t.Helper()
	root := canonicalrouting.CopyRootIngressServedSessionCleanup(t)
	memoryLine := ""
	if memory {
		memoryLine = "  memory: true\n"
	}
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "hold", "agents.yaml"), fmt.Sprintf(`load-agent:
  role: load_agent
  intent: prompts/load-agent.md
  model: resume-model
%s  subscriptions: [item.agent_hold]
  tools: [record_marker]
`, memoryLine))
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "hold", "tools.yaml"), fmt.Sprintf(`record_marker:
  description: Record exactly one settled marker
  handler_type: http
  input_schema:
    type: object
    properties: {message: {type: string}}
    required: [message]
  output_schema:
    type: object
    properties: {receipt: {type: string}}
    required: [receipt]
  response_success: {kind: http_status_2xx}
  http:
    method: POST
    url: %q
    body: {message: "{{input.message}}"}
`, toolURL))
	return root
}

func writeProviderResumeConfig(t *testing.T, backend, profile, root, providerURL string) string {
	t.Helper()
	configPath := writeStandingMockRuntimeConfig(t, backend, filepath.Join(root, "resume.sqlite"))
	if backend == "postgres" {
		configPath = writeChannelOnboardingPostgresRuntimeConfig(t, testutil.StartEmptyPostgresDSN(t))
	}
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := yaml.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	document["runtime"] = map[string]any{"recovery_on_startup": true}
	document["workspace"] = map[string]any{"backend": "host", "allow_exec_on_host": true}
	llm := map[string]any{"backend": profile, "models": map[string]any{"resume-model": map[string]any{profile: "resume-model"}},
		"session": map[string]any{"lock_ttl": "10s", "rotate_after_turns": 40, "rotate_on_parse_failures": 3}}
	if profile != "anthropic" {
		llm[profile] = map[string]any{"base_url": providerURL + "/v1"}
	}
	document["llm"] = llm
	raw, err = yaml.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return configPath
}

func requireProviderResumeDelivery(t *testing.T, endpoint, eventID string, provider *llmapi.Server, original llmapi.Request, toolCalls *atomic.Int32, exhausted bool, wantCalls int, wantTools int32) {
	t.Helper()
	deadline := time.NewTimer(servedEventPublishLifecycleProbeWaitTimeout)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		var event operatorread.OperatorEventFull
		requireServedJSONRPCResult(t, endpoint, "event.get", map[string]any{"event_id": eventID}, &event)
		for _, delivery := range event.Deliveries {
			if delivery.SubscriberType != "agent" {
				continue
			}
			if exhausted && delivery.Status == "dead_letter" && delivery.ReasonCode == "retry_exhausted" {
				if delivery.RetryCount != 1 || len(provider.Requests()) != wantCalls || toolCalls.Load() != wantTools {
					t.Fatalf("boot retry exceeded the shared budget or repeated a tool: delivery=%+v provider_calls=%d settled_tool_calls=%d", delivery, len(provider.Requests()), toolCalls.Load())
				}
				return
			}
			if delivery.Terminal && delivery.Status != "delivered" {
				code := ""
				if delivery.Failure != nil {
					code = delivery.Failure.Detail.Code
				}
				t.Fatalf("restart terminalized interrupted read-only call: status=%s reason=%s failure=%s provider_calls=%d settled_tool_calls=%d; want one exact reissue",
					delivery.Status, delivery.ReasonCode, code, len(provider.Requests()), toolCalls.Load())
			}
			if delivery.Status == "delivered" {
				if exhausted {
					t.Fatal("interrupted replacement completed without consuming the shared retry budget")
				}
				requests := provider.Requests()
				if len(requests) != wantCalls || toolCalls.Load() != wantTools || (len(original.Body) != 0 && !bytes.Equal(requests[wantCalls-1].Body, original.Body)) {
					t.Fatalf("resume replayed work or changed interrupted request: provider_calls=%d settled_tool_calls=%d", len(requests), toolCalls.Load())
				}
				if !strings.Contains(string(requests[wantCalls-1].Body), "settled-marker-1") {
					t.Fatal("resume lost the settled tool result")
				}
				return
			}
		}
		select {
		case <-deadline.C:
			t.Fatalf("resumed delivery did not settle: provider_calls=%d settled_tool_calls=%d", len(provider.Requests()), toolCalls.Load())
		case <-tick.C:
		}
	}
}

func providerResumeForkInput(t *testing.T, endpoint, sourceRunID string) servedEventPublishRPCResult {
	t.Helper()
	var list struct {
		Runs []operatorread.RunHeader `json:"runs"`
	}
	requireServedJSONRPCResult(t, endpoint, "run.list", map[string]any{}, &list)
	var runID string
	for _, run := range list.Runs {
		if run.Origin.SourceRunID() == sourceRunID {
			if runID != "" {
				t.Fatal("more than one selected child was created")
			}
			runID = run.RunID
		}
	}
	var events operatorread.OperatorEventListResult
	requireServedJSONRPCResult(t, endpoint, "event.list", map[string]any{"filter": map[string]any{"run_id": runID, "event_name": "item.agent_hold"}, "limit": 100}, &events)
	if runID == "" || len(events.Events) != 1 {
		t.Fatalf("selected interruption lacks its exact child input: run=%s events=%+v", runID, events)
	}
	return servedEventPublishRPCResult{RunID: runID, EventID: events.Events[0].EventID}
}
