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
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/tests/support/testutil"
	"github.com/division-sh/swarm/tests/support/testutil/llmapi"
	"gopkg.in/yaml.v3"
)

const providerResumeProcessEnv = "SWARM_TEST_PROVIDER_RESUME_PROCESS"

const uncalledAgentTurnChildEnv = "SWARM_TEST_UNCALLED_AGENT_TURN_CHILD"

type uncalledAgentClaim struct {
	Event, Run, Delivery string
	Version              int64
}

func TestServedInterruptedUncalledAgentTurnDeadLettersBothStores(t *testing.T) {
	if root := os.Getenv(uncalledAgentTurnChildEnv); root != "" {
		signal := os.NewFile(3, "uncalled-agent-claim")
		if signal == nil {
			t.Fatal("missing child observation pipe")
		}
		defer signal.Close()
		configureHostWorkspaceProofLifecycle(t)
		if err := os.Chdir(root); err != nil {
			t.Fatal(err)
		}
		observer := issue2564H2CompletionObserver(func(ctx context.Context, s lifecycleprobe.Signal) {
			if s.Kind != lifecycleprobe.DeliveryStatusChanged || s.SubscriberType != "agent" ||
				s.SubscriberID != "load-agent" || s.Status != string(deliverylifecycle.StatusInProgress) {
				return
			}
			claim, ok := deliverylifecycle.ClaimFromContext(ctx)
			if !ok || claim.Validate() != nil || claim.SubscriberID() != s.SubscriberID {
				t.Error("agent in-progress observation has no exact claim")
			} else if err := json.NewEncoder(signal).Encode(uncalledAgentClaim{s.EventID, claim.RunID(), claim.DeliveryID(), claim.Version()}); err != nil {
				t.Error(err)
			}
			// The parent owns the crash cut; don't enter provider execution.
			<-ctx.Done()
		})
		runOwnedMockLifecycleProcessEntry(t, observer)
		return
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			unsetStoreSelectorEnv(t)
			provider := llmapi.New(t, llmapi.Protocol("openai_compatible"), "/v1/chat/completions", "restart-key", func(llmapi.Request) (llmapi.Reply, error) {
				return llmapi.Reply{}, fmt.Errorf("uncalled agent turn reached its provider")
			})
			tool := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				t.Error("uncalled agent turn reached its tool")
				w.WriteHeader(http.StatusServiceUnavailable)
			}))
			t.Cleanup(tool.Close)
			source := writeProviderResumeSource(t, tool.URL, false)
			root := t.TempDir()
			location := filepath.Join(root, "resume.sqlite")
			var config string
			if backend == "postgres" {
				location = testutil.StartEmptyPostgresDSN(t)
				config = writeChannelOnboardingPostgresRuntimeConfig(t, location)
			} else {
				config = writeStandingMockRuntimeConfig(t, backend, location)
			}
			configureProviderResume(t, config, "openai_compatible", provider.URL)
			t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(root, "credentials.json"))
			secrets, err := credentials.NewFileStore(os.Getenv("SWARM_CREDENTIALS_FILE"))
			if err != nil {
				t.Fatal(err)
			}
			profile, err := llmselection.ResolveActiveBackend("openai_compatible")
			if err != nil {
				t.Fatal(err)
			}
			if err := secrets.Set(t.Context(), profile.Credential.EnvVar, "restart-key"); err != nil {
				t.Fatal(err)
			}
			request, err := json.Marshal(map[string]any{"ConfigPath": config, "Source": source, "Live": true,
				"Store": backend, "Token": apiv1.DefaultLoopbackAPIToken, "ShutdownGrace": int64(5 * time.Second)})
			if err != nil {
				t.Fatal(err)
			}
			env := []string{uncalledAgentTurnChildEnv + "=" + root, "SWARM_TEST_INTERNAL_MOCK_LIFECYCLE_REQUEST=" + string(request)}
			first, firstClaims := startUncalledAgentTurnChild(t, env)
			endpoint := first.endpoint(t) + "/v1/rpc"
			seed := requireServedEventPublishRPCResult(t, endpoint, map[string]any{"event_name": "item.received",
				"bundle_hash": servedEventPublishFixtureBundleHash(t, source), "payload": map[string]any{"item_id": "uncalled"}, "idempotency_key": "uncalled-seed"})
			waitProviderResumeSeedDelivery(t, endpoint, seed.EventID)
			input := requireServedEventPublishRPCResult(t, endpoint, map[string]any{"event_name": "item.agent_hold",
				"run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"note": "never called"}, "idempotency_key": "uncalled-agent-input"})
			reader, err := storetest.OpenReleaseProcessReadOnlyInspection(backend, location)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := reader.Close(); err != nil {
					t.Error(err)
				}
			}()
			var claim uncalledAgentClaim
			select {
			case claim = <-firstClaims:
			case <-time.After(servedEventPublishLifecycleProbeWaitTimeout):
				t.Fatalf("predecessor did not reach the claim cut: %s", first.output.String())
			}
			if claim.Event != input.EventID || claim.Run != seed.RunID || claim.Delivery == "" || claim.Version != 1 {
				t.Fatalf("wrong predecessor claim: %+v", claim)
			}
			requireUncalledAgentClaimStorage(t, reader, claim, deliverylifecycle.StatusInProgress)
			if len(provider.Requests()) != 0 {
				t.Fatal("predecessor made a provider call before crash")
			}
			if err := first.kill(); err != nil {
				t.Fatal(err)
			}

			second, secondClaims := startUncalledAgentTurnChild(t, env)
			endpoint = second.endpoint(t) + "/v1/rpc"
			letter := awaitUncalledAgentTurnDeadLetter(t, endpoint, reader, claim, secondClaims)
			const reason = "effect_recovery_prelaunch_abandoned"
			if letter.ClaimVersion != claim.Version || letter.Failure.Detail.Code != reason {
				t.Fatalf("wrong recovered dead letter: %+v", letter)
			}
			if err := second.stop(); err != nil {
				t.Fatal(err)
			}
			if next, ok := <-secondClaims; ok {
				t.Fatalf("dead-lettered turn reached the agent: %+v", next)
			}
			requireUncalledAgentClaimStorage(t, reader, claim, deliverylifecycle.StatusDeadLetter)
			if len(provider.Requests()) != 0 {
				t.Fatal("restart made a provider call for the dead-lettered turn")
			}
			const notice = "runtime.warn component=startup-recovery message=Agent turns dead-lettered at restart before any provider call: 1; they are not run again. List dead-lettered deliveries with swarm event list --delivery-status dead_letter"
			if output := second.output.String(); strings.Count(output, notice+"\n") != 1 || strings.Contains(output, "attempts abandoned before launch") {
				t.Fatalf("startup did not report exactly one uncalled turn without modifying attempt counts:\n%s", output)
			}
		})
	}
}

func startUncalledAgentTurnChild(t *testing.T, env []string) (*channelOnboardingCrashServeProcess, <-chan uncalledAgentClaim) {
	t.Helper()
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = read.Close(); _ = write.Close() })
	child := startServedCrashProcessBeforeReadiness(t, "TestServedInterruptedUncalledAgentTurnDeadLettersBothStores", env, write)
	_ = write.Close()
	claims := make(chan uncalledAgentClaim, 1)
	go func() {
		defer close(claims)
		var claim uncalledAgentClaim
		if err := json.NewDecoder(read).Decode(&claim); err == nil {
			claims <- claim
		}
	}()
	t.Cleanup(func() { _ = child.kill(); <-claims })
	return child, claims
}

func requireUncalledAgentClaimStorage(t *testing.T, reader storetest.ReleaseProcessReadOnlyInspection, want uncalledAgentClaim, status deliverylifecycle.Status) {
	t.Helper()
	if err := reader.InspectSnapshot(t.Context(), func(ctx context.Context) error {
		phases, err := storetest.ReadWorkspaceInvocationPhases(ctx, reader)
		if err != nil {
			return err
		}
		effects, err := storetest.ReadManagedTurnEffectStorage(ctx, reader, want.Run, "load-agent")
		if err != nil {
			return err
		}
		if effects.ProviderAttempts != 0 {
			return fmt.Errorf("claim cut provider attempts=%d, want zero", effects.ProviderAttempts)
		}
		for _, row := range phases.Deliveries {
			if row.DeliveryID != want.Delivery {
				continue
			}
			if row.EventID == want.Event && row.RunID == want.Run && row.SubscriberType == "agent" && row.SubscriberID == "load-agent" &&
				row.ClaimVersion == want.Version && row.Status == string(status) && row.AttemptOpenMarker != nil && *row.AttemptOpenMarker == (status == deliverylifecycle.StatusInProgress) {
				if status == deliverylifecycle.StatusInProgress && row.AttemptOutcome == nil ||
					status == deliverylifecycle.StatusDeadLetter && row.AttemptOutcome != nil && *row.AttemptOutcome == "dead_letter" {
					return nil
				}
			}
			return fmt.Errorf("native delivery %s/%s v%d, want %s %s v%d", row.EventID, row.Status, row.ClaimVersion, want.Event, status, want.Version)
		}
		return fmt.Errorf("native claim not found for delivery %s", want.Delivery)
	}); err != nil {
		t.Fatal(err)
	}
}

func awaitUncalledAgentTurnDeadLetter(t *testing.T, endpoint string, reader storetest.ReleaseProcessReadOnlyInspection, claim uncalledAgentClaim, nextClaims <-chan uncalledAgentClaim) operatorread.OperatorDeadLetterRecord {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), servedEventPublishLifecycleProbeWaitTimeout)
	defer cancel()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		var event operatorread.OperatorEventFull
		requireServedJSONRPCResult(t, endpoint, "event.get", map[string]any{"event_id": claim.Event}, &event)
		for _, letter := range event.DeadLetters {
			if letter.DeliveryID == claim.Delivery {
				return letter
			}
		}
		select {
		case next, ok := <-nextClaims:
			if !ok || next.Event != claim.Event || next.Run != claim.Run || next.Delivery != claim.Delivery || next.Version != claim.Version+1 {
				t.Fatalf("unexpected successor claim: %+v open=%t, predecessor=%+v", next, ok, claim)
			}
			requireUncalledAgentClaimStorage(t, reader, next, deliverylifecycle.StatusInProgress)
			t.Fatalf("restart acquired second native claim v%d for delivery %s instead of settling v%d", next.Version, next.Delivery, claim.Version)
		case <-ctx.Done():
			t.Fatalf("successor did not reach a second claim or a dead letter: deliveries=%+v", event.Deliveries)
		case <-tick.C:
		}
	}
}

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
	t.Run("nested_receiver", requireNestedReceiverProviderResume)
	for _, scenario := range []string{"openai_compatible", "openai_responses", "anthropic", "projected_initial"} {
		profile := scenario
		projected := scenario == "projected_initial"
		if projected {
			profile = "openai_compatible"
		}
		for _, backend := range []string{"sqlite", "postgres"} {
			for _, exhaust := range []bool{false, true} {
				for _, memory := range []bool{true, false} {
					for _, selected := range []bool{false, true} {
						if projected && (exhaust || !memory || selected) {
							continue
						}
						if (selected || exhaust) && profile != "openai_compatible" {
							continue
						}
						t.Run(fmt.Sprintf("%s/%s/exhausted_%t/memory_%t/selected_%t", scenario, backend, exhaust, memory, selected), func(t *testing.T) {
							isolateCLIAPIConfigEnv(t)
							unsetStoreSelectorEnv(t)
							var toolCalls atomic.Int32
							tool := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
								var input struct{ Message string }
								if r.Method != http.MethodPost || r.URL.Path != "/marker" ||
									json.NewDecoder(r.Body).Decode(&input) != nil || (input.Message != "only once" && input.Message != "after resume") {
									t.Error("authored tool received a different request")
									http.Error(w, "wrong tool request", http.StatusBadRequest)
									return
								}
								toolCalls.Add(1)
								w.Header().Set("Content-Type", "application/json")
								receipt := "settled-marker-1"
								if input.Message == "after resume" {
									receipt = "settled-marker-after-resume"
								}
								_ = json.NewEncoder(w).Encode(map[string]string{"receipt": receipt})
							}))
							t.Cleanup(tool.Close)
							source := writeProviderResumeSource(t, tool.URL+"/marker", memory)
							if projected {
								path := filepath.Join(source, "hold", "agents.yaml")
								raw, err := os.ReadFile(path)
								if err != nil || strings.Count(string(raw), "  model: resume-model\n") != 1 {
									t.Fatalf("exact resume agent declaration missing: %v", err)
								}
								writeWorkflowValidationFixtureFile(t, path, strings.Replace(string(raw), "  model: resume-model\n", "  model: resume-model\n  content: payload.note\n", 1))
							}
							interrupted := make(chan llmapi.Request, 1)
							release := make(chan struct{})
							var releaseOnce sync.Once
							unblock := func() { releaseOnce.Do(func() { close(release) }) }
							retryRelease := make(chan struct{})
							var retryReleaseOnce sync.Once
							unblockRetry := func() { retryReleaseOnce.Do(func() { close(retryRelease) }) }
							path := map[string]string{"anthropic": "/v1/messages", "openai_compatible": "/v1/chat/completions", "openai_responses": "/v1/responses"}[profile]
							wantCalls, wantTools := 3, int32(1)
							if projected {
								wantCalls, wantTools = 2, 0
							}
							if selected {
								wantCalls, wantTools = 5, 2
							}
							settledBeforeResume := wantTools
							if !exhaust && !projected {
								wantCalls++
								wantTools++
							}
							provider := llmapi.New(t, llmapi.Protocol(profile), path, "resume-key", func(request llmapi.Request) (llmapi.Reply, error) {
								reply := llmapi.Reply{ID: fmt.Sprintf("provider-resume-%d", request.Number), Model: "resume-model",
									TextID: fmt.Sprintf("text-%d", request.Number), InputTokens: 8, OutputTokens: 2, TotalTokens: 10}
								step := request.Number
								if projected {
									var wire struct {
										Messages []struct{ Role, Content string }
									}
									if err := json.Unmarshal(request.Body, &wire); err != nil || len(wire.Messages) == 0 || wire.Messages[len(wire.Messages)-1].Role != "user" || wire.Messages[len(wire.Messages)-1].Content != "one interrupted tool round" {
										return llmapi.Reply{}, fmt.Errorf("initial request lost its exact authored projection")
									}
									if step == 1 {
										interrupted <- request
										<-release
										return llmapi.Reply{StatusCode: http.StatusServiceUnavailable}, nil
									}
									if step != 2 {
										return llmapi.Reply{}, fmt.Errorf("unexpected projected provider redispatch %d", request.Number)
									}
									reply.Text = "continued exact projected input"
									return reply, nil
								}
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
									if toolCalls.Load() != settledBeforeResume || !bytes.Contains(request.Body, []byte("settled-marker-1")) {
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
									reply.Tools = []llmapi.ToolCall{{ID: "after-resume-call", OutputID: "after-resume-output", Name: "record_marker", Arguments: json.RawMessage(`{"message":"after resume"}`)}}
								case 4:
									if toolCalls.Load() != wantTools || !bytes.Contains(request.Body, []byte("settled-marker-after-resume")) {
										return llmapi.Reply{}, fmt.Errorf("ordinary provider continuation lacks the new settled tool result")
									}
									reply.Text = "continued after recovered call"
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
							if selected {
								waitProviderResumeSeedDelivery(t, endpoint, seed.EventID)
							}
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
							second := startServedCrashProcessBeforeReadiness(t, "TestInterruptedReadOnlyProviderTurnResumesOnlyInterruptedCallBothStores", environment)
							t.Cleanup(func() {
								if t.Failed() {
									t.Logf("restart process:\n%s", second.output.String())
								}
							})
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
							}
							endpoint = second.endpoint(t) + "/v1/rpc"
							requireProviderResumeDelivery(t, endpoint, business.EventID, provider, original, &toolCalls, exhaust, wantCalls, wantTools)
							if memory {
								var conversations operatorread.OperatorConversationListResult
								requireServedJSONRPCResult(t, endpoint, "conversation.list", map[string]any{"run_id": business.RunID, "agent_id": "load-agent", "flow_instance": "hold"}, &conversations)
								wantTurns := 3
								if projected {
									wantTurns = 1
								}
								if exhaust {
									wantTurns = 1
								}
								if len(conversations.Conversations) != 1 || conversations.Conversations[0].TurnCount != wantTurns {
									t.Fatalf("recovery changed the logical conversation cursor: %+v", conversations)
								}
							}
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
	configureProviderResume(t, configPath, profile, providerURL)
	return configPath
}

func configureProviderResume(t *testing.T, configPath, profile, providerURL string) {
	t.Helper()
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
		"session": map[string]any{"lock_ttl": "10s", "rotate_on_parse_failures": 3}}
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
}

func waitProviderResumeSeedDelivery(t *testing.T, endpoint, eventID string) {
	t.Helper()
	deadline := time.NewTimer(servedEventPublishLifecycleProbeWaitTimeout)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		var event operatorread.OperatorEventFull
		requireServedJSONRPCResult(t, endpoint, "event.get", map[string]any{"event_id": eventID}, &event)
		for _, delivery := range event.Deliveries {
			if delivery.SubscriberType != "node" {
				continue
			}
			if delivery.Terminal && delivery.Status != "delivered" {
				t.Fatalf("source seed did not settle: %+v", delivery)
			}
			if delivery.Status == "delivered" {
				return
			}
		}
		select {
		case <-deadline.C:
			t.Fatalf("source seed node delivery did not settle: event=%+v", event)
		case <-tick.C:
		}
	}
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
				if len(requests) != wantCalls || toolCalls.Load() != wantTools || (len(original.Body) != 0 && !bytes.Equal(requests[original.Number].Body, original.Body)) {
					t.Fatalf("resume replayed work or changed interrupted request: provider_calls=%d settled_tool_calls=%d", len(requests), toolCalls.Load())
				}
				if wantTools > 0 && !strings.Contains(string(requests[wantCalls-1].Body), "settled-marker-1") {
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
