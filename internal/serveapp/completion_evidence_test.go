package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type completion2270Transport struct {
	t        *testing.T
	profile  string
	requests *atomic.Int32
	base     http.RoundTripper
}

func TestServedCompletion2270RunServePublicReadbackBothStores(t *testing.T) {
	image := ""
	if os.Getenv("SWARM_TEST_WORKSPACE_MCP_DOCKER") == "1" {
		image = buildCompletion2270CLIImage(t)
	}
	for _, backend := range servedparity.RequiredBackends {
		for _, profileID := range []string{llmselection.BackendAnthropic, llmselection.BackendOpenAICompatible, llmselection.BackendOpenAIResponses, llmselection.BackendClaudeCLI} {
			for _, kind := range []string{"ordinary", "selected"} {
				t.Run(string(backend)+"/"+profileID+"/"+kind, func(t *testing.T) {
					if profileID == llmselection.BackendClaudeCLI {
						if os.Getenv("SWARM_TEST_WORKSPACE_MCP_DOCKER") != "1" {
							t.Skip("Docker-backed served CLI protocol proof; a skip earns no qualification credit")
						}
					}
					var rt servedControlProofRuntime
					var invocationProof completion2270InvocationProof
					var selected *selectedStoreOwner
					priorCapabilities := buildSelectedAPICapabilities
					buildSelectedAPICapabilities = func(owner *selectedStoreOwner, req selectedAPICapabilityRequest) (selectedAPICapabilities, error) {
						selected = owner
						caps, err := priorCapabilities(owner, req)
						if executor, ok := caps.RunFork.(apiv1.SelectedContractRunForkExecutor); ok {
							previous := executor.ExecuteSelectedContractRunFork
							executor.ExecuteSelectedContractRunFork = func(ctx context.Context, req runforkexecution.SelectedContractExecutionRequest) (runforkexecution.SelectedContractExecutionResult, error) {
								result, err := previous(ctx, req)
								if err != nil {
									t.Logf("served-selected owner failure: %v", err)
								}
								return result, err
							}
							caps.RunFork = executor
						}
						return caps, err
					}
					t.Cleanup(func() { buildSelectedAPICapabilities = priorCapabilities })
					probe := lifecycletest.New(t, lifecycletest.WithTimeout(servedEventPublishLifecycleProbeWaitTimeout))
					rt.Probe = probe
					root := writeServedLiveAgentFixture(t)
					if kind == "selected" {
						root = canonicalrouting.CopyForkReceiverBusinessMutationOwnership(t, false)
						writeSelectedForkAgentProofFixture(t, root, "loaded-decoy", "[receiver.closed]", "Source configuration must not execute at the selected frontier.", "return {'text': 'source only', 'usage': {'input_tokens': 2, 'output_tokens': 2}}")
					}
					opts := cliapp.ServeOptions{SourceRoot: root, PlatformSpecPath: defaultPlatformSpecPath,
						APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: true,
						TestLifecycleProbe: probe, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig()}
					rt.BundleHash = servedEventPublishFixtureBundleHash(t, opts.SourceRoot)
					configPath := writeServeRuntimeTestConfig(t)
					storeConfig := ""
					if backend == servedparity.BackendDefaultSQLite {
						unsetStoreSelectorEnv(t)
						stubServeRuntimeWorkspaceLifecycle(t)
						rt.Backend = "sqlite"
						storeConfig = fmt.Sprintf("store:\n  backend: sqlite\n  sqlite:\n    path: %s\n", filepath.Join(t.TempDir(), "store.db"))
						captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) {
							rt.DB, rt.Postgres, rt.SQLite = selectedRuntimeStoreForTest(t, p)
						})
					} else {
						rt.Backend = "postgres"
						_, rt.DB, rt.Postgres = installServeRuntimeEmptyPostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle { return serveRuntimeWorkspaceStub{} })
						opts.StoreMode, opts.StoreModeSet = "postgres", true
					}
					text := fmt.Sprintf("runtime:\n  recovery_on_startup: false\n%sllm:\n  backend: %s\n  models:\n    regular:\n      %s: request-A\n  openai_compatible:\n    base_url: http://completion-2270.test\n  openai_responses:\n    base_url: http://completion-2270.test\n  session:\n    lock_ttl: 10s\n    rotate_after_turns: 40\n    rotate_on_parse_failures: 3\n", storeConfig, profileID, profileID)
					wantObserved := "reported-B"
					if profileID == llmselection.BackendClaudeCLI {
						text += fmt.Sprintf("workspace:\n  image: %s\n", image)
						text = strings.Replace(text, "  session:\n", "  claude_cli:\n    command: completion-proof-cli\n    output_format: stream-json\n  session:\n", 1)
						opts.WorkspaceBackend, opts.WorkspaceBackendSet = workspace.BackendDocker, true
						network, err := exec.Command("docker", "network", "inspect", workspace.DefaultWorkspaceNetwork(), "--format", "{{json .IPAM.Config}}").CombinedOutput()
						var addresses []struct{ Gateway string }
						if err != nil || json.Unmarshal(network, &addresses) != nil || len(addresses) != 1 || net.ParseIP(addresses[0].Gateway) == nil {
							t.Fatalf("inspect exact Docker proof interface: %v: %s", err, network)
						}
						opts.MCPListenAddr = net.JoinHostPort(addresses[0].Gateway, "0")
						previous := cliapp.ConfiguredWorkspaceLifecycleForServe
						cliapp.ConfiguredWorkspaceLifecycleForServe = func(_ *config.Config, projection *sourceartifact.RuntimeProjection, source semanticview.Source, _ cliapp.WorkspaceMountSources, _ cliapp.WorkspaceBackendSelection) (cliapp.ServeWorkspaceLifecycle, error) {
							manager := workspace.NewDockerManager()
							settings := workspace.DefaultDockerConfig()
							settings.WorkspaceImage, settings.SourceProjection = image, projection
							manager.SetConfig(settings)
							manager.SetSemanticSource(source)
							invocationProof.add(manager)
							return manager, nil
						}
						t.Cleanup(func() { cliapp.ConfiguredWorkspaceLifecycleForServe = previous })
					} else {
						configureHostWorkspaceProofLifecycle(t)
					}
					text = withTestProviderTriggerPlatformInventory(t, text)
					if err := os.WriteFile(configPath, []byte(text), 0o600); err != nil {
						t.Fatal(err)
					}
					opts.ConfigPath = configPath
					credentialPath := filepath.Join(t.TempDir(), "credentials.json")
					t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
					credentials, err := runtimecredentials.NewFileStore(credentialPath)
					if err != nil {
						t.Fatal(err)
					}
					profile, err := llmselection.ResolveActiveBackend(profileID)
					if err != nil {
						t.Fatal(err)
					}
					if err := credentials.Set(context.Background(), profile.Credential.EnvVar, "test-key"); err != nil {
						t.Fatal(err)
					}
					var requests atomic.Int32
					previous := http.DefaultTransport
					http.DefaultTransport = completion2270Transport{t: t, profile: profileID, requests: &requests, base: previous}
					t.Cleanup(func() { http.DefaultTransport = previous })
					rt.Endpoint, rt.Runtime = startServedEventPublishFollowUpRuntime(t, opts)
					if profileID == llmselection.BackendClaudeCLI {
						requireCompletion2270NoBusinessStartup(t, rt, &invocationProof)
					}
					if kind == "selected" {
						proveCompletion2270ServedSelected(t, rt, selected, profileID, &requests, &invocationProof)
						return
					}
					proveCompletion2270ServedOrdinary(t, rt, profileID, wantObserved, &requests, &invocationProof)
				})
			}
		}
	}
}

func proveCompletion2270ServedOrdinary(t *testing.T, rt servedControlProofRuntime, profileID, wantObserved string, requests *atomic.Int32, proof *completion2270InvocationProof) {
	t.Helper()
	runID, eventID, _ := createServedControlWaitingRun(t, rt, "completion-2270-"+uuid.NewString())
	publishServedLiveAgentHoldEvent(t, rt, runID, eventID, "completion-2270")
	var conversations operatorread.OperatorConversationListResult
	requireServedJSONRPCResult(t, rt.Endpoint, "conversation.list", map[string]any{"run_id": runID, "agent_id": "load-agent"}, &conversations)
	if len(conversations.Conversations) != 1 {
		t.Fatalf("served conversation: %+v", conversations)
	}
	sessionID := conversations.Conversations[0].SessionID
	var page operatorread.OperatorConversationTurnListResult
	requireServedJSONRPCResult(t, rt.Endpoint, "conversation.list_turns", map[string]any{"session_id": sessionID}, &page)
	if len(page.Turns) != 1 || page.Turns[0].ResolvedModel != wantObserved || page.Turns[0].CompletionEvidence.Request.RequestedModel != "request-A" {
		t.Fatalf("served admitted/provider identities collapsed: %+v, calls=%d", page, requests.Load())
	}
	var detail operatorread.OperatorPublicConversationTurnDetail
	requireServedJSONRPCResult(t, rt.Endpoint, "conversation.get_turn", map[string]any{"session_id": sessionID, "turn_id": page.Turns[0].TurnID}, &detail)
	if detail.Turn.CompletionEvidence.Request.RequestedModel != "request-A" || detail.Turn.ResolvedModel != wantObserved {
		t.Fatalf("served detail lost evidence: %+v", detail.Turn)
	}
	proveCompletion2270ServedAgentHistory(t, rt, runID, detail.Turn)
	var usage operatorread.OperatorAgentUsage
	requireServedJSONRPCResult(t, rt.Endpoint, "agent.usage", map[string]any{"run_id": runID, "agent_id": "load-agent"}, &usage)
	if len(usage.Breakdown) != 1 || usage.Usage.Exact.LedgerEntries != 1 || usage.Usage.Exact.InputTokens != 12 || usage.Breakdown[0].ResolvedModel != wantObserved || usage.Breakdown[0].CompletionEvidence.Request.RequestedModel != "request-A" {
		t.Fatalf("served spend lost exact request/report separation: %+v", usage)
	}
	requireCompletion2270BusinessInvocation(t, rt, profileID, requests, proof, "")
}

func proveCompletion2270ServedAgentHistory(t *testing.T, rt servedControlProofRuntime, runID string, want operatorread.OperatorPublicConversationTurn) {
	t.Helper()
	var reads operatorread.AgentReader = rt.SQLite
	if rt.Postgres != nil {
		reads = rt.Postgres
	}
	identity, err := reads.ResolveOperatorAgentIdentity(context.Background(), runID, "load-agent", "")
	if err != nil {
		t.Fatal(err)
	}
	first, err := reads.LoadOperatorAgent(context.Background(), identity)
	if err != nil {
		t.Fatalf("native served last-turn projection: %+v, %v", first, err)
	}
	if first.LastTurnRef != nil {
		first.LastTurnRef.CompletionEvidence.Models[0].Model = "mutated-after-native-projection"
	}
	var result operatorread.OperatorAgentDetail
	requireServedJSONRPCResult(t, rt.Endpoint, "agent.get", map[string]any{"run_id": runID, "agent_id": "load-agent"}, &result)
	if result.Agent.Model != "regular" {
		t.Fatalf("configured alias adopted provider observation: %+v", result)
	}
	if result.LastTurnRef == nil {
		if result.Agent.Memory {
			t.Fatal("active stateful last-turn reference missing")
		}
		return
	}
	if result.LastTurnRef.TurnID != want.TurnID || result.LastTurnRef.ResolvedModel != want.ResolvedModel || !reflect.DeepEqual(result.LastTurnRef.CompletionEvidence, want.CompletionEvidence) {
		t.Fatalf("current alias replaced or mutated historical completion evidence: %+v", result)
	}
}

func proveCompletion2270ServedSelected(t *testing.T, rt servedControlProofRuntime, selected *selectedStoreOwner, profileID string, calls *atomic.Int32, proof *completion2270InvocationProof) {
	t.Helper()
	owner := selected.RuntimeDeps().EventStore
	workspaceProof := servedWorkspaceProofRuntime{Events: owner}
	seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "start.seeded", "bundle_hash": rt.BundleHash,
		"payload": map[string]any{"token": "completion-2270"}, "idempotency_key": "completion-2270-selected-seed"})
	waitWorkspaceProofPipelineHandoff(t, workspaceProof, seed.RunID)
	requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "start.requested", "run_id": seed.RunID, "source_event_id": seed.EventID,
		"payload": map[string]any{"token": "completion-2270"}, "idempotency_key": "completion-2270-selected-request"})
	waitWorkspaceProofPipelineHandoff(t, workspaceProof, seed.RunID)
	eventID := requireWorkspaceProofWorkReadyEvent(t, rt.Endpoint, owner, seed.RunID)
	parent := readWorkspaceProofSourceDomain(t, owner, seed.RunID)
	root := canonicalrouting.CopyForkReceiverBusinessMutationOwnership(t, false)
	writeSelectedForkAgentProofFixture(t, root, "selected observer", "[work.ready]", "Observe the admitted selected frontier.", "return {'text': 'observed', 'usage': {'input_tokens': 1, 'output_tokens': 1}}")
	fact, err := prepareServeSourceArtifact(servedControlProofAuthorActivityContext(t, rt), selected.SourceArtifactWriter(), loadWorkflowValidationBundleAt(t, root))
	if err != nil {
		t.Fatal(err)
	}
	params := map[string]any{"source_run_id": seed.RunID, "fork_event_id": eventID, "bundle_hash": fact.BundleHash(), "allow_source_freeze": true, "idempotency_key": "completion-2270-selected"}
	fork := requireSelectedForkExecutionRPCResult(t, rt.Endpoint, params)
	if fork.ForkRunID == "" || fork.ForkRunID == seed.RunID {
		t.Fatalf("served-selected launch: %+v, calls=%d", fork, calls.Load())
	}
	var list operatorread.OperatorConversationListResult
	requireServedJSONRPCResult(t, rt.Endpoint, "conversation.list", map[string]any{"run_id": fork.ForkRunID, "agent_id": "same-name"}, &list)
	if len(list.Conversations) != 1 {
		t.Fatalf("served-selected conversation: %+v", list)
	}
	var page operatorread.OperatorConversationTurnListResult
	requireServedJSONRPCResult(t, rt.Endpoint, "conversation.list_turns", map[string]any{"session_id": list.Conversations[0].SessionID}, &page)
	if len(page.Turns) != 1 {
		t.Fatal("served-selected completion was not persisted")
	}
	for _, turn := range page.Turns {
		if turn.CompletionEvidence.Request.RequestedModel != "request-A" || turn.ResolvedModel != "reported-B" || turn.Tokens == nil || turn.Tokens.Input != 12 || turn.Tokens.Output != 3 {
			t.Fatalf("served-selected request/observation evidence: %+v", turn)
		}
	}
	var usage operatorread.OperatorAgentUsage
	requireServedJSONRPCResult(t, rt.Endpoint, "agent.usage", map[string]any{"run_id": fork.ForkRunID, "agent_id": "same-name"}, &usage)
	if len(usage.Breakdown) != 1 || usage.Usage.Exact.LedgerEntries != 1 || usage.Usage.Exact.InputTokens != 12 || usage.Usage.Exact.OutputTokens != 3 {
		t.Fatalf("served-selected spend: %+v", usage)
	}
	attemptID := requireCompletion2270BusinessInvocation(t, rt, profileID, calls, proof, "")
	var replay apiv1.RunForkExecutionResult
	requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &replay)
	requireCompletion2270BusinessInvocation(t, rt, profileID, calls, proof, attemptID)
	if !reflect.DeepEqual(fork, replay) || !reflect.DeepEqual(parent, readWorkspaceProofSourceDomain(t, owner, seed.RunID)) {
		t.Fatal("served-selected replay changed parent evidence or repeated the provider effect")
	}
}

func buildCompletion2270CLIImage(t *testing.T) string {
	t.Helper()
	base, err := exec.Command("docker", "image", "inspect", "--format", "{{.Id}}", "swarm-workspace:latest").CombinedOutput()
	if err != nil {
		t.Fatalf("inspect protocol-proof base image: %v: %s", err, base)
	}
	root, image := t.TempDir(), "swarm-completion-proof:"+uuid.NewString()
	if err := os.WriteFile(filepath.Join(root, "completion-proof-cli"), []byte(completion2270CLIProgram), 0o755); err != nil {
		t.Fatal(err)
	}
	dockerfile := fmt.Sprintf("FROM %s\nCOPY completion-proof-cli /usr/local/bin/completion-proof-cli\n", strings.TrimSpace(string(base)))
	if err := os.WriteFile(filepath.Join(root, "Dockerfile"), []byte(dockerfile), 0o600); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command("docker", "build", "--network=none", "--tag", image, root).CombinedOutput(); err != nil {
		t.Fatalf("build first-party protocol fixture: %v: %s", err, output)
	}
	t.Cleanup(func() {
		if output, err := exec.Command("docker", "image", "rm", image).CombinedOutput(); err != nil {
			t.Errorf("remove owned protocol fixture image: %v: %s", err, output)
		}
	})
	return image
}

type completion2270ObservedAgent struct {
	runtimemanager.Agent
	t *testing.T
}

func (a completion2270ObservedAgent) OnEvent(ctx context.Context, event events.Event) ([]events.Event, error) {
	events, err := a.Agent.OnEvent(ctx, event)
	if err != nil {
		a.t.Logf("completion-2270 concrete provider failure: %+v", err)
	}
	return events, err
}

func (p completion2270Transport) RoundTrip(request *http.Request) (*http.Response, error) {
	p.t.Helper()
	if request.URL.Hostname() != "api.anthropic.com" && request.URL.Hostname() != "completion-2270.test" {
		if p.base == nil {
			return nil, fmt.Errorf("unexpected provider proof destination %q", request.URL.Host)
		}
		return p.base.RoundTrip(request)
	}
	var body map[string]any
	if err := json.NewDecoder(request.Body).Decode(&body); err != nil || body["model"] != "request-A" {
		return nil, fmt.Errorf("provider primitive did not consume admitted request: body=%v error=%v", body, err)
	}
	p.requests.Add(1)
	var response string
	switch p.profile {
	case llmselection.BackendAnthropic:
		response = `{"model":"reported-B","content":[{"type":"text","text":"ok"}],"usage":{"input_tokens":12,"output_tokens":3}}`
	case llmselection.BackendOpenAICompatible:
		response = `{"model":"reported-B","choices":[{"message":{"role":"assistant","content":"ok"}}],"usage":{"prompt_tokens":12,"completion_tokens":3}}`
	case llmselection.BackendOpenAIResponses:
		response = `{"id":"response-2270","model":"reported-B","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":12,"output_tokens":3}}`
	default:
		return nil, fmt.Errorf("unexpected fake HTTP provider %q", p.profile)
	}
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(response))}, nil
}

func TestServedCompletion2270EvidenceBothStores(t *testing.T) {
	for _, profileID := range []string{llmselection.BackendAnthropic, llmselection.BackendOpenAICompatible, llmselection.BackendOpenAIResponses, llmselection.BackendClaudeCLI} {
		for _, backendName := range []string{"sqlite", "postgres"} {
			t.Run(profileID+"/"+backendName, func(t *testing.T) {
				backend := newClaudeAttemptProofBackend(t, backendName)
				t.Setenv("ANTHROPIC_API_KEY", "test-key")
				t.Setenv("OPENAI_API_KEY", "test-key")
				t.Setenv("OPENAI_COMPATIBLE_API_KEY", "test-key")
				t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "test-key")
				t.Setenv("SWARM_CLAUDE_USE_MCP", "0")
				cfg := &config.Config{LLM: config.LLMConfig{Backend: profileID, Models: llmselection.ModelAliases{"regular": {profileID: "request-A"}}}}
				cfg.LLM.OpenAICompatible.BaseURL = "http://completion-2270.test"
				cfg.LLM.OpenAIResponses.BaseURL = "http://completion-2270.test"
				cfg.LLM.ClaudeCLI.Command, cfg.LLM.ClaudeCLI.OutputFormat = "claude", "stream-json"
				cfg.Workspace.DockerBin = filepath.Join(t.TempDir(), "docker")
				t.Setenv("SWARM_CLAUDE_ATTEMPT_PROOF_CAPTURE", t.TempDir())
				t.Setenv("SWARM_CLAUDE_ATTEMPT_PROOF_MODE", "success")
				writeClaudeAttemptProofDocker(t, cfg.Workspace.DockerBin)
				var requests, callbacks atomic.Int32
				previousTransport := http.DefaultTransport
				http.DefaultTransport = completion2270Transport{t: t, profile: profileID, requests: &requests}
				t.Cleanup(func() { http.DefaultTransport = previousTransport })
				bus, work, _ := newClaudeAttemptProofEventBus(t, backend, 1)
				factory := runtimellm.RuntimeFactory{Cfg: cfg, Sessions: backend.sessions, LiveSessions: runtimellm.NewTransientLiveSessionAcquirer(backend.sessions),
					Conversations: backend.store, LockOwner: "completion-2270", Workspaces: claudeAttemptProofWorkspace{}, Events: bus,
					Credentials: runtimecredentials.NewEnvStore(), CompletionController: liveTestCompletionController(backend.store, backend.store, backend.store, claudeAttemptProofSpendProjection{})}
				provider, err := factory.Build()
				if err != nil {
					t.Fatal(err)
				}
				manager := runtimemanager.NewAgentManagerWithOptions(bus, func(actor runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
					return completion2270ObservedAgent{Agent: &claudeAttemptProofAgent{runtime: provider, config: actor, calls: &callbacks}, t: t}, nil
				}, runtimemanager.AgentManagerOptions{ExecutionPosture: executionposture.Live, BaseContext: backend.context(),
					LifecycleStore: storetest.AgentLifecycleFixture(t, backend.store), DeliveryStore: backend.store,
					SemanticSource: claudeAttemptProofSemanticSource(), Sessions: backend.sessions, SessionResetter: backend.store,
					PersistenceRoles: selectedStoreManagerPersistenceRoles(backend.store, bus), LLMBackend: profileID, ModelAliases: cfg.LLM.Models,
					WorkOwner: work, ReceiverExecution: eventreceiver.NormalExecution()}, backend.store)
				actor := claudeAttemptProofAgentConfig()
				profile, err := llmselection.ResolveActiveBackend(profileID)
				if err != nil {
					t.Fatal(err)
				}
				actor.LLMBackend, actor.ResolvedLLMBackend, actor.ResolvedLLMProvider = profileID, profileID, profile.Provider
				actor.ResolvedLLMTransport, actor.ResolvedModel = profile.Transport, "request-A"
				installClaudeAttemptProofManagerTopology(t, backend, manager, bus, actor)
				runClaudeAttemptProofManager(t, backend, manager)
				defer func() {
					if err := manager.Shutdown(); err != nil {
						t.Errorf("shutdown manager: %v", err)
					}
				}()
				eventID := publishClaudeAttemptProofEvent(t, backend, bus)
				waitClaudeAttemptProofReceipt(t, backend, eventID, runtimemanager.ReceiptStatusProcessed, &callbacks)
				read := backend.store.(operatorread.ConversationReader)
				conversations, err := read.ListOperatorConversations(backend.context(), operatorread.OperatorConversationListOptions{AgentID: actor.ID})
				if err != nil || len(conversations.Conversations) != 1 {
					t.Fatalf("conversation read: %+v, %v", conversations, err)
				}
				page, err := read.ListOperatorConversationTurns(backend.context(), operatorread.OperatorConversationTurnListOptions{SessionID: conversations.Conversations[0].SessionID})
				if err != nil || len(page.Turns) != 1 {
					t.Fatalf("turn read: %+v, %v", page, err)
				}
				turn := page.Turns[0]
				wantObserved := "reported-B"
				if profileID == llmselection.BackendClaudeCLI {
					wantObserved = "claude-proof"
				}
				if turn.CompletionEvidence.Request.RequestedModel != "request-A" || turn.CompletionEvidence.Request.ModelAlias != "regular" || turn.ResolvedModel != wantObserved || turn.Tokens == nil || turn.Tokens.Input != 12 {
					t.Fatalf("supported manager execution lost independent evidence: %+v", turn)
				}
				if profileID != llmselection.BackendClaudeCLI && requests.Load() != 1 {
					t.Fatalf("provider invoked %d times", requests.Load())
				}
			})
		}
	}
}
