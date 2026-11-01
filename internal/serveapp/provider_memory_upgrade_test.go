package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/llmapi"
	"gopkg.in/yaml.v3"
)

func TestProviderMemorySurvivesAdmittedBundleUpgradeBothStores(t *testing.T) {
	for _, profile := range []string{"openai_compatible", "openai_responses", "anthropic", "claude_cli"} {
		for _, backend := range servedparity.RequiredBackends {
			t.Run(profile+"/"+string(backend), func(t *testing.T) {
				isolateCLIAPIConfigEnv(t)
				unsetStoreSelectorEnv(t)
				stubServeRuntimeWorkspaceLifecycle(t)
				root := canonicalrouting.CopyRootIngressServedSessionCleanup(t)
				setUpgradeMemoryAgent(t, root, "old")
				configPath := writeServeRuntimeTestConfig(t)
				storeConfig := ""
				probe := lifecycletest.New(t, lifecycletest.WithTimeout(servedEventPublishLifecycleProbeWaitTimeout))
				opts := cliapp.ServeOptions{SourceRoot: root, ConfigPath: configPath, SelfCheck: true, Verbose: true,
					APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", TestLifecycleProbe: probe}
				if backend == servedparity.BackendDefaultSQLite {
					storeConfig = fmt.Sprintf("store:\n  backend: sqlite\n  sqlite:\n    path: %s\n", filepath.Join(t.TempDir(), "store.db"))
				} else {
					configureCompletion2270PostgresFixture(t)
					opts.StoreMode, opts.StoreModeSet = "postgres", true
				}
				text := fmt.Sprintf("runtime:\n  recovery_on_startup: true\n%sllm:\n  backend: %s\n  models:\n    memory-old: {%s: memory-old}\n    memory-new: {%s: memory-new}\n  session:\n    lock_ttl: 10s\n    rotate_after_turns: 40\n    rotate_on_parse_failures: 3\n", storeConfig, profile, profile, profile)
				var provider *llmapi.Server
				if profile == "claude_cli" {
					if os.Getenv("SWARM_TEST_WORKSPACE_MCP_DOCKER") != "1" {
						t.Skip("Docker-backed fake CLI journey; a skip is not execution proof")
					}
					configureUpgradeMemoryCLI(t, &opts, &text)
				} else {
					configureHostWorkspaceProofLifecycle(t)
					path := map[string]string{"anthropic": "/v1/messages", "openai_compatible": "/v1/chat/completions", "openai_responses": "/v1/responses"}[profile]
					provider = llmapi.New(t, llmapi.Protocol(profile), path, "memory-key", upgradeMemoryReply)
					if profile == "anthropic" {
						redirectExternalHosts(t, map[string]string{"api.anthropic.com": provider.URL})
					} else {
						text += fmt.Sprintf("  %s:\n    base_url: %s/v1\n", profile, provider.URL)
					}
				}
				if err := os.WriteFile(configPath, []byte(withTestProviderTriggerPlatformInventory(t, text)), 0o600); err != nil {
					t.Fatal(err)
				}
				credentialPath := filepath.Join(t.TempDir(), "credentials.json")
				t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
				credentials, err := runtimecredentials.NewFileStore(credentialPath)
				if err != nil {
					t.Fatal(err)
				}
				selection, err := llmselection.ResolveActiveBackend(profile)
				if err != nil {
					t.Fatal(err)
				}
				if err := credentials.Set(context.Background(), selection.Credential.EnvVar, "memory-key"); err != nil {
					t.Fatal(err)
				}
				process := startServeRuntimeTestProcess(t, opts)
				defer func() {
					if process != nil && process.stop() != 0 {
						t.Error("memory upgrade runtime did not shut down cleanly")
					}
				}()
				process.waitForReadyLine()
				endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
				lastEventID := ""
				defer func() {
					if t.Failed() && process != nil {
						if lastEventID != "" {
							var event operatorread.OperatorEventFull
							requireServedJSONRPCResult(t, endpoint, "event.get", map[string]any{"event_id": lastEventID}, &event)
							readback, _ := json.Marshal(event)
							t.Logf("upgrade event readback: %s", readback)
						}
						var conversations operatorread.OperatorConversationListResult
						requireServedJSONRPCResult(t, endpoint, "conversation.list", map[string]any{}, &conversations)
						readback, _ := json.Marshal(conversations)
						t.Logf("upgrade conversation readback: %s", readback)
						for _, conversation := range conversations.Conversations {
							var turns operatorread.OperatorConversationTurnListResult
							requireServedJSONRPCResult(t, endpoint, "conversation.list_turns", map[string]any{"session_id": conversation.SessionID}, &turns)
							readback, _ := json.Marshal(turns)
							t.Logf("upgrade turn readback: %s", readback)
						}
						if provider != nil {
							t.Logf("upgrade provider requests: %d", len(provider.Requests()))
						}
					}
				}()
				seed := requireServedEventPublishRPCResult(t, endpoint, map[string]any{
					"event_name": "item.received", "bundle_hash": servedEventPublishFixtureBundleHash(t, root),
					"payload": map[string]any{"item_id": "memory-upgrade"}, "idempotency_key": "memory-seed",
				})
				probe.Expect(seed.EventID).NodeDelivered(identitytest.RootNode(t, "item-handler").Key()).Within(servedEventPublishLifecycleProbeWaitTimeout)
				lastEventID = publishUpgradeMemoryTurn(t, endpoint, seed.RunID, seed.EventID, 1, probe)
				session := requireProviderMemoryTurns(t, endpoint, seed.RunID, 1)
				if code := process.stop(); code != 0 {
					t.Fatalf("stop before upgrade: exit=%d", code)
				}
				process = nil
				setUpgradeMemoryAgent(t, root, "new")
				process = startServeRuntimeTestProcess(t, opts)
				process.waitForReadyLine()
				endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
				lastEventID = publishUpgradeMemoryTurn(t, endpoint, seed.RunID, seed.EventID, 2, probe)
				if current := requireProviderMemoryTurns(t, endpoint, seed.RunID, 2); current != session {
					t.Fatalf("upgrade replaced memory session: before=%s after=%s", session, current)
				}
				if provider != nil && len(provider.Requests()) != 2 {
					t.Fatalf("provider requests=%d, want exactly two turns", len(provider.Requests()))
				}
			})
		}
	}
}

func setUpgradeMemoryAgent(t *testing.T, root, revision string) {
	t.Helper()
	path := filepath.Join(root, "hold", "agents.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var agents map[string]any
	if err := yaml.Unmarshal(raw, &agents); err != nil {
		t.Fatal(err)
	}
	agents["load-agent"].(map[string]any)["model"] = "memory-" + revision
	raw, err = yaml.Marshal(agents)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "hold", "prompts", "load-agent.md"), []byte("Memory upgrade prompt "+revision+".\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func publishUpgradeMemoryTurn(t *testing.T, endpoint, runID, sourceEventID string, turn int, probe *lifecycletest.Probe) string {
	t.Helper()
	result := requireServedEventPublishRPCResult(t, endpoint, map[string]any{
		"event_name": "item.agent_hold", "run_id": runID, "source_event_id": sourceEventID,
		"payload": map[string]any{"note": fmt.Sprintf("upgrade input %d", turn)}, "idempotency_key": fmt.Sprintf("memory-turn-%d", turn),
	})
	if result.RunID != runID || result.EventID == "" || result.NewRunCreated {
		t.Fatalf("memory turn changed run or lost its event: %+v", result)
	}
	matched := false
	for _, delivery := range result.Deliveries {
		if delivery.SubscriberType == "agent" {
			matched = true
			probe.Expect(result.EventID).AgentDelivered(delivery.SubscriberID).Within(servedEventPublishLifecycleProbeWaitTimeout)
		}
	}
	if !matched {
		t.Fatal("memory turn has no admitted agent delivery")
	}
	return result.EventID
}

func upgradeMemoryReply(request llmapi.Request) (llmapi.Reply, error) {
	revision := "old"
	if request.Number == 2 {
		revision = "new"
	}
	var body struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(request.Body, &body); err != nil {
		return llmapi.Reply{}, err
	}
	if request.Number > 2 || body.Model != "memory-"+revision || !strings.Contains(string(request.Body), "Memory upgrade prompt "+revision) ||
		!strings.Contains(string(request.Body), fmt.Sprintf("upgrade input %d", request.Number)) {
		return llmapi.Reply{}, fmt.Errorf("turn %d lost its current prompt, model or input", request.Number)
	}
	if request.Number == 2 && (!strings.Contains(string(request.Body), "upgrade input 1") || !strings.Contains(string(request.Body), "upgrade answer 1")) {
		return llmapi.Reply{}, fmt.Errorf("upgraded turn lost its retained user/assistant history")
	}
	return llmapi.Reply{ID: fmt.Sprintf("memory-%d", request.Number), TextID: fmt.Sprintf("answer-%d", request.Number), Model: body.Model, Text: fmt.Sprintf("upgrade answer %d", request.Number), InputTokens: 10, OutputTokens: 3, TotalTokens: 13}, nil
}

func configureUpgradeMemoryCLI(t *testing.T, opts *cliapp.ServeOptions, text *string) {
	t.Helper()
	program := strings.Replace(completion2270CLIProgram, "if (!startup && flags.get('--model') !== 'request-A') throw Error('missing exact business model');", `
if (!startup) {
  const revision = flags.get('--model') === 'memory-old' ? 'old' : 'new';
  if (flags.get('--model') !== 'memory-' + revision || !flags.get('--system-prompt')?.includes('Memory upgrade prompt ' + revision)) throw Error('upgraded invocation lost current prompt/model');
  if (revision === 'new') {
    const parent = flags.get('--resume');
    if (!parent || !flags.get('--fork-session')) throw Error('upgraded invocation lost provider parent');
    const directory = process.env.CLAUDE_CONFIG_DIR + '/projects/' + process.cwd().replace(/[^a-zA-Z0-9]/g, '-');
    if (!fs.readFileSync(directory + '/' + parent + '.jsonl', 'utf8').includes('upgrade input 1')) throw Error('upgraded invocation lost provider history');
  }
}`, 1)
	program = strings.Replace(program, "{type: 'assistant', sessionId: session}", "{type: 'assistant', sessionId: session, input}", 1)
	image := buildCompletion2270CLIImageWithProgram(t, program)
	*text += fmt.Sprintf("  claude_cli:\n    command: completion-proof-cli\n    output_format: stream-json\nworkspace:\n  image: %s\n", image)
	opts.WorkspaceBackend, opts.WorkspaceBackendSet = workspace.BackendDocker, true
	network, err := exec.Command("docker", "network", "inspect", workspace.DefaultWorkspaceNetwork(), "--format", "{{json .IPAM.Config}}").CombinedOutput()
	var addresses []struct{ Gateway string }
	if err != nil || json.Unmarshal(network, &addresses) != nil || len(addresses) != 1 || net.ParseIP(addresses[0].Gateway) == nil {
		t.Fatalf("inspect Docker fixture interface: %v: %s", err, network)
	}
	opts.MCPListenAddr = net.JoinHostPort(addresses[0].Gateway, "0")
	previous := cliapp.ConfiguredWorkspaceLifecycleForServe
	cliapp.ConfiguredWorkspaceLifecycleForServe = func(_ *config.Config, projection *sourceartifact.RuntimeProjection, source semanticview.Source, _ cliapp.WorkspaceMountSources, _ cliapp.WorkspaceBackendSelection) (cliapp.ServeWorkspaceLifecycle, error) {
		manager := workspace.NewDockerManager()
		settings := workspace.DefaultDockerConfig()
		settings.WorkspaceImage, settings.SourceProjection = image, projection
		manager.SetConfig(settings)
		manager.SetSemanticSource(source)
		return manager, nil
	}
	t.Cleanup(func() { cliapp.ConfiguredWorkspaceLifecycleForServe = previous })
}
