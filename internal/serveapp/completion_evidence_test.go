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
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/effects"
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
	emit     bool
}

func TestServedCompletion2270RunServePublicReadbackBothStores(t *testing.T) {
	image := ""
	if os.Getenv("SWARM_TEST_WORKSPACE_MCP_DOCKER") == "1" {
		image = buildCompletion2270CLIImage(t)
	}
	for _, backend := range servedparity.RequiredBackends {
		for _, profileID := range []string{llmselection.BackendAnthropic, llmselection.BackendOpenAICompatible, llmselection.BackendOpenAIResponses, llmselection.BackendClaudeCLI} {
			for _, kind := range []string{"ordinary", "ordinary_emit", "siblings", "selected"} {
				t.Run(string(backend)+"/"+profileID+"/"+kind, func(t *testing.T) {
					if profileID == llmselection.BackendClaudeCLI {
						if os.Getenv("SWARM_TEST_WORKSPACE_MCP_DOCKER") != "1" {
							t.Skip("Docker-backed served CLI protocol proof; a skip earns no qualification credit")
						}
					}
					var rt completion2270ServedProof
					var invocationProof completion2270InvocationProof
					var selected *selectedStoreOwner
					priorCapabilities := buildSelectedAPICapabilities
					buildSelectedAPICapabilities = func(owner *selectedStoreOwner, req selectedAPICapabilityRequest) (selectedAPICapabilities, error) {
						selected = owner
						rt.Selected = owner
						if profileID == llmselection.BackendClaudeCLI {
							var err error
							req, err = completion2270ObserveSelectedWorkspace(req, &invocationProof)
							if err != nil {
								return selectedAPICapabilities{}, err
							}
						}
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
					if kind == "ordinary_emit" {
						root = canonicalrouting.CopyRootIngressServedCompletionEmit(t)
					}
					if kind == "siblings" {
						root = canonicalrouting.CopyRootIngressServedCompletionSiblings(t)
					}
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
					} else {
						rt.Backend = "postgres"
						configureCompletion2270PostgresFixture(t)
						stubServeRuntimeWorkspaceLifecycle(t)
						opts.StoreMode, opts.StoreModeSet = "postgres", true
					}
					text := fmt.Sprintf("runtime:\n  recovery_on_startup: true\n%sllm:\n  backend: %s\n  models:\n    regular:\n      %s: request-A\n  openai_compatible:\n    base_url: http://completion-2270.test\n  openai_responses:\n    base_url: http://completion-2270.test\n  session:\n    lock_ttl: 10s\n    rotate_after_turns: 40\n    rotate_on_parse_failures: 3\n", storeConfig, profileID, profileID)
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
						opts.TestRuntimeContextsReadyHook = invocationProof.observe
						cliapp.ConfiguredWorkspaceLifecycleForServe = func(_ *config.Config, projection *sourceartifact.RuntimeProjection, source semanticview.Source, _ cliapp.WorkspaceMountSources, _ cliapp.WorkspaceBackendSelection) (cliapp.ServeWorkspaceLifecycle, error) {
							manager := workspace.NewDockerManager()
							settings := workspace.DefaultDockerConfig()
							settings.WorkspaceImage, settings.SourceProjection = image, projection
							manager.SetConfig(settings)
							manager.SetSemanticSource(source)
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
					http.DefaultTransport = completion2270Transport{t: t, profile: profileID, requests: &requests, base: previous, emit: kind == "ordinary_emit" || kind == "siblings"}
					t.Cleanup(func() { http.DefaultTransport = previous })
					process := startServeRuntimeTestProcess(t, opts)
					process.waitForReadyLine()
					rt.Endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
					rt.AuthorContext = servedRuntimeProofAuthorActivityContext(t, servedTestProcessRuntime(t, process), rt.BundleHash)
					if profileID == llmselection.BackendClaudeCLI {
						requireCompletion2270NoBusinessStartup(t, rt.Selected.Effects(), &invocationProof)
					}
					if kind == "selected" {
						proveCompletion2270ServedSelected(t, rt, selected, profileID, &requests, &invocationProof)
						return
					}
					var runID string
					var before cliapp.DiagnosticRunDiagnosisResult
					var siblingInvocations []completion2270Invocation
					if kind == "siblings" {
						runID, before, siblingInvocations = proveCompletion2720ServedSiblings(t, rt, profileID, &requests, &invocationProof)
					} else {
						runID, before = proveCompletion2270ServedOrdinary(t, rt, profileID, wantObserved, &requests, &invocationProof, kind == "ordinary_emit")
					}
					if code := process.stop(); code != 0 {
						t.Fatalf("join completed provider runtime: code=%d\n%s", code, process.outputString())
					}
					process = startServeRuntimeTestProcess(t, opts)
					process.waitForReadyLine()
					rt.Endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
					var after cliapp.DiagnosticRunDiagnosisResult
					requireServedJSONRPCResult(t, rt.Endpoint, "run.diagnose", map[string]any{"run_id": runID}, &after)
					if after.OperationalState == nil || *after.OperationalState != "stalled" || !reflect.DeepEqual(before.Attributions, after.Attributions) || !reflect.DeepEqual(before.Instances, after.Instances) {
						t.Fatalf("restart lost or replaced completed-turn evidence: before=%+v after=%+v", before, after)
					}
					if profileID == llmselection.BackendClaudeCLI {
						attempts, err := storetest.ReadCompletionAttemptStorage(context.Background(), rt.Selected.Effects(), "claude_cli", "")
						want := 1
						if kind == "siblings" {
							want = 2
						}
						if err != nil || len(attempts) != want {
							t.Fatalf("restart relaunched CLI provider work: %+v %v", attempts, err)
						}
						for _, attempt := range attempts {
							if attempt.State != string(effects.StateSettled) {
								t.Fatalf("restart unsettled a completed CLI attempt: %+v", attempt)
							}
							if kind == "siblings" {
								matched := false
								for _, invocation := range siblingInvocations {
									matched = matched || invocation.AttemptID == attempt.AttemptID
								}
								if !matched {
									t.Fatalf("native attempt has no original physical invocation: %+v", attempt)
								}
								matched = false
								for _, attribution := range before.Attributions {
									matched = matched || slices.Contains(attribution.ProviderTurns, attempt.TargetID)
								}
								if !matched {
									t.Fatalf("native attempt has no exact public provider turn: %+v", attempt)
								}
							}
						}
					} else if want := int32(1); requests.Load() != want && !(kind == "siblings" && requests.Load() == 2) {
						t.Fatalf("restart re-prompted completed agent: calls=%d", requests.Load())
					}
				})
			}
		}
	}
}

func proveCompletion2720ServedSiblings(t *testing.T, rt completion2270ServedProof, profile string, calls *atomic.Int32, proof *completion2270InvocationProof) (string, cliapp.DiagnosticRunDiagnosisResult, []completion2270Invocation) {
	t.Helper()
	runID, seed := createCompletion2270WaitingRun(t, rt)
	publishCompletion2270BusinessEvent(t, rt, runID, seed)
	var outputs operatorread.OperatorEventListResult
	requireServedJSONRPCResult(t, rt.Endpoint, "event.list", map[string]any{"filter": map[string]any{"run_id": runID, "event_name": "external.observed"}, "limit": 100}, &outputs)
	if len(outputs.Events) != 1 || len(outputs.Events[0].Deliveries) != 1 || outputs.Events[0].Deliveries[0].SubscriberType != "agent" {
		t.Fatalf("same-slug fixture lost its exact receiving agent: %+v", outputs)
	}
	output := outputs.Events[0]
	// Async agent execution may settle before or after dispatch returns. Both
	// facts must hold within the same budget, without imposing a fake order.
	deadline := time.Now().Add(servedEventPublishLifecycleProbeWaitTimeout)
	rt.Probe.Expect(output.EventID).PostCommitDispatchCompleted().Within(servedEventPublishLifecycleProbeWaitTimeout)
	remaining := time.Until(deadline)
	if remaining <= 0 {
		t.Fatal("dispatch consumed the complete sibling settlement deadline")
	}
	rt.Probe.Expect(output.EventID).AgentDelivered(output.Deliveries[0].SubscriberID).Within(remaining)
	var diagnosis cliapp.DiagnosticRunDiagnosisResult
	requireServedJSONRPCResult(t, rt.Endpoint, "run.diagnose", map[string]any{"run_id": runID}, &diagnosis)
	if diagnosis.OperationalState == nil || *diagnosis.OperationalState != "stalled" || len(diagnosis.Attributions) != 2 {
		t.Fatalf("same-slug simultaneous stalls were collapsed: %+v", diagnosis)
	}
	byPath := map[string]operatorread.RunTurnAttribution{}
	for _, attribution := range diagnosis.Attributions {
		if attribution.AgentID == "" || attribution.DeliveryID == "" || attribution.LogicalTurnID == "" || attribution.Historical || len(attribution.ProviderTurns) != 1 {
			t.Fatalf("inexact same-slug turn: %+v", attribution)
		}
		byPath[attribution.FlowInstance] = attribution
	}
	root, child := byPath[runID], byPath["observation"]
	if root.Emission != operatorread.RunEmissionAccepted || !reflect.DeepEqual(root.EmittedEvents, []operatorread.RunAcceptedEmission{{EventID: output.EventID, EventType: output.EventName}}) || len(root.GrantedEmitTools) != 1 ||
		child.Emission != operatorread.RunEmissionNone || child.EventID != output.EventID || len(child.EmittedEvents) != 0 || len(child.GrantedEmitTools) != 0 ||
		root.DeliveryID == child.DeliveryID || root.LogicalTurnID == child.LogicalTurnID || root.ProviderTurns[0] == child.ProviderTurns[0] {
		t.Fatalf("same authored slug crossed turn/grant/publication ownership: root=%+v child=%+v", root, child)
	}
	var conversations operatorread.OperatorConversationListResult
	requireServedJSONRPCResult(t, rt.Endpoint, "conversation.list", map[string]any{"run_id": runID}, &conversations)
	if len(conversations.Conversations) != 2 {
		t.Fatalf("same-slug scopes lost an independent conversation: %+v", conversations)
	}
	seenTurns := map[string]bool{}
	for _, conversation := range conversations.Conversations {
		var page operatorread.OperatorConversationTurnListResult
		requireServedJSONRPCResult(t, rt.Endpoint, "conversation.list_turns", map[string]any{"session_id": conversation.SessionID}, &page)
		if len(page.Turns) != 1 {
			t.Fatalf("a scoped conversation lost its exact turn: %+v", page)
		}
		turn := page.Turns[0]
		var attribution operatorread.RunTurnAttribution
		matched := 0
		for _, candidate := range byPath {
			if slices.Contains(candidate.ProviderTurns, turn.TurnID) {
				attribution, matched = candidate, matched+1
			}
		}
		if matched != 1 || seenTurns[turn.TurnID] || turn.TriggerEventID != attribution.EventID || conversation.AgentID != attribution.AgentID {
			t.Fatalf("public turn/history crossed sibling origin: %+v, attribution=%+v", page, attribution)
		}
		seenTurns[turn.TurnID] = true
	}
	stdout, stderr, code := runServedCLICommand(t, rt.Endpoint, []string{"run", "status", runID, "--json"})
	var cliDiagnosis cliapp.DiagnosticRunDiagnosisResult
	if code != 0 || json.Unmarshal([]byte(stdout), &cliDiagnosis) != nil || !reflect.DeepEqual(diagnosis, cliDiagnosis) {
		t.Fatalf("CLI JSON lost a sibling attribution: code=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
	stdout, stderr, code = runServedCLICommand(t, rt.Endpoint, []string{"run", "status", runID})
	for _, value := range []string{"stalled", root.DeliveryID, child.DeliveryID, root.LogicalTurnID, child.LogicalTurnID, "no accepted emission", "1 accepted emission(s)"} {
		if code != 0 || !strings.Contains(stdout, value) {
			t.Fatalf("CLI text lost %q: code=%d stdout=%s stderr=%s", value, code, stdout, stderr)
		}
	}
	var invocations []completion2270Invocation
	if profile == llmselection.BackendClaudeCLI {
		var err error
		invocations, err = proof.read(context.Background())
		if err != nil || len(invocations) != 2 || invocations[0].InvocationID == invocations[1].InvocationID || invocations[0].AttemptID == invocations[1].AttemptID {
			t.Fatalf("expected exactly two original physical CLI invocations: %+v %v", invocations, err)
		}
	} else if calls.Load() != 2 {
		t.Fatalf("two business turns were re-prompted/re-driven: provider calls=%d", calls.Load())
	}
	return runID, diagnosis, invocations
}

func proveCompletion2270ServedOrdinary(t *testing.T, rt completion2270ServedProof, profileID, wantObserved string, requests *atomic.Int32, proof *completion2270InvocationProof, emit bool) (string, cliapp.DiagnosticRunDiagnosisResult) {
	t.Helper()
	runID, eventID := createCompletion2270WaitingRun(t, rt)
	publishCompletion2270BusinessEvent(t, rt, runID, eventID)
	var conversations operatorread.OperatorConversationListResult
	requireServedJSONRPCResult(t, rt.Endpoint, "conversation.list", map[string]any{"run_id": runID, "agent_id": "load-agent"}, &conversations)
	if len(conversations.Conversations) != 1 {
		t.Fatalf("served conversation: %+v", conversations)
	}
	sessionID := conversations.Conversations[0].SessionID
	var page operatorread.OperatorConversationTurnListResult
	requireServedJSONRPCResult(t, rt.Endpoint, "conversation.list_turns", map[string]any{"session_id": sessionID}, &page)
	wantTurns := 1
	if len(page.Turns) != wantTurns || page.Turns[0].ResolvedModel != wantObserved || page.Turns[0].CompletionEvidence.Request.RequestedModel != "request-A" {
		t.Fatalf("served admitted/provider identities collapsed: %+v, calls=%d", page, requests.Load())
	}
	var detail operatorread.OperatorPublicConversationTurnDetail
	requireServedJSONRPCResult(t, rt.Endpoint, "conversation.get_turn", map[string]any{"session_id": sessionID, "turn_id": page.Turns[0].TurnID}, &detail)
	if detail.Turn.CompletionEvidence.Request.RequestedModel != "request-A" || detail.Turn.ResolvedModel != wantObserved {
		t.Fatalf("served detail lost evidence: %+v", detail.Turn)
	}
	if emit && profileID != llmselection.BackendClaudeCLI {
		for _, activity := range detail.Turn.Activity {
			if activity.Kind == "publish" && activity.EventType == "external.observed" {
				t.Fatal("pre-tool provider row was rewritten to fabricate terminal publication evidence")
			}
		}
	}
	proveCompletion2270ServedAgentHistory(t, rt, runID, detail.Turn)
	var usage operatorread.OperatorAgentUsage
	requireServedJSONRPCResult(t, rt.Endpoint, "agent.usage", map[string]any{"run_id": runID, "agent_id": "load-agent"}, &usage)
	if len(usage.Breakdown) != 1 || usage.Usage.Exact.LedgerEntries != wantTurns || usage.Usage.Exact.InputTokens != int64(12*wantTurns) || usage.Breakdown[0].ResolvedModel != wantObserved || usage.Breakdown[0].CompletionEvidence.Request.RequestedModel != "request-A" {
		t.Fatalf("served spend lost exact request/report separation: %+v", usage)
	}
	attemptID := ""
	if profileID == llmselection.BackendClaudeCLI {
		attemptID = requireCompletion2270BusinessInvocation(t, rt.Selected.Effects(), profileID, requests, proof, "")
	} else if requests.Load() != int32(wantTurns) {
		t.Fatalf("provider calls=%d, want exactly %d", requests.Load(), wantTurns)
	}
	var diagnosis cliapp.DiagnosticRunDiagnosisResult
	if emit {
		var outputs operatorread.OperatorEventListResult
		requireServedJSONRPCResult(t, rt.Endpoint, "event.list", map[string]any{"filter": map[string]any{"run_id": runID, "event_name": "external.observed"}, "limit": 100}, &outputs)
		if len(outputs.Events) != 1 {
			t.Fatalf("provider's terminal publication was not accepted: %+v", outputs)
		}
		output := outputs.Events[0]
		if output.SourceEventID != detail.Turn.TriggerEventID || output.ProducerType != events.EventProducerAgent || output.Source != "load-agent" || len(output.Deliveries) != 1 || output.Deliveries[0].SubscriberType != "node" {
			t.Fatalf("terminal output lost its exact agent origin/consumer: %+v", output)
		}
		rt.Probe.Expect(output.EventID).NodeDelivered(output.Deliveries[0].SubscriberID).PostCommitDispatchCompleted().Within(servedEventPublishLifecycleProbeWaitTimeout)
		if _, err := rt.Selected.Runs().LoadRunDebugReport(context.Background(), runID, operatorread.RunDebugQueryOptions{}); err != nil {
			t.Fatalf("native accepted-output diagnostics: %v", err)
		}
	}
	requireServedJSONRPCResult(t, rt.Endpoint, "run.diagnose", map[string]any{"run_id": runID}, &diagnosis)
	if diagnosis.OperationalState == nil || *diagnosis.OperationalState != "stalled" {
		t.Fatalf("completed silent provider turn did not expose a stalled run: %+v", diagnosis)
	}
	var exact *operatorread.RunTurnAttribution
	for i := range diagnosis.Attributions {
		if diagnosis.Attributions[i].AgentID == "load-agent" {
			if exact != nil {
				t.Fatalf("silent provider turn acquired duplicate blame: %+v", diagnosis.Attributions)
			}
			exact = &diagnosis.Attributions[i]
		}
	}
	wantEmission, emissionText, wantOutputs := operatorread.RunEmissionNone, "no accepted emission", 0
	if emit {
		wantEmission, emissionText, wantOutputs = operatorread.RunEmissionAccepted, "1 accepted emission(s)", 1
	}
	if exact == nil || exact.EventID == "" || exact.EventID == eventID || exact.EventType != "item.processed" || exact.DeliveryID == "" || exact.LogicalTurnID == "" || exact.Historical ||
		exact.Emission != wantEmission || exact.EvidenceReason != "" || len(exact.ProviderTurns) != wantTurns || len(exact.EmittedEvents) != wantOutputs {
		t.Fatalf("public diagnostics lost the exact completed silent turn: %+v, turn=%+v", exact, detail.Turn)
	}
	for _, turn := range page.Turns {
		if !slices.Contains(exact.ProviderTurns, turn.TurnID) {
			t.Fatalf("attribution omitted physical provider turn %s: %+v", turn.TurnID, exact)
		}
	}
	if emit {
		var output operatorread.OperatorEventFull
		requireServedJSONRPCResult(t, rt.Endpoint, "event.get", map[string]any{"event_id": exact.EmittedEvents[0].EventID}, &output)
		if output.EventName != "external.observed" || output.SourceEventID != exact.EventID || output.RunID != runID {
			t.Fatalf("accepted emission has a foreign committed origin: %+v", output)
		}
	}
	stdout, stderr, code := runServedCLICommand(t, rt.Endpoint, []string{"run", "status", runID, "--json"})
	var cliDiagnosis cliapp.DiagnosticRunDiagnosisResult
	if code != 0 || json.Unmarshal([]byte(stdout), &cliDiagnosis) != nil || !reflect.DeepEqual(diagnosis, cliDiagnosis) {
		t.Fatalf("CLI JSON disagrees with the public attribution: code=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
	stdout, stderr, code = runServedCLICommand(t, rt.Endpoint, []string{"run", "status", runID})
	for _, want := range []string{"stalled", "load-agent", exact.EventID, exact.DeliveryID, exact.LogicalTurnID, emissionText} {
		if code != 0 || !strings.Contains(stdout, want) {
			t.Fatalf("CLI text omitted %q: code=%d stdout=%s stderr=%s", want, code, stdout, stderr)
		}
	}
	if profileID == llmselection.BackendClaudeCLI {
		requireCompletion2270BusinessInvocation(t, rt.Selected.Effects(), profileID, requests, proof, attemptID)
	} else if requests.Load() != int32(wantTurns) {
		t.Fatalf("diagnostics re-invoked provider: calls=%d want%d", requests.Load(), wantTurns)
	}
	return runID, diagnosis
}

func proveCompletion2270ServedAgentHistory(t *testing.T, rt completion2270ServedProof, runID string, want operatorread.OperatorPublicConversationTurn) {
	t.Helper()
	reads := rt.Selected.Agents()
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

func proveCompletion2270ServedSelected(t *testing.T, rt completion2270ServedProof, selected *selectedStoreOwner, profileID string, calls *atomic.Int32, proof *completion2270InvocationProof) {
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
	fact, err := prepareServeSourceArtifact(rt.AuthorContext, selected.SourceArtifactWriter(), loadWorkflowValidationBundleAt(t, root))
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
	attemptID := requireCompletion2270BusinessInvocation(t, rt.Selected.Effects(), profileID, calls, proof, "")
	var replay apiv1.RunForkExecutionResult
	requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &replay)
	requireCompletion2270BusinessInvocation(t, rt.Selected.Effects(), profileID, calls, proof, attemptID)
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
	if err := os.WriteFile(filepath.Join(root, completion2270InvocationFile), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	// Fresh native workspace volumes inherit only this fixture-owned witness.
	dockerfile := fmt.Sprintf("FROM %s\nCOPY completion-proof-cli /usr/local/bin/completion-proof-cli\nCOPY --chown=10001:10001 %s /workspace/%s\n", strings.TrimSpace(string(base)), completion2270InvocationFile, completion2270InvocationFile)
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
	ordinal := p.requests.Add(1)
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
	if p.emit && ordinal == 1 {
		switch p.profile {
		case llmselection.BackendAnthropic:
			response = `{"model":"reported-B","content":[{"type":"tool_use","id":"terminal-emission","name":"emit_external_observed","input":{}}],"stop_reason":"tool_use","usage":{"input_tokens":12,"output_tokens":3}}`
		case llmselection.BackendOpenAICompatible:
			response = `{"model":"reported-B","choices":[{"message":{"role":"assistant","tool_calls":[{"id":"terminal-emission","type":"function","function":{"name":"emit_external_observed","arguments":"{}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":12,"completion_tokens":3}}`
		case llmselection.BackendOpenAIResponses:
			response = `{"id":"response-emit","model":"reported-B","output":[{"type":"function_call","call_id":"terminal-emission","name":"emit_external_observed","arguments":"{}"}],"usage":{"input_tokens":12,"output_tokens":3}}`
		}
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
