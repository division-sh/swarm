package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type completion2270Transport struct {
	t        *testing.T
	profile  string
	requests *atomic.Int32
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
