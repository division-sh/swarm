package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

const (
	hitlProviderTestBundleHash = "bundle-v2:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	hitlProviderTestRunID      = "33333333-3333-4333-8333-333333333333"
	hitlProviderTestMailboxID  = "7b5cd83d-b971-4d49-b9ce-46e4b31823a5"
)

func TestManagedProvidersExecuteCanonicalNotifyHumanThroughSelectedStore(t *testing.T) {
	for _, backend := range []string{
		llmselection.BackendAnthropic,
		llmselection.BackendOpenAICompatible,
		llmselection.BackendOpenAIResponses,
		llmselection.BackendMock,
	} {
		t.Run(backend, func(t *testing.T) {
			store := &providerMailboxStore{mailboxStoreStub: mailboxStoreStub{id: hitlProviderTestMailboxID}}
			exec := NewExecutorWithOptions(nil, ExecutorOptions{MailboxStore: store, AuthorityProvider: allowMailboxAuthority{}})
			actor := managedHITLProviderActor(t, backend)
			definitions := exec.ToolDefinitionsForActor(actor)
			definition, ok := definitionMap(definitions)[NotifyHumanToolName]
			if !ok || definition.Description != managedHITLToolDescriptors()[0].description {
				t.Fatalf("real executor definitions = %#v, want canonical notify_human descriptor", definitions)
			}

			provider := newManagedHITLProviderFixture(t, backend, actor)
			ctx := managedHITLProviderContext(t, provider.harness, actor)
			seed := managedHITLProviderSeed(t, actor, provider.runtime)
			event := eventtest.ExistingRunRootIngress(
				eventtest.UUID("managed-hitl-provider:"+backend), events.EventType("work.requested"), "operator", "task-1",
				json.RawMessage(`{"candidate":"case-7"}`), 0, hitlProviderTestRunID, events.EventEnvelope{}, time.Unix(1, 0).UTC(),
			)
			ctx = runtimecorrelation.WithInboundEvent(ctx, event)

			withoutExecutor, err := llm.NewManagedConversation(seed, "mutation", definitions, agentmemory.Authored(false), 4, provider.runtime)
			if err != nil {
				t.Fatalf("NewManagedConversation mutation: %v", err)
			}
			if _, err := withoutExecutor.RunManaged(ctx, agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: event}); err == nil || !strings.Contains(err.Error(), "managed_capability_executor_missing") {
				t.Fatalf("missing real executor mutation error = %v", err)
			}

			conversation, err := llm.NewManagedConversation(seed, "task-1", definitions, agentmemory.Authored(false), 4, provider.runtime)
			if err != nil {
				t.Fatalf("NewManagedConversation: %v", err)
			}
			conversation.SetToolExecutor(exec)
			response, err := conversation.RunManaged(ctx, agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: event})
			if err != nil {
				t.Fatalf("RunManaged: %v", err)
			}
			if response == nil || response.Message.Content != "done" {
				t.Fatalf("response = %#v, want done after canonical tool continuation", response)
			}
			if store.inserts != 1 || store.last.Type != NotifyHumanMailboxItemType || store.last.Summary != "Strong match found" {
				t.Fatalf("selected-store notice = inserts %d item %#v", store.inserts, store.last)
			}
			if store.last.FromAgent != actor.ID || store.last.EntityID != actor.EntityID || store.last.FlowInstance != actor.CanonicalFlowPath() {
				t.Fatalf("selected-store notice provenance = %#v, actor %#v", store.last, actor)
			}
			if !provider.continuationObserved() {
				t.Fatalf("provider continuation did not receive exact queued/%s result; requests=%s", hitlProviderTestMailboxID, provider.requestBodies())
			}
		})
	}
}

type providerMailboxStore struct {
	mailboxStoreStub
	inserts int
}

func (s *providerMailboxStore) InsertMailboxItem(ctx context.Context, item MailboxItem) (string, error) {
	s.inserts++
	return s.mailboxStoreStub.InsertMailboxItem(ctx, item)
}

type managedHITLProviderFixture struct {
	runtime llm.Runtime
	harness *effecttest.Harness
	backend string
	mu      sync.Mutex
	bodies  [][]byte
}

func (f *managedHITLProviderFixture) continuationObserved() bool {
	if f.backend == llmselection.BackendMock {
		// The mock module parses and asserts the exact result before returning done.
		return true
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.bodies) == 0 {
		return false
	}
	var body any
	if err := json.Unmarshal(f.bodies[len(f.bodies)-1], &body); err != nil {
		return false
	}
	return containsExactQueuedToolResult(body)
}

func (f *managedHITLProviderFixture) requestBodies() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return fmt.Sprintf("%q", f.bodies)
}

func containsExactQueuedToolResult(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		if len(typed) == 2 && typed["status"] == "queued" && typed["mailbox_id"] == hitlProviderTestMailboxID {
			return true
		}
		for _, nested := range typed {
			if containsExactQueuedToolResult(nested) {
				return true
			}
		}
	case []any:
		for _, nested := range typed {
			if containsExactQueuedToolResult(nested) {
				return true
			}
		}
	case string:
		var nested any
		encoded := strings.TrimPrefix(typed, "Tool result:\n")
		if json.Unmarshal([]byte(encoded), &nested) == nil && containsExactQueuedToolResult(nested) {
			return true
		}
	}
	return false
}

func newManagedHITLProviderFixture(t *testing.T, backend string, actor models.AgentConfig) *managedHITLProviderFixture {
	t.Helper()
	fixture := &managedHITLProviderFixture{harness: effecttest.New(), backend: backend}
	fixture.harness.Token.AgentID = actor.ID
	fixture.harness.Token.Identity = actor.Identity

	var server *httptest.Server
	if backend != llmselection.BackendMock {
		server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read provider request: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			fixture.mu.Lock()
			fixture.bodies = append(fixture.bodies, body)
			call := len(fixture.bodies)
			fixture.mu.Unlock()
			w.Header().Set("content-type", "application/json")
			switch backend {
			case llmselection.BackendAnthropic:
				if call == 1 {
					_, _ = io.WriteString(w, `{"model":"test-model","usage":{"input_tokens":7,"output_tokens":3},"content":[{"type":"tool_use","id":"call_1","name":"notify_human","input":{"summary":"Strong match found","context":{"candidate":"case-7"}}}]}`)
				} else {
					_, _ = io.WriteString(w, `{"model":"test-model","usage":{"input_tokens":9,"output_tokens":2},"content":[{"type":"text","text":"done"}]}`)
				}
			case llmselection.BackendOpenAICompatible:
				if call == 1 {
					_, _ = io.WriteString(w, `{"model":"test-model","choices":[{"message":{"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"notify_human","arguments":"{\"summary\":\"Strong match found\",\"context\":{\"candidate\":\"case-7\"}}"}}]}}],"usage":{"prompt_tokens":7,"completion_tokens":3,"total_tokens":10}}`)
				} else {
					_, _ = io.WriteString(w, `{"model":"test-model","choices":[{"message":{"role":"assistant","content":"done"}}],"usage":{"prompt_tokens":9,"completion_tokens":2,"total_tokens":11}}`)
				}
			case llmselection.BackendOpenAIResponses:
				if call == 1 {
					_, _ = io.WriteString(w, `{"id":"resp_1","model":"test-model","output":[{"id":"fc_1","type":"function_call","call_id":"call_1","name":"notify_human","arguments":"{\"summary\":\"Strong match found\",\"context\":{\"candidate\":\"case-7\"}}"}],"usage":{"input_tokens":7,"output_tokens":3,"total_tokens":10}}`)
				} else {
					_, _ = io.WriteString(w, `{"id":"resp_2","model":"test-model","output":[{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}],"usage":{"input_tokens":9,"output_tokens":2,"total_tokens":11}}`)
				}
			}
		}))
		t.Cleanup(server.Close)
	}

	if backend == llmselection.BackendAnthropic {
		originalTransport := http.DefaultTransport
		serverURL := server.URL
		http.DefaultTransport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			clone := req.Clone(req.Context())
			clone.URL.Scheme = "http"
			clone.URL.Host = strings.TrimPrefix(serverURL, "http://")
			return server.Client().Transport.RoundTrip(clone)
		})
		t.Cleanup(func() { http.DefaultTransport = originalTransport })
	}

	cfg := managedHITLProviderConfig(backend, func() string {
		if server != nil {
			return server.URL
		}
		return ""
	}())
	t.Setenv("ANTHROPIC_API_KEY", "test-key")
	t.Setenv("OPENAI_COMPATIBLE_API_KEY", "test-key")
	t.Setenv("OPENAI_API_KEY", "test-key")
	registry := sessions.NewInMemoryRegistry(time.Second)
	controller := runtimeeffects.NewCompletionController(fixture.harness, fixture.harness, fixture.harness, fixture.harness).WithExecutionPosture(executionposture.Live)
	runtime, err := (llm.RuntimeFactory{
		Cfg: cfg, Sessions: registry, LiveSessions: llm.NewTransientLiveSessionAcquirer(registry), LockOwner: "hitl-provider-test",
		Credentials: runtimecredentials.NewEnvStore(), CompletionController: controller,
	}).Build()
	if err != nil {
		t.Fatalf("build %s runtime: %v", backend, err)
	}
	fixture.runtime = runtime
	return fixture
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func managedHITLProviderConfig(backend, baseURL string) *config.Config {
	return &config.Config{LLM: config.LLMConfig{
		Backend:          backend,
		Session:          config.LLMSessionConfig{LockTTL: time.Second, RotateAfterTurns: 40, RotateOnParseFailures: 3},
		ClaudeAPI:        config.ClaudeAPIConfig{DefaultModel: "test-model", HaikuModel: "test-model"},
		OpenAICompatible: config.OpenAICompatibleConfig{BaseURL: baseURL, DefaultModel: "test-model", LowCostModel: "test-model"},
		OpenAIResponses:  config.OpenAIResponsesConfig{BaseURL: baseURL},
	}}
}

func managedHITLProviderActor(t *testing.T, backend string) models.AgentConfig {
	t.Helper()
	identity := agentidentitytest.RootRuntime(t, "hitl-agent", "hitl-provider-conformance")
	actor := models.AgentConfig{
		ID: "hitl-agent", Identity: identity, Role: "matcher", ExecutionMode: runtimeeffects.ExecutionModeLive,
		EntityID: "11111111-1111-4111-8111-111111111111", FlowPath: "", Memory: agentmemory.Authored(false),
	}
	if backend == llmselection.BackendMock {
		source := []byte(fmt.Sprintf(`
import json

def handle(input):
    assert input["tools"][0]["name"] == "notify_human"
    if not input["tool_results"]:
        return {"calls": [{"id": "call_1", "name": "notify_human", "arguments": {"summary": "Strong match found", "context": {"candidate": "case-7"}}}], "usage": {"input_tokens": 7, "output_tokens": 3}}
    continuation = json.loads(input["tool_results"][0]["content"])
    batch = continuation if isinstance(continuation, list) else continuation["tool_result"]
    assert len(batch) == 1
    assert batch[0]["name"] == "notify_human"
    assert batch[0]["ok"] is True
    assert batch[0]["result"] == {"status": "queued", "mailbox_id": "%s"}
    return {"text": "done", "usage": {"input_tokens": 9, "output_tokens": 2}}
`, hitlProviderTestMailboxID))
		actor.ExecutionMode = runtimeeffects.ExecutionModeMock
		actor.Mock = mockperformance.Performance{
			Kind: mockperformance.KindPython, SourcePath: "mocks/hitl_provider.py", Source: source,
			Digest: "sha256:" + runtimeeffects.Fingerprint(source),
		}
	}
	return actor
}

func managedHITLProviderSeed(t *testing.T, actor models.AgentConfig, runtime llm.Runtime) agentframe.SessionSeed {
	t.Helper()
	contract, ok := llm.ProviderContractForRuntime(runtime)
	if !ok {
		t.Fatalf("runtime %T has no provider contract", runtime)
	}
	intent, err := agentintent.Resolve(agentintent.SourceInline, "inline", "agents.yaml#agents.hitl-agent.intent", "Complete the admitted business work using only authorized capabilities.")
	if err != nil {
		t.Fatalf("resolve intent: %v", err)
	}
	prompt, err := agentintent.IntentOnlyPrompt(intent)
	if err != nil {
		t.Fatalf("derive prompt: %v", err)
	}
	providerPrompt, err := agentintent.AssembleProviderPrompt(intent, nil, prompt, agentintent.RuntimeEnvironmentContext())
	if err != nil {
		t.Fatalf("assemble provider prompt: %v", err)
	}
	return agentframe.SessionSeed{
		AgentIdentity: actor.Identity, Role: actor.Role, Intent: intent, ProviderPrompt: providerPrompt,
		RuntimeMode: contract.RuntimeMode, Provider: contract.Provider, Transport: string(contract.Transport),
		ModelAlias: "regular", Model: "test-model",
	}
}

func managedHITLProviderContext(t *testing.T, harness *effecttest.Harness, actor models.AgentConfig) context.Context {
	t.Helper()
	ctx := harness.CompletionContext("managed-hitl-provider:" + actor.ID)
	admission, ok := managedexecution.FromContext(ctx)
	if !ok {
		t.Fatal("managed HITL provider context requires execution admission")
	}
	admission.BundleHash = hitlProviderTestBundleHash
	ctx = managedexecution.WithAdmission(ctx, admission)
	ctx = models.WithActor(ctx, actor)
	ctx = runtimecorrelation.WithRunID(ctx, hitlProviderTestRunID)
	ctx = agentmemory.WithExecution(ctx, agentmemory.Authored(false), agentmemory.Identity{RunID: hitlProviderTestRunID, Agent: actor.Identity})
	if actor.ExecutionMode == runtimeeffects.ExecutionModeMock {
		ctx = runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeMock)
	}
	fact, err := runtimecorrelation.NewSourceArtifactFact(hitlProviderTestBundleHash)
	if err != nil {
		t.Fatalf("bundle source fact: %v", err)
	}
	ctx = runtimecorrelation.WithSourceArtifactFact(ctx, fact)
	process := worklifetime.NewProcess()
	owner, err := process.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: "hitl-provider-runtime", BundleHash: hitlProviderTestBundleHash})
	if err != nil {
		t.Fatalf("create runtime occurrence: %v", err)
	}
	t.Cleanup(func() {
		waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := owner.RetireAndWait(waitCtx); err != nil {
			t.Errorf("retire runtime occurrence: %v", err)
		}
		if _, err := process.Join(waitCtx); err != nil {
			t.Errorf("join process owner: %v", err)
		}
	})
	return worklifetime.WithOccurrence(worklifetime.WithProcess(ctx, process), owner)
}
