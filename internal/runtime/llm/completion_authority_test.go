package llm

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/google/uuid"
)

func TestManagedSessionAdoptionFailsClosedBeforeProviderDispatch(t *testing.T) {
	harness := effecttest.New()
	setEffectHarnessAgent(t, harness, "agent-1", "support/inst-1")
	ctx := testManagedConversationContext(t, harness, "agent-1", "support/inst-1", "support")
	registry := sessions.NewInMemoryRegistry(time.Second)
	identity := testMemoryIdentity("agent-1", "support/inst-1")
	current, err := registry.Acquire(ctx, identity, "worker-1")
	if err != nil {
		t.Fatalf("seed durable session: %v", err)
	}
	if err := registry.Release(ctx, current); err != nil {
		t.Fatalf("release durable session: %v", err)
	}

	var providerRequests atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		providerRequests.Add(1)
	}))
	defer provider.Close()

	runtime := NewAnthropicAPIRuntime(&config.Config{}, registry, "worker-1", nil, nil)
	runtime.apiURL = provider.URL
	runtime.apiKey = "test-key"
	runtime.completionController = liveTestCompletionController(harness, harness, harness, harness)
	conversation := newTestManagedConversation(t, "agent-1", "support/inst-1", "support", nil, testMemory(), 1, runtime)
	conversation.SetToolExecutor(openAIToolExecutor{})
	conversation.Session = &Session{
		ID:             uuid.NewString(),
		AgentID:        "agent-1",
		Memory:         testMemory(),
		MemoryIdentity: identity,
		SystemPrompt:   conversation.SystemPrompt,
	}

	_, err = conversation.RunManaged(ctx, agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: testManagedEvent("agent-1")})
	failure, ok := runtimefailures.As(err)
	if !ok || failure.Failure.Detail.Code != "managed_capability_turn_identity_mismatch" {
		t.Fatalf("managed adoption failure = %#v, want managed_capability_turn_identity_mismatch", failure)
	}
	if conversation.Session.ID != current.SessionID {
		t.Fatalf("adopted session id = %q, want durable %q", conversation.Session.ID, current.SessionID)
	}
	if got := providerRequests.Load(); got != 0 {
		t.Fatalf("provider requests = %d, want fail closed before dispatch", got)
	}
}

func TestAllManagedAdaptersGateAdoptedSessionIdentityBeforeProviderLaunch(t *testing.T) {
	tests := []struct {
		file       string
		adoption   string
		launchCall string
	}{
		{file: "api_runtime.go", adoption: "if lease.SessionID != s.ID", launchCall: "r.sendAdmittedRequest("},
		{file: "openai_compatible_runtime.go", adoption: "if lease.SessionID != s.ID", launchCall: "r.sendAdmittedRequest("},
		{file: "openai_responses_runtime.go", adoption: "if lease.SessionID != s.ID", launchCall: "r.sendAdmittedRequest("},
		{file: "cli_runtime.go", adoption: "if lease.SessionID != s.ID", launchCall: "r.runWithPreparedPrompt("},
		{file: "mock_runtime.go", adoption: "if lease.SessionID != session.ID", launchCall: "executeMockCompletion("},
	}
	for _, tt := range tests {
		t.Run(tt.file, func(t *testing.T) {
			raw, err := os.ReadFile(tt.file)
			if err != nil {
				t.Fatalf("read adapter: %v", err)
			}
			source := string(raw)
			acquire := strings.Index(source, "acquireContinuedMemory(")
			adopt := strings.Index(source, tt.adoption)
			gate := strings.Index(source, "prepareCompletionContext(")
			launch := strings.Index(source, tt.launchCall)
			if acquire < 0 || adopt <= acquire || gate <= adopt || launch <= gate {
				t.Fatalf("adapter chronology acquire=%d adopt=%d gate=%d launch=%d", acquire, adopt, gate, launch)
			}
		})
	}
}

func TestCompletionAttemptHeartbeatLossCancelsExecutionAndForcesUncertainty(t *testing.T) {
	harness := effecttest.New()
	injected := errors.New("injected completion heartbeat failure")
	harness.HeartbeatErr = injected
	harness.HeartbeatFailAfter = 1
	ctx := llmTestWorkContext(t, harness.CompletionContext("heartbeat-loss"))
	handle, err := runtimeeffects.BeginCompletion(ctx, "anthropic_api", []byte("heartbeat"), nil)
	if err != nil {
		t.Fatalf("authorize completion: %v", err)
	}
	heartbeatCtx, heartbeat, err := startCompletionAttemptHeartbeatWithTiming(ctx, handle, time.Millisecond, time.Minute)
	if err != nil {
		t.Fatalf("start completion heartbeat: %v", err)
	}
	select {
	case <-heartbeatCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("heartbeat failure did not cancel provider execution context")
	}
	if !errors.Is(context.Cause(heartbeatCtx), injected) {
		t.Fatalf("heartbeat cancellation cause=%v, want injected failure", context.Cause(heartbeatCtx))
	}
	dispatch := &completionDispatch{handle: handle, state: runtimeeffects.StateSettled}
	err = finishCompletionDispatchHeartbeat(dispatch, heartbeat, nil)
	if err == nil || dispatch.state != runtimeeffects.StateOutcomeUncertain {
		t.Fatalf("heartbeat finish err=%v state=%s, want outcome uncertainty", err, dispatch.state)
	}
	failure, ok := runtimefailures.As(err)
	if !ok || failure.Failure.Detail.Code != "completion_attempt_heartbeat_lost" {
		t.Fatalf("heartbeat failure=%v, want completion_attempt_heartbeat_lost", err)
	}
}

func TestCompletionHeartbeatRetirementHandoff(t *testing.T) {
	harness := effecttest.New()
	process := worklifetime.NewProcess()
	owner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: "completion-runtime",
		BundleHash:        "completion-bundle",
	})
	if err != nil {
		t.Fatalf("new runtime occurrence: %v", err)
	}
	ctx := worklifetime.WithOccurrence(harness.CompletionContext("retirement"), owner)
	handle, err := runtimeeffects.BeginCompletion(ctx, "anthropic_api", []byte("heartbeat"), nil)
	if err != nil {
		t.Fatalf("authorize completion: %v", err)
	}
	_, heartbeat, err := startCompletionAttemptHeartbeatWithTiming(ctx, handle, time.Hour, time.Minute)
	if err != nil {
		t.Fatalf("start completion heartbeat: %v", err)
	}
	if got := owner.ActiveCount(); got != 1 {
		t.Fatalf("active completion heartbeat work = %d, want 1", got)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := owner.RetireAndWait(waitCtx); err != nil {
		t.Fatalf("retire runtime occurrence: %v", err)
	}
	if err := heartbeat.Stop(); err != nil {
		t.Fatalf("stop retired completion heartbeat: %v", err)
	}
	if got := owner.ActiveCount(); got != 0 {
		t.Fatalf("active completion heartbeat work after retirement = %d, want 0", got)
	}
	if _, err := process.Join(waitCtx); err != nil {
		t.Fatalf("join process: %v", err)
	}
}

func TestCompletionHeartbeatUsesProcessRootForOperatorWork(t *testing.T) {
	harness := effecttest.New()
	process := worklifetime.NewProcess()
	ctx := worklifetime.WithProcess(harness.CompletionContext("operator-process"), process)
	handle, err := runtimeeffects.BeginCompletion(ctx, "anthropic_api", []byte("heartbeat"), nil)
	if err != nil {
		t.Fatalf("authorize completion: %v", err)
	}
	_, heartbeat, err := startCompletionAttemptHeartbeatWithTiming(ctx, handle, time.Hour, time.Minute)
	if err != nil {
		t.Fatalf("start completion heartbeat: %v", err)
	}
	if got := process.ActiveCount(); got != 1 {
		t.Fatalf("active process-root heartbeat work = %d, want 1", got)
	}
	if err := heartbeat.Stop(); err != nil {
		t.Fatalf("stop process-root heartbeat: %v", err)
	}
	if got := process.ActiveCount(); got != 0 {
		t.Fatalf("active process-root heartbeat work after stop = %d, want 0", got)
	}
	joinCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := process.Join(joinCtx); err != nil {
		t.Fatalf("join process: %v", err)
	}
}

func TestCompletionAdaptersDoNotRetainLegacyTurnOrBudgetOwners(t *testing.T) {
	files := []string{
		"api_runtime.go",
		"cli_runtime.go",
		"cli_runtime_helpers.go",
		"factory.go",
		"openai_compatible_runtime.go",
		"openai_responses_runtime.go",
	}
	forbidden := []string{
		"AppendAgentTurn(",
		"persistValidationTurn",
		"TurnPersistence",
		"BudgetGuard",
		"LockExecutionScope(",
		"IsEntityEmergency(",
	}
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		for _, token := range forbidden {
			if strings.Contains(string(raw), token) {
				t.Errorf("%s retains retired completion owner %q", file, token)
			}
		}
	}
}

func TestCompletionBudgetScopes(t *testing.T) {
	configured := &config.Config{Extensions: map[string]any{
		"budget": map[string]any{
			"system_monthly_cap":     100,
			"global_monthly_cap":     50,
			"per_entity_monthly_cap": 20,
		},
	}}
	for _, tc := range []struct {
		name     string
		cfg      *config.Config
		entityID string
		want     []runtimeeffects.BudgetAdmissionScope
	}{
		{name: "nil config"},
		{name: "no caps", cfg: &config.Config{}, want: []runtimeeffects.BudgetAdmissionScope{}},
		{name: "system and global", cfg: configured, want: []runtimeeffects.BudgetAdmissionScope{
			{Kind: "system", CapUSD: 100},
			{Kind: "global", CapUSD: 50},
		}},
		{name: "system and entity", cfg: configured, entityID: " entity-1 ", want: []runtimeeffects.BudgetAdmissionScope{
			{Kind: "system", CapUSD: 100},
			{Kind: "entity", Key: "entity-1", CapUSD: 20},
		}},
		{name: "entity without positive entity cap falls through to neither global nor entity", cfg: &config.Config{Extensions: map[string]any{
			"budget": map[string]any{"system_monthly_cap": 10, "global_monthly_cap": 5, "per_entity_monthly_cap": 0},
		}}, entityID: "entity-1", want: []runtimeeffects.BudgetAdmissionScope{{Kind: "system", CapUSD: 10}}},
		{name: "nonpositive caps ignored", cfg: &config.Config{Extensions: map[string]any{
			"budget": map[string]any{"system_monthly_cap": 0, "global_monthly_cap": -1, "per_entity_monthly_cap": -1},
		}}, want: []runtimeeffects.BudgetAdmissionScope{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := completionBudgetScopes(tc.cfg, tc.entityID); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("completionBudgetScopes() = %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestClaudeCompletionUsageFromRawDirectAndStream(t *testing.T) {
	result := `{"type":"result","model":"claude-sonnet-4","total_cost_usd":0.0123,"usage":{"input_tokens":100,"output_tokens":25,"cache_read_input_tokens":30,"cache_creation_input_tokens":10,"cache_creation":{"ephemeral_5m_input_tokens":6,"ephemeral_1h_input_tokens":4}}}`
	for _, tc := range []struct {
		name string
		raw  string
	}{
		{name: "direct", raw: result},
		{name: "stream", raw: "{\"type\":\"system\",\"subtype\":\"init\"}\n" + result + "\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			usage, err := claudeCompletionUsageFromRaw([]byte(tc.raw), "fallback")
			if err != nil {
				t.Fatal(err)
			}
			if usage.Exactness != runtimeeffects.CompletionUsageExact || usage.ResolvedModel != "claude-sonnet-4" ||
				usage.InputTokens == nil || *usage.InputTokens != 100 || usage.OutputTokens == nil || *usage.OutputTokens != 25 ||
				usage.CacheCreation5mInputTokens == nil || *usage.CacheCreation5mInputTokens != 6 ||
				usage.CacheCreation1hInputTokens == nil || *usage.CacheCreation1hInputTokens != 4 ||
				usage.ProviderReportedCostUSD == nil || *usage.ProviderReportedCostUSD != 0.0123 {
				t.Fatalf("usage = %#v", usage)
			}
		})
	}
}

func TestClaudeCompletionUsageFromRawRejectsInvalidCacheSubtotals(t *testing.T) {
	_, err := claudeCompletionUsageFromRaw([]byte(`{"type":"result","model":"claude-sonnet-4","usage":{"input_tokens":1,"output_tokens":1,"cache_creation_input_tokens":10,"cache_creation":{"ephemeral_5m_input_tokens":6,"ephemeral_1h_input_tokens":5}}}`), "")
	if err == nil {
		t.Fatal("invalid cache subtotals were accepted")
	}
}
