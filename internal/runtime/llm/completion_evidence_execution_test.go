package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

func requireImmutableCompletion2270Continuation(t testing.TB, attempt runtimeeffects.Attempt, want runtimeeffects.CompletionUsage) {
	t.Helper()
	first, ok := attempt.CompletionContinuation()
	if !ok {
		t.Fatal("missing admitted continuation")
	}
	decoded, err := completionContinuationForPayload(first.Payload, attempt.Adapter)
	if err != nil || !reflect.DeepEqual(decoded.Usage, want) {
		t.Fatalf("initial continuation evidence: %+v, %v", decoded, err)
	}
	decoded.Usage.Evidence.Request.RequestedModel = "current-config-not-admission"
	decoded.Usage.Evidence.Models[0].Model = "mutated-readback"
	*decoded.Usage.InputTokens = 999
	first.Request[0], first.Payload[0] = 'X', 'X'
	second, ok := attempt.CompletionContinuation()
	if !ok || runtimeeffects.Fingerprint(second.Request) != runtimeeffects.Fingerprint([]byte("exact-original-request")) {
		t.Fatal("continuation readback mutated frozen request bytes")
	}
	repeated, err := completionContinuationForPayload(second.Payload, attempt.Adapter)
	if err != nil || !reflect.DeepEqual(repeated.Usage, want) {
		t.Fatalf("continuation readback aliases mutable evidence: %+v, %v", repeated, err)
	}
}

type completion2270APIRuntime interface {
	Runtime
	continueSession(context.Context, *Session, Message, *managedProviderCall) (*Response, error)
}

func TestCompletion2270APIExecutionEvidence(t *testing.T) {
	for _, protocol := range []struct{ profile, adapter, input, output, content string }{
		{llmselection.BackendAnthropic, "anthropic_api", "input_tokens", "output_tokens", `"content":[{"type":"text","text":"done"}]`},
		{llmselection.BackendOpenAICompatible, "openai_compatible", "prompt_tokens", "completion_tokens", `"choices":[{"message":{"role":"assistant","content":"done"}}]`},
		{llmselection.BackendOpenAIResponses, "openai_responses", "input_tokens", "output_tokens", `"id":"response-2270","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}]`},
	} {
		for _, tc := range []struct {
			name, model, tokens, wantModel string
			wantError, exact               bool
		}{
			{"reported", `"model":"reported-B",`, "12", "reported-B", false, true},
			{"absent-model", "", "12", "", false, true},
			{"invalid-model", `"model":17,`, "12", "", true, true},
			{"duplicate-model", `"model":"first","model":"second",`, "12", "", false, true},
			{"missing-usage", `"model":"reported-B",`, "", "reported-B", true, false},
			{"invalid-usage", `"model":"reported-B",`, `"invalid"`, "reported-B", true, false},
		} {
			t.Run(protocol.profile+"/"+tc.name, func(t *testing.T) {
				var calls atomic.Int32
				body := "{" + tc.model + protocol.content
				if tc.tokens != "" {
					body += fmt.Sprintf(`,"usage":{"%s":%s,"%s":3}`, protocol.input, tc.tokens, protocol.output)
				}
				body += "}"
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					var request map[string]any
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request["model"] != "effect-test-model" {
						t.Errorf("primitive request: %+v, %v", request, err)
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(body))
				}))
				defer server.Close()
				harness := effecttest.New()
				setEffectHarnessAgent(t, harness, "agent-1", "test/stateless")
				rt := completion2270RuntimeForExecution(t, protocol.profile, server.URL, harness)
				ctx := runtimeactors.WithActor(harness.CompletionContext(t.Name()), runtimeactors.AgentConfig{ExecutionMode: "live", ID: "agent-1", Model: "regular", FlowPath: "test/stateless"})
				ctx = withTestStatelessMemory(t, ctx, "agent-1", "test/stateless")
				session, err := rt.StartSession(ctx, "agent-1", managedProviderPromptForEffectTest(t), nil)
				if err != nil {
					t.Fatal(err)
				}
				ctx = managedProviderTestContext(t, ctx, rt, session, nil)
				managed := managedProviderCallForEffectTest(t, ctx)
				_, err = rt.continueSession(ctx, session, managedProviderInputForEffectTest(t, managed), managed)
				if (err != nil) != tc.wantError || calls.Load() != 1 {
					t.Fatalf("execution verdict changed or provider repeated: calls=%d, error=%v", calls.Load(), err)
				}
				requireCompletion2270APISettlement(t, harness, protocol.adapter, tc.wantModel, tc.exact)
			})
		}
	}
}

func requireCompletion2270APISettlement(t testing.TB, harness *effecttest.Harness, adapter, wantModel string, exact bool) {
	t.Helper()
	settled := harness.CompletionSettlementsForAdapter(adapter)
	if len(settled) != 1 {
		t.Fatalf("completion settlements: %+v", settled)
	}
	usage := settled[0].Usage
	if usage.ResolvedModel != wantModel || usage.Evidence.Request.RequestedModel != "effect-test-model" || (usage.Exactness == runtimeeffects.CompletionUsageExact) != exact {
		t.Fatalf("execution erased independent evidence: %+v", usage)
	}
	if err := usage.ValidateEvidence(); err != nil {
		t.Fatal(err)
	}
	if exact && (*usage.InputTokens != 12 || *usage.OutputTokens != 3) {
		t.Fatalf("token evidence: %+v", usage)
	}
	if wantModel != "" && (len(usage.Evidence.Models) != 1 || usage.Evidence.Models[0].Source != runtimeeffects.CompletionModelResponse) {
		t.Fatalf("unqualified model observation: %+v", usage.Evidence)
	}
	if usage.ProviderReportedCostUSD != nil {
		t.Fatal("execution invented reported price")
	}
	provenance := runtimeeffects.CompletionCostUnavailable
	if exact {
		provenance = runtimeeffects.CompletionCostEstimated
	}
	if usage.Evidence.CostProvenance != provenance {
		t.Fatalf("tokens falsely imply price exactness: %+v", usage.Evidence)
	}
}

func completion2270RuntimeForExecution(t *testing.T, profile, endpoint string, harness *effecttest.Harness) completion2270APIRuntime {
	t.Helper()
	cfg := &config.Config{LLM: config.LLMConfig{Backend: profile, Models: llmselection.ModelAliases{"regular": {profile: "effect-test-model"}}}}
	cfg.LLM.OpenAICompatible.BaseURL, cfg.LLM.OpenAIResponses.BaseURL = endpoint, endpoint
	registry := sessions.NewInMemoryRegistry(time.Second)
	controller := liveTestCompletionController(harness, harness, harness, harness)
	switch profile {
	case llmselection.BackendAnthropic:
		rt := NewAnthropicAPIRuntime(cfg, registry, "worker-2270", nil, nil)
		rt.apiURL, rt.credentialCache, rt.completionController = endpoint, providerCredentialCache{value: "test-key"}, controller
		return rt
	case llmselection.BackendOpenAICompatible:
		rt := NewOpenAICompatibleRuntime(cfg, registry, "worker-2270", nil, nil)
		rt.credentials, rt.completionController = testProviderCredentialResolver(t, llmselection.OpenAICompatibleCredentialEnv, "test-key"), controller
		return rt
	default:
		rt := NewOpenAIResponsesRuntime(cfg, registry, "worker-2270", nil, nil)
		rt.credentials, rt.completionController = testProviderCredentialResolver(t, llmselection.OpenAIResponsesCredentialEnv, "test-key"), controller
		return rt
	}
}
