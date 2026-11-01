package llm

import (
	"context"
	"strings"
	"testing"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/testutil/llmapi"
)

func TestManagedProviderHTTPRejections(t *testing.T) {
	for _, adapter := range []string{"anthropic_api", "openai_compatible", "openai_responses"} {
		for _, test := range []struct {
			name      string
			status    int
			body      string
			class     runtimefailures.Class
			retryable bool
		}{
			{"bad_request", 400, `{"error":{"message":"Invalid assistant message: content or tool_calls must be set"}}`, runtimefailures.ClassConnectorFailure, false},
			{"plain_refusal", 422, "request rejected before generation", runtimefailures.ClassConnectorFailure, false},
			{"unauthorized", 401, `{"error":{"message":"credential rejected"}}`, runtimefailures.ClassAuthenticationNeeded, false},
			{"forbidden", 403, `{"error":{"message":"model access denied"}}`, runtimefailures.ClassAuthorizationDenied, false},
			{"request_timeout", 408, `{"error":{"message":"request timed out"}}`, runtimefailures.ClassTimeout, true},
			{"rate_limited", 429, `{"error":{"message":"rate limited"}}`, runtimefailures.ClassConnectorFailure, true},
		} {
			t.Run(adapter+"/"+test.name, func(t *testing.T) {
				protocol, path := llmapi.OpenAICompatible, "/v1/chat/completions"
				if adapter == "anthropic_api" {
					protocol, path = llmapi.Anthropic, "/v1/messages"
				} else if adapter == "openai_responses" {
					protocol, path = llmapi.OpenAIResponses, "/v1/responses"
				}
				server := llmapi.New(t, protocol, path, "test", func(llmapi.Request) (llmapi.Reply, error) {
					return llmapi.Reply{StatusCode: test.status, ErrorBody: test.body}, nil
				})
				harness := effecttest.New()
				ctx := llmTestWorkContext(t, managedEffectHarnessContext(t, harness, adapter+"-"+test.name))
				dispatch, err := sendProviderStatusRequest(t, ctx, adapter, server)
				failure := runtimefailures.Normalize(err, "test", "provider_refusal")
				if err == nil || failure.Class != test.class || failure.Retryable != test.retryable || failure.Detail.Attributes["response_body"] != test.body || !strings.Contains(err.Error(), test.body) {
					t.Fatalf("HTTP %d lost its known refusal/body: failure=%+v err=%v", test.status, failure, err)
				}
				if dispatch.state != runtimeeffects.StateTerminalFailure {
					t.Fatalf("HTTP %d dispatch state=%s, want recorded terminal failure", test.status, dispatch.state)
				}
				settleEffectTestCompletionFailure(t, ctx, dispatch, err, dispatch.state)
				if err := harness.RequireState(adapter, runtimeeffects.StateTerminalFailure); err != nil {
					t.Fatal(err)
				}
				if len(server.Requests()) != 1 {
					t.Fatalf("provider refusal was redispatched: %d requests", len(server.Requests()))
				}
			})
		}
	}
}

func sendProviderStatusRequest(t *testing.T, ctx context.Context, adapter string, server *llmapi.Server) (*completionDispatch, error) {
	t.Helper()
	managed := managedProviderCallForEffectTest(t, ctx)
	payload := []byte(`{"model":"test"}`)
	switch adapter {
	case "anthropic_api":
		runtime := &AnthropicAPIRuntime{httpClient: server.Client(), apiURL: server.URL + "/v1/messages", credentialCache: providerCredentialCache{value: "test"}}
		_, _, dispatch, err := runtime.sendRequest(ctx, payload, managed)
		return dispatch, err
	case "openai_compatible":
		runtime := &OpenAICompatibleRuntime{httpClient: server.Client(), baseURL: server.URL, credentialCache: providerCredentialCache{value: "test"}}
		_, _, dispatch, err := runtime.sendRequest(ctx, payload, managed)
		return dispatch, err
	case "openai_responses":
		runtime := &OpenAIResponsesRuntime{httpClient: server.Client(), baseURL: server.URL, credentialCache: providerCredentialCache{value: "test"}}
		_, _, dispatch, err := runtime.sendRequest(ctx, payload, managed)
		return dispatch, err
	default:
		t.Fatalf("unknown test adapter %s", adapter)
		return nil, nil
	}
}
