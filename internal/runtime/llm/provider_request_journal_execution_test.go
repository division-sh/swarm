package llm

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentframe"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
)

type providerJournalExecutionTransport func(*http.Request) (*http.Response, error)

func (f providerJournalExecutionTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestManagedHTTPJournalCaptureRefusesChangedPhysicalRequest(t *testing.T) {
	for _, adapter := range []string{"anthropic_api", "openai_compatible", "openai_responses"} {
		for _, cut := range []string{"unchanged", "body", "method", "destination", "header", "duplicate_header", "logical_operation"} {
			t.Run(adapter+"/"+cut, func(t *testing.T) {
				ctx, harness, session, input, managed, body := managedProviderJournalExecutionInputs(t, adapter)
				captured, err := prepareManagedProviderRequestJournal(ctx, adapter, "http://provider.test/request", session, input, body, managed)
				if err != nil || captured == nil || captured.requestJournal == nil || managed.requestJournal != nil {
					t.Fatalf("capture did not freeze an independent request: %v", err)
				}
				// Later cache state cannot change the request being admitted.
				session.SystemPrompt, session.ID = "later prompt", "later session"
				session.Memory.Enabled = !session.Memory.Enabled
				request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://provider.test/request", nil)
				if err != nil {
					t.Fatal(err)
				}
				request.Header.Set("content-type", "application/json")
				request.Header.Set("authorization", "Bearer secret-not-for-storage")
				if adapter == "anthropic_api" {
					request.Header.Set("anthropic-version", "2023-06-01")
				}
				switch cut {
				case "body":
					body = append([]byte(nil), body...)
					body[0] = ' '
				case "method":
					request.Method = http.MethodPut
				case "destination":
					request.URL.Path = "/other"
				case "header":
					request.Header.Set("content-type", "text/plain")
				case "duplicate_header":
					request.Header.Add("content-type", "application/json")
				case "logical_operation":
					ctx = runtimeeffects.WithLogicalOperationIdentity(ctx, "another-operation")
				}
				var handle *runtimeeffects.Handle
				err = validateHTTPProviderRequestJournal(adapter, request, captured)
				if err == nil {
					handle, err = runtimeeffects.BeginJournaledManagedCompletion(ctx, adapter, body, captured.frame, *captured.requestJournal, nil)
				}
				if cut == "unchanged" {
					if err != nil || handle == nil || len(harness.Authorizations) != 1 {
						t.Fatalf("frozen admission failed: %v", err)
					}
					for _, admission := range harness.Authorizations {
						if admission.ProviderRequestJournal == nil || admission.ProviderRequestJournal.SessionID == session.ID {
							t.Fatal("admission lost its frozen original session")
						}
					}
				} else if err == nil || handle != nil || len(harness.Authorizations) != 0 {
					t.Fatal("changed request crossed the original admission boundary")
				}
			})
		}
	}
}

func TestManagedHTTPJournalUnsupportedDestinationDoesNotPersistCredentials(t *testing.T) {
	for _, destination := range []string{"http://user:private-value@provider.test/request", "http://provider.test/request?key=private-value"} {
		ctx, _, session, input, managed, body := managedProviderJournalExecutionInputs(t, "anthropic_api")
		captured, err := prepareManagedProviderRequestJournal(ctx, "anthropic_api", destination, session, input, body, managed)
		if err != nil || captured == nil || captured.requestJournal != nil {
			t.Fatal("unsupported resume destination must not change original-call admission or persist its credentials")
		}
		body[0] = ' '
		if _, err := prepareManagedProviderRequestJournal(ctx, "anthropic_api", destination, session, input, body, managed); err == nil {
			t.Fatal("unsupported resume destination bypassed independent body validation")
		}
	}
}

func managedProviderJournalExecutionInputs(t *testing.T, adapter string) (context.Context, *effecttest.Harness, *Session, Message, *managedProviderCall, []byte) {
	t.Helper()
	backend := adapter
	if adapter == "anthropic_api" {
		backend = llmselection.BackendAnthropic
	}
	profile := mustAdmissionProfile(t, backend)
	model := mustAdmissionModel(t, profile, llmselection.ModelAliasRegular)
	harness := effecttest.New()
	ctx := llmTestWorkContext(t, managedEffectHarnessContext(t, harness, t.Name()))
	ctx = withEffectTestRequest(t, ctx, profile, model)
	managed := managedProviderCallForEffectTest(t, ctx)
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok {
		t.Fatal("missing managed surface")
	}
	authority, ok := runtimeeffects.CompletionAuthorityFromContext(ctx)
	if !ok {
		t.Fatal("missing managed authority")
	}
	role, content, err := managed.frame.ProviderInput()
	if err != nil {
		t.Fatal(err)
	}
	session := &Session{ID: surface.Authority.SessionID, AgentID: managed.frame.Session.AgentIdentity.AgentID(),
		Memory: authority.Target.Memory, SystemPrompt: managed.frame.Session.ProviderPrompt, TurnCount: surface.Authority.TurnOrdinal - 1}
	input := Message{Role: role, Content: content}
	body := renderProviderJournalFixture(t, adapter, session, input, model.ConcreteModel)
	return ctx, harness, session, input, managed, body
}

func TestManagedHTTPExecutionRetainsRequestJournalBeforeInvocation(t *testing.T) {
	for _, adapter := range []struct{ backend, name, response string }{
		{llmselection.BackendAnthropic, "anthropic_api", `{"model":"test-model","usage":{"input_tokens":1,"output_tokens":1},"content":[{"type":"text","text":"done"}]}`},
		{llmselection.BackendOpenAICompatible, "openai_compatible", `{"model":"test-model","usage":{"prompt_tokens":1,"completion_tokens":1},"choices":[{"message":{"role":"assistant","content":"done"}}]}`},
		{llmselection.BackendOpenAIResponses, "openai_responses", `{"id":"response","model":"test-model","usage":{"input_tokens":1,"output_tokens":1},"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}]}`},
	} {
		t.Run(adapter.name, func(t *testing.T) {
			harness := effecttest.New()
			ctx := testManagedConversationContext(t, harness, "agent-1", "support/inst-1", "support")
			actor, _ := runtimeactors.ActorFromContext(ctx)
			actor.EntityID = "entity-1"
			ctx = runtimeactors.WithActor(ctx, actor)
			runtime := completion2270RuntimeForExecution(t, adapter.backend, "http://provider.test", harness)
			calls := 0
			client := &http.Client{Transport: providerJournalExecutionTransport(func(request *http.Request) (*http.Response, error) {
				calls++
				if err := harness.RequireState(adapter.name, runtimeeffects.StateLaunched); err != nil {
					return nil, err
				}
				body, err := io.ReadAll(request.Body)
				if err != nil {
					return nil, err
				}
				if len(harness.Authorizations) != 1 {
					return nil, fmt.Errorf("provider invocation requires exactly one original admission")
				}
				for _, admission := range harness.Authorizations {
					journal := admission.ProviderRequestJournal
					if journal == nil || admission.AgentFrame == nil || admission.CapabilitySurface == nil {
						t.Errorf("provider invocation preceded its frozen request journal")
						return nil, fmt.Errorf("provider invocation preceded its frozen request journal")
					}
					if journal.Method != request.Method || journal.Destination != request.URL.String() {
						return nil, fmt.Errorf("provider invocation differs from its frozen physical destination")
					}
					rebuilt, err := RebuildProviderRequestJournal(*journal, *admission.AgentFrame, *admission.CapabilitySurface, admission.RequestFingerprint)
					if err != nil || !bytes.Equal(rebuilt, body) {
						return nil, fmt.Errorf("provider invocation differs from its independently rebuilt journal: %w", err)
					}
					encoded, err := runtimeeffects.EncodeProviderRequestJournal(*journal)
					if err != nil || bytes.Contains(encoded, []byte("test-key")) || bytes.Contains(encoded, []byte("authorization")) {
						return nil, fmt.Errorf("provider request journal must exclude transport credentials")
					}
				}
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(adapter.response)), Header: make(http.Header)}, nil
			})}
			switch runtime := runtime.(type) {
			case *AnthropicAPIRuntime:
				runtime.httpClient = client
			case *OpenAICompatibleRuntime:
				runtime.httpClient = client
			case *OpenAIResponsesRuntime:
				runtime.httpClient = client
			}
			conversation := newTestManagedConversation(t, "agent-1", "support/inst-1", "support", nil, testMemory(), 2, runtime)
			conversation.SetToolExecutor(openAIToolExecutor{})
			response, err := conversation.RunManaged(ctx, agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: testManagedEvent("agent-1")})
			if err != nil || response == nil || response.Message.Content != "done" || calls != 1 {
				if failure, ok := runtimefailures.As(err); ok {
					t.Logf("failure classification: %v", failure.Failure.Detail.Attributes)
				}
				t.Fatalf("journaled provider execution: calls=%d response_present=%v error=%v", calls, response != nil, err)
			}
		})
	}
}
