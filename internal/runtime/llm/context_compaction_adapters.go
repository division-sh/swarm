package llm

import (
	"context"
	"encoding/json"

	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
)

func (r *AnthropicAPIRuntime) contextCompactor() contextCompactionAdapter {
	profile, _ := llmselection.ResolveActiveBackend(llmselection.BackendAnthropic)
	return contextCompactionAdapter{
		profile: profile, destination: r.apiURL, cfg: r.cfg, controller: r.completionController,
		store: r.conversations, admission: r.providerAdmission, publisher: r.events,
		build: func(session *Session, input Message, model string) ([]byte, error) {
			request, err := r.buildRequest(session, input, model)
			if err != nil {
				return nil, err
			}
			return json.Marshal(request)
		},
		compact: r.buildContextCompactionRequest,
		send: func(ctx context.Context, payload []byte, call *managedProviderCall) ([]byte, *completionDispatch, error) {
			raw, _, dispatch, err := r.sendRequest(ctx, payload, call)
			return raw, dispatch, err
		},
	}
}

func (r *OpenAICompatibleRuntime) contextCompactor() contextCompactionAdapter {
	profile, _ := llmselection.ResolveActiveBackend(llmselection.BackendOpenAICompatible)
	return contextCompactionAdapter{
		profile: profile, destination: openAICompatibleChatCompletionsURL(r.baseURL), cfg: r.cfg, controller: r.completionController,
		store: r.conversations, admission: r.providerAdmission, publisher: r.events,
		build: func(session *Session, input Message, model string) ([]byte, error) {
			request, err := r.buildRequest(session, input, model)
			if err != nil {
				return nil, err
			}
			return json.Marshal(request)
		},
		compact: func(session *Session, model, method string) ([]byte, error) {
			return r.buildContextCompactionRequest(session, model)
		},
		send: func(ctx context.Context, payload []byte, call *managedProviderCall) ([]byte, *completionDispatch, error) {
			raw, _, dispatch, err := r.sendRequest(ctx, payload, call)
			return raw, dispatch, err
		},
	}
}

func (r *OpenAIResponsesRuntime) contextCompactor() contextCompactionAdapter {
	profile, _ := llmselection.ResolveActiveBackend(llmselection.BackendOpenAIResponses)
	return contextCompactionAdapter{
		profile: profile, destination: openAIResponsesURL(r.baseURL), cfg: r.cfg, controller: r.completionController,
		store: r.conversations, admission: r.providerAdmission, publisher: r.events,
		build: func(session *Session, input Message, model string) ([]byte, error) {
			request, err := r.buildRequest(session, input, model)
			if err != nil {
				return nil, err
			}
			return json.Marshal(request)
		},
		compact: r.buildContextCompactionRequest,
		send: func(ctx context.Context, payload []byte, call *managedProviderCall) ([]byte, *completionDispatch, error) {
			raw, _, dispatch, err := r.sendRequest(ctx, payload, call)
			return raw, dispatch, err
		},
	}
}
