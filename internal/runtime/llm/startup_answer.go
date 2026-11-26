package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/google/uuid"
)

// StartupAnswerObserver is the raw-observation seam for injected runtimes.
// It cannot create a managed session or supply native execution authority.
type StartupAnswerObserver interface {
	ObserveStartupAnswer(context.Context, llmselection.Profile, llmselection.ResolvedModel) ([]byte, error)
}

func ObserveInjectedStartupAnswer(ctx context.Context, cfg *config.Config, profile llmselection.Profile, model llmselection.ResolvedModel, runtime Runtime) error {
	observer, ok := runtime.(StartupAnswerObserver)
	if !ok {
		return fmt.Errorf("injected runtime does not provide a startup answer observer")
	}
	if _, err := RequireProviderContractForProfile(profile, runtime); err != nil {
		return err
	}
	raw, err := observer.ObserveStartupAnswer(ctx, profile, model)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if profile.ID == llmselection.BackendClaudeCLI {
		return validateClaudeStartupAnswer(raw)
	}
	_, decode, err := startupAnswerRequest(ctx, cfg, profile, model)
	if err != nil {
		return err
	}
	answer, err := decode(raw)
	if err != nil {
		return err
	}
	return validateStartupAnswer(answer)
}

// ObserveProviderStartupAnswer completes a single no-tool request. Grouping by
// provider/credential belongs to the boot stage, not to this transport owner.
func ObserveProviderStartupAnswer(ctx context.Context, cfg *config.Config, profile llmselection.Profile, model llmselection.ResolvedModel, credentials ProviderCredentialResolver, target *workspace.Target) error {
	if cfg == nil {
		return fmt.Errorf("admitted runtime configuration is required")
	}
	if profile.ID == llmselection.BackendClaudeCLI {
		return observeClaudeStartupAnswer(ctx, cfg, model, credentials, target)
	}
	request, decode, err := startupAnswerRequest(ctx, cfg, profile, model)
	if err != nil {
		return err
	}
	credential, err := credentials.Resolve(ctx, profile)
	if err != nil {
		return err
	}
	if profile.ID == llmselection.BackendAnthropic {
		request.Header.Set("x-api-key", credential.Value)
		request.Header.Set("anthropic-version", "2023-06-01")
	} else {
		request.Header.Set("Authorization", "Bearer "+credential.Value)
	}
	request.Header.Set("content-type", "application/json")
	client := &http.Client{Timeout: 120 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	if response.StatusCode >= 300 {
		if credential.Value != "" {
			body = []byte(strings.ReplaceAll(string(body), credential.Value, "[redacted]"))
		}
		return providerStatusFailure(profile.Provider, response.StatusCode, body)
	}
	answer, err := decode(body)
	if err != nil {
		return err
	}
	return validateStartupAnswer(answer)
}

func startupAnswerRequest(ctx context.Context, cfg *config.Config, profile llmselection.Profile, model llmselection.ResolvedModel) (*http.Request, func([]byte) (Response, error), error) {
	session := &Session{}
	input := Message{Role: "user", Content: cliStartupProbePrompt}
	var payload any
	var endpoint string
	var decode func([]byte) (Response, error)
	var err error
	switch profile.ID {
	case llmselection.BackendAnthropic:
		adapter := &AnthropicAPIRuntime{apiURL: "https://api.anthropic.com/v1/messages"}
		payload, err = adapter.buildRequest(session, input, model.ConcreteModel)
		endpoint = adapter.apiURL
		decode = func(raw []byte) (Response, error) {
			var parsed anthropicResponse
			if err := json.Unmarshal(raw, &parsed); err != nil {
				return Response{}, err
			}
			if parsed.Error.Message != "" {
				return Response{}, fmt.Errorf("provider response contains an error")
			}
			return convertAnthropicResponse(parsed, nil), nil
		}
	case llmselection.BackendOpenAICompatible:
		var base string
		base, err = llmselection.ResolveBaseURL(profile, cfg.LLM.OpenAICompatible.BaseURL)
		if err != nil {
			return nil, nil, err
		}
		adapter := &OpenAICompatibleRuntime{baseURL: base}
		payload, err = adapter.buildRequest(session, input, model.ConcreteModel)
		endpoint = openAICompatibleChatCompletionsURL(base)
		decode = func(raw []byte) (Response, error) {
			var parsed openAICompatibleResponse
			if err := json.Unmarshal(raw, &parsed); err != nil {
				return Response{}, err
			}
			if parsed.Error.Message != "" {
				return Response{}, fmt.Errorf("provider response contains an error")
			}
			return convertOpenAICompatibleResponse(parsed, nil)
		}
	case llmselection.BackendOpenAIResponses:
		var base string
		base, err = llmselection.ResolveBaseURL(profile, cfg.LLM.OpenAIResponses.BaseURL)
		if err != nil {
			return nil, nil, err
		}
		adapter := &OpenAIResponsesRuntime{baseURL: base}
		payload, err = adapter.buildRequest(session, input, model.ConcreteModel)
		endpoint = base + "/responses"
		decode = func(raw []byte) (Response, error) {
			var parsed openAIResponsesResponse
			if err := json.Unmarshal(raw, &parsed); err != nil {
				return Response{}, err
			}
			if parsed.Error.Message != "" {
				return Response{}, fmt.Errorf("provider response contains an error")
			}
			return convertOpenAIResponsesResponse(parsed, nil)
		}
	default:
		return nil, nil, fmt.Errorf("unsupported provider backend %q", profile.ID)
	}
	if err != nil {
		return nil, nil, err
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, nil, err
	}
	decodeBody := decode
	decode = func(raw []byte) (Response, error) {
		if err := requireStartupAnswerCompletion(profile.ID, raw); err != nil {
			return Response{}, err
		}
		return decodeBody(raw)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	return request, decode, err
}

func requireStartupAnswerCompletion(backend string, raw []byte) error {
	var terminal struct {
		Status     string `json:"status"`
		StopReason string `json:"stop_reason"`
		Choices    []struct {
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(raw, &terminal); err != nil {
		return err
	}
	complete := false
	switch backend {
	case llmselection.BackendAnthropic:
		complete = terminal.StopReason == "end_turn" || terminal.StopReason == "stop_sequence"
	case llmselection.BackendOpenAICompatible:
		complete = len(terminal.Choices) != 0 && terminal.Choices[0].FinishReason == "stop"
	case llmselection.BackendOpenAIResponses:
		complete = terminal.Status == "completed"
	}
	if !complete {
		return fmt.Errorf("provider startup response did not complete successfully")
	}
	return nil
}

func observeClaudeStartupAnswer(ctx context.Context, cfg *config.Config, model llmselection.ResolvedModel, credentials ProviderCredentialResolver, target *workspace.Target) error {
	r := &ClaudeCLIRuntime{cfg: cfg, providerCredentials: credentials}
	ctx = actors.WithActor(ctx, actors.AgentConfig{})
	bounded, cancel := context.WithTimeout(ctx, r.effectiveCLITimeout(ctx))
	defer cancel()
	args := []string{"-p", "--session-id", uuid.NewString(), "--output-format", "json", "--tools", "", "--allowedTools", ""}
	if model.ConcreteModel != "" {
		args = append(args, "--model", model.ConcreteModel)
	}
	cmd, err := r.buildCommand(bounded, args, target)
	if err != nil {
		return err
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdin, cmd.Stdout, cmd.Stderr = strings.NewReader(cliStartupProbePrompt), &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return errors.Join(bounded.Err(), claudeCLIProcessFailure(summarizeCLIErrorOutput(stderr.String(), claudeCommandSecrets(cmd)...), "", "claude_cli_startup_probe_failed", "startup_probe", err))
	}
	return validateClaudeStartupAnswer(stdout.Bytes())
}

func validateClaudeStartupAnswer(raw []byte) error {
	var result struct {
		Type    string `json:"type"`
		IsError bool   `json:"is_error"`
	}
	if json.Unmarshal(raw, &result) != nil || result.Type != "result" || result.IsError {
		return fmt.Errorf("provider startup response did not complete successfully")
	}
	return validateStartupAnswer(*parseCLIResponse(raw))
}

func validateStartupAnswer(answer Response) error {
	if strings.TrimSpace(answer.Message.Content) == "" || len(answer.ToolCalls) != 0 {
		return fmt.Errorf("provider startup response did not complete successfully")
	}
	return nil
}
