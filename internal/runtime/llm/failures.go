package llm

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

func providerStatusFailure(provider string, status int, body []byte) error {
	attributes := map[string]any{"provider": strings.TrimSpace(provider), "status": status, "response_body": string(body)}
	class, code := runtimefailures.ClassConnectorFailure, "provider_http_status"
	switch status {
	case http.StatusUnauthorized:
		attributes["auth_kind"] = "provider_credential"
		class, code = runtimefailures.ClassAuthenticationNeeded, "provider_unauthorized"
	case http.StatusForbidden:
		attributes["action"] = "llm_provider_request"
		class, code = runtimefailures.ClassAuthorizationDenied, "provider_forbidden"
	case http.StatusPaymentRequired:
		code = "provider_credit_exhausted"
	case http.StatusRequestTimeout:
		class, code = runtimefailures.ClassTimeout, "provider_request_timeout"
	}
	return fmt.Errorf("%w: %s", runtimefailures.New(class, code, "llm-provider", "http_status", attributes), body)
}

func claudeCLIProcessFailure(stderrText, stdoutText, detailCode, operation string, cause error) error {
	if isClaudeAuthOutput(stderrText) || isClaudeAuthOutput(stdoutText) {
		return runtimefailures.Wrap(runtimefailures.ClassAuthenticationNeeded, "provider_unauthorized", "claude-cli-adapter", operation, map[string]any{
			"auth_kind": "provider_credential",
			"provider":  "claude",
		}, cause)
	}
	return runtimefailures.Wrap(runtimefailures.ClassConnectorFailure, detailCode, "claude-cli-adapter", operation, nil, cause)
}

func sessionAcquireFailure(err error, agentID string) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sessions.ErrSessionLeased) {
		return runtimefailures.Wrap(runtimefailures.ClassDependencyUnavailable, "session_currently_leased", "llm-runtime", "acquire_session", map[string]any{
			"agent_id": strings.TrimSpace(agentID),
		}, err)
	}
	return runtimefailures.Wrap(runtimefailures.ClassDependencyUnavailable, "session_acquire_failed", "llm-runtime", "acquire_session", map[string]any{
		"agent_id": strings.TrimSpace(agentID),
	}, err)
}

func agentTurnFailure(err error, operation string) *runtimefailures.Envelope {
	if err == nil {
		return nil
	}
	failure := runtimefailures.Normalize(err, "llm-runtime", strings.TrimSpace(operation))
	return &failure
}
