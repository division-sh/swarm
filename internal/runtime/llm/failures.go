package llm

import (
	"errors"
	"net/http"
	"strings"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

func providerStatusFailure(provider string, status int) error {
	attributes := map[string]any{"provider": strings.TrimSpace(provider), "status": status}
	switch status {
	case http.StatusUnauthorized:
		attributes["auth_kind"] = "provider_credential"
		return runtimefailures.New(runtimefailures.ClassAuthenticationNeeded, "provider_unauthorized", "llm-provider", "http_status", attributes)
	case http.StatusForbidden:
		attributes["action"] = "llm_provider_request"
		return runtimefailures.New(runtimefailures.ClassAuthorizationDenied, "provider_forbidden", "llm-provider", "http_status", attributes)
	case http.StatusPaymentRequired:
		return runtimefailures.New(runtimefailures.ClassConnectorFailure, "provider_credit_exhausted", "llm-provider", "http_status", attributes)
	case http.StatusRequestTimeout:
		return runtimefailures.New(runtimefailures.ClassTimeout, "provider_request_timeout", "llm-provider", "http_status", attributes)
	default:
		return runtimefailures.New(runtimefailures.ClassConnectorFailure, "provider_http_status", "llm-provider", "http_status", attributes)
	}
}

func budgetEmergencyFailure(entityID string) error {
	return runtimefailures.New(runtimefailures.ClassBudgetExhausted, "spend_budget_emergency", "llm-runtime", "budget_admission", map[string]any{
		"budget_kind": "spend",
		"entity_id":   strings.TrimSpace(entityID),
	})
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
