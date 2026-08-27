package tools

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

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type httpToolStatusError struct {
	ToolName   string
	StatusCode int
	Body       any
	Secrets    []string
}

func (e httpToolStatusError) Error() string {
	return runtimemanagedcredentials.RedactString(
		fmt.Sprintf("http tool %s returned status %d: %s", e.ToolName, e.StatusCode, strings.TrimSpace(asString(e.Body))),
		e.Secrets...,
	)
}

func (e *Executor) execHTTPTool(ctx context.Context, actor models.AgentConfig, tool ExecutionTool, input any) (any, error) {
	toolName := tool.Name()
	httpExecution, ok := tool.HTTPExecution()
	if !ok {
		return nil, runtimefailures.New(runtimefailures.ClassSchemaInvalid, "http_tool_configuration_missing", "tool-executor", "execute_http_tool", map[string]any{"tool": toolName})
	}
	payload := map[string]any{}
	if err := decodeToolInput(input, &payload); err != nil {
		return nil, runtimefailures.Wrap(runtimefailures.ClassSchemaInvalid, "http_tool_input_invalid", "tool-executor", "execute_http_tool", map[string]any{"tool": toolName}, err)
	}
	if payload == nil {
		payload = map[string]any{}
	}
	credentials, err := e.resolveToolCredentialsForActor(ctx, actor, tool.Credentials())
	if err != nil {
		return nil, runtimefailures.Wrap(runtimefailures.ClassAuthenticationNeeded, "tool_credential_required", "tool-executor", "resolve_http_tool_credentials", map[string]any{"auth_kind": "tool_credential", "tool": toolName}, err)
	}
	request, err := httpExecution.Prepare(payload, credentials)
	if err != nil {
		return nil, runtimefailures.Wrap(runtimefailures.ClassSchemaInvalid, "http_tool_request_invalid", "tool-executor", "resolve_http_tool_request", map[string]any{"tool": toolName}, err)
	}
	headers := request.Headers()
	managedAuth, err := e.resolveManagedCredentialForActor(ctx, actor, tool)
	if err != nil {
		return nil, httpToolAuthenticationFailure(err, toolName, "resolve_managed_credential")
	}
	authSecrets := make([]string, 0, len(credentials))
	for _, value := range credentials {
		if secret := strings.TrimSpace(asString(value)); secret != "" {
			authSecrets = append(authSecrets, secret)
		}
	}
	if managedAuth != nil {
		if err := runtimemanagedcredentials.ApplyHTTPAuthorization(headers, managedAuth.HTTPAuthorization(), false); err != nil {
			return nil, httpToolAuthenticationFailure(err, toolName, "apply_managed_credential")
		}
		authSecrets = append(authSecrets, managedAuth.SecretValues()...)
	}

	var bodyReader io.Reader
	if body := request.Body(); len(body) > 0 {
		bodyReader = bytes.NewReader(body)
	}

	result, err := e.execHTTPRequestOnce(ctx, request.Method(), request.URL(), headers, bodyReader, request.Timeout(), tool, authSecrets)
	if err != nil {
		return nil, classifyHTTPToolFailure(err, toolName)
	}
	return result, nil
}

func httpToolAuthenticationFailure(err error, toolName, operation string) error {
	if _, ok := runtimefailures.As(err); ok {
		return err
	}
	return runtimefailures.Wrap(runtimefailures.ClassAuthenticationNeeded, "managed_credential_required", "tool-executor", operation, map[string]any{"auth_kind": "managed_credential", "tool": strings.TrimSpace(toolName)}, err)
}

func classifyHTTPToolFailure(err error, toolName string) error {
	if err == nil {
		return nil
	}
	if _, ok := runtimefailures.As(err); ok {
		return err
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return runtimefailures.Wrap(runtimefailures.ClassTimeout, "http_tool_timeout", "tool-executor", "execute_http_tool", map[string]any{"tool": strings.TrimSpace(toolName)}, err)
	}
	var statusErr httpToolStatusError
	if errors.As(err, &statusErr) {
		attributes := map[string]any{"status": statusErr.StatusCode, "tool": strings.TrimSpace(toolName)}
		switch statusErr.StatusCode {
		case http.StatusUnauthorized:
			attributes["auth_kind"] = "provider_credential"
			return runtimefailures.Wrap(runtimefailures.ClassAuthenticationNeeded, "provider_unauthorized", "tool-executor", "execute_http_tool", attributes, err)
		case http.StatusForbidden:
			attributes["action"] = "execute_http_tool"
			return runtimefailures.Wrap(runtimefailures.ClassAuthorizationDenied, "provider_forbidden", "tool-executor", "execute_http_tool", attributes, err)
		case http.StatusPaymentRequired:
			return runtimefailures.Wrap(runtimefailures.ClassConnectorFailure, "provider_credit_exhausted", "tool-executor", "execute_http_tool", attributes, err)
		case http.StatusRequestTimeout:
			return runtimefailures.Wrap(runtimefailures.ClassTimeout, "provider_request_timeout", "tool-executor", "execute_http_tool", attributes, err)
		default:
			return runtimefailures.Wrap(runtimefailures.ClassConnectorFailure, "provider_http_status", "tool-executor", "execute_http_tool", attributes, err)
		}
	}
	return runtimefailures.Wrap(runtimefailures.ClassConnectorFailure, "http_tool_request_failed", "tool-executor", "execute_http_tool", map[string]any{"tool": strings.TrimSpace(toolName)}, err)
}

type managedHTTPAuth struct {
	StoreKey string
	Token    string
	Record   runtimemanagedcredentials.Record
	Header   string
	Prefix   string
}

func (a managedHTTPAuth) SecretValues() []string {
	secrets := a.Record.SecretValues()
	token := strings.TrimSpace(a.Token)
	if token != "" {
		secrets = append(secrets, token)
	}
	return secrets
}

func (a managedHTTPAuth) HTTPAuthorization() runtimemanagedcredentials.HTTPAuthorization {
	return runtimemanagedcredentials.HTTPAuthorization{
		CredentialKey: a.StoreKey,
		AccessToken:   a.Token,
		Header:        a.Header,
		Prefix:        a.Prefix,
	}
}

func (e *Executor) resolveManagedCredentialForActor(ctx context.Context, actor models.AgentConfig, tool ExecutionTool) (*managedHTTPAuth, error) {
	ref, ok := tool.ManagedCredentialExecution()
	if !ok {
		return nil, nil
	}
	toolName := tool.Name()
	key := ref.Key()
	e.mu.RLock()
	source := e.workflowSource
	e.mu.RUnlock()
	flowID := emitActorFlowID(source, actor, "")
	storeKey, mapped := semanticview.CredentialStoreKeyForActorFlow(source, actor.ID, flowID, key)
	if mapped && storeKey == "" {
		return nil, fmt.Errorf("managed credential %q is not declared and bound for imported package actor %s", key, strings.TrimSpace(actor.ID))
	}
	if storeKey == "" {
		return nil, fmt.Errorf("managed credential %q does not resolve to a deployment credential key", key)
	}
	if ref.InstallationIDInput() != "" {
		return nil, fmt.Errorf("tool %s managed_credential.installation_id_input is supported only for activity input resolution", toolName)
	}
	token, record, err := e.managedTokenSource().AccessToken(ctx, runtimemanagedcredentials.AccessTokenRequest{
		Key:          storeKey,
		GrantType:    ref.GrantType(),
		Scopes:       ref.Scopes(),
		GrantModel:   ref.GrantModel(),
		TokenRequest: ref.TokenRequest(),
	})
	if err != nil {
		redacted := fmt.Errorf("%s", runtimemanagedcredentials.RedactString(err.Error(), record.SecretValues()...))
		return nil, httpToolAuthenticationFailure(redacted, toolName, "access_managed_credential")
	}
	return &managedHTTPAuth{
		StoreKey: storeKey,
		Token:    token,
		Record:   record,
		Header:   ref.Header(),
		Prefix:   ref.Prefix(),
	}, nil
}

func (e *Executor) managedTokenSource() *runtimemanagedcredentials.TokenSource {
	return &runtimemanagedcredentials.TokenSource{
		Store:      e.managedCredentials,
		HTTPClient: e.httpClient,
	}
}

func (e *Executor) execHTTPRequestOnce(ctx context.Context, method, url string, headers http.Header, body io.Reader, timeout time.Duration, tool ExecutionTool, secrets []string) (any, error) {
	if err := e.admitExternalDispatch(ctx, e.httpToolExternalDispatchPolicy(tool)); err != nil {
		return nil, err
	}
	toolName := tool.Name()
	responseSuccess, hasResponseSuccess := tool.ResponseSuccessPolicy()
	responseMapping, hasResponseMapping := tool.CompiledResponseMapping()
	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = io.ReadAll(body)
		if err != nil {
			return nil, fmt.Errorf("read HTTP tool request body: %w", err)
		}
		body = bytes.NewReader(bodyBytes)
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, method, url, body)
	if err != nil {
		return nil, err
	}
	for key, values := range headers {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	fingerprintHeaders := req.Header.Clone()
	for key, values := range fingerprintHeaders {
		for index, value := range values {
			values[index] = runtimemanagedcredentials.RedactString(value, secrets...)
		}
		fingerprintHeaders[key] = values
	}
	headerEvidence, err := json.Marshal(fingerprintHeaders)
	if err != nil {
		return nil, fmt.Errorf("encode HTTP tool request headers: %w", err)
	}
	requestEvidence := append([]byte(method+"\x00"+url+"\x00"), headerEvidence...)
	requestEvidence = append(requestEvidence, '\x00')
	requestEvidence = append(requestEvidence, bodyBytes...)
	attempt, err := runtimeeffects.Begin(ctx, "authored_http_tool", requestEvidence, map[string]string{"tool": toolName})
	if err != nil {
		return nil, err
	}
	if err := attempt.MarkLaunched(ctx); err != nil {
		return nil, err
	}
	resp, err := e.httpClient.Do(req)
	if err != nil {
		return nil, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "http_tool_attempt_outcome_unconfirmed", "tool-executor", "execute_http_tool", map[string]any{"tool": toolName, "stage": "transport"}, err)
	}
	defer resp.Body.Close()
	rawBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "http_tool_attempt_outcome_unconfirmed", "tool-executor", "execute_http_tool", map[string]any{"tool": toolName, "stage": "read_response"}, err)
	}
	parsedBody := parseHTTPResponseBody(resp, rawBody)
	parsedBody = runtimemanagedcredentials.RedactValue(parsedBody, secrets...)
	if resp.StatusCode >= 400 {
		statusErr := httpToolStatusError{ToolName: toolName, StatusCode: resp.StatusCode, Body: parsedBody, Secrets: secrets}
		return nil, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "http_tool_status_effect_outcome_unconfirmed", "tool-executor", "execute_http_tool", map[string]any{"tool": toolName, "status": resp.StatusCode}, statusErr)
	}
	responseEnv := map[string]any{
		"response": map[string]any{
			"status":  resp.StatusCode,
			"headers": flattenHTTPHeaders(resp.Header),
			"body":    parsedBody,
		},
	}
	if hasResponseSuccess {
		if err := responseSuccess.Evaluate(responseEnv); err != nil {
			err = fmt.Errorf("%s", runtimemanagedcredentials.RedactString(err.Error(), secrets...))
			cause := runtimefailures.Wrap(runtimefailures.ClassConnectorFailure, "provider_response_rejected", "tool-executor", "validate_http_response", map[string]any{"tool": toolName, "status": resp.StatusCode}, err)
			return nil, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "http_tool_result_effect_outcome_unconfirmed", "tool-executor", "validate_http_response", map[string]any{"tool": toolName, "status": resp.StatusCode}, cause)
		}
	}
	result := parsedBody
	if hasResponseMapping {
		mapped, err := responseMapping.Render(responseEnv)
		if err != nil {
			return nil, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "http_tool_result_effect_outcome_unconfirmed", "tool-executor", "map_response", map[string]any{"tool": toolName, "status": resp.StatusCode}, err)
		}
		result = mapped
	}
	if !tool.value.outputSchema.IsZero() {
		if err := tool.value.outputSchema.Validate(result); err != nil {
			cause := runtimefailures.Wrap(runtimefailures.ClassConnectorFailure, "provider_response_schema_invalid", "tool-executor", "validate_projected_response", map[string]any{"tool": toolName, "status": resp.StatusCode}, err)
			return nil, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "http_tool_result_effect_outcome_unconfirmed", "tool-executor", "validate_projected_response", map[string]any{"tool": toolName, "status": resp.StatusCode}, cause)
		}
	}
	if err := attempt.Succeed(ctx, map[string]any{"status": resp.StatusCode, "response_fingerprint": runtimeeffects.Fingerprint(rawBody)}); err != nil {
		return nil, err
	}
	return result, nil
}

func (e *Executor) execMCPTool(ctx context.Context, actor models.AgentConfig, tool ExecutionTool, input any) (any, error) {
	if e.mcpClient == nil {
		return nil, fmt.Errorf("mcp client is not configured")
	}
	policy, err := e.mcpToolExternalDispatchPolicy(tool)
	if err != nil {
		return nil, err
	}
	if err := e.admitExternalDispatch(ctx, policy); err != nil {
		return nil, err
	}
	e.mu.RLock()
	source := e.workflowSource
	e.mu.RUnlock()
	flowID := emitActorFlowID(source, actor, "")
	return e.mcpClient.CallWithCredentialKeyResolver(ctx, tool.Name(), input, func(key string) (string, error) {
		storeKey, mapped := semanticview.CredentialStoreKeyForActorFlow(source, actor.ID, flowID, key)
		if mapped && storeKey == "" {
			return "", fmt.Errorf("credential %q is not declared and bound for imported package actor %s", key, strings.TrimSpace(actor.ID))
		}
		return storeKey, nil
	})
}

func (e *Executor) resolveToolCredentialsForActor(ctx context.Context, actor models.AgentConfig, keys []string) (map[string]any, error) {
	e.mu.RLock()
	source := e.workflowSource
	e.mu.RUnlock()
	flowID := emitActorFlowID(source, actor, "")
	return e.resolveToolCredentialsWithMapper(ctx, keys, func(key string) (string, error) {
		storeKey, mapped := semanticview.CredentialStoreKeyForActorFlow(source, actor.ID, flowID, key)
		if mapped && storeKey == "" {
			return "", fmt.Errorf("credential %q is not declared and bound for imported package actor %s", key, strings.TrimSpace(actor.ID))
		}
		return storeKey, nil
	})
}

func (e *Executor) resolveToolCredentialsWithMapper(ctx context.Context, keys []string, mapKey func(string) (string, error)) (map[string]any, error) {
	out := make(map[string]any, len(keys))
	for _, key := range keys {
		storeKey, err := mapKey(key)
		if err != nil {
			return nil, err
		}
		if storeKey == "" {
			return nil, fmt.Errorf("credential %q does not resolve to a deployment credential key", key)
		}
		if e.credentials == nil {
			return nil, fmt.Errorf("credential store is not configured")
		}
		value, ok, err := e.credentials.Get(ctx, storeKey)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("missing credential %q", storeKey)
		}
		out[key] = value
	}
	return out, nil
}

func parseHTTPResponseBody(resp *http.Response, raw []byte) any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	if strings.Contains(strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Type"))), "json") {
		var parsed any
		if err := json.Unmarshal(raw, &parsed); err == nil {
			return parsed
		}
	}
	return string(raw)
}

func flattenHTTPHeaders(headers http.Header) map[string]any {
	out := make(map[string]any, len(headers))
	for key, values := range headers {
		key = strings.ToLower(strings.TrimSpace(key))
		if key == "" || len(values) == 0 {
			continue
		}
		if len(values) == 1 {
			out[key] = values[0]
			continue
		}
		items := make([]any, 0, len(values))
		for _, value := range values {
			items = append(items, value)
		}
		out[key] = items
	}
	return out
}
