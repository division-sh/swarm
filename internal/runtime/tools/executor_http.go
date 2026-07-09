package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

var toolTemplatePattern = regexp.MustCompile(`\{\{\s*([^{}]+?)\s*\}\}`)

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

func (e *Executor) execHTTPTool(ctx context.Context, actor models.AgentConfig, tool RegisteredTool, input any) (any, error) {
	if tool.HTTP == nil {
		return nil, fmt.Errorf("http tool %s is missing http configuration", tool.Name)
	}
	payload := map[string]any{}
	if err := decodeToolInput(input, &payload); err != nil {
		return nil, err
	}
	if payload == nil {
		payload = map[string]any{}
	}
	credentials, err := e.resolveToolCredentialsForActor(ctx, actor, tool.Credentials)
	if err != nil {
		return nil, err
	}
	templateEnv := map[string]any{
		"input":       payload,
		"credentials": credentials,
	}

	resolvedURL, err := resolveHTTPURLTemplate(tool.HTTP.URL, templateEnv)
	if err != nil {
		return nil, err
	}
	url := strings.TrimSpace(resolvedURL)
	if url == "" {
		return nil, fmt.Errorf("http tool %s resolved an empty url", tool.Name)
	}

	headers := make(http.Header, len(tool.HTTP.Headers))
	for key, value := range tool.HTTP.Headers {
		resolved, err := resolveTemplateValue(value, templateEnv)
		if err != nil {
			return nil, err
		}
		headers.Set(strings.TrimSpace(key), strings.TrimSpace(asString(resolved)))
	}
	managedAuth, err := e.resolveManagedCredentialForActor(ctx, actor, tool)
	if err != nil {
		return nil, err
	}
	authSecrets := []string{}
	if managedAuth != nil {
		if err := applyManagedCredentialHeader(headers, managedAuth, false); err != nil {
			return nil, err
		}
		authSecrets = append(authSecrets, managedAuth.SecretValues()...)
	}

	var bodyReader io.Reader
	if tool.HTTP.Body != nil {
		resolvedBody, err := resolveTemplateTree(tool.HTTP.Body, templateEnv)
		if err != nil {
			return nil, err
		}
		raw, err := json.Marshal(resolvedBody)
		if err != nil {
			return nil, err
		}
		bodyReader = bytes.NewReader(raw)
		if strings.TrimSpace(headers.Get("Content-Type")) == "" {
			headers.Set("Content-Type", "application/json")
		}
	}

	method := strings.ToUpper(strings.TrimSpace(tool.HTTP.Method))
	if method == "" {
		method = http.MethodGet
	}
	timeout := 30 * time.Second
	if tool.HTTP.TimeoutSeconds > 0 {
		timeout = time.Duration(tool.HTTP.TimeoutSeconds) * time.Second
	}

	maxRetries := 0
	if tool.HTTP.Retry.MaxRetries > 0 {
		maxRetries = tool.HTTP.Retry.MaxRetries
	}
	backoff := strings.ToLower(strings.TrimSpace(tool.HTTP.Retry.Backoff))
	var lastErr error
	refreshedAfterUnauthorized := false
	for attempt := 0; attempt <= maxRetries; attempt++ {
		result, err := e.execHTTPRequestOnce(ctx, method, url, headers, bodyReader, timeout, tool, authSecrets)
		if err == nil {
			return result, nil
		}
		lastErr = err
		var statusErr httpToolStatusError
		if managedAuth != nil && errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusUnauthorized && !refreshedAfterUnauthorized {
			refreshedAfterUnauthorized = true
			token, record, refreshErr := e.managedTokenSource().Refresh(ctx, managedAuth.StoreKey)
			if refreshErr != nil {
				return nil, fmt.Errorf("%s", runtimemanagedcredentials.RedactString(refreshErr.Error(), append(authSecrets, record.SecretValues()...)...))
			}
			managedAuth.Token = token
			managedAuth.Record = record
			authSecrets = append(authSecrets, managedAuth.SecretValues()...)
			if err := applyManagedCredentialHeader(headers, managedAuth, true); err != nil {
				return nil, err
			}
			rewindBodyReader(bodyReader)
			attempt--
			continue
		}
		if isExternalDispatchRateLimited(err) {
			break
		}
		if attempt == maxRetries {
			break
		}
		sleep := time.Second
		if backoff == "exponential" {
			sleep = time.Duration(1<<attempt) * time.Second
		}
		time.Sleep(sleep)
		rewindBodyReader(bodyReader)
	}
	return nil, lastErr
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

func (e *Executor) resolveManagedCredentialForActor(ctx context.Context, actor models.AgentConfig, tool RegisteredTool) (*managedHTTPAuth, error) {
	if tool.ManagedCredential == nil {
		return nil, nil
	}
	ref := *tool.ManagedCredential
	key := strings.TrimSpace(ref.Key)
	if key == "" {
		return nil, fmt.Errorf("tool %s managed_credential.key is required", strings.TrimSpace(tool.Name))
	}
	e.mu.RLock()
	source := e.workflowSource
	e.mu.RUnlock()
	flowID := emitActorFlowID(source, actor, "")
	storeKey, mapped := semanticview.CredentialStoreKeyForActorFlow(source, actor.ID, flowID, key)
	if mapped && strings.TrimSpace(storeKey) == "" {
		return nil, fmt.Errorf("managed credential %q is not declared and bound for imported package actor %s", key, strings.TrimSpace(actor.ID))
	}
	storeKey = strings.TrimSpace(storeKey)
	if storeKey == "" {
		return nil, fmt.Errorf("managed credential %q does not resolve to a deployment credential key", key)
	}
	if strings.TrimSpace(ref.InstallationIDInput) != "" {
		return nil, fmt.Errorf("tool %s managed_credential.installation_id_input is supported only for activity input resolution", strings.TrimSpace(tool.Name))
	}
	token, record, err := e.managedTokenSource().AccessToken(ctx, runtimemanagedcredentials.AccessTokenRequest{
		Key:          storeKey,
		GrantType:    ref.GrantType,
		Scopes:       ref.Scopes,
		GrantModel:   ref.GrantModel,
		TokenRequest: ref.TokenRequest,
	})
	if err != nil {
		return nil, fmt.Errorf("%s", runtimemanagedcredentials.RedactString(err.Error(), record.SecretValues()...))
	}
	header := strings.TrimSpace(ref.Header)
	if header == "" {
		header = "Authorization"
	}
	prefix := strings.TrimSpace(ref.Prefix)
	if prefix == "" && strings.EqualFold(header, "Authorization") {
		prefix = "Bearer"
	}
	return &managedHTTPAuth{
		StoreKey: storeKey,
		Token:    token,
		Record:   record,
		Header:   header,
		Prefix:   prefix,
	}, nil
}

func (e *Executor) managedTokenSource() *runtimemanagedcredentials.TokenSource {
	return &runtimemanagedcredentials.TokenSource{
		Store:      e.managedCredentials,
		HTTPClient: e.httpClient,
	}
}

func applyManagedCredentialHeader(headers http.Header, auth *managedHTTPAuth, replace bool) error {
	if auth == nil {
		return nil
	}
	header := strings.TrimSpace(auth.Header)
	if header == "" {
		header = "Authorization"
	}
	if existing := strings.TrimSpace(headers.Get(header)); existing != "" && !replace {
		return fmt.Errorf("managed credential cannot set %s because the header is already configured", header)
	}
	value := strings.TrimSpace(auth.Token)
	if value == "" {
		return fmt.Errorf("managed credential %q did not provide an access token", auth.StoreKey)
	}
	if prefix := strings.TrimSpace(auth.Prefix); prefix != "" {
		value = prefix + " " + value
	}
	headers.Set(header, value)
	return nil
}

func rewindBodyReader(body io.Reader) {
	if seeker, ok := body.(io.Seeker); ok {
		_, _ = seeker.Seek(0, io.SeekStart)
	}
}

func (e *Executor) execHTTPRequestOnce(ctx context.Context, method, url string, headers http.Header, body io.Reader, timeout time.Duration, tool RegisteredTool, secrets []string) (any, error) {
	if err := e.admitExternalDispatch(ctx, e.httpToolExternalDispatchPolicy(tool)); err != nil {
		return nil, err
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
	resp, err := e.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	rawBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	parsedBody := parseHTTPResponseBody(resp, rawBody)
	parsedBody = runtimemanagedcredentials.RedactValue(parsedBody, secrets...)
	if resp.StatusCode >= 400 {
		return nil, httpToolStatusError{ToolName: tool.Name, StatusCode: resp.StatusCode, Body: parsedBody, Secrets: secrets}
	}
	responseEnv := map[string]any{
		"response": map[string]any{
			"status":  resp.StatusCode,
			"headers": flattenHTTPHeaders(resp.Header),
			"body":    parsedBody,
		},
	}
	if err := evaluateHTTPResponseSuccess(tool.Name, tool.ResponseSuccess, responseEnv, secrets); err != nil {
		return nil, err
	}
	if len(tool.ResponseMapping) == 0 {
		return parsedBody, nil
	}
	return resolveTemplateTree(tool.ResponseMapping, responseEnv)
}

func evaluateHTTPResponseSuccess(toolName string, check *runtimecontracts.HTTPResponseSuccess, responseEnv map[string]any, secrets []string) error {
	if check == nil {
		return nil
	}
	path := strings.TrimSpace(check.Path)
	if path == "" {
		return fmt.Errorf("http tool %s response_success.path is required", strings.TrimSpace(toolName))
	}
	got, err := lookupTemplatePath(responseEnv, path)
	if err != nil {
		return fmt.Errorf("http tool %s response_success path %q did not resolve", strings.TrimSpace(toolName), path)
	}
	if responseSuccessValuesEqual(got, check.Equals) {
		return nil
	}
	return fmt.Errorf("%s", runtimemanagedcredentials.RedactString(
		fmt.Sprintf("http tool %s response_success failed: %s = %s, want %s", strings.TrimSpace(toolName), path, asString(got), asString(check.Equals)),
		secrets...,
	))
}

func responseSuccessValuesEqual(got, want any) bool {
	switch wantTyped := want.(type) {
	case bool:
		gotTyped, ok := got.(bool)
		return ok && gotTyped == wantTyped
	case string:
		gotTyped, ok := got.(string)
		return ok && gotTyped == wantTyped
	case int:
		gotFloat, ok := responseSuccessFloat(got)
		return ok && gotFloat == float64(wantTyped)
	case int64:
		gotFloat, ok := responseSuccessFloat(got)
		return ok && gotFloat == float64(wantTyped)
	case float64:
		gotFloat, ok := responseSuccessFloat(got)
		return ok && gotFloat == wantTyped
	case float32:
		gotFloat, ok := responseSuccessFloat(got)
		return ok && gotFloat == float64(wantTyped)
	default:
		return fmt.Sprint(got) == fmt.Sprint(want)
	}
}

func responseSuccessFloat(value any) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case json.Number:
		parsed, err := typed.Float64()
		return parsed, err == nil
	default:
		return 0, false
	}
}

func (e *Executor) execMCPTool(ctx context.Context, actor models.AgentConfig, tool RegisteredTool, input any) (any, error) {
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
	return e.mcpClient.CallWithCredentialKeyResolver(ctx, tool.Name, input, func(key string) (string, error) {
		storeKey, mapped := semanticview.CredentialStoreKeyForActorFlow(source, actor.ID, flowID, key)
		if mapped && strings.TrimSpace(storeKey) == "" {
			return "", fmt.Errorf("credential %q is not declared and bound for imported package actor %s", key, strings.TrimSpace(actor.ID))
		}
		return storeKey, nil
	})
}

func (e *Executor) resolveToolCredentials(ctx context.Context, keys []string) (map[string]any, error) {
	return e.resolveToolCredentialsWithMapper(ctx, keys, func(key string) (string, error) { return key, nil })
}

func (e *Executor) resolveToolCredentialsForActor(ctx context.Context, actor models.AgentConfig, keys []string) (map[string]any, error) {
	e.mu.RLock()
	source := e.workflowSource
	e.mu.RUnlock()
	flowID := emitActorFlowID(source, actor, "")
	return e.resolveToolCredentialsWithMapper(ctx, keys, func(key string) (string, error) {
		storeKey, mapped := semanticview.CredentialStoreKeyForActorFlow(source, actor.ID, flowID, key)
		if mapped && strings.TrimSpace(storeKey) == "" {
			return "", fmt.Errorf("credential %q is not declared and bound for imported package actor %s", key, strings.TrimSpace(actor.ID))
		}
		return storeKey, nil
	})
}

func (e *Executor) resolveToolCredentialsWithMapper(ctx context.Context, keys []string, mapKey func(string) (string, error)) (map[string]any, error) {
	out := make(map[string]any, len(keys))
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		storeKey, err := mapKey(key)
		if err != nil {
			return nil, err
		}
		storeKey = strings.TrimSpace(storeKey)
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

func resolveTemplateTree(value any, env map[string]any) (any, error) {
	switch typed := value.(type) {
	case string:
		return resolveTemplateValue(typed, env)
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			resolved, err := resolveTemplateTree(item, env)
			if err != nil {
				return nil, err
			}
			out[key] = resolved
		}
		return out, nil
	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			resolved, err := resolveTemplateTree(item, env)
			if err != nil {
				return nil, err
			}
			out = append(out, resolved)
		}
		return out, nil
	default:
		return value, nil
	}
}

func resolveTemplateValue(raw string, env map[string]any) (any, error) {
	matches := toolTemplatePattern.FindAllStringSubmatchIndex(raw, -1)
	if len(matches) == 0 {
		return raw, nil
	}
	if len(matches) == 1 && matches[0][0] == 0 && matches[0][1] == len(raw) {
		path := strings.TrimSpace(raw[matches[0][2]:matches[0][3]])
		return lookupTemplatePath(env, path)
	}
	var builder strings.Builder
	last := 0
	for _, match := range matches {
		builder.WriteString(raw[last:match[0]])
		path := strings.TrimSpace(raw[match[2]:match[3]])
		value, err := lookupTemplatePath(env, path)
		if err != nil {
			return nil, err
		}
		builder.WriteString(asString(value))
		last = match[1]
	}
	builder.WriteString(raw[last:])
	return builder.String(), nil
}

func resolveHTTPURLTemplate(raw string, env map[string]any) (string, error) {
	matches := toolTemplatePattern.FindAllStringSubmatchIndex(raw, -1)
	if len(matches) == 0 {
		return raw, nil
	}
	if len(matches) == 1 && matches[0][0] == 0 && matches[0][1] == len(raw) {
		path := strings.TrimSpace(raw[matches[0][2]:matches[0][3]])
		value, err := lookupTemplatePath(env, path)
		if err != nil {
			return "", err
		}
		return asString(value), nil
	}
	var builder strings.Builder
	last := 0
	for _, match := range matches {
		builder.WriteString(raw[last:match[0]])
		path := strings.TrimSpace(raw[match[2]:match[3]])
		value, err := lookupTemplatePath(env, path)
		if err != nil {
			return "", err
		}
		builder.WriteString(escapeHTTPURLTemplateComponent(raw, match[0], match[1], asString(value)))
		last = match[1]
	}
	builder.WriteString(raw[last:])
	return builder.String(), nil
}

func escapeHTTPURLTemplateComponent(raw string, start, end int, value string) string {
	if httpURLTemplateOffsetInQuery(raw, start) {
		return strings.ReplaceAll(url.QueryEscape(value), "+", "%20")
	}
	if httpURLTemplatePlaceholderInURLBaseOrAuthority(raw, start, end, value) {
		return value
	}
	return url.PathEscape(value)
}

func httpURLTemplateOffsetInQuery(raw string, offset int) bool {
	queryStart := strings.Index(raw, "?")
	if queryStart < 0 || queryStart > offset {
		return false
	}
	fragmentStart := strings.Index(raw, "#")
	return fragmentStart < 0 || offset < fragmentStart
}

func httpURLTemplatePlaceholderInURLBaseOrAuthority(raw string, start, end int, value string) bool {
	prefix := raw[:start]
	suffix := raw[end:]
	if strings.HasPrefix(suffix, "://") {
		return true
	}
	if strings.HasSuffix(prefix, "://") {
		return true
	}
	schemeIndex := strings.LastIndex(prefix, "://")
	if schemeIndex >= 0 {
		authorityPrefix := prefix[schemeIndex+len("://"):]
		return !strings.ContainsAny(authorityPrefix, "/?#")
	}
	if start == 0 {
		return httpURLTemplateValueHasSchemeAuthority(value)
	}
	return false
}

func httpURLTemplateValueHasSchemeAuthority(value string) bool {
	parsed, err := url.Parse(strings.TrimSpace(value))
	return err == nil && parsed.Scheme != "" && parsed.Host != ""
}

func lookupTemplatePath(env map[string]any, path string) (any, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("empty template variable")
	}
	parts := splitTemplatePath(path)
	var current any = env
	for _, part := range parts {
		switch typed := current.(type) {
		case map[string]any:
			next, ok := typed[part]
			if !ok {
				return nil, fmt.Errorf("template variable %q is not available", path)
			}
			current = next
		case []any:
			index, err := strconv.Atoi(part)
			if err != nil || index < 0 || index >= len(typed) {
				return nil, fmt.Errorf("template variable %q is not available", path)
			}
			current = typed[index]
		default:
			return nil, fmt.Errorf("template variable %q is not available", path)
		}
	}
	return current, nil
}

func splitTemplatePath(path string) []string {
	replacer := strings.NewReplacer("[", ".", "]", "")
	parts := strings.Split(replacer.Replace(path), ".")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
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
