package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

var toolTemplatePattern = regexp.MustCompile(`\{\{\s*([^{}]+?)\s*\}\}`)

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
	for attempt := 0; attempt <= maxRetries; attempt++ {
		result, err := e.execHTTPRequestOnce(ctx, method, url, headers, bodyReader, timeout, tool)
		if err == nil {
			return result, nil
		}
		lastErr = err
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
		if seeker, ok := bodyReader.(io.Seeker); ok {
			_, _ = seeker.Seek(0, io.SeekStart)
		}
	}
	return nil, lastErr
}

func (e *Executor) execHTTPRequestOnce(ctx context.Context, method, url string, headers http.Header, body io.Reader, timeout time.Duration, tool RegisteredTool) (any, error) {
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
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("http tool %s returned status %d: %s", tool.Name, resp.StatusCode, strings.TrimSpace(asString(parsedBody)))
	}
	if len(tool.ResponseMapping) == 0 {
		return parsedBody, nil
	}
	responseEnv := map[string]any{
		"response": map[string]any{
			"status":  resp.StatusCode,
			"headers": flattenHTTPHeaders(resp.Header),
			"body":    parsedBody,
		},
	}
	return resolveTemplateTree(tool.ResponseMapping, responseEnv)
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
