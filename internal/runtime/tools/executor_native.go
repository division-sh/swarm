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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

type nativeBashInput struct {
	Command        string `json:"command"`
	TimeoutSeconds int    `json:"timeout_seconds"`
}

type nativeFileReadInput struct {
	Path string `json:"path"`
}

type nativeFileWriteInput struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

type nativeWebSearchInput struct {
	Query      string `json:"query"`
	MaxResults int    `json:"max_results"`
}

type webSearchProviderConfig struct {
	Provider          string
	CredentialsKey    string
	MaxResultsDefault int
	HTTP              *runtimecontracts.HTTPToolSpec
	ResponsePath      string
	FieldMapping      map[string]string
	PolicyOwnerKey    string
	RateLimit         externalDispatchRateLimitConfig
}

func (e *Executor) execNativeBash(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	var in nativeBashInput
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	command := strings.TrimSpace(in.Command)
	if command == "" {
		return nil, fmt.Errorf("bash.command is required")
	}
	timeout := 30 * time.Second
	if in.TimeoutSeconds > 0 {
		timeout = time.Duration(in.TimeoutSeconds) * time.Second
	}
	target, err := e.resolveNativeWorkspace(ctx, actor)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	stdout, stderr, exitCode, execErr := e.runWorkspaceCommand(ctx, target, "native_bash", timeout, "", "sh", "-lc", command)
	duration := time.Since(start)
	if execErr != nil && (ctx.Err() != nil || errors.Is(execErr, context.DeadlineExceeded)) {
		return nil, execErr
	}
	if execErr != nil && exitCode == -1 {
		return nil, execErr
	}
	return map[string]any{
		"stdout":      string(stdout),
		"stderr":      string(stderr),
		"exit_code":   exitCode,
		"duration_ms": duration.Milliseconds(),
	}, nil
}

func (e *Executor) execNativeReadFile(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	var in nativeFileReadInput
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	target, err := e.resolveNativeWorkspace(ctx, actor)
	if err != nil {
		return nil, err
	}
	if err := target.ExecutionTarget().Require(workspace.ExecutionCapabilityFileRead); err != nil {
		return nil, err
	}
	execTarget := target.ExecutionTarget()
	if execTarget.Mode == workspace.ExecutionModeHostLocal {
		return execNativeHostReadFile(execTarget, in.Path)
	}
	path, err := resolveNativeReadPath(target, in.Path)
	if err != nil {
		return nil, err
	}
	stdout, stderr, exitCode, execErr := e.runWorkspaceCommand(ctx, target, "native_read_file", 30*time.Second, "", "sh", "-lc", `cat -- "$1"`, "swarm-read-file", path)
	if execErr != nil || exitCode != 0 {
		return nil, fmt.Errorf("read_file failed for %s: %s", path, strings.TrimSpace(string(stderr)))
	}
	return map[string]any{
		"content":    string(stdout),
		"size_bytes": len(stdout),
	}, nil
}

func (e *Executor) execNativeWriteFile(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	var in nativeFileWriteInput
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	target, err := e.resolveNativeWorkspace(ctx, actor)
	if err != nil {
		return nil, err
	}
	if err := target.ExecutionTarget().Require(workspace.ExecutionCapabilityFileWrite); err != nil {
		return nil, err
	}
	execTarget := target.ExecutionTarget()
	if execTarget.Mode == workspace.ExecutionModeHostLocal {
		return execNativeHostWriteFile(ctx, execTarget, in.Path, in.Content)
	}
	path, err := resolveNativeWritePath(target, in.Path)
	if err != nil {
		return nil, err
	}
	_, stderr, exitCode, execErr := e.runWorkspaceCommand(ctx, target, "native_write_file", 30*time.Second, in.Content, "sh", "-lc", `dir="$(dirname -- "$1")" && mkdir -p -- "$dir" && tmp="$(mktemp "$dir/.swarm-write.XXXXXX")" && trap 'rm -f -- "$tmp"' EXIT && cat > "$tmp" && sync "$tmp" && mv -f -- "$tmp" "$1"`, "swarm-write-file", path)
	if execErr != nil || exitCode != 0 {
		return nil, fmt.Errorf("write_file failed for %s: %s", path, strings.TrimSpace(string(stderr)))
	}
	return map[string]any{
		"bytes_written": len([]byte(in.Content)),
	}, nil
}

func (e *Executor) execNativeWebSearch(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	var in nativeWebSearchInput
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	query := strings.TrimSpace(in.Query)
	if query == "" {
		return nil, fmt.Errorf("web_search.query is required")
	}
	cfg, err := e.resolveWebSearchProviderConfig(actor)
	if err != nil {
		return nil, err
	}
	maxResults := in.MaxResults
	if maxResults <= 0 {
		maxResults = cfg.MaxResultsDefault
	}
	if maxResults <= 0 {
		maxResults = 10
	}
	credentialValue := ""
	if strings.TrimSpace(cfg.CredentialsKey) != "" {
		credentials, err := e.resolveToolCredentialsForActor(ctx, actor, []string{cfg.CredentialsKey})
		if err != nil {
			return nil, err
		}
		credentialValue = strings.TrimSpace(asString(credentials[cfg.CredentialsKey]))
	}
	results, err := e.executeWebSearch(ctx, cfg, query, maxResults, credentialValue)
	if err != nil {
		return nil, err
	}
	return map[string]any{"results": results}, nil
}

func (e *Executor) executeWebSearch(ctx context.Context, cfg webSearchProviderConfig, query string, maxResults int, credentialValue string) ([]map[string]any, error) {
	switch strings.ToLower(strings.TrimSpace(cfg.Provider)) {
	case "brave":
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("https://api.search.brave.com/res/v1/web/search?q=%s&count=%d", url.QueryEscape(query), maxResults), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("X-Subscription-Token", credentialValue)
		return e.doNormalizedSearch(ctx, req, "web.results", map[string]string{"title": "title", "url": "url", "snippet": "description"}, e.webSearchExternalDispatchPolicy(cfg))
	case "serper":
		body, _ := json.Marshal(map[string]any{"q": query, "num": maxResults})
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://google.serper.dev/search", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-API-KEY", credentialValue)
		return e.doNormalizedSearch(ctx, req, "organic", map[string]string{"title": "title", "url": "link", "snippet": "snippet"}, e.webSearchExternalDispatchPolicy(cfg))
	case "tavily":
		body, _ := json.Marshal(map[string]any{"api_key": credentialValue, "query": query, "max_results": maxResults})
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.tavily.com/search", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		return e.doNormalizedSearch(ctx, req, "results", map[string]string{"title": "title", "url": "url", "snippet": "content"}, e.webSearchExternalDispatchPolicy(cfg))
	case "custom":
		return e.executeCustomWebSearch(ctx, cfg, query, maxResults, credentialValue)
	default:
		return nil, fmt.Errorf("unsupported web_search provider %q", cfg.Provider)
	}
}

func (e *Executor) executeCustomWebSearch(ctx context.Context, cfg webSearchProviderConfig, query string, maxResults int, credentialValue string) ([]map[string]any, error) {
	if cfg.HTTP == nil {
		return nil, fmt.Errorf("custom web_search provider requires http configuration")
	}
	credentials := map[string]any{}
	if strings.TrimSpace(cfg.CredentialsKey) != "" {
		credentials[cfg.CredentialsKey] = credentialValue
	}
	templateEnv := map[string]any{
		"input": map[string]any{
			"query":       query,
			"max_results": maxResults,
		},
		"credentials": credentials,
	}
	resolvedURL, err := resolveHTTPURLTemplate(cfg.HTTP.URL, templateEnv)
	if err != nil {
		return nil, err
	}
	url := strings.TrimSpace(resolvedURL)
	headers := make(http.Header, len(cfg.HTTP.Headers))
	for key, value := range cfg.HTTP.Headers {
		resolved, err := resolveTemplateValue(value, templateEnv)
		if err != nil {
			return nil, err
		}
		headers.Set(strings.TrimSpace(key), strings.TrimSpace(asString(resolved)))
	}
	var body bytes.Buffer
	if cfg.HTTP.Body != nil {
		resolvedBody, err := resolveTemplateTree(cfg.HTTP.Body, templateEnv)
		if err != nil {
			return nil, err
		}
		raw, err := json.Marshal(resolvedBody)
		if err != nil {
			return nil, err
		}
		body.Write(raw)
		if headers.Get("Content-Type") == "" {
			headers.Set("Content-Type", "application/json")
		}
	}
	method := strings.ToUpper(strings.TrimSpace(cfg.HTTP.Method))
	if method == "" {
		method = http.MethodGet
	}
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body.Bytes()))
	if err != nil {
		return nil, err
	}
	req.Header = headers
	return e.doNormalizedSearch(ctx, req, cfg.ResponsePath, cfg.FieldMapping, e.webSearchExternalDispatchPolicy(cfg))
}

func (e *Executor) doNormalizedSearch(ctx context.Context, req *http.Request, responsePath string, fieldMapping map[string]string, policy externalDispatchAdmissionPolicy) ([]map[string]any, error) {
	if err := e.admitExternalDispatch(ctx, policy); err != nil {
		return nil, err
	}
	attempt, err := runtimeeffects.Begin(ctx, "native_web_search", []byte(req.Method+"\x00"+req.URL.String()), nil)
	if err != nil {
		return nil, err
	}
	if err := attempt.MarkLaunched(ctx); err != nil {
		return nil, err
	}
	resp, err := e.httpClient.Do(req)
	if err != nil {
		return nil, attempt.Fail(ctx, runtimeeffects.StateTerminalFailure, runtimefailures.ClassConnectorFailure, "native_web_search_transport_failed", "tool-executor", "web_search", map[string]any{"stage": "transport"}, err)
	}
	defer resp.Body.Close()
	raw, err := ioReadAll(resp)
	if err != nil {
		return nil, attempt.Fail(ctx, runtimeeffects.StateTerminalFailure, runtimefailures.ClassConnectorFailure, "native_web_search_read_failed", "tool-executor", "web_search", map[string]any{"stage": "read"}, err)
	}
	parsed := parseHTTPResponseBody(resp, raw)
	if resp.StatusCode >= 400 {
		err := fmt.Errorf("web_search returned status %d: %s", resp.StatusCode, strings.TrimSpace(asString(parsed)))
		return nil, attempt.Fail(ctx, runtimeeffects.StateTerminalFailure, runtimefailures.ClassConnectorFailure, "native_web_search_status_failed", "tool-executor", "web_search", map[string]any{"status": resp.StatusCode}, err)
	}
	arrayValue, err := nestedValue(parsed, responsePath)
	if err != nil {
		return nil, attempt.Fail(ctx, runtimeeffects.StateTerminalFailure, runtimefailures.ClassConnectorFailure, "native_web_search_mapping_failed", "tool-executor", "web_search", map[string]any{"response_path": responsePath}, err)
	}
	items, ok := arrayValue.([]any)
	if !ok {
		err := fmt.Errorf("web_search response path %q did not resolve to an array", responsePath)
		return nil, attempt.Fail(ctx, runtimeeffects.StateTerminalFailure, runtimefailures.ClassConnectorFailure, "native_web_search_mapping_failed", "tool-executor", "web_search", map[string]any{"response_path": responsePath}, err)
	}
	results := make([]map[string]any, 0, len(items))
	for _, item := range items {
		row, ok := normalizeAnyMap(item)
		if !ok {
			continue
		}
		title, _ := nestedValue(row, fieldMapping["title"])
		link, _ := nestedValue(row, fieldMapping["url"])
		snippet, _ := nestedValue(row, fieldMapping["snippet"])
		result := map[string]any{
			"title":   strings.TrimSpace(asString(title)),
			"url":     strings.TrimSpace(asString(link)),
			"snippet": strings.TrimSpace(asString(snippet)),
		}
		results = append(results, result)
	}
	if err := attempt.Succeed(ctx, map[string]any{"status": resp.StatusCode, "response_fingerprint": runtimeeffects.Fingerprint(raw)}); err != nil {
		return nil, err
	}
	return results, nil
}

func (e *Executor) resolveNativeWorkspace(ctx context.Context, actor models.AgentConfig) (*workspace.Target, error) {
	if e == nil || e.workspaces == nil {
		return nil, nil
	}
	return e.workspaces.ResolveWorkspace(ctx, actor)
}

func (e *Executor) runWorkspaceCommand(ctx context.Context, target *workspace.Target, adapter string, timeout time.Duration, stdin string, args ...string) ([]byte, []byte, int, error) {
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	execTarget := target.ExecutionTarget()
	if err := execTarget.Require(workspace.ExecutionCapabilityNativeCommand); err != nil {
		return nil, nil, -1, err
	}
	if len(args) == 0 {
		return nil, nil, -1, fmt.Errorf("workspace command args are required")
	}
	if strings.TrimSpace(args[0]) == "" {
		return nil, nil, -1, fmt.Errorf("workspace command executable is required")
	}
	if e != nil && e.execWorkspaceFn != nil {
		return e.execWorkspaceFn(runCtx, execTarget, timeout, stdin, args...)
	}
	var cmd *exec.Cmd
	switch execTarget.Mode {
	case workspace.ExecutionModeDockerContainer:
		dockerBin := workspaceDockerBin(e.cfg)
		dockerArgs := []string{"exec", "-i"}
		if workdir := strings.TrimSpace(execTarget.Workdir); workdir != "" {
			dockerArgs = append(dockerArgs, "-w", workdir)
		}
		dockerArgs = append(dockerArgs, strings.TrimSpace(execTarget.Container))
		dockerArgs = append(dockerArgs, args...)
		cmd = exec.CommandContext(runCtx, dockerBin, dockerArgs...)
	case workspace.ExecutionModeHostLocal:
		hostWorkdir, err := hostNativeCommandWorkdir(execTarget)
		if err != nil {
			return nil, nil, -1, err
		}
		cmd = exec.CommandContext(runCtx, strings.TrimSpace(args[0]), args[1:]...)
		cmd.Dir = hostWorkdir
	default:
		return nil, nil, -1, fmt.Errorf("%s", execTarget.UnsupportedMessage(workspace.ExecutionCapabilityNativeCommand))
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	attempt, err := runtimeeffects.Begin(ctx, adapter, []byte(strings.Join(args, "\x00")+"\x00"+stdin), map[string]string{"execution_mode": string(execTarget.Mode)})
	if err != nil {
		return nil, nil, -1, err
	}
	if err := cmd.Start(); err != nil {
		return stdout.Bytes(), stderr.Bytes(), -1, attempt.Fail(ctx, runtimeeffects.StateTerminalFailure, runtimefailures.ClassDependencyUnavailable, "native_command_start_failed", "tool-executor", adapter, map[string]any{"execution_mode": string(execTarget.Mode)}, err)
	}
	if err := attempt.MarkLaunched(ctx); err != nil {
		_ = cmd.Process.Kill()
		return stdout.Bytes(), stderr.Bytes(), -1, err
	}
	err = cmd.Wait()
	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		}
		registration, _ := runtimeeffects.RegistrationFor(adapter)
		state, class, code := runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "native_command_effect_outcome_unconfirmed"
		if registration.Class == runtimeeffects.EffectReadOnly {
			state, class, code = runtimeeffects.StateTerminalFailure, runtimefailures.ClassConnectorFailure, "native_read_command_failed"
		}
		return stdout.Bytes(), stderr.Bytes(), exitCode, attempt.Fail(ctx, state, class, code, "tool-executor", adapter, map[string]any{"execution_mode": string(execTarget.Mode), "exit_code": exitCode}, err)
	}
	if err := attempt.Succeed(ctx, map[string]any{"execution_mode": string(execTarget.Mode), "exit_code": exitCode, "stdout_fingerprint": runtimeeffects.Fingerprint(stdout.Bytes())}); err != nil {
		return stdout.Bytes(), stderr.Bytes(), exitCode, err
	}
	return stdout.Bytes(), stderr.Bytes(), exitCode, err
}

func hostNativeCommandWorkdir(execTarget workspace.ExecutionTarget) (string, error) {
	workdir := strings.TrimSpace(execTarget.Workdir)
	if workdir == "" {
		workdir = workspace.LogicalWorkspaceMount
	}
	resolved, err := execTarget.ResolveHostPath(workdir, workspace.PathAccessWrite)
	if err != nil {
		return "", fmt.Errorf("host native command workspace path is unavailable: %w", err)
	}
	return resolved.HostPath, nil
}

func resolveNativeReadPath(target *workspace.Target, raw string) (string, error) {
	return target.ExecutionTarget().ResolvePath(raw, workspace.PathAccessRead)
}

func resolveNativeWritePath(target *workspace.Target, raw string) (string, error) {
	return target.ExecutionTarget().ResolvePath(raw, workspace.PathAccessWrite)
}

func execNativeHostReadFile(target workspace.ExecutionTarget, rawPath string) (any, error) {
	resolved, err := target.ResolveHostPath(rawPath, workspace.PathAccessRead)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(resolved.HostPath)
	if err != nil {
		return nil, fmt.Errorf("read_file failed for %s: %s", resolved.LogicalPath, hostFileErrorMessage(err))
	}
	return map[string]any{
		"content":    string(data),
		"size_bytes": len(data),
	}, nil
}

func execNativeHostWriteFile(ctx context.Context, target workspace.ExecutionTarget, rawPath string, content string) (any, error) {
	resolved, err := target.ResolveHostPath(rawPath, workspace.PathAccessWrite)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(resolved.HostPath), 0o700); err != nil {
		return nil, fmt.Errorf("write_file failed for %s: %s", resolved.LogicalPath, hostFileErrorMessage(err))
	}
	data := []byte(content)
	attempt, err := runtimeeffects.Begin(ctx, "native_write_file", append([]byte(resolved.LogicalPath+"\x00"), data...), map[string]string{"logical_path": resolved.LogicalPath, "execution_mode": string(workspace.ExecutionModeHostLocal)})
	if err != nil {
		return nil, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(resolved.HostPath), ".swarm-write-*")
	if err != nil {
		return nil, err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := attempt.MarkLaunched(ctx); err != nil {
		_ = tmp.Close()
		return nil, err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return nil, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "native_file_write_host_outcome_unconfirmed", "tool-executor", "write_file", map[string]any{"logical_path": resolved.LogicalPath, "mutation_stage": "write_temp"}, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return nil, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "native_file_write_host_outcome_unconfirmed", "tool-executor", "write_file", map[string]any{"logical_path": resolved.LogicalPath, "mutation_stage": "sync_temp"}, err)
	}
	if err := tmp.Close(); err != nil {
		return nil, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "native_file_write_host_outcome_unconfirmed", "tool-executor", "write_file", map[string]any{"logical_path": resolved.LogicalPath, "mutation_stage": "close_temp"}, err)
	}
	if err := os.Rename(tmpPath, resolved.HostPath); err != nil {
		return nil, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "native_file_write_host_outcome_unconfirmed", "tool-executor", "write_file", map[string]any{"logical_path": resolved.LogicalPath, "mutation_stage": "replace"}, err)
	}
	if err := attempt.Succeed(ctx, map[string]any{"logical_path": resolved.LogicalPath, "content_fingerprint": runtimeeffects.Fingerprint(data)}); err != nil {
		return nil, err
	}
	return map[string]any{
		"bytes_written": len(data),
	}, nil
}

func hostFileErrorMessage(err error) string {
	switch {
	case os.IsNotExist(err):
		return "file does not exist"
	case os.IsPermission(err):
		return "permission denied"
	default:
		return "file is unavailable"
	}
}

func (e *Executor) resolveWebSearchProviderConfig(actor models.AgentConfig) (webSearchProviderConfig, error) {
	e.mu.RLock()
	source := e.workflowSource
	e.mu.RUnlock()
	flowID := emitActorFlowID(source, actor, "")
	return resolveWebSearchProviderConfigFromSourceForFlow(source, flowID)
}

func resolveWebSearchProviderConfigFromSource(source semanticview.Source) (webSearchProviderConfig, error) {
	return resolveWebSearchProviderConfigFromSourceForFlow(source, "")
}

func resolveWebSearchProviderConfigFromSourceForFlow(source semanticview.Source, flowID string) (webSearchProviderConfig, error) {
	if source == nil {
		return webSearchProviderConfig{}, fmt.Errorf("web_search provider is unavailable without a workflow source")
	}
	resolution, ok := semanticview.PolicyValueForFlowWithOwner(source, strings.TrimSpace(flowID), "web_search_provider")
	if !ok {
		return webSearchProviderConfig{}, fmt.Errorf("policy.web_search_provider is not configured")
	}
	value := resolution.Value
	root, ok := normalizeAnyMap(value.Value)
	if !ok {
		return webSearchProviderConfig{}, fmt.Errorf("policy.web_search_provider must be a mapping")
	}
	cfg := webSearchProviderConfig{
		Provider:          strings.ToLower(strings.TrimSpace(asString(root["provider"]))),
		CredentialsKey:    strings.TrimSpace(asString(root["credentials_key"])),
		MaxResultsDefault: asInt(root["max_results_default"]),
		ResponsePath:      strings.TrimSpace(asString(root["response_path"])),
		FieldMapping:      map[string]string{},
		PolicyOwnerKey:    strings.TrimSpace(resolution.OwnerKey),
	}
	rateLimit, _, err := parseExternalDispatchRateLimit(asString(root["rate_limit"]), asString(root["rate_limit_max_wait"]))
	if err != nil {
		return webSearchProviderConfig{}, fmt.Errorf("policy.web_search_provider: %w", err)
	}
	cfg.RateLimit = rateLimit
	if cfg.MaxResultsDefault <= 0 {
		cfg.MaxResultsDefault = 10
	}
	if httpRaw, ok := normalizeAnyMap(root["http"]); ok {
		spec := &runtimecontracts.HTTPToolSpec{}
		raw, err := json.Marshal(httpRaw)
		if err == nil && json.Unmarshal(raw, spec) == nil {
			cfg.HTTP = spec
		}
	}
	if mappingRaw, ok := normalizeAnyMap(root["field_mapping"]); ok {
		for _, key := range []string{"title", "url", "snippet"} {
			cfg.FieldMapping[key] = strings.TrimSpace(asString(mappingRaw[key]))
		}
	}
	switch cfg.Provider {
	case "brave", "serper", "tavily":
		return cfg, nil
	case "custom":
		if cfg.HTTP == nil {
			return webSearchProviderConfig{}, fmt.Errorf("custom web_search provider requires http configuration")
		}
		if cfg.ResponsePath == "" {
			return webSearchProviderConfig{}, fmt.Errorf("custom web_search provider requires response_path")
		}
		for _, key := range []string{"title", "url", "snippet"} {
			if cfg.FieldMapping[key] == "" {
				return webSearchProviderConfig{}, fmt.Errorf("custom web_search provider requires field_mapping.%s", key)
			}
		}
		return cfg, nil
	default:
		return webSearchProviderConfig{}, fmt.Errorf("unsupported web_search provider %q", cfg.Provider)
	}
}

func normalizeAnyMap(value any) (map[string]any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		return typed, true
	case map[any]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[strings.TrimSpace(asString(key))] = item
		}
		return out, true
	default:
		return nil, false
	}
}

func nestedValue(root any, path string) (any, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return root, nil
	}
	current := root
	for _, part := range splitTemplatePath(path) {
		switch typed := current.(type) {
		case map[string]any:
			next, ok := typed[part]
			if !ok {
				return nil, fmt.Errorf("response path %q is not available", path)
			}
			current = next
		case []any:
			return nil, fmt.Errorf("response path %q cannot descend into an array without an index", path)
		default:
			return nil, fmt.Errorf("response path %q is not available", path)
		}
	}
	return current, nil
}

func ioReadAll(resp *http.Response) ([]byte, error) {
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return raw, nil
}
