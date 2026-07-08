package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
)

const (
	defaultCLIAPIServer = "http://127.0.0.1:8081"
	cliAPIRPCPath       = "/v1/rpc"
	cliAPIWSPath        = "/v1/ws"

	cliServeAPIListenAddrEnv = "SWARM_API_LISTEN_ADDR"
	cliServeMCPListenAddrEnv = "SWARM_MCP_LISTEN_ADDR"

	serveAPITokenFileFlagSource   = "--api-token-file"
	serveAPITokenFileConfigSource = "config serve.api_token_file"
)

type rootCommandOptions struct {
	apiServer string
	// apiRPCEndpointOverride is for test injection and run-owned connection URLs after
	// those paths have already resolved their own API endpoint semantics.
	apiRPCEndpointOverride string
	apiTokenFile           string
	contextName            string
	swarmDir               string
	rootFlags              *rootCommandFlagState
	repoRoot               string
	apiCommandClass        cliAPICommandClass
	apiCommandName         string
	disableLocalTargeting  bool
	httpClient             *http.Client
	input                  io.Reader
	stdinIsTerminal        func() bool
	runServe               func(context.Context, string, serveOptions) int
	runReadyTimeout        time.Duration
	runReadyPoll           time.Duration
	runStatusPoll          time.Duration
}

func defaultRootCommandOptions() rootCommandOptions {
	return rootCommandOptions{
		httpClient:      &http.Client{Timeout: 30 * time.Second},
		input:           os.Stdin,
		stdinIsTerminal: processStdinIsTerminal,
		runServe:        runServeRuntime,
		runReadyTimeout: 30 * time.Second,
		runReadyPoll:    250 * time.Millisecond,
		runStatusPoll:   time.Second,
	}
}

type rootCommandFlagState struct {
	swarmDir      string
	swarmDirSet   bool
	configPath    string
	configPathSet bool
}

func (opts rootCommandOptions) ensureRootFlagState() rootCommandOptions {
	if opts.rootFlags == nil {
		opts.rootFlags = &rootCommandFlagState{swarmDir: opts.swarmDir}
	}
	return opts
}

func (opts rootCommandOptions) swarmDirResolutionOptions() cliSwarmDirOptions {
	if opts.rootFlags != nil {
		return cliSwarmDirOptions{SwarmDir: opts.rootFlags.swarmDir, SwarmDirFlagSet: opts.rootFlags.swarmDirSet}
	}
	return cliSwarmDirOptions{SwarmDir: opts.swarmDir, SwarmDirFlagSet: strings.TrimSpace(opts.swarmDir) != ""}
}

func (opts rootCommandOptions) unifiedConfigLoadOptions() unifiedConfigLoadOptions {
	if opts.rootFlags != nil && opts.rootFlags.configPathSet {
		return unifiedConfigLoadOptions{RepoRoot: opts.repoRoot, ExplicitPath: opts.rootFlags.configPath}
	}
	return unifiedConfigLoadOptions{RepoRoot: opts.repoRoot}
}

func processStdinIsTerminal() bool {
	info, err := os.Stdin.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}

type cliAPIClient struct {
	endpoint   string
	token      string
	target     cliAPITargetResolution
	httpClient *http.Client
}

func newCLIAPIClient(opts rootCommandOptions) (*cliAPIClient, error) {
	settings, err := resolveCLIAPISettings(opts)
	if err != nil {
		return nil, err
	}
	client := opts.httpClient
	if client == nil {
		client = http.DefaultClient
	}
	return &cliAPIClient{endpoint: settings.rpcEndpoint, token: settings.token, target: settings.target, httpClient: client}, nil
}

type cliAPISettings struct {
	rpcEndpoint   string
	token         string
	tokenSource   string
	tokenExplicit bool
	target        cliAPITargetResolution
}

type cliAPIConfigFile struct {
	APIServer          string `yaml:"api_server"`
	APITokenFile       string `yaml:"api_token_file"`
	SwarmDir           string `yaml:"swarm_dir"`
	SwarmDirSet        bool   `yaml:"-"`
	ContractsPath      string `yaml:"contracts_path"`
	PlatformSpecPath   string `yaml:"platform_spec_path"`
	ServeAPIListenAddr string `yaml:"serve_api_listen_addr"`
	ServeMCPListenAddr string `yaml:"serve_mcp_listen_addr"`
	ServeAPITokenFile  string `yaml:"serve_api_token_file"`
}

type cliAPIValidationError struct {
	message string
}

func (e *cliAPIValidationError) Error() string {
	if e == nil {
		return ""
	}
	return e.message
}

type cliAPIAuthConfigError struct {
	message string
}

func (e *cliAPIAuthConfigError) Error() string {
	if e == nil {
		return ""
	}
	return e.message
}

func resolveCLIAPISettings(opts rootCommandOptions) (cliAPISettings, error) {
	opts = opts.ensureRootFlagState()
	cfg, err := loadCLIAPIConfigFileWithOptions(opts.unifiedConfigLoadOptions())
	if err != nil {
		return cliAPISettings{}, err
	}
	if err := rejectRemovedClientAPIEnvSources(); err != nil {
		return cliAPISettings{}, err
	}
	target, err := resolveCLIAPITarget(opts, cfg)
	if err != nil {
		return cliAPISettings{}, err
	}
	token, err := resolveCLIAPITokenForTarget(opts, cfg, target)
	if err != nil {
		return cliAPISettings{}, err
	}
	return cliAPISettings{rpcEndpoint: target.rpcEndpoint, token: token.token, tokenSource: token.source, tokenExplicit: token.explicit, target: target}, nil
}

type cliAPITokenResolution struct {
	token    string
	source   string
	explicit bool
}

func resolveCLIAPIToken(opts rootCommandOptions, cfg cliAPIConfigFile, rpcEndpoint string) (cliAPITokenResolution, error) {
	if err := rejectRemovedClientAPIEnvSources(); err != nil {
		return cliAPITokenResolution{}, err
	}
	if tokenFile := strings.TrimSpace(opts.apiTokenFile); tokenFile != "" {
		return readCLIAPIExplicitTokenFile(tokenFile, "--api-token-file")
	}
	if tokenFile := strings.TrimSpace(cfg.APITokenFile); tokenFile != "" {
		return readCLIAPIExplicitTokenFile(tokenFile, "config connection.api_token_file")
	}
	if cliAPIRPCEndpointAllowsDefaultToken(rpcEndpoint) {
		return cliAPITokenResolution{
			token:  apiv1.DefaultLoopbackAPIToken,
			source: string(apiv1.AuthTokenSourceBuiltInLoopbackToken),
		}, nil
	}
	return cliAPITokenResolution{}, errCLIAPITokenRequired
}

func rejectRemovedClientAPIEnvSources() error {
	replacements := map[string]string{
		"SWARM_API_SERVER":     "use --api-server, --context, project/selected context, or config api_server",
		"SWARM_API_TOKEN":      "use --api-token-file, context descriptor auth, or config connection.api_token_file",
		"SWARM_API_TOKEN_FILE": "use --api-token-file, context descriptor auth, or config connection.api_token_file",
	}
	var found []string
	for _, name := range []string{"SWARM_API_SERVER", "SWARM_API_TOKEN", "SWARM_API_TOKEN_FILE"} {
		if strings.TrimSpace(os.Getenv(name)) != "" {
			found = append(found, fmt.Sprintf("%s (%s)", name, replacements[name]))
		}
	}
	if len(found) == 0 {
		return nil
	}
	return &cliAPIValidationError{message: "client-side API environment sources are no longer accepted: " + strings.Join(found, "; ")}
}

type cliServeListenerAddressOptions struct {
	APIListenAddr        string
	MCPListenAddr        string
	APIListenAddrFlagSet bool
	MCPListenAddrFlagSet bool
	ConfigPath           string
	RepoRoot             string
}

func resolveCLIServeListenerAddresses(opts cliServeListenerAddressOptions) (string, string, error) {
	apiAddr, apiResolved := resolveCLIServeListenerAddressHighPriority(
		opts.APIListenAddr,
		opts.APIListenAddrFlagSet,
		cliServeAPIListenAddrEnv,
	)
	mcpAddr, mcpResolved := resolveCLIServeListenerAddressHighPriority(
		opts.MCPListenAddr,
		opts.MCPListenAddrFlagSet,
		cliServeMCPListenAddrEnv,
	)
	if apiResolved && mcpResolved {
		return apiAddr, mcpAddr, nil
	}
	cfg, err := loadCLIAPIConfigFileWithOptions(unifiedConfigLoadOptions{RepoRoot: opts.RepoRoot, ExplicitPath: opts.ConfigPath})
	if err != nil {
		return "", "", err
	}
	if !apiResolved {
		apiAddr = defaultAPIListenAddr
		if config := strings.TrimSpace(cfg.ServeAPIListenAddr); config != "" {
			apiAddr = config
		}
	}
	if !mcpResolved {
		mcpAddr = defaultMCPListenAddr
		if config := strings.TrimSpace(cfg.ServeMCPListenAddr); config != "" {
			mcpAddr = config
		}
	}
	return apiAddr, mcpAddr, nil
}

func resolveCLIServeListenerAddressHighPriority(flagValue string, flagSet bool, envName string) (string, bool) {
	if flagSet {
		return flagValue, true
	}
	if env := strings.TrimSpace(os.Getenv(envName)); env != "" {
		return env, true
	}
	return "", false
}

func resolveServeAPIAuth(repo string, opts serveOptions) (apiv1.AuthTokenResolution, error) {
	if err := rejectRemovedServeAPIEnvSource(); err != nil {
		return apiv1.AuthTokenResolution{}, err
	}
	if opts.APITokenFileFlagSet || strings.TrimSpace(opts.APITokenFile) != "" {
		tokenFile := strings.TrimSpace(opts.APITokenFile)
		if tokenFile == "" {
			return apiv1.AuthTokenResolution{}, &cliAPIAuthConfigError{message: serveAPITokenFileFlagSource + " is blank"}
		}
		return readServeAPITokenFile(tokenFile, serveAPITokenFileFlagSource)
	}
	cfg, err := loadCLIAPIConfigFileWithOptions(unifiedConfigLoadOptions{RepoRoot: repo, ExplicitPath: opts.ConfigPath})
	if err != nil {
		return apiv1.AuthTokenResolution{}, err
	}
	if tokenFile := strings.TrimSpace(cfg.ServeAPITokenFile); tokenFile != "" {
		return readServeAPITokenFile(tokenFile, serveAPITokenFileConfigSource)
	}
	return defaultServeAPIAuthResolution(), nil
}

func rejectRemovedServeAPIEnvSource() error {
	if strings.TrimSpace(os.Getenv("SWARM_API_TOKEN")) == "" {
		return nil
	}
	return &cliAPIValidationError{message: "server-side API environment source is no longer accepted: SWARM_API_TOKEN (use swarm serve --api-token-file or config serve.api_token_file)"}
}

func readServeAPITokenFile(tokenFile, source string) (apiv1.AuthTokenResolution, error) {
	token, err := readCLIAPITokenFile(tokenFile, source)
	if err != nil {
		return apiv1.AuthTokenResolution{}, err
	}
	absoluteTokenFile, err := filepath.Abs(strings.TrimSpace(tokenFile))
	if err != nil {
		return apiv1.AuthTokenResolution{}, &cliAPIAuthConfigError{message: fmt.Sprintf("resolve %s path: %v", source, err)}
	}
	return apiv1.AuthTokenResolution{
		Tokens:    []string{token},
		Source:    apiv1.AuthTokenSource(source),
		Explicit:  true,
		TokenFile: absoluteTokenFile,
	}, nil
}

func defaultServeAPIAuthResolution() apiv1.AuthTokenResolution {
	return apiv1.AuthTokenResolution{
		Tokens: []string{apiv1.DefaultLoopbackAPIToken},
		Source: apiv1.AuthTokenSourceBuiltInLoopbackToken,
	}
}

func readCLIAPIExplicitTokenFile(tokenFile, source string) (cliAPITokenResolution, error) {
	token, err := readCLIAPITokenFile(tokenFile, source)
	if err != nil {
		return cliAPITokenResolution{}, err
	}
	return cliAPITokenResolution{token: token, source: source, explicit: true}, nil
}

func cliAPIRPCEndpointAllowsDefaultToken(endpoint string) bool {
	parsed, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil {
		return false
	}
	return apiv1.DefaultLoopbackAPITokenAllowedHost(parsed.Hostname())
}

func readCLIAPITokenFile(tokenFile, source string) (string, error) {
	raw, err := os.ReadFile(tokenFile)
	if err != nil {
		return "", &cliAPIAuthConfigError{message: fmt.Sprintf("read %s: %v", source, err)}
	}
	token := strings.TrimSpace(string(raw))
	if token == "" {
		return "", &cliAPIAuthConfigError{message: fmt.Sprintf("%s is blank", source)}
	}
	return token, nil
}

func loadCLIAPIConfigFile() (cliAPIConfigFile, error) {
	return loadCLIAPIConfigFileWithOptions(unifiedConfigLoadOptions{})
}

func loadCLIAPIConfigFileWithOptions(opts unifiedConfigLoadOptions) (cliAPIConfigFile, error) {
	result, err := loadUnifiedConfig(opts)
	if err != nil {
		return cliAPIConfigFile{}, err
	}
	return result.CLI, nil
}

func normalizeCLIAPIRPCEndpoint(raw, source string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", &cliAPIValidationError{message: fmt.Sprintf("%s must be a valid http(s) URL: %v", source, err)}
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", &cliAPIValidationError{message: fmt.Sprintf("%s must use http or https", source)}
	}
	if strings.TrimSpace(parsed.Host) == "" {
		return "", &cliAPIValidationError{message: fmt.Sprintf("%s must include a host", source)}
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", &cliAPIValidationError{message: fmt.Sprintf("%s must not include query or fragment", source)}
	}
	if strings.TrimRight(parsed.Path, "/") != strings.TrimSuffix(cliAPIRPCPath, "/") {
		return "", &cliAPIValidationError{message: fmt.Sprintf("%s path must be %s", source, cliAPIRPCPath)}
	}
	return parsed.String(), nil
}

func cliAPIRPCEndpointFromServer(raw, source string) (string, error) {
	parsed, err := normalizeCLIAPIServerBase(raw, source)
	if err != nil {
		return "", err
	}
	parsed.Path = joinURLPath(parsed.Path, cliAPIRPCPath)
	return parsed.String(), nil
}

func cliAPIWebSocketEndpointFromRPC(rpcEndpoint string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(rpcEndpoint))
	if err != nil {
		return "", fmt.Errorf("derive /v1/ws endpoint: %w", err)
	}
	switch parsed.Scheme {
	case "http":
		parsed.Scheme = "ws"
	case "https":
		parsed.Scheme = "wss"
	default:
		return "", fmt.Errorf("derive /v1/ws endpoint: unsupported API scheme %q", parsed.Scheme)
	}
	apiPath := strings.TrimRight(parsed.Path, "/")
	if !strings.HasSuffix(apiPath, cliAPIRPCPath) {
		return "", fmt.Errorf("derive /v1/ws endpoint: API endpoint must end in /v1/rpc")
	}
	parsed.Path = strings.TrimSuffix(apiPath, cliAPIRPCPath) + cliAPIWSPath
	return parsed.String(), nil
}

func normalizeCLIAPIServerBase(raw, source string) (*url.URL, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil, &cliAPIValidationError{message: fmt.Sprintf("%s must be a valid http(s) URL: %v", source, err)}
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, &cliAPIValidationError{message: fmt.Sprintf("%s must use http or https", source)}
	}
	if strings.TrimSpace(parsed.Host) == "" {
		return nil, &cliAPIValidationError{message: fmt.Sprintf("%s must include a host", source)}
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, &cliAPIValidationError{message: fmt.Sprintf("%s must not include query or fragment", source)}
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	if endpointPath := cliAPIEndpointShapedPath(parsed.Path); endpointPath != "" {
		return nil, &cliAPIValidationError{message: fmt.Sprintf("%s must be an API server base URL, not a direct %s endpoint", source, endpointPath)}
	}
	return parsed, nil
}

func cliAPIEndpointShapedPath(rawPath string) string {
	trimmed := strings.TrimRight(rawPath, "/")
	if trimmed == "" || trimmed == "/" {
		return ""
	}
	switch {
	case trimmed == cliAPIRPCPath || strings.HasSuffix(trimmed, cliAPIRPCPath):
		return cliAPIRPCPath
	case trimmed == cliAPIWSPath || strings.HasSuffix(trimmed, cliAPIWSPath):
		return cliAPIWSPath
	default:
		return ""
	}
}

func joinURLPath(basePath, suffix string) string {
	basePath = strings.TrimSpace(basePath)
	if basePath == "" || basePath == "/" {
		return suffix
	}
	return path.Join(basePath, suffix)
}

type jsonRPCRequest struct {
	JSONRPC string         `json:"jsonrpc"`
	ID      string         `json:"id"`
	Method  string         `json:"method"`
	Params  map[string]any `json:"params"`
}

type jsonRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *jsonRPCError   `json:"error,omitempty"`
}

type jsonRPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *jsonRPCError) Error() string {
	if e == nil {
		return ""
	}
	if code := applicationErrorCode(e.Data); code != "" {
		message := fmt.Sprintf("%s: %s", code, e.Message)
		if details := applicationErrorDetails(e.Data); details != "" {
			message = fmt.Sprintf("%s (details: %s)", message, details)
		}
		return message
	}
	message := strings.TrimSpace(e.Message)
	if message == "" {
		message = "JSON-RPC error"
	}
	if e.Code != 0 {
		message = fmt.Sprintf("JSON-RPC %d: %s", e.Code, message)
	}
	if details := standardJSONRPCErrorDetails(e.Data); details != "" {
		message = fmt.Sprintf("%s (%s)", message, details)
	}
	return message
}

func applicationErrorCode(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var data struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(raw, &data); err != nil {
		return ""
	}
	return strings.TrimSpace(data.Code)
}

func applicationErrorDetails(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var data struct {
		Details map[string]any `json:"details"`
	}
	if err := json.Unmarshal(raw, &data); err != nil || len(data.Details) == 0 {
		return ""
	}
	return formatCLIErrorDetails(data.Details)
}

func standardJSONRPCErrorDetails(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var data struct {
		CorrelationID string `json:"correlation_id"`
		Details       any    `json:"details"`
	}
	if err := json.Unmarshal(raw, &data); err != nil {
		return ""
	}
	parts := make([]string, 0, 2)
	if correlationID := strings.TrimSpace(data.CorrelationID); correlationID != "" {
		parts = append(parts, fmt.Sprintf("correlation_id=%s", correlationID))
	}
	if details := standardJSONRPCDetailValue(data.Details); details != "" {
		parts = append(parts, "details: "+details)
	}
	return strings.Join(parts, ", ")
}

func standardJSONRPCDetailValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case map[string]any:
		return formatCLIErrorDetails(typed)
	default:
		return applicationErrorDetailValue(typed)
	}
}

func formatCLIErrorDetails(details map[string]any) string {
	keys := make([]string, 0, len(details))
	for key := range details {
		if strings.TrimSpace(key) != "" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		if value := applicationErrorDetailValue(details[key]); value != "" {
			parts = append(parts, fmt.Sprintf("%s=%s", key, value))
		}
	}
	return strings.Join(parts, ", ")
}

func applicationErrorDetailValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(typed)
	case float64, bool:
		return fmt.Sprint(typed)
	default:
		raw, err := json.Marshal(typed)
		if err != nil || string(raw) == "null" {
			return ""
		}
		return string(raw)
	}
}

type cliAPIHTTPError struct {
	surface    string
	endpoint   string
	statusCode int
	message    string
}

func (e *cliAPIHTTPError) Error() string {
	if e == nil {
		return ""
	}
	surface := strings.TrimSpace(e.surface)
	if surface == "" {
		surface = "runtime API"
	}
	return fmt.Sprintf("%s returned status %d", surface, e.statusCode)
}

type cliAPITransportError struct {
	surface   string
	endpoint  string
	operation string
	err       error
}

func (e *cliAPITransportError) Error() string {
	if e == nil {
		return ""
	}
	surface := strings.TrimSpace(e.surface)
	if surface == "" {
		surface = "runtime API"
	}
	operation := strings.TrimSpace(e.operation)
	if operation == "" {
		operation = "request"
	}
	return fmt.Sprintf("%s %s failed", surface, operation)
}

func (e *cliAPITransportError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

type cliAPIProtocolError struct {
	surface   string
	endpoint  string
	operation string
	err       error
}

func (e *cliAPIProtocolError) Error() string {
	if e == nil {
		return ""
	}
	surface := strings.TrimSpace(e.surface)
	if surface == "" {
		surface = "runtime API"
	}
	operation := strings.TrimSpace(e.operation)
	if operation == "" {
		operation = "response"
	}
	return fmt.Sprintf("%s invalid %s", surface, operation)
}

func (e *cliAPIProtocolError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func (c *cliAPIClient) call(ctx context.Context, method string, params map[string]any, result any) error {
	requestID := "swarm-cli:" + method
	body, err := json.Marshal(jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      requestID,
		Method:  method,
		Params:  params,
	})
	if err != nil {
		return fmt.Errorf("encode JSON-RPC request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build v1 RPC request: %w", err)
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return &cliAPITransportError{surface: "runtime API", endpoint: c.endpoint, operation: "request", err: err}
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return &cliAPITransportError{surface: "runtime API", endpoint: c.endpoint, operation: "response read", err: err}
	}
	if resp.StatusCode != http.StatusOK {
		message := strings.TrimSpace(string(raw))
		if message == "" {
			message = http.StatusText(resp.StatusCode)
		}
		return &cliAPIHTTPError{surface: "runtime API", endpoint: c.endpoint, statusCode: resp.StatusCode, message: message}
	}

	var envelope jsonRPCResponse
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return &cliAPIProtocolError{surface: "runtime API", endpoint: c.endpoint, operation: "response", err: err}
	}
	if envelope.JSONRPC != "2.0" {
		return &cliAPIProtocolError{surface: "runtime API", endpoint: c.endpoint, operation: "response", err: fmt.Errorf("jsonrpc=%q", envelope.JSONRPC)}
	}
	responseID, ok := envelope.ID.(string)
	if !ok || responseID != requestID {
		return &cliAPIProtocolError{surface: "runtime API", endpoint: c.endpoint, operation: "response", err: fmt.Errorf("id=%s, want %q", formatJSONRPCID(envelope.ID), requestID)}
	}
	if envelope.Error != nil {
		return envelope.Error
	}
	if len(envelope.Result) == 0 || string(envelope.Result) == "null" {
		return fmt.Errorf("JSON-RPC response missing result")
	}
	if result == nil {
		return nil
	}
	if err := json.Unmarshal(envelope.Result, result); err != nil {
		return fmt.Errorf("decode JSON-RPC result: %w", err)
	}
	return nil
}

func formatJSONRPCID(id any) string {
	if id == nil {
		return "<missing>"
	}
	if value, ok := id.(string); ok {
		return fmt.Sprintf("%q", value)
	}
	return fmt.Sprintf("%v", id)
}
