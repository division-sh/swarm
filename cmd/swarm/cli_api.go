package main

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
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"

	"gopkg.in/yaml.v3"
)

const (
	defaultCLIAPIServer = "http://127.0.0.1:8081"
	cliAPIRPCPath       = "/v1/rpc"
	cliAPIWSPath        = "/v1/ws"

	cliServeAPIListenAddrEnv = "SWARM_API_LISTEN_ADDR"
	cliServeMCPListenAddrEnv = "SWARM_MCP_LISTEN_ADDR"
)

type rootCommandOptions struct {
	apiServer string
	// apiRPCEndpointOverride is for test injection and run-owned connection URLs after
	// those paths have already resolved their own API endpoint semantics.
	apiRPCEndpointOverride string
	apiTokenFile           string
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

func processStdinIsTerminal() bool {
	info, err := os.Stdin.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}

type cliAPIClient struct {
	endpoint   string
	token      string
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
	return &cliAPIClient{endpoint: settings.rpcEndpoint, token: settings.token, httpClient: client}, nil
}

type cliAPISettings struct {
	rpcEndpoint   string
	token         string
	tokenSource   string
	tokenExplicit bool
}

type cliAPIConfigFile struct {
	APIServer          string `yaml:"api_server"`
	APITokenFile       string `yaml:"api_token_file"`
	ContractsPath      string `yaml:"contracts_path"`
	PlatformSpecPath   string `yaml:"platform_spec_path"`
	ServeAPIListenAddr string `yaml:"serve_api_listen_addr"`
	ServeMCPListenAddr string `yaml:"serve_mcp_listen_addr"`
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
	cfg, err := loadCLIAPIConfigFile()
	if err != nil {
		return cliAPISettings{}, err
	}
	endpoint, err := resolveCLIAPIRPCEndpoint(opts, cfg)
	if err != nil {
		return cliAPISettings{}, err
	}
	token, err := resolveCLIAPIToken(opts, cfg, endpoint)
	if err != nil {
		return cliAPISettings{}, err
	}
	return cliAPISettings{rpcEndpoint: endpoint, token: token.token, tokenSource: token.source, tokenExplicit: token.explicit}, nil
}

func resolveCLIAPIRPCEndpoint(opts rootCommandOptions, cfg cliAPIConfigFile) (string, error) {
	if endpoint := strings.TrimSpace(opts.apiRPCEndpointOverride); endpoint != "" {
		return normalizeCLIAPIRPCEndpoint(endpoint, "internal API endpoint")
	}
	server := firstNonEmpty(
		opts.apiServer,
		os.Getenv("SWARM_API_SERVER"),
		cfg.APIServer,
		defaultCLIAPIServer,
	)
	return cliAPIRPCEndpointFromServer(server, "API server")
}

type cliAPITokenResolution struct {
	token    string
	source   string
	explicit bool
}

func resolveCLIAPIToken(opts rootCommandOptions, cfg cliAPIConfigFile, rpcEndpoint string) (cliAPITokenResolution, error) {
	if tokenFile := strings.TrimSpace(opts.apiTokenFile); tokenFile != "" {
		return readCLIAPIExplicitTokenFile(tokenFile, "--api-token-file")
	}
	if token := strings.TrimSpace(os.Getenv("SWARM_API_TOKEN")); token != "" {
		return cliAPITokenResolution{token: token, source: string(apiv1.AuthTokenSourceEnvironment), explicit: true}, nil
	}
	if tokenFile := strings.TrimSpace(os.Getenv("SWARM_API_TOKEN_FILE")); tokenFile != "" {
		return readCLIAPIExplicitTokenFile(tokenFile, "SWARM_API_TOKEN_FILE")
	}
	if tokenFile := strings.TrimSpace(cfg.APITokenFile); tokenFile != "" {
		return readCLIAPIExplicitTokenFile(tokenFile, "config api_token_file")
	}
	if cliAPIRPCEndpointAllowsDefaultToken(rpcEndpoint) {
		return cliAPITokenResolution{
			token:  apiv1.DefaultLoopbackAPIToken,
			source: string(apiv1.AuthTokenSourceBuiltInLoopbackToken),
		}, nil
	}
	return cliAPITokenResolution{}, errCLIAPITokenRequired
}

type cliServeListenerAddressOptions struct {
	APIListenAddr        string
	MCPListenAddr        string
	APIListenAddrFlagSet bool
	MCPListenAddrFlagSet bool
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
	cfg, err := loadCLIAPIConfigFile()
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
	configPath, explicit, err := cliAPIConfigPath()
	if err != nil {
		return cliAPIConfigFile{}, err
	}
	raw, err := os.ReadFile(configPath)
	if err != nil {
		if !explicit && errors.Is(err, os.ErrNotExist) {
			return cliAPIConfigFile{}, nil
		}
		source := "CLI config"
		if explicit {
			source = "SWARM_CONFIG"
		}
		return cliAPIConfigFile{}, &cliAPIValidationError{message: fmt.Sprintf("read %s: %v", source, err)}
	}
	return parseCLIAPIConfigFile(configPath, raw)
}

func cliAPIConfigPath() (string, bool, error) {
	if raw, ok := os.LookupEnv("SWARM_CONFIG"); ok && strings.TrimSpace(raw) != "" {
		return strings.TrimSpace(raw), true, nil
	}
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", false, &cliAPIValidationError{message: fmt.Sprintf("resolve XDG CLI config path: %v", err)}
	}
	return filepath.Join(dir, "swarm", "config.yaml"), false, nil
}

func parseCLIAPIConfigFile(configPath string, raw []byte) (cliAPIConfigFile, error) {
	var decoded map[string]any
	if err := yaml.Unmarshal(raw, &decoded); err != nil {
		return cliAPIConfigFile{}, &cliAPIValidationError{message: fmt.Sprintf("parse CLI config %s: %v", configPath, err)}
	}
	if decoded == nil {
		return cliAPIConfigFile{}, nil
	}
	for key, value := range decoded {
		switch key {
		case "api_server", "api_token_file", "contracts_path", "platform_spec_path", "serve_api_listen_addr", "serve_mcp_listen_addr":
			if value == nil {
				continue
			}
			if _, ok := value.(string); !ok {
				return cliAPIConfigFile{}, &cliAPIValidationError{message: fmt.Sprintf("CLI config %s must be a string", key)}
			}
		default:
			return cliAPIConfigFile{}, &cliAPIValidationError{message: fmt.Sprintf("unsupported CLI config key %q", key)}
		}
	}
	var cfg cliAPIConfigFile
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return cliAPIConfigFile{}, &cliAPIValidationError{message: fmt.Sprintf("decode CLI config %s: %v", configPath, err)}
	}
	return cfg, nil
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
		return fmt.Sprintf("%s: %s", code, e.Message)
	}
	return e.Message
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

type cliAPIHTTPError struct {
	surface    string
	statusCode int
	message    string
}

func (e *cliAPIHTTPError) Error() string {
	if e == nil {
		return ""
	}
	surface := strings.TrimSpace(e.surface)
	if surface == "" {
		surface = "v1 RPC"
	}
	return fmt.Sprintf("%s HTTP %d: %s", surface, e.statusCode, e.message)
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
		return fmt.Errorf("v1 RPC request failed: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("read v1 RPC response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		message := strings.TrimSpace(string(raw))
		if message == "" {
			message = http.StatusText(resp.StatusCode)
		}
		return &cliAPIHTTPError{statusCode: resp.StatusCode, message: message}
	}

	var envelope jsonRPCResponse
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return fmt.Errorf("decode JSON-RPC response: %w", err)
	}
	if envelope.JSONRPC != "2.0" {
		return fmt.Errorf("malformed JSON-RPC response: jsonrpc=%q", envelope.JSONRPC)
	}
	responseID, ok := envelope.ID.(string)
	if !ok || responseID != requestID {
		return fmt.Errorf("malformed JSON-RPC response: id=%s, want %q", formatJSONRPCID(envelope.ID), requestID)
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
