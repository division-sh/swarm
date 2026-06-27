package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type ServerConfig struct {
	Name             string
	Description      string
	Transport        string
	URL              string
	Command          string
	Args             []string
	Prefix           string
	CredentialsKey   string
	RateLimit        string
	RateLimitMaxWait string
}

type DiscoveredTool struct {
	Name        string
	RemoteName  string
	ServerName  string
	Prefix      string
	Description string
	InputSchema any
}

type Client struct {
	store      runtimecredentials.Store
	httpClient *http.Client

	mu      sync.RWMutex
	servers map[string]*registeredServer
	tools   map[string]DiscoveredTool
}

type CredentialKeyResolver func(string) (string, error)

type registeredServer struct {
	cfg   ServerConfig
	stdio *stdioRPCClient
}

func NewClient(store runtimecredentials.Store) *Client {
	return &Client{
		store: store,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		servers: map[string]*registeredServer{},
		tools:   map[string]DiscoveredTool{},
	}
}

func (c *Client) Refresh(ctx context.Context, source semanticview.Source) []error {
	configs, err := parseServerConfigs(source)
	if err != nil {
		return []error{err}
	}
	nextServers := make(map[string]*registeredServer, len(configs))
	nextTools := map[string]DiscoveredTool{}
	errs := make([]error, 0)

	for _, cfg := range configs {
		server := &registeredServer{cfg: cfg}
		if strings.EqualFold(strings.TrimSpace(cfg.Transport), "stdio") {
			stdio, stdioErr := newStdioRPCClient(cfg.Command, cfg.Args)
			if stdioErr != nil {
				errs = append(errs, fmt.Errorf("mcp server %s: %w", cfg.Name, stdioErr))
				continue
			}
			server.stdio = stdio
		}
		tools, toolErr := c.discoverServerTools(ctx, source, server)
		if toolErr != nil {
			if server.stdio != nil {
				_ = server.stdio.Close()
			}
			errs = append(errs, fmt.Errorf("mcp server %s: %w", cfg.Name, toolErr))
			continue
		}
		nextServers[cfg.Name] = server
		for _, tool := range tools {
			nextTools[tool.Name] = tool
		}
		slog.Info("mcp client discovered server tools",
			"server", cfg.Name,
			"prefix", cfg.Prefix,
			"count", len(tools),
			"tools", discoveredToolNames(tools),
		)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	for name, server := range c.servers {
		if _, keep := nextServers[name]; keep {
			continue
		}
		if server.stdio != nil {
			_ = server.stdio.Close()
		}
	}
	c.servers = nextServers
	c.tools = nextTools
	return errs
}

func (c *Client) DiscoveredTools() map[string]DiscoveredTool {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]DiscoveredTool, len(c.tools))
	for key, value := range c.tools {
		out[key] = value
	}
	return out
}

func (c *Client) ServerConfig(name string) (ServerConfig, bool) {
	if c == nil {
		return ServerConfig{}, false
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return ServerConfig{}, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	server := c.servers[name]
	if server == nil {
		return ServerConfig{}, false
	}
	return server.cfg, true
}

func (c *Client) Call(ctx context.Context, prefixedName string, arguments any) (any, error) {
	return c.CallWithCredentialKeyResolver(ctx, prefixedName, arguments, nil)
}

func (c *Client) CallWithCredentialKeyResolver(ctx context.Context, prefixedName string, arguments any, resolver CredentialKeyResolver) (any, error) {
	if c == nil {
		return nil, fmt.Errorf("mcp client is not configured")
	}
	prefixedName = strings.TrimSpace(prefixedName)
	if prefixedName == "" {
		return nil, fmt.Errorf("mcp tool name is required")
	}
	c.mu.RLock()
	tool, ok := c.tools[prefixedName]
	server := c.servers[tool.ServerName]
	c.mu.RUnlock()
	if !ok || server == nil {
		return nil, fmt.Errorf("mcp tool %s is unavailable", prefixedName)
	}
	req := RPCRequest{
		JSONRPC: "2.0",
		Method:  "tools/call",
		Params: map[string]any{
			"name":      tool.RemoteName,
			"arguments": arguments,
		},
		ID: prefixedName + "-call",
	}
	resp, err := c.callServerWithCredentialKeyResolver(ctx, server, req, resolver)
	if err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("mcp tool %s failed: %s", prefixedName, strings.TrimSpace(resp.Error.Message))
	}
	if result, ok := resp.Result.(map[string]any); ok {
		if content, ok := result["content"].([]any); ok && len(content) == 1 {
			if item, ok := content[0].(map[string]any); ok {
				if text := strings.TrimSpace(anyString(item["text"])); text != "" && len(result) <= 2 {
					return text, nil
				}
			}
		}
		if inner, ok := result["structuredContent"]; ok {
			return inner, nil
		}
	}
	return resp.Result, nil
}

func (c *Client) discoverServerTools(ctx context.Context, source semanticview.Source, server *registeredServer) ([]DiscoveredTool, error) {
	if _, err := c.callServer(ctx, server, RPCRequest{
		JSONRPC: "2.0",
		Method:  "initialize",
		Params: map[string]any{
			"protocolVersion": "2025-03-26",
			"capabilities":    map[string]any{},
			"clientInfo": map[string]any{
				"name":    "swarm",
				"version": "1.0.0",
			},
		},
		ID: server.cfg.Name + "-initialize",
	}); err != nil {
		return nil, err
	}
	_, _ = c.callServer(ctx, server, RPCRequest{
		JSONRPC: "2.0",
		Method:  "notifications/initialized",
		Params:  map[string]any{},
	})
	resp, err := c.callServer(ctx, server, RPCRequest{
		JSONRPC: "2.0",
		Method:  "tools/list",
		Params:  map[string]any{},
		ID:      server.cfg.Name + "-tools-list",
	})
	if err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, fmt.Errorf(strings.TrimSpace(resp.Error.Message))
	}
	result, ok := resp.Result.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("tools/list returned invalid result payload")
	}
	rawTools, ok := result["tools"].([]any)
	if !ok {
		return nil, fmt.Errorf("tools/list returned no tools array")
	}
	out := make([]DiscoveredTool, 0, len(rawTools))
	for _, raw := range rawTools {
		item, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		remoteName := strings.TrimSpace(asString(item["name"]))
		if remoteName == "" {
			continue
		}
		out = append(out, DiscoveredTool{
			Name:        strings.TrimSpace(server.cfg.Prefix) + "." + remoteName,
			RemoteName:  remoteName,
			ServerName:  server.cfg.Name,
			Prefix:      server.cfg.Prefix,
			Description: strings.TrimSpace(asString(item["description"])),
			InputSchema: item["inputSchema"],
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (c *Client) callServer(ctx context.Context, server *registeredServer, req RPCRequest) (RPCResponse, error) {
	return c.callServerWithCredentialKeyResolver(ctx, server, req, nil)
}

func (c *Client) callServerWithCredentialKeyResolver(ctx context.Context, server *registeredServer, req RPCRequest, resolver CredentialKeyResolver) (RPCResponse, error) {
	switch strings.ToLower(strings.TrimSpace(server.cfg.Transport)) {
	case "stdio":
		if server.stdio == nil {
			return RPCResponse{}, fmt.Errorf("stdio client is not configured")
		}
		return server.stdio.Call(ctx, req)
	case "http", "":
		return c.callHTTPServerWithCredentialKeyResolver(ctx, server.cfg, req, resolver)
	default:
		return RPCResponse{}, fmt.Errorf("unsupported mcp transport %q", server.cfg.Transport)
	}
}

func (c *Client) callHTTPServer(ctx context.Context, cfg ServerConfig, req RPCRequest) (RPCResponse, error) {
	return c.callHTTPServerWithCredentialKeyResolver(ctx, cfg, req, nil)
}

func (c *Client) callHTTPServerWithCredentialKeyResolver(ctx context.Context, cfg ServerConfig, req RPCRequest, resolver CredentialKeyResolver) (RPCResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return RPCResponse{}, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.URL, bytes.NewReader(body))
	if err != nil {
		return RPCResponse{}, err
	}
	httpReq.Header.Set("content-type", "application/json")
	httpReq.Header.Set("mcp-protocol-version", "2025-03-26")
	credentialKey := strings.TrimSpace(cfg.CredentialsKey)
	if resolver != nil && credentialKey != "" {
		resolved, err := resolver(credentialKey)
		if err != nil {
			return RPCResponse{}, err
		}
		credentialKey = strings.TrimSpace(resolved)
	}
	if token, ok, err := c.credentialValue(ctx, credentialKey); err != nil {
		return RPCResponse{}, err
	} else if ok && strings.TrimSpace(token) != "" {
		httpReq.Header.Set("authorization", "Bearer "+strings.TrimSpace(token))
	}
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return RPCResponse{}, err
	}
	defer resp.Body.Close()
	var decoded RPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return RPCResponse{}, err
	}
	return decoded, nil
}

func (c *Client) credentialValue(ctx context.Context, key string) (string, bool, error) {
	if c == nil || c.store == nil || strings.TrimSpace(key) == "" {
		return "", false, nil
	}
	return c.store.Get(ctx, key)
}

type stdioRPCClient struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Reader
	mu     sync.Mutex
	nextID atomic.Int64
}

func newStdioRPCClient(command string, args []string) (*stdioRPCClient, error) {
	command = strings.TrimSpace(command)
	if command == "" {
		return nil, fmt.Errorf("stdio command is required")
	}
	cmd := exec.Command(command, args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	client := &stdioRPCClient{
		cmd:    cmd,
		stdin:  stdin,
		stdout: bufio.NewReader(stdout),
	}
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			slog.Warn("mcp stdio stderr", "line", line)
		}
	}()
	return client, nil
}

func (c *stdioRPCClient) Close() error {
	if c == nil || c.cmd == nil || c.cmd.Process == nil {
		return nil
	}
	_ = c.stdin.Close()
	return c.cmd.Process.Kill()
}

func (c *stdioRPCClient) Call(ctx context.Context, req RPCRequest) (RPCResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if req.ID == nil && !strings.HasPrefix(strings.TrimSpace(req.Method), "notifications/") {
		req.ID = c.nextID.Add(1)
	}
	raw, err := json.Marshal(req)
	if err != nil {
		return RPCResponse{}, err
	}
	raw = append(raw, '\n')
	if _, err := c.stdin.Write(raw); err != nil {
		return RPCResponse{}, err
	}
	if req.ID == nil {
		return RPCResponse{}, nil
	}
	for {
		select {
		case <-ctx.Done():
			return RPCResponse{}, ctx.Err()
		default:
		}
		line, err := c.stdout.ReadBytes('\n')
		if err != nil {
			return RPCResponse{}, err
		}
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var resp RPCResponse
		if err := json.Unmarshal(line, &resp); err != nil {
			continue
		}
		if fmt.Sprint(resp.ID) != fmt.Sprint(req.ID) {
			continue
		}
		return resp, nil
	}
}

func parseServerConfigs(source semanticview.Source) ([]ServerConfig, error) {
	value, ok := semanticview.PolicyValueForFlow(source, "", "mcp_servers")
	if !ok {
		return nil, nil
	}
	policyVars := rootPolicyTemplateVars(source)
	root, ok := policyMap(value.Value)
	if !ok {
		return nil, fmt.Errorf("mcp_servers must be a mapping")
	}
	names := make([]string, 0, len(root))
	for name := range root {
		name = strings.TrimSpace(name)
		if name != "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	out := make([]ServerConfig, 0, len(names))
	usedPrefixes := map[string]string{}
	for _, name := range names {
		item, ok := policyMap(root[name])
		if !ok {
			return nil, fmt.Errorf("mcp_servers.%s must be a mapping", name)
		}
		cfg := ServerConfig{
			Name:             name,
			Description:      strings.TrimSpace(anyString(item["description"])),
			Transport:        strings.ToLower(strings.TrimSpace(anyString(item["transport"]))),
			URL:              strings.TrimSpace(renderPolicyTemplate(anyString(item["url"]), policyVars)),
			Command:          strings.TrimSpace(renderPolicyTemplate(anyString(item["command"]), policyVars)),
			Prefix:           strings.TrimSpace(anyString(item["prefix"])),
			CredentialsKey:   strings.TrimSpace(anyString(item["credentials_key"])),
			RateLimit:        anyString(item["rate_limit"]),
			RateLimitMaxWait: anyString(item["rate_limit_max_wait"]),
			Args:             renderPolicyStringSlice(stringSlice(item["args"]), policyVars),
		}
		if cfg.Transport == "" {
			cfg.Transport = "http"
		}
		if cfg.Prefix == "" {
			return nil, fmt.Errorf("mcp_servers.%s.prefix is required", name)
		}
		if other, exists := usedPrefixes[cfg.Prefix]; exists {
			return nil, fmt.Errorf("mcp server prefix %q is duplicated by %s and %s", cfg.Prefix, other, name)
		}
		usedPrefixes[cfg.Prefix] = name
		out = append(out, cfg)
	}
	return out, nil
}

func policyMap(value any) (map[string]any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = item
		}
		return out, true
	default:
		return nil, false
	}
}

func stringSlice(value any) []string {
	switch typed := value.(type) {
	case []string:
		return append([]string{}, typed...)
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			text := strings.TrimSpace(anyString(item))
			if text != "" {
				out = append(out, text)
			}
		}
		return out
	default:
		return nil
	}
}

func anyString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	default:
		raw, err := json.Marshal(typed)
		if err != nil {
			return ""
		}
		return string(raw)
	}
}

func discoveredToolNames(items []DiscoveredTool) []string {
	if len(items) == 0 {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if name := strings.TrimSpace(item.Name); name != "" {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

func rootPolicyTemplateVars(source semanticview.Source) map[string]string {
	if source == nil {
		return nil
	}
	doc := source.ResolvedPolicyForFlow("")
	if len(doc.Values) == 0 {
		return nil
	}
	out := make(map[string]string, len(doc.Values))
	for key, value := range doc.Values {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		switch typed := value.Value.(type) {
		case string:
			out[key] = strings.TrimSpace(typed)
		}
	}
	return out
}

func renderPolicyStringSlice(values []string, vars map[string]string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(renderPolicyTemplate(value, vars))
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func renderPolicyTemplate(raw string, vars map[string]string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" || len(vars) == 0 {
		return raw
	}
	replacer := make([]string, 0, len(vars)*4)
	for key, value := range vars {
		replacer = append(replacer, "{{"+key+"}}", value, "{"+key+"}", value)
	}
	return strings.NewReplacer(replacer...).Replace(raw)
}
