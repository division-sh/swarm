package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
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
	Name       string
	RemoteName string
	ServerName string
	Prefix     string
	Contract   runtimecontracts.ToolSchemaEntry
}

type DiscoveryOptions struct {
	ServerTimeout time.Duration
}

type Client struct {
	store      runtimecredentials.Store
	httpClient *http.Client

	mu            sync.RWMutex
	servers       map[string]*registeredServer
	tools         map[string]DiscoveredTool
	closed        bool
	refreshMu     sync.Mutex
	refreshCancel context.CancelFunc
	refreshDone   chan struct{}
	closeOnce     sync.Once
	closeDone     chan struct{}
	closeErr      error
	cleanupErr    error
}

type CredentialKeyResolver func(string) (string, error)

type registeredServer struct {
	cfg   ServerConfig
	stdio *stdioRPCClient
}

func NewClient(store runtimecredentials.Store) *Client {
	transport := http.DefaultTransport
	if owned, ok := transport.(*http.Transport); ok {
		transport = owned.Clone()
	}
	return &Client{
		store: store,
		httpClient: &http.Client{
			Timeout:   30 * time.Second,
			Transport: transport,
		},
		servers:   map[string]*registeredServer{},
		tools:     map[string]DiscoveredTool{},
		closeDone: make(chan struct{}),
	}
}

func (c *Client) Refresh(ctx context.Context, source semanticview.Source, opts DiscoveryOptions) []error {
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()
	if opts.ServerTimeout < 0 {
		return []error{fmt.Errorf("MCP discovery timeout must not be negative")}
	}
	if err := ctx.Err(); err != nil {
		return []error{err}
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return []error{fmt.Errorf("MCP client is closed")}
	}
	ctx, cancelRefresh := context.WithCancel(ctx)
	refreshDone := make(chan struct{})
	c.refreshCancel, c.refreshDone = cancelRefresh, refreshDone
	c.mu.Unlock()
	defer func() {
		cancelRefresh()
		c.mu.Lock()
		c.refreshCancel, c.refreshDone = nil, nil
		close(refreshDone)
		c.mu.Unlock()
	}()
	ctx = runtimeeffects.WithDifferentOwner(ctx, runtimeeffects.OwnerRuntimeDependency)
	configs, err := ServerConfigs(source)
	if err != nil {
		return []error{err}
	}
	nextServers := make(map[string]*registeredServer, len(configs))
	nextTools := map[string]DiscoveredTool{}
	errs := make([]error, 0)

	for _, cfg := range configs {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		server := &registeredServer{cfg: cfg}
		if strings.EqualFold(strings.TrimSpace(cfg.Transport), "stdio") {
			stdio, stdioErr := newStdioRPCClient(cfg.Command, cfg.Args)
			if stdioErr != nil {
				errs = append(errs, fmt.Errorf("mcp server %s: %w", cfg.Name, stdioErr))
				continue
			}
			server.stdio = stdio
		}
		serverCtx := ctx
		cancelServer := func() {}
		if opts.ServerTimeout > 0 {
			serverCtx, cancelServer = context.WithTimeout(ctx, opts.ServerTimeout)
		}
		tools, toolErr := c.discoverServerTools(serverCtx, source, server)
		cancelServer()
		if toolErr != nil {
			if server.stdio != nil {
				cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
				cleanupErr := server.stdio.Close(cleanupCtx)
				c.recordCleanupError(cleanupErr)
				toolErr = errors.Join(toolErr, cleanupErr)
				cancel()
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
	previous := c.servers
	if c.closed {
		previous = nextServers
		errs = append(errs, fmt.Errorf("MCP client closed during discovery"))
	} else {
		c.servers, c.tools = nextServers, nextTools
	}
	c.mu.Unlock()
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	if err := closeRegisteredServers(cleanupCtx, previous); err != nil {
		c.recordCleanupError(err)
		errs = append(errs, err)
	}
	cancel()
	return errs
}

func (c *Client) Close(ctx context.Context) error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		c.mu.Lock()
		servers, cancelRefresh, refreshDone := c.servers, c.refreshCancel, c.refreshDone
		c.closed = true
		c.servers, c.tools = nil, nil
		c.mu.Unlock()
		if cancelRefresh != nil {
			cancelRefresh()
		}
		go func() {
			defer close(c.closeDone)
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			c.httpClient.CloseIdleConnections()
			cleanupErr := closeRegisteredServers(cleanupCtx, servers)
			if refreshDone != nil {
				select {
				case <-refreshDone:
				case <-cleanupCtx.Done():
					cleanupErr = errors.Join(cleanupErr, fmt.Errorf("MCP discovery did not join during cleanup: %w", cleanupCtx.Err()))
				}
			}
			c.mu.Lock()
			c.closeErr = errors.Join(c.cleanupErr, cleanupErr)
			c.mu.Unlock()
		}()
	})
	select {
	case <-c.closeDone:
		return c.closeErr
	case <-ctx.Done():
		return fmt.Errorf("MCP client cleanup did not complete: %w", ctx.Err())
	}
}

func (c *Client) recordCleanupError(err error) {
	if err != nil {
		c.mu.Lock()
		c.cleanupErr = errors.Join(c.cleanupErr, err)
		c.mu.Unlock()
	}
}

func closeRegisteredServers(ctx context.Context, servers map[string]*registeredServer) error {
	names := make([]string, 0, len(servers))
	for name := range servers {
		names = append(names, name)
	}
	sort.Strings(names)
	var errs []error
	for _, name := range names {
		if stdio := servers[name].stdio; stdio != nil {
			if err := stdio.Close(ctx); err != nil {
				errs = append(errs, fmt.Errorf("mcp server %s cleanup: %w", name, err))
			}
		}
	}
	return errors.Join(errs...)
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
		return nil, runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "mcp_client_unavailable", "mcp-client", "tools/call", nil)
	}
	prefixedName = strings.TrimSpace(prefixedName)
	if prefixedName == "" {
		return nil, runtimefailures.New(runtimefailures.ClassSchemaInvalid, "mcp_tool_name_required", "mcp-client", "tools/call", nil)
	}
	c.mu.RLock()
	tool, ok := c.tools[prefixedName]
	server := c.servers[tool.ServerName]
	c.mu.RUnlock()
	if !ok || server == nil {
		return nil, runtimefailures.New(runtimefailures.ClassTargetUnreachable, "mcp_tool_unavailable", "mcp-client", "tools/call", map[string]any{"tool": prefixedName})
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
		err := externalMCPRPCExecutionFailure(resp.Error, server.cfg, req)
		return nil, resp.effect.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "mcp_jsonrpc_error_effect_outcome_unconfirmed", "mcp-client", "tools/call", map[string]any{"server": server.cfg.Name, "method": req.Method, "tool": tool.RemoteName, "rpc_code": resp.Error.Code}, err)
	}
	result, err := projectExternalToolCallResult(resp, server.cfg, req)
	if err != nil {
		return nil, resp.effect.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "mcp_tool_result_effect_outcome_unconfirmed", "mcp-client", "tools/call", map[string]any{"server": server.cfg.Name, "method": req.Method, "tool": tool.RemoteName}, err)
	}
	if err := resp.effect.Succeed(ctx, map[string]any{"server": server.cfg.Name, "method": req.Method, "tool": tool.RemoteName}); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) discoverServerTools(ctx context.Context, source semanticview.Source, server *registeredServer) ([]DiscoveredTool, error) {
	initializeRequest := RPCRequest{
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
	}
	initializeResponse, err := c.callServer(ctx, server, initializeRequest)
	if err != nil {
		return nil, err
	}
	if initializeResponse.Error != nil {
		return nil, externalMCPRPCExecutionFailure(initializeResponse.Error, server.cfg, initializeRequest)
	}
	if _, err := c.callServer(ctx, server, RPCRequest{
		JSONRPC: "2.0",
		Method:  "notifications/initialized",
		Params:  map[string]any{},
	}); err != nil {
		return nil, err
	}
	toolsListRequest := RPCRequest{
		JSONRPC: "2.0",
		Method:  "tools/list",
		Params:  map[string]any{},
		ID:      server.cfg.Name + "-tools-list",
	}
	resp, err := c.callServer(ctx, server, toolsListRequest)
	if err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, externalMCPRPCExecutionFailure(resp.Error, server.cfg, toolsListRequest)
	}
	result, ok := resp.Result.(map[string]any)
	if !ok {
		return nil, externalMCPFailure(runtimefailures.ClassConnectorFailure, "mcp_tool_catalog_invalid", server.cfg, toolsListRequest, nil, nil)
	}
	rawTools, ok := result["tools"].([]any)
	if !ok {
		return nil, externalMCPFailure(runtimefailures.ClassConnectorFailure, "mcp_tool_catalog_invalid", server.cfg, toolsListRequest, nil, nil)
	}
	out := make([]DiscoveredTool, 0, len(rawTools))
	for _, raw := range rawTools {
		item, ok := raw.(map[string]any)
		if !ok {
			return nil, externalMCPFailure(runtimefailures.ClassConnectorFailure, "mcp_tool_catalog_invalid", server.cfg, toolsListRequest, nil, nil)
		}
		remoteName := strings.TrimSpace(asString(item["name"]))
		if remoteName == "" {
			return nil, externalMCPFailure(runtimefailures.ClassConnectorFailure, "mcp_tool_catalog_invalid", server.cfg, toolsListRequest, nil, nil)
		}
		rawSchema, ok := item["inputSchema"].(map[string]any)
		if !ok {
			return nil, externalMCPFailure(runtimefailures.ClassConnectorFailure, "mcp_tool_catalog_invalid", server.cfg, toolsListRequest, map[string]any{"tool": remoteName}, nil)
		}
		inputSchema, err := runtimecontracts.AdmitToolInputSchemaMap(rawSchema)
		if err != nil {
			return nil, externalMCPFailure(runtimefailures.ClassConnectorFailure, "mcp_tool_catalog_invalid", server.cfg, toolsListRequest, map[string]any{"tool": remoteName}, err)
		}
		binding, err := runtimecontracts.NewToolMCPBinding(server.cfg.Name, remoteName)
		if err != nil {
			return nil, externalMCPFailure(runtimefailures.ClassConnectorFailure, "mcp_tool_catalog_invalid", server.cfg, toolsListRequest, map[string]any{"tool": remoteName}, err)
		}
		contract, err := runtimecontracts.NewToolSchemaEntry(
			runtimecontracts.WithToolDescription(asString(item["description"])),
			runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerMCP),
			runtimecontracts.WithToolSchemas(inputSchema, runtimecontracts.ToolInputSchema{}),
			runtimecontracts.WithToolMCP(binding),
		)
		if err != nil {
			return nil, externalMCPFailure(runtimefailures.ClassConnectorFailure, "mcp_tool_catalog_invalid", server.cfg, toolsListRequest, map[string]any{"tool": remoteName}, err)
		}
		out = append(out, DiscoveredTool{
			Name:       strings.TrimSpace(server.cfg.Prefix) + "." + remoteName,
			RemoteName: remoteName,
			ServerName: server.cfg.Name,
			Prefix:     server.cfg.Prefix,
			Contract:   contract,
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
			return RPCResponse{}, externalMCPFailure(runtimefailures.ClassDependencyUnavailable, "mcp_stdio_client_unavailable", server.cfg, req, nil, nil)
		}
		return server.stdio.Call(ctx, server.cfg, req)
	case "http", "":
		return c.callHTTPServerWithCredentialKeyResolver(ctx, server.cfg, req, resolver)
	default:
		return RPCResponse{}, externalMCPFailure(runtimefailures.ClassSchemaInvalid, "mcp_transport_unsupported", server.cfg, req, nil, nil)
	}
}

func (c *Client) callHTTPServer(ctx context.Context, cfg ServerConfig, req RPCRequest) (RPCResponse, error) {
	return c.callHTTPServerWithCredentialKeyResolver(ctx, cfg, req, nil)
}

func (c *Client) callHTTPServerWithCredentialKeyResolver(ctx context.Context, cfg ServerConfig, req RPCRequest, resolver CredentialKeyResolver) (response RPCResponse, resultErr error) {
	body, err := json.Marshal(req)
	if err != nil {
		return RPCResponse{}, externalMCPTransportFailure(err, cfg, req)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.URL, bytes.NewReader(body))
	if err != nil {
		return RPCResponse{}, externalMCPTransportFailure(err, cfg, req)
	}
	httpReq.Header.Set("content-type", "application/json")
	httpReq.Header.Set("mcp-protocol-version", "2025-03-26")
	credentialKey := strings.TrimSpace(cfg.CredentialsKey)
	if credentialKey != "" {
		if resolver != nil {
			resolved, resolveErr := resolver(credentialKey)
			if resolveErr != nil || resolved == "" {
				return RPCResponse{}, externalMCPFailure(runtimefailures.ClassAuthenticationNeeded, "mcp_credential_required", cfg, req, map[string]any{"auth_kind": "mcp_credential"}, resolveErr)
			}
			credentialKey = resolved
		}
		if c.store == nil {
			return RPCResponse{}, externalMCPFailure(runtimefailures.ClassAuthenticationNeeded, "mcp_credential_required", cfg, req, map[string]any{"auth_kind": "mcp_credential"}, nil)
		}
		token, ok, getErr := c.credentialValue(ctx, credentialKey)
		if getErr != nil {
			return RPCResponse{}, externalMCPFailure(runtimefailures.ClassDependencyUnavailable, "mcp_credential_store_unavailable", cfg, req, nil, getErr)
		}
		if !ok || strings.TrimSpace(token) == "" {
			return RPCResponse{}, externalMCPFailure(runtimefailures.ClassAuthenticationNeeded, "mcp_credential_required", cfg, req, map[string]any{"auth_kind": "mcp_credential"}, nil)
		}
		httpReq.Header.Set("authorization", "Bearer "+strings.TrimSpace(token))
	}
	fingerprintBody, err := mcpEffectFingerprint(req)
	if err != nil {
		return RPCResponse{}, err
	}
	attempt, err := runtimeeffects.Begin(ctx, "mcp_tools_call_http", fingerprintBody, map[string]string{"server": cfg.Name, "method": req.Method})
	if err != nil {
		return RPCResponse{}, err
	}
	if err := continueCommittedEffectLaunch(ctx, attempt, attempt.MarkLaunched(ctx)); err != nil {
		return RPCResponse{}, err
	}
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		cause := externalMCPTransportFailure(err, cfg, req)
		return RPCResponse{}, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "mcp_http_attempt_outcome_unconfirmed", "mcp-client", req.Method, map[string]any{"server": cfg.Name, "method": req.Method, "stage": "transport"}, cause)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("MCP HTTP response cleanup failed: %w", err))
		}
	}()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		cause := externalMCPHTTPStatusFailure(resp.StatusCode, cfg, req)
		return RPCResponse{}, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "mcp_http_status_effect_outcome_unconfirmed", "mcp-client", req.Method, map[string]any{"server": cfg.Name, "method": req.Method, "status": resp.StatusCode}, cause)
	}
	raw, actual, err := readBoundedMCPWire(resp.Body)
	if err != nil {
		cause := externalMCPTransportFailure(err, cfg, req)
		return RPCResponse{}, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "mcp_http_attempt_outcome_unconfirmed", "mcp-client", req.Method, map[string]any{"server": cfg.Name, "method": req.Method, "stage": "read"}, cause)
	}
	if actual > MaxWireResponseBytes {
		cause := externalMCPWireLimitFailure(actual, cfg, req)
		return RPCResponse{}, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "mcp_http_attempt_outcome_unconfirmed", "mcp-client", req.Method, map[string]any{"server": cfg.Name, "method": req.Method, "stage": "wire_limit"}, cause)
	}
	if req.ID == nil && strings.HasPrefix(req.Method, "notifications/") && len(bytes.TrimSpace(raw)) == 0 {
		return RPCResponse{}, attempt.Succeed(ctx, map[string]any{"server": cfg.Name, "method": req.Method})
	}
	decoded, err := DecodeRPCResponse(raw, req)
	if err != nil {
		cause := externalMCPRPCInvalidFailure(err, cfg, req)
		return RPCResponse{}, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "mcp_http_attempt_outcome_unconfirmed", "mcp-client", req.Method, map[string]any{"server": cfg.Name, "method": req.Method, "stage": "decode"}, cause)
	}
	decoded.effect = attempt
	return decoded, nil
}

func readBoundedMCPWire(reader io.Reader) ([]byte, int, error) {
	raw, err := io.ReadAll(io.LimitReader(reader, MaxWireResponseBytes+1))
	if err != nil {
		return nil, 0, err
	}
	return raw, len(raw), nil
}

func (c *Client) credentialValue(ctx context.Context, key string) (string, bool, error) {
	if c == nil || c.store == nil || strings.TrimSpace(key) == "" {
		return "", false, nil
	}
	return c.store.Get(ctx, key)
}

type stdioRPCClient struct {
	cmd        *exec.Cmd
	stdin      io.WriteCloser
	stdout     *bufio.Reader
	mu         sync.Mutex
	stdoutPipe io.ReadCloser
	stderrPipe io.ReadCloser
	stderrDone chan struct{}
	stderrErr  error
	closeOnce  sync.Once
	closeDone  chan struct{}
	closeErr   error
	nextID     atomic.Int64
}

func newStdioRPCClient(command string, args []string) (*stdioRPCClient, error) {
	command = strings.TrimSpace(command)
	if command == "" {
		return nil, fmt.Errorf("stdio command is required")
	}
	cmd := exec.Command(command, args...)
	configureStdioProcess(cmd)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stderr.Close()
		return nil, err
	}
	client := &stdioRPCClient{
		cmd:        cmd,
		stdin:      stdin,
		stdout:     bufio.NewReader(stdout),
		stdoutPipe: stdout,
		stderrPipe: stderr,
		stderrDone: make(chan struct{}),
		closeDone:  make(chan struct{}),
	}
	go func() {
		defer close(client.stderrDone)
		// Provider stderr may contain credentials; drain it without public logging.
		_, client.stderrErr = io.Copy(io.Discard, stderr)
	}()
	return client, nil
}

func (c *stdioRPCClient) Close(ctx context.Context) error {
	if c == nil || c.cmd == nil || c.cmd.Process == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		go func() {
			defer close(c.closeDone)
			var errs []error
			killErr := killStdioProcess(c.cmd)
			if killErr != nil && !errors.Is(killErr, os.ErrProcessDone) {
				errs = append(errs, killErr)
			}
			for _, pipe := range []io.Closer{c.stdin, c.stdoutPipe, c.stderrPipe} {
				if pipe == nil {
					continue
				}
				if err := pipe.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
					errs = append(errs, err)
				}
			}
			<-c.stderrDone
			if c.stderrErr != nil && !errors.Is(c.stderrErr, os.ErrClosed) {
				errs = append(errs, fmt.Errorf("MCP stderr drain failed"))
			}
			if err := c.cmd.Wait(); err != nil && !stdioProcessWasKilled(err) {
				errs = append(errs, err)
			}
			c.closeErr = errors.Join(errs...)
		}()
	})
	select {
	case <-c.closeDone:
		return c.closeErr
	case <-ctx.Done():
		return fmt.Errorf("MCP stdio process cleanup did not complete: %w", ctx.Err())
	}
}

func (c *stdioRPCClient) Call(ctx context.Context, cfg ServerConfig, req RPCRequest) (RPCResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return RPCResponse{}, externalMCPTransportFailure(err, cfg, req)
	}
	cancelDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(cancelDone)
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = c.Close(cleanupCtx)
	})
	defer func() {
		if !stop() {
			<-cancelDone
		}
	}()
	if req.ID == nil && !strings.HasPrefix(strings.TrimSpace(req.Method), "notifications/") {
		req.ID = c.nextID.Add(1)
	}
	raw, err := json.Marshal(req)
	if err != nil {
		return RPCResponse{}, externalMCPTransportFailure(err, cfg, req)
	}
	raw = append(raw, '\n')
	fingerprintBody, err := mcpEffectFingerprint(req)
	if err != nil {
		return RPCResponse{}, err
	}
	attempt, err := runtimeeffects.Begin(ctx, "mcp_tools_call_stdio", fingerprintBody, map[string]string{"server": cfg.Name, "method": req.Method})
	if err != nil {
		return RPCResponse{}, err
	}
	if err := continueCommittedEffectLaunch(ctx, attempt, attempt.MarkLaunched(ctx)); err != nil {
		return RPCResponse{}, err
	}
	if _, err := c.stdin.Write(raw); err != nil {
		cause := externalMCPTransportFailure(err, cfg, req)
		return RPCResponse{}, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "mcp_stdio_attempt_outcome_unconfirmed", "mcp-client", req.Method, map[string]any{"server": cfg.Name, "method": req.Method, "stage": "write"}, cause)
	}
	if req.ID == nil {
		if err := attempt.Succeed(ctx, map[string]any{"server": cfg.Name, "method": req.Method}); err != nil {
			return RPCResponse{}, err
		}
		return RPCResponse{}, nil
	}
	for {
		type frameResult struct {
			frame  []byte
			actual int
			err    error
		}
		frameCh := make(chan frameResult, 1)
		go func() {
			frame, actual, err := readBoundedStdioFrame(c.stdout)
			frameCh <- frameResult{frame: frame, actual: actual, err: err}
		}()
		var read frameResult
		select {
		case <-ctx.Done():
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			cleanupErr := c.Close(cleanupCtx)
			cancel()
			// Closing the owned pipe releases and joins the frame reader.
			if c.stdoutPipe != nil {
				<-frameCh
			}
			cause := externalMCPTransportFailure(errors.Join(ctx.Err(), cleanupErr), cfg, req)
			return RPCResponse{}, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "mcp_stdio_attempt_outcome_unconfirmed", "mcp-client", req.Method, map[string]any{"server": cfg.Name, "method": req.Method, "stage": "wait"}, cause)
		case read = <-frameCh:
		}
		if read.err != nil {
			cause := externalMCPTransportFailure(read.err, cfg, req)
			return RPCResponse{}, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "mcp_stdio_attempt_outcome_unconfirmed", "mcp-client", req.Method, map[string]any{"server": cfg.Name, "method": req.Method, "stage": "read"}, cause)
		}
		if read.actual > MaxWireResponseBytes {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			cleanupErr := c.Close(cleanupCtx)
			cancel()
			cause := errors.Join(externalMCPWireLimitFailure(read.actual, cfg, req), cleanupErr)
			return RPCResponse{}, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "mcp_stdio_attempt_outcome_unconfirmed", "mcp-client", req.Method, map[string]any{"server": cfg.Name, "method": req.Method, "stage": "wire_limit"}, cause)
		}
		line := bytes.TrimSpace(read.frame)
		if len(line) == 0 {
			continue
		}
		resp, err := DecodeRPCResponse(line, req)
		if err != nil {
			cause := externalMCPRPCInvalidFailure(err, cfg, req)
			return RPCResponse{}, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "mcp_stdio_attempt_outcome_unconfirmed", "mcp-client", req.Method, map[string]any{"server": cfg.Name, "method": req.Method, "stage": "decode"}, cause)
		}
		resp.effect = attempt
		return resp, nil
	}
}

func mcpEffectFingerprint(req RPCRequest) ([]byte, error) {
	req.ID = nil
	raw, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("encode MCP effect fingerprint: %w", err)
	}
	return raw, nil
}

func readBoundedStdioFrame(reader *bufio.Reader) ([]byte, int, error) {
	frame := make([]byte, 0, 4096)
	for {
		fragment, err := reader.ReadSlice('\n')
		if len(frame)+len(fragment) > MaxWireResponseBytes {
			return nil, MaxWireResponseBytes + 1, nil
		}
		frame = append(frame, fragment...)
		switch err {
		case nil:
			return frame, len(frame), nil
		case bufio.ErrBufferFull:
			continue
		default:
			return nil, len(frame), err
		}
	}
}

func ServerConfigs(source semanticview.Source) ([]ServerConfig, error) {
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
