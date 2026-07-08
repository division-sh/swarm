package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	runCommandMethodHealth         = "health.check"
	runCommandMethodStart          = "run.start"
	runCommandMethodGet            = "run.get"
	runCommandMethodStop           = "run.stop"
	runCommandMethodSubscribeTrace = "run.subscribe_trace"
	runCommandStatusCompleted      = "completed"
	runCommandStatusFailed         = "failed"
	runCommandStatusCancelled      = "cancelled"
	runCommandStatusForked         = "forked"
)

type runCommandOptions struct {
	apiOptions        rootCommandOptions
	eventName         string
	payloadPath       string
	connectURL        string
	noFollow          bool
	reattachRunID     string
	bundleHash        string
	bundleFingerprint string
	configPath        string
	backend           string
	contractsPath     string
	dataSource        string
	platformSpecPath  string
	idempotencyKey    string
	runID             string
	apiPort           int
	mcpPort           int
	detach            bool
	changedFlags      map[string]bool
}

type runStartResult struct {
	RunID  string `json:"run_id"`
	Status string `json:"status"`
}

type runCommandOKResult struct {
	OK bool `json:"ok"`
}

type runTraceSubscriptionResult struct {
	SubscriptionID string `json:"subscription_id"`
}

type runTraceNotification struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  struct {
		Subscription string                `json:"subscription"`
		Result       diagnosticRunTraceRow `json:"result"`
	} `json:"params"`
}

type runTraceSubscription struct {
	conn           *websocket.Conn
	endpoint       string
	subscriptionID string
	rows           chan diagnosticRunTraceRow
	errs           chan error
}

func newRunCommand(repo string, rootOpts rootCommandOptions) *cobra.Command {
	opts := runCommandOptions{apiOptions: rootOpts}
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start a workflow run on a running runtime, or reattach to one.",
		Example: `  swarm run start --event <event-name> --payload payload.json
  swarm run start --reattach <run-id>`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			runOpts := opts
			runOpts.changedFlags = runCommandChangedFlags(cmd)
			return runRunCommand(cmd.Context(), repo, cmd.OutOrStdout(), cmd.ErrOrStderr(), runOpts)
		},
	}
	cmd.Flags().StringVar(&opts.eventName, "event", "", "Declared event name to publish as the run trigger")
	cmd.Flags().StringVar(&opts.payloadPath, "payload", "", "Path to JSON object payload file")
	cmd.Flags().StringVar(&opts.connectURL, "connect", "", "Existing Swarm API base URL")
	cmd.Flags().BoolVar(&opts.noFollow, "no-follow", false, "Start through a connected server and print the run id without opening a trace subscription")
	cmd.Flags().StringVar(&opts.reattachRunID, "reattach", "", "Existing run id to reattach to")
	cmd.Flags().StringVar(&opts.bundleHash, "bundle-hash", "", "Expected server canonical bundle hash")
	cmd.Flags().StringVar(&opts.bundleFingerprint, "bundle-fingerprint", "", "Expected server bundle fingerprint")
	cmd.Flags().StringVar(&opts.configPath, "config", "", "Path to Swarm runtime config for local foreground startup")
	cmd.Flags().StringVar(&opts.backend, "backend", "", "LLM backend profile for local foreground startup: anthropic, claude_cli, openai_compatible, or openai_responses")
	cmd.Flags().StringVar(&opts.contractsPath, "contracts", "", "Path to Swarm contract bundle root for local foreground startup")
	cmd.Flags().StringVar(&opts.dataSource, "data", "", "Path to agent-visible read-only /data reference directory")
	cmd.Flags().StringVar(&opts.platformSpecPath, "platform-spec", "", "Path to platform spec yaml for local foreground startup")
	cmd.Flags().StringVar(&opts.idempotencyKey, "idempotency-key", "", "Optional idempotency key for run.start")
	_ = cmd.Flags().MarkHidden("idempotency-key")
	cmd.Flags().StringVar(&opts.runID, "run-id", "", "Optional caller-provided run id for run.start")
	cmd.Flags().IntVar(&opts.apiPort, "api-port", 0, "Local API listener port for local foreground startup")
	cmd.Flags().IntVar(&opts.mcpPort, "mcp-port", 0, "Reserved local MCP port for local foreground startup")
	cmd.Flags().BoolVar(&opts.detach, "detach", false, "Unsupported in CLI v2; use --connect with --no-follow")
	return cmd
}

func runCommandChangedFlags(cmd *cobra.Command) map[string]bool {
	changed := map[string]bool{}
	cmd.Flags().Visit(func(flag *pflag.Flag) {
		changed[flag.Name] = true
	})
	return changed
}

func runRunCommand(ctx context.Context, repo string, out, errOut io.Writer, opts runCommandOptions) error {
	if err := opts.validate(); err != nil {
		writeCLIAPIError(errOut, err)
		return commandExitError{code: 2}
	}
	apiOpts, wsEndpoint, err := opts.runtimeEndpoints()
	if err != nil {
		writeCLIAPIError(errOut, err)
		return commandExitError{code: 2}
	}
	apiOpts.disableLocalTargeting = true
	opts.apiOptions = apiOpts

	if strings.TrimSpace(opts.reattachRunID) != "" {
		return runReattachCommand(ctx, out, errOut, opts, wsEndpoint)
	}

	payload, err := loadRunCommandPayload(opts.payloadPath)
	if err != nil {
		writeCLIAPIError(errOut, err)
		return commandExitError{code: 2}
	}

	var stopLocal func()
	if strings.TrimSpace(opts.connectURL) == "" {
		repo = assetCommandRepoRoot(repo)
		var err error
		opts, err = opts.withLocalForegroundServeAuth()
		if err != nil {
			writeCLIAPIError(errOut, err)
			return commandExitError{code: runCommandErrorExitCode(err)}
		}
		if _, err := newCLIAPIClient(opts.apiOptions); err != nil {
			writeCLIAPIError(errOut, err)
			return commandExitError{code: runCommandErrorExitCode(err)}
		}
		stopLocal, err = startLocalRunServe(ctx, repo, opts)
		if err != nil {
			writeCLIAPIError(errOut, err)
			return commandExitError{code: runCommandErrorExitCode(err)}
		}
		defer stopLocal()
	}

	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		writeCLIAPIError(errOut, err)
		return commandExitError{code: runCommandErrorExitCode(err)}
	}
	health, err := runCommandHealth(ctx, client)
	if err != nil {
		writeCLIAPIError(errOut, err)
		return commandExitError{code: runCommandErrorExitCode(err)}
	}
	if expected := strings.TrimSpace(opts.bundleFingerprint); expected != "" && expected != health.Bundle.Fingerprint {
		fmt.Fprintf(errOut, "bundle fingerprint mismatch: server=%s expected=%s\n", health.Bundle.Fingerprint, expected)
		return commandExitError{code: 6}
	}
	traceReplaySince := time.Now().UTC()
	start, err := runCommandStart(ctx, client, health, opts, payload)
	if err != nil {
		writeCLIAPIError(errOut, err)
		return commandExitError{code: runCommandErrorExitCode(err)}
	}
	writeRunCommandStarted(out, start)
	if opts.noFollow {
		writeRunCommandNoFollowGuidance(out, start.RunID, opts.connectURL)
		return nil
	}
	return followRunCommand(ctx, out, errOut, client, opts, wsEndpoint, start.RunID, &traceReplaySince, true)
}

func (o runCommandOptions) validate() error {
	if o.detach {
		return fmt.Errorf("ERROR: `--detach` is not supported in CLI v2. Use `swarm serve` plus `swarm run start --connect <url> --event <name> --payload <file> --no-follow`.")
	}
	if o.apiPort < 0 || o.apiPort > 65535 || (o.changedFlags["api-port"] && o.apiPort == 0) {
		return fmt.Errorf("--api-port must be between 1 and 65535")
	}
	if o.mcpPort < 0 || o.mcpPort > 65535 {
		return fmt.Errorf("--mcp-port must be between 1 and 65535")
	}
	if o.changedFlags["bundle-hash"] && o.changedFlags["bundle-fingerprint"] {
		return fmt.Errorf("--bundle-hash is mutually exclusive with --bundle-fingerprint")
	}
	if o.changedFlags["bundle-hash"] {
		bundleHash := strings.TrimSpace(o.bundleHash)
		if bundleHash == "" {
			return fmt.Errorf("--bundle-hash must be non-empty")
		}
		if !cliBundleHashPattern.MatchString(bundleHash) {
			return fmt.Errorf("--bundle-hash must be bundle-v1:sha256:<64 lowercase hex>")
		}
	}
	if o.changedFlags["bundle-fingerprint"] {
		fingerprint := strings.TrimSpace(o.bundleFingerprint)
		if fingerprint == "" {
			return fmt.Errorf("--bundle-fingerprint must be non-empty")
		}
		if !cliBundleFingerprintPattern.MatchString(fingerprint) {
			return fmt.Errorf("--bundle-fingerprint must be sha256:<64 lowercase hex>")
		}
	}
	if o.changedFlags["mcp-port"] {
		return fmt.Errorf("--mcp-port is not supported until the serve owner can bind MCP explicitly")
	}
	if o.changedFlags["data"] && strings.TrimSpace(o.dataSource) == "" {
		return fmt.Errorf("--data must be non-empty")
	}
	if o.changedFlags["api-port"] {
		_, defaultMCPPort, err := net.SplitHostPort(defaultMCPListenAddr)
		if err != nil {
			return fmt.Errorf("default MCP listener address %q is invalid: %w", defaultMCPListenAddr, err)
		}
		if strconv.Itoa(o.apiPort) == defaultMCPPort {
			return fmt.Errorf("--api-port %d conflicts with default MCP listener %s", o.apiPort, defaultMCPListenAddr)
		}
	}
	if o.noFollow && strings.TrimSpace(o.connectURL) == "" {
		return fmt.Errorf("--no-follow requires --connect")
	}
	if o.noFollow && strings.TrimSpace(o.reattachRunID) != "" {
		return fmt.Errorf("--no-follow and --reattach are mutually exclusive")
	}
	if strings.TrimSpace(o.reattachRunID) != "" {
		if strings.TrimSpace(o.eventName) != "" || strings.TrimSpace(o.payloadPath) != "" || strings.TrimSpace(o.idempotencyKey) != "" || strings.TrimSpace(o.runID) != "" {
			return fmt.Errorf("--reattach is mutually exclusive with --event, --payload, --idempotency-key, and --run-id")
		}
		for _, flag := range []string{"bundle-hash", "bundle-fingerprint", "config", "backend", "contracts", "data", "platform-spec", "api-port", "mcp-port"} {
			if o.changedFlags[flag] {
				return fmt.Errorf("--reattach is mutually exclusive with --%s", flag)
			}
		}
		return nil
	}
	if strings.TrimSpace(o.connectURL) != "" {
		for _, flag := range []string{"config", "backend", "contracts", "data", "platform-spec", "api-port"} {
			if o.changedFlags[flag] {
				return fmt.Errorf("--%s requires local foreground mode and cannot be used with --connect", flag)
			}
		}
	}
	if strings.TrimSpace(o.eventName) == "" {
		return fmt.Errorf("--event is required")
	}
	if strings.TrimSpace(o.payloadPath) == "" {
		return fmt.Errorf("--payload is required")
	}
	return nil
}

func (o runCommandOptions) withLocalForegroundServeAuth() (runCommandOptions, error) {
	auth, err := resolveServeAPIAuth(defaultServeOptions())
	if err != nil {
		return o, err
	}
	if tokenFile := strings.TrimSpace(auth.TokenFile); tokenFile != "" {
		o.apiOptions.apiTokenFile = tokenFile
	}
	return o, nil
}

func (o runCommandOptions) runtimeEndpoints() (rootCommandOptions, string, error) {
	opts := o.apiOptions
	var rpcEndpoint string
	var wsEndpoint string
	if connect := strings.TrimSpace(o.connectURL); connect != "" {
		var err error
		rpcEndpoint, wsEndpoint, err = normalizeRunCommandConnectURL(connect)
		if err != nil {
			return opts, "", err
		}
	} else if o.apiPort > 0 {
		rpcEndpoint = "http://127.0.0.1:" + strconv.Itoa(o.apiPort) + "/v1/rpc"
		wsEndpoint = "ws://127.0.0.1:" + strconv.Itoa(o.apiPort) + "/v1/ws"
	} else {
		rpcEndpoint = strings.TrimSpace(opts.apiRPCEndpointOverride)
		if rpcEndpoint == "" {
			var err error
			rpcEndpoint, err = cliAPIRPCEndpointFromServer(defaultCLIAPIServer, "API server")
			if err != nil {
				return opts, "", err
			}
		}
		var err error
		wsEndpoint, err = runCommandWebSocketEndpoint(rpcEndpoint)
		if err != nil {
			return opts, "", err
		}
	}
	opts.apiRPCEndpointOverride = rpcEndpoint
	return opts, wsEndpoint, nil
}

func normalizeRunCommandConnectURL(raw string) (string, string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", "", fmt.Errorf("--connect must be a valid http(s) URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", "", fmt.Errorf("--connect must use http or https")
	}
	if strings.TrimSpace(parsed.Host) == "" {
		return "", "", fmt.Errorf("--connect must include a host")
	}
	base := *parsed
	base.RawQuery = ""
	base.Fragment = ""
	base.Path = strings.TrimRight(base.Path, "/")
	if base.Path == "" {
		base.Path = "/v1/rpc"
	} else if base.Path != "/v1/rpc" {
		return "", "", fmt.Errorf("--connect path must be empty or /v1/rpc")
	}
	ws := base
	if ws.Scheme == "https" {
		ws.Scheme = "wss"
	} else {
		ws.Scheme = "ws"
	}
	ws.Path = strings.TrimSuffix(base.Path, "/v1/rpc") + "/v1/ws"
	return base.String(), ws.String(), nil
}

func runCommandWebSocketEndpoint(rpcEndpoint string) (string, error) {
	return cliAPIWebSocketEndpointFromRPC(rpcEndpoint)
}

func loadRunCommandPayload(path string) (map[string]any, error) {
	raw, err := os.ReadFile(strings.TrimSpace(path))
	if err != nil {
		return nil, fmt.Errorf("read --payload: %w", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, fmt.Errorf("--payload must be a JSON object: %w", err)
	}
	if payload == nil {
		return nil, fmt.Errorf("--payload must be a JSON object")
	}
	return payload, nil
}

func startLocalRunServe(ctx context.Context, repo string, opts runCommandOptions) (func(), error) {
	runServe := opts.apiOptions.runServe
	if runServe == nil {
		runServe = runServeRuntime
	}
	resolvedPaths, err := resolveCLIContractPlatformSpecPaths(repo, cliContractPlatformSpecPathOptions{
		ContractsPath:    opts.contractsPath,
		PlatformSpecPath: opts.platformSpecPath,
	})
	if err != nil {
		return nil, err
	}
	releaseProjectClaim, err := prepareLocalRunProjectClaim(ctx, repo, opts, resolvedPaths)
	if err != nil {
		return nil, err
	}
	serveOpts := defaultServeOptions()
	swarmDirOpts := opts.apiOptions.swarmDirResolutionOptions()
	serveOpts.SwarmDir = swarmDirOpts.SwarmDir
	serveOpts.SwarmDirSet = swarmDirOpts.SwarmDirFlagSet
	serveOpts.ConfigPath = opts.configPath
	serveOpts.Backend = opts.backend
	serveOpts.ContractsPath = resolvedPaths.ContractsPath
	serveOpts.DataSource = opts.dataSource
	serveOpts.PlatformSpecPath = resolvedPaths.PlatformSpecPath
	serveOpts.LocalRun = true
	if opts.apiPort > 0 {
		serveOpts.APIListenAddr = net.JoinHostPort("127.0.0.1", strconv.Itoa(opts.apiPort))
	}
	serveCtx, cancel := context.WithCancel(ctx)
	done := make(chan int, 1)
	go func() {
		done <- runServe(serveCtx, repo, serveOpts)
	}()
	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
		if releaseProjectClaim != nil {
			releaseProjectClaim()
			releaseProjectClaim = nil
		}
	}
	if err := waitForRunCommandReady(ctx, opts, done); err != nil {
		stop()
		return nil, err
	}
	return stop, nil
}

func prepareLocalRunProjectClaim(ctx context.Context, repo string, opts runCommandOptions, resolvedPaths cliContractPlatformSpecPaths) (func(), error) {
	project := resolveLocalRuntimeStateProject(repo, resolvedPaths)
	if strings.TrimSpace(project.CanonicalProjectRoot) == "" {
		return nil, nil
	}
	swarmDir, err := resolveCLISwarmDir(opts.apiOptions.swarmDirResolutionOptions())
	if err != nil {
		return nil, err
	}
	contextName := localProjectContextName(project.CanonicalProjectRoot)
	registry := newLocalContextRegistry(swarmDir.Path)
	cliProject := cliProjectResolution{
		contractsPath:        project.ContractsPath,
		projectRoot:          project.ProjectRoot,
		canonicalProjectRoot: project.CanonicalProjectRoot,
	}
	if err := guardServeProjectContext(ctx, registry, cliProject, contextName, false); err != nil {
		return nil, fmt.Errorf("local swarm run start requires exclusive project runtime: %w; use --connect to target an existing runtime explicitly or stop the existing project runtime", err)
	}
	release, err := registry.AcquireProjectClaim(project.CanonicalProjectRoot, contextName)
	if err != nil {
		return nil, err
	}
	return release, nil
}

func waitForRunCommandReady(ctx context.Context, opts runCommandOptions, done <-chan int) error {
	timeout := opts.apiOptions.runReadyTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	poll := opts.apiOptions.runReadyPoll
	if poll <= 0 {
		poll = 250 * time.Millisecond
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		select {
		case code := <-done:
			if code == 0 {
				return fmt.Errorf("local serve exited before readiness")
			}
			return fmt.Errorf("local serve exited before readiness: code=%d", code)
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("local serve did not become ready before timeout")
		case <-ticker.C:
			client, err := newCLIAPIClient(opts.apiOptions)
			if err != nil {
				return err
			}
			if _, err := runCommandHealth(ctx, client); err == nil {
				return nil
			} else if runCommandErrorExitCode(err) == 4 {
				return err
			}
		}
	}
}

func runCommandHealth(ctx context.Context, client *cliAPIClient) (diagnosticHealthCheckResult, error) {
	var result diagnosticHealthCheckResult
	if err := client.call(ctx, runCommandMethodHealth, map[string]any{}, &result); err != nil {
		return diagnosticHealthCheckResult{}, err
	}
	if err := validateDiagnosticHealthCheck(result); err != nil {
		return diagnosticHealthCheckResult{}, err
	}
	if result.Ready == nil || !*result.Ready || result.DBOK == nil || !*result.DBOK || result.RuntimeOK == nil || !*result.RuntimeOK {
		return diagnosticHealthCheckResult{}, fmt.Errorf("runtime is not ready")
	}
	return result, nil
}

func runCommandStart(ctx context.Context, client *cliAPIClient, health diagnosticHealthCheckResult, opts runCommandOptions, payload map[string]any) (runStartResult, error) {
	params := map[string]any{
		"event_name": strings.TrimSpace(opts.eventName),
		"payload":    payload,
	}
	if bundleHash := strings.TrimSpace(opts.bundleHash); bundleHash != "" {
		params["bundle_hash"] = bundleHash
	} else if fingerprint := strings.TrimSpace(opts.bundleFingerprint); fingerprint != "" {
		params["bundle_ref"] = map[string]any{"fingerprint": fingerprint}
	} else if bundleHash := strings.TrimSpace(health.Bundle.BundleHash); bundleHash != "" {
		params["bundle_hash"] = bundleHash
	}
	if runID := strings.TrimSpace(opts.runID); runID != "" {
		params["run_id"] = runID
	}
	if key := strings.TrimSpace(opts.idempotencyKey); key != "" {
		params["idempotency_key"] = key
	}
	var result runStartResult
	if err := client.call(ctx, runCommandMethodStart, params, &result); err != nil {
		return runStartResult{}, err
	}
	if err := validateRunStartResult(result); err != nil {
		return runStartResult{}, err
	}
	return result, nil
}

func validateRunStartResult(result runStartResult) error {
	if strings.TrimSpace(result.RunID) == "" {
		return fmt.Errorf("malformed run.start result: run_id is required")
	}
	status := strings.TrimSpace(result.Status)
	if status == "" {
		return fmt.Errorf("malformed run.start result: status is required")
	}
	if _, ok := diagnosticValidRunStatuses[status]; !ok {
		return fmt.Errorf("malformed run.start result: status=%q is not a valid RunStatus", status)
	}
	return nil
}

func runReattachCommand(ctx context.Context, out, errOut io.Writer, opts runCommandOptions, wsEndpoint string) error {
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		writeCLIAPIError(errOut, err)
		return commandExitError{code: runCommandErrorExitCode(err)}
	}
	runID := strings.TrimSpace(opts.reattachRunID)
	run, err := runCommandGet(ctx, client, runID)
	if err != nil {
		writeCLIAPIError(errOut, err)
		return commandExitError{code: runCommandErrorExitCode(err)}
	}
	if runCommandTerminalStatus(run.Status) {
		writeRunCommandTerminalSummary(out, run)
		return runCommandTerminalExit(run.Status)
	}
	writeRunCommandReattached(out, run)
	return followRunCommand(ctx, out, errOut, client, opts, wsEndpoint, runID, nil, false)
}

func followRunCommand(ctx context.Context, out, errOut io.Writer, client *cliAPIClient, opts runCommandOptions, wsEndpoint, runID string, replaySince *time.Time, stopOnInterrupt bool) error {
	sub, err := subscribeRunTrace(ctx, wsEndpoint, client.token, runID, replaySince, nil)
	if err != nil {
		writeCLIAPIError(errOut, err)
		return commandExitError{code: runCommandErrorExitCode(err)}
	}
	defer sub.close()
	rows := sub.rows
	poll := opts.apiOptions.runStatusPoll
	if poll <= 0 {
		poll = time.Second
	}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			if stopOnInterrupt {
				if err := runCommandStop(context.Background(), client, runID); err != nil {
					fmt.Fprintln(errOut, "interrupted; run.stop failed")
					writeCLIAPIError(errOut, err)
					return commandExitError{code: 130}
				}
			}
			if stopOnInterrupt {
				fmt.Fprintln(errOut, "interrupted; requested run.stop")
			} else {
				fmt.Fprintln(errOut, "detached from run trace")
			}
			return commandExitError{code: 130}
		case row, ok := <-rows:
			if !ok {
				rows = nil
				continue
			}
			writeRunCommandTraceRow(out, row)
		case err := <-sub.errs:
			if err != nil {
				writeCLIAPIError(errOut, err)
				return commandExitError{code: runCommandErrorExitCode(err)}
			}
		case <-ticker.C:
			run, err := runCommandGet(ctx, client, runID)
			if err != nil {
				writeCLIAPIError(errOut, err)
				return commandExitError{code: runCommandErrorExitCode(err)}
			}
			if runCommandTerminalStatus(run.Status) {
				writeRunCommandTerminalSummary(out, run)
				return runCommandTerminalExit(run.Status)
			}
		}
	}
}

func subscribeRunTrace(ctx context.Context, wsEndpoint, token, runID string, replaySince *time.Time, extraParams map[string]any) (*runTraceSubscription, error) {
	header := http.Header{"Authorization": []string{"Bearer " + token}}
	conn, resp, err := websocket.DefaultDialer.DialContext(ctx, wsEndpoint, header)
	if err != nil {
		if resp != nil {
			return nil, cliAPIWebSocketHTTPError("runtime event stream", wsEndpoint, resp)
		}
		return nil, &cliAPITransportError{surface: "runtime event stream", endpoint: wsEndpoint, operation: "dial", err: err}
	}
	requestID := "swarm-cli:" + runCommandMethodSubscribeTrace
	params := map[string]any{"run_id": runID}
	for name, value := range extraParams {
		params[name] = value
	}
	if replaySince != nil {
		params["replay_since"] = replaySince.UTC().Format(time.RFC3339Nano)
	}
	if err := conn.WriteJSON(jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      requestID,
		Method:  runCommandMethodSubscribeTrace,
		Params:  params,
	}); err != nil {
		conn.Close()
		return nil, &cliAPITransportError{surface: "runtime event stream", endpoint: wsEndpoint, operation: "subscription request", err: err}
	}
	var envelope jsonRPCResponse
	if err := cliAPIReadWebSocketJSON(conn, "runtime event stream", wsEndpoint, "subscription response", &envelope); err != nil {
		conn.Close()
		return nil, err
	}
	if envelope.JSONRPC != "2.0" {
		conn.Close()
		return nil, &cliAPIProtocolError{surface: "runtime event stream", endpoint: wsEndpoint, operation: "subscription response", err: fmt.Errorf("jsonrpc=%q", envelope.JSONRPC)}
	}
	if id, ok := envelope.ID.(string); !ok || id != requestID {
		conn.Close()
		return nil, &cliAPIProtocolError{surface: "runtime event stream", endpoint: wsEndpoint, operation: "subscription response", err: fmt.Errorf("id=%s, want %q", formatJSONRPCID(envelope.ID), requestID)}
	}
	if envelope.Error != nil {
		conn.Close()
		return nil, envelope.Error
	}
	var result runTraceSubscriptionResult
	if err := json.Unmarshal(envelope.Result, &result); err != nil {
		conn.Close()
		return nil, &cliAPIProtocolError{surface: "runtime event stream", endpoint: wsEndpoint, operation: "subscription result", err: err}
	}
	if strings.TrimSpace(result.SubscriptionID) == "" {
		conn.Close()
		return nil, &cliAPIProtocolError{surface: "runtime event stream", endpoint: wsEndpoint, operation: "subscription result", err: fmt.Errorf("subscription_id is required")}
	}
	sub := &runTraceSubscription{
		conn:           conn,
		endpoint:       wsEndpoint,
		subscriptionID: result.SubscriptionID,
		rows:           make(chan diagnosticRunTraceRow, 16),
		errs:           make(chan error, 1),
	}
	go sub.readLoop()
	return sub, nil
}

func (s *runTraceSubscription) readLoop() {
	defer close(s.rows)
	for {
		var notification runTraceNotification
		if err := cliAPIReadWebSocketJSON(s.conn, "runtime event stream", s.endpoint, "notification read", &notification); err != nil {
			if cliAPIIsNormalWebSocketClose(err) {
				return
			}
			s.reportError(err)
			return
		}
		if notification.JSONRPC != "2.0" || notification.Method != "rpc.subscription" {
			s.reportError(&cliAPIProtocolError{surface: "runtime event stream", endpoint: s.endpoint, operation: "notification", err: fmt.Errorf("malformed run.subscribe_trace notification")})
			return
		}
		if notification.Params.Subscription != s.subscriptionID {
			s.reportError(&cliAPIProtocolError{surface: "runtime event stream", endpoint: s.endpoint, operation: "notification", err: fmt.Errorf("subscription mismatch")})
			return
		}
		row := notification.Params.Result
		if err := validateRunCommandTraceRow(row); err != nil {
			s.reportError(&cliAPIProtocolError{surface: "runtime event stream", endpoint: s.endpoint, operation: "notification", err: err})
			return
		}
		select {
		case s.rows <- row:
		default:
			s.reportError(fmt.Errorf("run.subscribe_trace notification queue overflow"))
			return
		}
	}
}

func (s *runTraceSubscription) reportError(err error) {
	select {
	case s.errs <- err:
	default:
	}
}

func validateRunCommandTraceRow(row diagnosticRunTraceRow) error {
	if strings.TrimSpace(row.EventID) == "" {
		return fmt.Errorf("malformed run.subscribe_trace notification: event_id is required")
	}
	if strings.TrimSpace(row.EventName) == "" {
		return fmt.Errorf("malformed run.subscribe_trace notification: event_name is required")
	}
	if err := validateRequiredTimestamp("run.subscribe_trace.event_created_at", row.EventCreatedAt); err != nil {
		return err
	}
	return nil
}

func (s *runTraceSubscription) close() {
	if s == nil || s.conn == nil {
		return
	}
	_ = s.conn.Close()
}

func runCommandGet(ctx context.Context, client *cliAPIClient, runID string) (diagnosticRunHeader, error) {
	var result diagnosticRunGetResult
	if err := client.call(ctx, runCommandMethodGet, map[string]any{"run_id": runID}, &result); err != nil {
		return diagnosticRunHeader{}, err
	}
	if err := validateDiagnosticRunHeader("run", result.Run); err != nil {
		return diagnosticRunHeader{}, err
	}
	return result.Run, nil
}

func runCommandStop(ctx context.Context, client *cliAPIClient, runID string) error {
	stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var result runCommandOKResult
	if err := client.call(stopCtx, runCommandMethodStop, map[string]any{"run_id": runID}, &result); err != nil {
		return err
	}
	if !result.OK {
		return fmt.Errorf("malformed run.stop result: ok must be true")
	}
	return nil
}

func runCommandTerminalStatus(status string) bool {
	switch strings.TrimSpace(status) {
	case runCommandStatusCompleted, runCommandStatusFailed, runCommandStatusCancelled, runCommandStatusForked:
		return true
	default:
		return false
	}
}

func runCommandTerminalExit(status string) error {
	switch strings.TrimSpace(status) {
	case runCommandStatusFailed, runCommandStatusCancelled:
		return commandExitError{code: 7}
	default:
		return nil
	}
}

func runCommandErrorExitCode(err error) int {
	return cliAPIErrorExitCode(err, cliAPIErrorClassifier{
		notFoundCodes: []string{"RUN_NOT_FOUND"},
		conflictCodes: []string{
			"BUNDLE_MISMATCH",
			"BUNDLE_SCOPE_REQUIRED",
			"BUNDLE_UNAVAILABLE",
			"BUNDLE_DATA_INTEGRITY_ERROR",
			"UNSUPPORTED_BUNDLE_HASH",
			"UNSUPPORTED_BUNDLE_REF",
			"EVENT_NOT_DECLARED",
			"EVENT_PUBLISH_FAILED",
			"PAYLOAD_VALIDATION_FAILED",
			"IDEMPOTENCY_CONFLICT",
		},
	})
}

func writeRunCommandStarted(out io.Writer, result runStartResult) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "run started: run_id=%s status=%s\n", result.RunID, result.Status)
}

func writeRunCommandNoFollowGuidance(out io.Writer, runID, connectURL string) {
	if out == nil {
		return
	}
	connect := strings.TrimSpace(connectURL)
	if connect != "" {
		fmt.Fprintf(out, "reattach: swarm run start --connect %s --reattach %s\n", connect, runID)
		return
	}
	fmt.Fprintf(out, "reattach: swarm run start --reattach %s\n", runID)
}

func writeRunCommandReattached(out io.Writer, run diagnosticRunHeader) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "reattached: run_id=%s status=%s\n", run.RunID, run.Status)
}

func writeRunCommandTraceRow(out io.Writer, row diagnosticRunTraceRow) {
	if out == nil {
		return
	}
	fields := []string{
		"event_id=" + row.EventID,
		"event_name=" + row.EventName,
		"at=" + row.EventCreatedAt,
	}
	if row.EntityID != "" {
		fields = append(fields, "entity_id="+row.EntityID)
	}
	if row.DeliveryStatus != "" {
		fields = append(fields, "delivery_status="+row.DeliveryStatus)
	}
	if row.SubscriberType != "" || row.SubscriberID != "" {
		fields = append(fields, "subscriber="+strings.Trim(row.SubscriberType+"/"+row.SubscriberID, "/"))
	}
	if row.SessionID != "" {
		fields = append(fields, "session_id="+row.SessionID)
	}
	if row.TurnID != "" {
		fields = append(fields, "turn_id="+row.TurnID)
	}
	fmt.Fprintf(out, "trace %s\n", strings.Join(fields, " "))
}

func writeRunCommandTerminalSummary(out io.Writer, run diagnosticRunHeader) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "run terminal: run_id=%s status=%s trigger=%s event_count=%d entity_count=%d\n",
		run.RunID, run.Status, run.TriggerEventType, intValue(run.EventCount), intValue(run.EntityCount))
	if run.ErrorSummary != "" {
		fmt.Fprintf(out, "error=%s\n", run.ErrorSummary)
	}
}

func intValue(v *int) int {
	if v == nil {
		return 0
	}
	return *v
}
