package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/google/uuid"
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
	defaultRunTraceAttachTimeout   = 5 * time.Second
	runTraceObserverDetachedType   = "run_trace_observer_detached"
	runTraceObserverWarning        = "warning"
)

type runTraceDetachReason string

const (
	runTraceDetachAttachFailed                runTraceDetachReason = "attach_failed"
	runTraceDetachAttachTimedOut              runTraceDetachReason = "attach_timed_out"
	runTraceDetachSubscriptionResponseInvalid runTraceDetachReason = "subscription_response_invalid"
	runTraceDetachTransportLost               runTraceDetachReason = "transport_lost"
	runTraceDetachNotificationInvalid         runTraceDetachReason = "notification_invalid"
	runTraceDetachSubscriptionMismatch        runTraceDetachReason = "subscription_mismatch"
	runTraceDetachTraceRowInvalid             runTraceDetachReason = "trace_row_invalid"
	runTraceDetachStreamClosed                runTraceDetachReason = "stream_closed"
	runTraceDetachQueueOverflow               runTraceDetachReason = "queue_overflow"
	runTraceDetachResponseOverBudget          runTraceDetachReason = "response_over_budget"
)

type runCommandOptions struct {
	apiOptions       rootCommandOptions
	eventName        string
	payloadPath      string
	connectURL       string
	noFollow         bool
	reattachRunID    string
	bundleHash       string
	configPath       string
	backend          string
	contractsPath    string
	dataImports      []string
	dataPins         []string
	platformSpecPath string
	idempotencyKey   string
	runID            string
	apiPort          int
	mcpPort          int
	detach           bool
	changedFlags     map[string]bool
}

type runStartResult struct {
	RunID       string                  `json:"run_id"`
	Status      string                  `json:"status"`
	DataBinding durabledata.DataBinding `json:"data_binding"`
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
	done           chan struct{}
	closeOnce      sync.Once
}

type runTraceObserverError struct {
	reason runTraceDetachReason
	err    error
}

func (e *runTraceObserverError) Error() string {
	if e == nil || e.err == nil {
		return ""
	}
	return e.err.Error()
}

func (e *runTraceObserverError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

type runTraceObserverDetachedFact struct {
	Type            string               `json:"type"`
	Severity        string               `json:"severity"`
	ReasonCode      runTraceDetachReason `json:"reason_code"`
	RunID           string               `json:"run_id"`
	RunContinues    bool                 `json:"run_continues"`
	ReattachCommand string               `json:"reattach_command"`
	Message         string               `json:"message,omitempty"`
}

type foregroundRunTraceObserver struct {
	rows     <-chan diagnosticRunTraceRow
	detached <-chan runTraceDetach
	cancel   context.CancelFunc
	done     <-chan struct{}
	stopOnce sync.Once
}

type foregroundRunTraceRenderer struct {
	detached chan runTraceObserverDetachedFact
	workers  sync.WaitGroup
	stopOnce sync.Once
}

func newRunCommand(root InvocationRoot, rootOpts rootCommandOptions) *cobra.Command {
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
			if path, set, err := effectiveCommandConfigPath(cmd, runOpts.configPath, runOpts.changedFlags["config"]); err != nil {
				return err
			} else if set {
				runOpts.configPath = path
				runOpts.changedFlags["config"] = true
				rootFlags := rootCommandFlagState{}
				if runOpts.apiOptions.rootFlags != nil {
					rootFlags = *runOpts.apiOptions.rootFlags
				}
				rootFlags.configPath = path
				rootFlags.configPathSet = true
				runOpts.apiOptions.rootFlags = &rootFlags
			}
			return runRunCommand(cmd.Context(), root, cmd.OutOrStdout(), cmd.ErrOrStderr(), runOpts)
		},
	}
	cmd.Flags().StringVar(&opts.eventName, "event", "", "Declared event name to publish as the run trigger")
	cmd.Flags().StringVar(&opts.payloadPath, "payload", "", "Path to JSON object payload file")
	cmd.Flags().StringVar(&opts.connectURL, "connect", "", "Existing Swarm API base URL")
	cmd.Flags().BoolVar(&opts.noFollow, "no-follow", false, "Start through a connected server and print the run id without opening a trace subscription")
	cmd.Flags().StringVar(&opts.reattachRunID, "reattach", "", "Existing run id to reattach to")
	cmd.Flags().StringVar(&opts.bundleHash, "bundle-hash", "", "Expected server canonical bundle hash")
	cmd.Flags().StringVar(&opts.configPath, "config", "", "Path to swarm.yaml config for local foreground startup")
	cmd.Flags().StringVar(&opts.backend, "backend", "", "LLM backend profile for local foreground startup: anthropic, claude_cli, openai_compatible, or openai_responses")
	cmd.Flags().StringVar(&opts.contractsPath, "contracts", "", "Path to Swarm contract bundle root for local foreground startup")
	cmd.Flags().StringArrayVar(&opts.dataImports, "data", nil, "Fused immutable data import and pin: name=file.jsonl (repeatable)")
	cmd.Flags().StringArrayVar(&opts.dataPins, "pin", nil, "Exact data version pin: name@head, name@vN, or name@ResourceVersionID (repeatable)")
	cmd.Flags().StringVar(&opts.platformSpecPath, "platform-spec", "", retiredPlatformSpecFlagHelp)
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

func runRunCommand(ctx context.Context, root InvocationRoot, out, errOut io.Writer, opts runCommandOptions) error {
	opts.apiOptions.invocationRoot = root
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

	payload, err := loadRunCommandPayload(root.Resolve(opts.payloadPath))
	if err != nil {
		writeCLIAPIError(errOut, err)
		return commandExitError{code: 2}
	}

	var stopLocal func()
	if strings.TrimSpace(opts.connectURL) == "" {
		var err error
		opts, err = opts.withLocalForegroundServeAuth(root)
		if err != nil {
			writeCLIAPIError(errOut, err)
			return commandExitError{code: runCommandErrorExitCode(err)}
		}
		if _, err := newCLIAPIClient(opts.apiOptions); err != nil {
			writeCLIAPIError(errOut, err)
			return commandExitError{code: runCommandErrorExitCode(err)}
		}
		stopLocal, err = startLocalRunServe(ctx, root, opts, errOut)
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
	traceReplaySince := time.Now().UTC()
	start, err := runCommandStart(ctx, root, client, health, opts, payload)
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
	if o.mcpPort < 0 || o.mcpPort > 65535 || (o.changedFlags["mcp-port"] && o.mcpPort == 0) {
		return fmt.Errorf("--mcp-port must be between 1 and 65535")
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
	if o.changedFlags["platform-spec"] {
		return fmt.Errorf("--platform-spec is retired; the swarm binary embeds its own platform spec. Use config paths.platform_spec_path only for platform spec development")
	}
	for _, value := range o.dataImports {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("--data must be name=file.jsonl")
		}
	}
	for _, value := range o.dataPins {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("--pin must be name@head, name@vN, or name@ResourceVersionID")
		}
	}
	if o.changedFlags["api-port"] {
		effectiveMCPAddr := defaultMCPListenAddr
		if o.changedFlags["mcp-port"] {
			effectiveMCPAddr = net.JoinHostPort("127.0.0.1", strconv.Itoa(o.mcpPort))
		}
		_, mcpPort, err := net.SplitHostPort(effectiveMCPAddr)
		if err != nil {
			return fmt.Errorf("MCP listener address %q is invalid: %w", effectiveMCPAddr, err)
		}
		if strconv.Itoa(o.apiPort) == mcpPort {
			return fmt.Errorf("--api-port %d conflicts with MCP listener %s", o.apiPort, effectiveMCPAddr)
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
		for _, flag := range []string{"bundle-hash", "config", "backend", "contracts", "data", "pin", "api-port", "mcp-port"} {
			if o.changedFlags[flag] {
				return fmt.Errorf("--reattach is mutually exclusive with --%s", flag)
			}
		}
		return nil
	}
	if strings.TrimSpace(o.connectURL) != "" {
		for _, flag := range []string{"config", "backend", "contracts", "api-port", "mcp-port"} {
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

func (o runCommandOptions) withLocalForegroundServeAuth(root InvocationRoot) (runCommandOptions, error) {
	serveOpts := DefaultServeOptions()
	serveOpts.ConfigPath = o.configPath
	auth, err := ResolveServeAPIAuth(root, serveOpts)
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

type runStartupOutput struct {
	mu      sync.Mutex
	buffer  bytes.Buffer
	discard bool
}

func (o *runStartupOutput) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.discard {
		return len(p), nil
	}
	return o.buffer.Write(p)
}

func (o *runStartupOutput) finish() []byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.discard = true
	output := append([]byte(nil), o.buffer.Bytes()...)
	o.buffer.Reset()
	return output
}

func startLocalRunServe(ctx context.Context, root InvocationRoot, opts runCommandOptions, startupErrOut io.Writer) (func(), error) {
	opts.apiOptions.invocationRoot = root
	repo := root.Path()
	runServe := opts.apiOptions.runServe
	if runServe == nil {
		return nil, fmt.Errorf("serve runtime is unavailable")
	}
	resolvedPaths, err := ResolveCLIContractPlatformSpecPaths(repo, CLIContractPlatformSpecPathOptions{
		ContractsPath:    opts.contractsPath,
		PlatformSpecPath: opts.platformSpecPath,
		ConfigPath:       opts.configPath,
	})
	if err != nil {
		return nil, err
	}
	releaseProjectClaim, err := prepareLocalRunProjectClaim(ctx, repo, opts, resolvedPaths)
	if err != nil {
		return nil, err
	}
	serveOpts := DefaultServeOptions()
	swarmDirOpts := opts.apiOptions.swarmDirResolutionOptions()
	serveOpts.SwarmDir = swarmDirOpts.SwarmDir
	serveOpts.SwarmDirSet = swarmDirOpts.SwarmDirFlagSet
	serveOpts.ConfigPath = opts.configPath
	serveOpts.Backend = opts.backend
	serveOpts.ContractsPath = resolvedPaths.ContractsPath
	serveOpts.PlatformSpecPath = resolvedPaths.PlatformSpecPath
	serveOpts.LocalRun = true
	var startupOutput runStartupOutput
	serveOpts.Output = &startupOutput
	serveOpts.ErrorOutput = &startupOutput
	if opts.apiPort > 0 {
		serveOpts.APIListenAddr = net.JoinHostPort("127.0.0.1", strconv.Itoa(opts.apiPort))
	}
	if opts.mcpPort > 0 {
		serveOpts.MCPListenAddr = net.JoinHostPort("127.0.0.1", strconv.Itoa(opts.mcpPort))
	}
	serveCtx, cancel := context.WithCancel(ctx)
	done := make(chan int, 1)
	go func() {
		done <- runServe(serveCtx, root, serveOpts)
		close(done)
	}()
	stop := func() {
		cancel()
		<-done
		if releaseProjectClaim != nil {
			releaseProjectClaim()
			releaseProjectClaim = nil
		}
	}
	if err := waitForRunCommandReady(ctx, opts, done); err != nil {
		stop()
		if output := startupOutput.finish(); startupErrOut != nil && len(output) > 0 {
			_, _ = startupErrOut.Write(output)
		}
		return nil, err
	}
	startupOutput.finish()
	return stop, nil
}

func prepareLocalRunProjectClaim(ctx context.Context, repo string, opts runCommandOptions, resolvedPaths CLIContractPlatformSpecPaths) (func(), error) {
	project := resolveLocalRuntimeStateProject(repo, resolvedPaths)
	if strings.TrimSpace(project.CanonicalProjectRoot) == "" {
		return nil, nil
	}
	cliCfg, err := loadCLICommandConfigWithOptions(unifiedConfigLoadOptions{RepoRoot: repo, ExplicitPath: opts.configPath})
	if err != nil {
		return nil, err
	}
	swarmDir, err := resolveCLISwarmDirFromConfig(opts.apiOptions.invocationRoot, opts.apiOptions.swarmDirResolutionOptions(), cliCfg)
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

func runCommandStart(ctx context.Context, root InvocationRoot, client *cliAPIClient, health diagnosticHealthCheckResult, opts runCommandOptions, payload map[string]any) (runStartResult, error) {
	params := map[string]any{
		"event_name": strings.TrimSpace(opts.eventName),
		"payload":    payload,
	}
	if bundleHash := strings.TrimSpace(opts.bundleHash); bundleHash != "" {
		params["bundle_hash"] = bundleHash
	} else if bundleHash := strings.TrimSpace(health.Bundle.BundleHash); bundleHash != "" {
		params["bundle_hash"] = bundleHash
	}
	runID := strings.TrimSpace(opts.runID)
	if len(opts.dataImports) > 0 || len(opts.dataPins) > 0 {
		if runID == "" {
			runID = uuid.NewString()
		}
		bundleHash, _ := params["bundle_hash"].(string)
		data, err := buildRunDataEnvelope(ctx, root, client, bundleHash, runID, opts.dataImports, opts.dataPins)
		if err != nil {
			return runStartResult{}, err
		}
		params["data"] = data
	}
	if runID != "" {
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
	switch result.DataBinding.State {
	case "none":
		if result.DataBinding.RunID != "" || result.DataBinding.Evidence != nil {
			return fmt.Errorf("malformed run.start result: unbound data_binding contains evidence")
		}
	case "bound":
		if result.DataBinding.RunID != result.RunID || result.DataBinding.Evidence == nil || result.DataBinding.PinCount < 1 || result.DataBinding.ImportCount < 0 {
			return fmt.Errorf("malformed run.start result: bound data_binding is contradictory")
		}
	default:
		return fmt.Errorf("malformed run.start result: data_binding.state is required")
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
	poll := opts.apiOptions.runStatusPoll
	if poll <= 0 {
		poll = time.Second
	}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	observer := startForegroundRunTraceObserver(
		ctx,
		wsEndpoint,
		client.token,
		runID,
		replaySince,
		opts.apiOptions.runTraceAttachTimeout,
	)
	renderer := startForegroundRunTraceRenderer(out, errOut, observer.rows)
	defer func() {
		observer.stop()
		renderer.stop()
	}()
	detached := observer.detached
	reportDetach := func(det runTraceDetach) {
		renderer.enqueueDetached(runTraceObserverDetachedFact{
			Type:            runTraceObserverDetachedType,
			Severity:        runTraceObserverWarning,
			ReasonCode:      det.reason,
			RunID:           runID,
			RunContinues:    true,
			ReattachCommand: runCommandReattachGuidance(runID, opts.connectURL),
			Message:         det.message,
		})
		detached = nil
	}
	settleObservation := func() {
		observer.stop()
		select {
		case det := <-detached:
			reportDetach(det)
		default:
		}
		renderer.stop()
	}
	for {
		select {
		case det := <-detached:
			reportDetach(det)
			continue
		default:
		}
		select {
		case <-ctx.Done():
			var stopErr error
			if stopOnInterrupt {
				stopErr = runCommandStop(context.Background(), client, runID)
			}
			settleObservation()
			if stopErr != nil {
				fmt.Fprintln(errOut, "interrupted; run.stop failed")
				writeCLIAPIError(errOut, stopErr)
				return commandExitError{code: 130}
			}
			if stopOnInterrupt {
				fmt.Fprintln(errOut, "interrupted; requested run.stop")
			} else {
				fmt.Fprintln(errOut, "detached from run trace")
			}
			return commandExitError{code: 130}
		case det := <-detached:
			reportDetach(det)
		case <-ticker.C:
			run, err := runCommandGet(ctx, client, runID)
			if err != nil {
				settleObservation()
				writeCLIAPIError(errOut, err)
				return commandExitError{code: runCommandErrorExitCode(err)}
			}
			if runCommandTerminalStatus(run.Status) {
				settleObservation()
				writeRunCommandTerminalSummary(out, run)
				return runCommandTerminalExit(run.Status)
			}
		}
	}
}

func startForegroundRunTraceRenderer(out, errOut io.Writer, rows <-chan diagnosticRunTraceRow) *foregroundRunTraceRenderer {
	renderer := &foregroundRunTraceRenderer{
		detached: make(chan runTraceObserverDetachedFact, 1),
	}
	renderer.workers.Add(2)
	go func() {
		defer renderer.workers.Done()
		writer := &runTraceRowLineWriter{}
		for row := range rows {
			writer.Write(out, row)
		}
	}()
	go func() {
		defer renderer.workers.Done()
		for fact := range renderer.detached {
			writeRunTraceObserverDetached(errOut, fact)
		}
	}()
	return renderer
}

func (r *foregroundRunTraceRenderer) enqueueDetached(fact runTraceObserverDetachedFact) {
	select {
	case r.detached <- fact:
	default:
		panic("foreground run trace renderer received more than one detach fact")
	}
}

func (r *foregroundRunTraceRenderer) stop() {
	if r == nil {
		return
	}
	r.stopOnce.Do(func() {
		close(r.detached)
		r.workers.Wait()
	})
}

func startForegroundRunTraceObserver(ctx context.Context, wsEndpoint, token, runID string, replaySince *time.Time, attachTimeout time.Duration) *foregroundRunTraceObserver {
	if attachTimeout <= 0 {
		attachTimeout = defaultRunTraceAttachTimeout
	}
	observerCtx, cancel := context.WithCancel(ctx)
	rows := make(chan diagnosticRunTraceRow)
	detached := make(chan runTraceDetach, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(rows)

		attachCtx, cancelAttach := context.WithTimeout(observerCtx, attachTimeout)
		sub, err := subscribeRunTrace(attachCtx, wsEndpoint, token, runID, replaySince, nil)
		attachTimedOut := errors.Is(attachCtx.Err(), context.DeadlineExceeded)
		cancelAttach()
		if err != nil {
			if observerCtx.Err() != nil {
				return
			}
			det := runTraceDetachFromError(err, runTraceDetachAttachFailed)
			if attachTimedOut {
				det = runTraceDetach{reason: runTraceDetachAttachTimedOut}
			}
			detached <- det
			return
		}
		defer sub.close()
		if attachTimedOut {
			detached <- runTraceDetach{reason: runTraceDetachAttachTimedOut}
			return
		}

		for {
			if det, terminated := runTraceSubscriptionDetachReason(sub); terminated {
				detached <- det
				return
			}
			select {
			case <-observerCtx.Done():
				settleRunTraceSubscription(sub, detached)
				return
			case row, ok := <-sub.rows:
				if !ok {
					det, terminated := runTraceSubscriptionDetachReason(sub)
					if !terminated {
						det = runTraceDetach{reason: runTraceDetachStreamClosed}
					}
					detached <- det
					return
				}
				select {
				case rows <- row:
				case <-observerCtx.Done():
					settleRunTraceSubscription(sub, detached)
					return
				case err := <-sub.errs:
					detached <- runTraceDetachFromError(err, runTraceDetachTransportLost)
					return
				case <-sub.done:
					det, _ := runTraceSubscriptionDetachReason(sub)
					detached <- det
					return
				}
			case err := <-sub.errs:
				if err != nil {
					detached <- runTraceDetachFromError(err, runTraceDetachTransportLost)
					return
				}
			}
		}
	}()
	return &foregroundRunTraceObserver{rows: rows, detached: detached, cancel: cancel, done: done}
}

// settleRunTraceSubscription closes and joins the subscription read loop, then
// publishes the pending detach reason if the read loop reported an error.
// Closing first makes an in-flight error publication (e.g. queue overflow)
// durable before the check — its send happens-before close(done). A healthy
// stream close (no error) stays silent: the cancel path must not synthesize
// stream_closed for a subscription that closed normally.
func settleRunTraceSubscription(sub *runTraceSubscription, detached chan<- runTraceDetach) {
	if sub == nil {
		return
	}
	sub.close()
	select {
	case err := <-sub.errs:
		if err != nil {
			detached <- runTraceDetachFromError(err, runTraceDetachTransportLost)
		}
	default:
	}
}

func runTraceSubscriptionDetachReason(sub *runTraceSubscription) (runTraceDetach, bool) {
	select {
	case err := <-sub.errs:
		return runTraceDetachFromError(err, runTraceDetachTransportLost), true
	case <-sub.done:
		select {
		case err := <-sub.errs:
			return runTraceDetachFromError(err, runTraceDetachTransportLost), true
		default:
			return runTraceDetach{reason: runTraceDetachStreamClosed}, true
		}
	default:
		return runTraceDetach{}, false
	}
}

func (o *foregroundRunTraceObserver) stop() {
	if o == nil {
		return
	}
	o.stopOnce.Do(func() {
		o.cancel()
		<-o.done
	})
}

type runTraceDetach struct {
	reason  runTraceDetachReason
	message string
}

func runTraceDetachFromError(err error, fallback runTraceDetachReason) runTraceDetach {
	var budgetErr *cliAPIResponseBudgetError
	if errors.As(err, &budgetErr) && budgetErr != nil {
		return runTraceDetach{reason: runTraceDetachResponseOverBudget, message: budgetErr.Error()}
	}
	var observerErr *runTraceObserverError
	if errors.As(err, &observerErr) && observerErr != nil && observerErr.reason != "" {
		return runTraceDetach{reason: observerErr.reason}
	}
	return runTraceDetach{reason: fallback}
}

func wrapRunTraceObserverError(reason runTraceDetachReason, err error) error {
	if err == nil {
		return nil
	}
	return &runTraceObserverError{reason: reason, err: err}
}

func subscribeRunTrace(ctx context.Context, wsEndpoint, token, runID string, replaySince *time.Time, extraParams map[string]any) (*runTraceSubscription, error) {
	header := http.Header{"Authorization": []string{"Bearer " + token}}
	conn, resp, err := websocket.DefaultDialer.DialContext(ctx, wsEndpoint, header)
	if err != nil {
		if resp != nil {
			return nil, wrapRunTraceObserverError(runTraceDetachAttachFailed, cliAPIWebSocketHTTPError("runtime event stream", wsEndpoint, resp))
		}
		return nil, wrapRunTraceObserverError(runTraceDetachAttachFailed, &cliAPITransportError{surface: "runtime event stream", endpoint: wsEndpoint, operation: "dial", err: err})
	}
	stopCancellationCloser := closeRunTraceWebSocketOnContext(ctx, conn)
	defer stopCancellationCloser()
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
		return nil, wrapRunTraceObserverError(runTraceDetachAttachFailed, &cliAPITransportError{surface: "runtime event stream", endpoint: wsEndpoint, operation: "subscription request", err: err})
	}
	var envelope jsonRPCResponse
	if err := cliAPIReadWebSocketJSON(conn, "run.subscribe_trace", "runtime event stream", wsEndpoint, "subscription response", &envelope); err != nil {
		conn.Close()
		var protocolErr *cliAPIProtocolError
		if errors.As(err, &protocolErr) {
			return nil, wrapRunTraceObserverError(runTraceDetachSubscriptionResponseInvalid, err)
		}
		return nil, wrapRunTraceObserverError(runTraceDetachAttachFailed, err)
	}
	if envelope.JSONRPC != "2.0" {
		conn.Close()
		return nil, wrapRunTraceObserverError(runTraceDetachSubscriptionResponseInvalid, &cliAPIProtocolError{surface: "runtime event stream", endpoint: wsEndpoint, operation: "subscription response", err: fmt.Errorf("jsonrpc=%q", envelope.JSONRPC)})
	}
	if id, ok := envelope.ID.(string); !ok || id != requestID {
		conn.Close()
		return nil, wrapRunTraceObserverError(runTraceDetachSubscriptionResponseInvalid, &cliAPIProtocolError{surface: "runtime event stream", endpoint: wsEndpoint, operation: "subscription response", err: fmt.Errorf("id=%s, want %q", formatJSONRPCID(envelope.ID), requestID)})
	}
	if envelope.Error != nil {
		conn.Close()
		return nil, wrapRunTraceObserverError(runTraceDetachAttachFailed, envelope.Error)
	}
	var result runTraceSubscriptionResult
	if err := json.Unmarshal(envelope.Result, &result); err != nil {
		conn.Close()
		return nil, wrapRunTraceObserverError(runTraceDetachSubscriptionResponseInvalid, &cliAPIProtocolError{surface: "runtime event stream", endpoint: wsEndpoint, operation: "subscription result", err: err})
	}
	if strings.TrimSpace(result.SubscriptionID) == "" {
		conn.Close()
		return nil, wrapRunTraceObserverError(runTraceDetachSubscriptionResponseInvalid, &cliAPIProtocolError{surface: "runtime event stream", endpoint: wsEndpoint, operation: "subscription result", err: fmt.Errorf("subscription_id is required")})
	}
	sub := &runTraceSubscription{
		conn:           conn,
		endpoint:       wsEndpoint,
		subscriptionID: result.SubscriptionID,
		rows:           make(chan diagnosticRunTraceRow, 16),
		errs:           make(chan error, 1),
		done:           make(chan struct{}),
	}
	go sub.readLoop()
	return sub, nil
}

func (s *runTraceSubscription) readLoop() {
	defer close(s.done)
	defer close(s.rows)
	for {
		var notification runTraceNotification
		if err := cliAPIReadWebSocketJSON(s.conn, "run.subscribe_trace", "runtime event stream", s.endpoint, "notification read", &notification); err != nil {
			if cliAPIIsNormalWebSocketClose(err) {
				return
			}
			var protocolErr *cliAPIProtocolError
			if errors.As(err, &protocolErr) {
				s.reportError(wrapRunTraceObserverError(runTraceDetachNotificationInvalid, err))
			} else {
				s.reportError(wrapRunTraceObserverError(runTraceDetachTransportLost, err))
			}
			return
		}
		if notification.JSONRPC != "2.0" || notification.Method != "rpc.subscription" {
			s.reportError(wrapRunTraceObserverError(runTraceDetachNotificationInvalid, &cliAPIProtocolError{surface: "runtime event stream", endpoint: s.endpoint, operation: "notification", err: fmt.Errorf("malformed run.subscribe_trace notification")}))
			return
		}
		if notification.Params.Subscription != s.subscriptionID {
			s.reportError(wrapRunTraceObserverError(runTraceDetachSubscriptionMismatch, &cliAPIProtocolError{surface: "runtime event stream", endpoint: s.endpoint, operation: "notification", err: fmt.Errorf("subscription mismatch")}))
			return
		}
		row := notification.Params.Result
		if err := validateRunCommandTraceRow(row); err != nil {
			s.reportError(wrapRunTraceObserverError(runTraceDetachTraceRowInvalid, &cliAPIProtocolError{surface: "runtime event stream", endpoint: s.endpoint, operation: "notification", err: err}))
			return
		}
		select {
		case s.rows <- row:
		default:
			s.reportError(wrapRunTraceObserverError(runTraceDetachQueueOverflow, fmt.Errorf("run.subscribe_trace notification queue overflow")))
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
	s.closeOnce.Do(func() {
		_ = s.conn.Close()
	})
	if s.done != nil {
		<-s.done
	}
}

func closeRunTraceWebSocketOnContext(ctx context.Context, conn *websocket.Conn) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-stop:
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() { close(stop) })
		<-done
	}
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
	fmt.Fprintf(out, "run started: run_id=%s status=%s\n", result.RunID, formatCLIHumanCode(cliHumanCodeRunStatus, result.Status))
}

func writeRunCommandNoFollowGuidance(out io.Writer, runID, connectURL string) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "reattach: %s\n", runCommandReattachGuidance(runID, connectURL))
}

func runCommandReattachGuidance(runID, connectURL string) string {
	if connect := strings.TrimSpace(connectURL); connect != "" {
		return fmt.Sprintf("swarm run start --connect %s --reattach %s", connect, runID)
	}
	return fmt.Sprintf("swarm run start --reattach %s", runID)
}

func writeRunTraceObserverDetached(out io.Writer, fact runTraceObserverDetachedFact) {
	if out == nil {
		return
	}
	writeRunTraceObserverDetachedWithMode(out, cliOutputIsTerminal(out), fact)
}

func writeRunTraceObserverDetachedWithMode(out io.Writer, terminal bool, fact runTraceObserverDetachedFact) {
	if out == nil {
		return
	}
	if terminal {
		fmt.Fprintf(
			out,
			"WARNING: run trace observer detached for run %s (%s); the run continues. Reattach with `%s`.\n",
			fact.RunID,
			fact.ReasonCode,
			fact.ReattachCommand,
		)
		return
	}
	_ = json.NewEncoder(out).Encode(fact)
}

func writeRunCommandReattached(out io.Writer, run diagnosticRunHeader) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "reattached: run_id=%s status=%s\n", run.RunID, formatCLIHumanCode(cliHumanCodeRunStatus, run.Status))
}

type runTraceRowLineWriter struct {
	startedAt *time.Time
}

func (w *runTraceRowLineWriter) Write(out io.Writer, row diagnosticRunTraceRow) {
	if out == nil {
		return
	}
	at, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(row.EventCreatedAt))
	if err == nil {
		at = at.UTC()
		if w.startedAt == nil {
			startedAt := at
			w.startedAt = &startedAt
		}
	}
	relativeAt := strings.TrimSpace(row.EventCreatedAt)
	if err == nil && w.startedAt != nil {
		relativeAt = formatTraceOffset(at.Sub(*w.startedAt))
	}
	fields := []string{
		relativeAt,
		row.EventName,
		"id=" + row.EventID,
	}
	if row.EntityID != "" {
		fields = append(fields, "entity="+row.EntityID)
	}
	if row.DeliveryStatus != "" {
		fields = append(fields, "delivery="+formatCLIHumanCode(cliHumanCodeDeliveryStatus, row.DeliveryStatus))
	}
	if subscriber := formatTraceSubscriber(row); subscriber != "-" {
		fields = append(fields, "subscriber="+subscriber)
	}
	if row.SessionID != "" {
		fields = append(fields, "session="+row.SessionID)
	}
	if turn := firstNonEmpty(row.TurnID, row.TurnTriggerEventType); turn != "" {
		fields = append(fields, "turn="+turn)
	}
	fmt.Fprintf(out, "trace %s\n", strings.Join(fields, " "))
}

func writeRunCommandTerminalSummary(out io.Writer, run diagnosticRunHeader) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "run terminal: run_id=%s status=%s origin=%s event_count=%d entity_count=%d\n",
		run.RunID, formatCLIHumanCode(cliHumanCodeRunStatus, run.Status), diagnosticRunOriginLabel(run.Origin), intValue(run.EventCount), intValue(run.EntityCount))
	if run.Failure != nil {
		fmt.Fprintf(out, "failure=%s/%s message=%s remediation=%s\n", run.Failure.Class, run.Failure.Detail.Code, run.Failure.Message, run.Failure.Remediation)
	}
	if run.ControlReason != "" {
		fmt.Fprintf(out, "control_reason=%s\n", run.ControlReason)
	}
}

func intValue(v *int) int {
	if v == nil {
		return 0
	}
	return *v
}
