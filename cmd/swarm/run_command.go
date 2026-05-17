package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/spf13/cobra"
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
	bundleFingerprint string
	contractsPath     string
	platformSpecPath  string
	idempotencyKey    string
	runID             string
	apiPort           int
	mcpPort           int
	detach            bool
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
	subscriptionID string
	rows           chan diagnosticRunTraceRow
	errs           chan error
}

func newRunCommand(repo string, rootOpts rootCommandOptions) *cobra.Command {
	opts := runCommandOptions{apiOptions: rootOpts}
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Start or reattach to a Swarm run through v1 RPC and trace subscriptions.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runRunCommand(cmd.Context(), repo, cmd.OutOrStdout(), cmd.ErrOrStderr(), opts)
		},
	}
	cmd.Flags().StringVar(&opts.eventName, "event", "", "Declared event name to publish as the run trigger")
	cmd.Flags().StringVar(&opts.payloadPath, "payload", "", "Path to JSON object payload file")
	cmd.Flags().StringVar(&opts.connectURL, "connect", "", "Existing Swarm API base URL")
	cmd.Flags().BoolVar(&opts.noFollow, "no-follow", false, "Start through a connected server and print the run id without opening a trace subscription")
	cmd.Flags().StringVar(&opts.reattachRunID, "reattach", "", "Existing run id to reattach to")
	cmd.Flags().StringVar(&opts.bundleFingerprint, "bundle-fingerprint", "", "Expected server bundle fingerprint")
	cmd.Flags().StringVar(&opts.contractsPath, "contracts", "", "Path to Swarm contract bundle root for local foreground startup")
	cmd.Flags().StringVar(&opts.platformSpecPath, "platform-spec", defaultPlatformSpecPath, "Path to platform spec yaml for local foreground startup")
	cmd.Flags().StringVar(&opts.idempotencyKey, "idempotency-key", "", "Optional idempotency key for run.start")
	cmd.Flags().StringVar(&opts.runID, "run-id", "", "Optional caller-provided run id for run.start")
	cmd.Flags().IntVar(&opts.apiPort, "api-port", 0, "Local API/health port for local foreground startup")
	cmd.Flags().IntVar(&opts.mcpPort, "mcp-port", 0, "Reserved local MCP port for local foreground startup")
	cmd.Flags().BoolVar(&opts.detach, "detach", false, "Unsupported in CLI v2; use --connect with --no-follow")
	return cmd
}

func runRunCommand(ctx context.Context, repo string, out, errOut io.Writer, opts runCommandOptions) error {
	if err := opts.validate(); err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: 2}
	}
	apiOpts, wsEndpoint, err := opts.runtimeEndpoints()
	if err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: 2}
	}
	opts.apiOptions = apiOpts

	if strings.TrimSpace(opts.reattachRunID) != "" {
		return runReattachCommand(ctx, out, errOut, opts, wsEndpoint)
	}

	payload, err := loadRunCommandPayload(opts.payloadPath)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: 2}
	}

	var stopLocal func()
	if strings.TrimSpace(opts.connectURL) == "" {
		if err := loadRepoDotEnv(repo); err != nil {
			fmt.Fprintf(errOut, "load .env: %v\n", err)
			return commandExitError{code: 3}
		}
		if _, err := newCLIAPIClient(opts.apiOptions); err != nil {
			fmt.Fprintln(errOut, err)
			return commandExitError{code: runCommandErrorExitCode(err)}
		}
		stopLocal, err = startLocalRunServe(ctx, repo, opts)
		if err != nil {
			fmt.Fprintln(errOut, err)
			return commandExitError{code: 3}
		}
		defer stopLocal()
	}

	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: runCommandErrorExitCode(err)}
	}
	health, err := runCommandHealth(ctx, client)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: runCommandErrorExitCode(err)}
	}
	if expected := strings.TrimSpace(opts.bundleFingerprint); expected != "" && expected != health.Bundle.Fingerprint {
		fmt.Fprintf(errOut, "bundle fingerprint mismatch: server=%s expected=%s\n", health.Bundle.Fingerprint, expected)
		return commandExitError{code: 6}
	}
	start, err := runCommandStart(ctx, client, health, opts, payload)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: runCommandErrorExitCode(err)}
	}
	writeRunCommandStarted(out, start)
	if opts.noFollow {
		writeRunCommandNoFollowGuidance(out, start.RunID, opts.connectURL)
		return nil
	}
	return followRunCommand(ctx, out, errOut, client, opts, wsEndpoint, start.RunID, true)
}

func (o runCommandOptions) validate() error {
	if o.detach {
		return fmt.Errorf("ERROR: `swarm run --detach` is not supported in CLI v2. Use `swarm serve` plus `swarm run --connect <url> --event <name> --payload <file> --no-follow`.")
	}
	if o.apiPort < 0 || o.apiPort > 65535 {
		return fmt.Errorf("--api-port must be between 1 and 65535")
	}
	if o.mcpPort < 0 || o.mcpPort > 65535 {
		return fmt.Errorf("--mcp-port must be between 1 and 65535")
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
		return nil
	}
	if strings.TrimSpace(o.eventName) == "" {
		return fmt.Errorf("--event is required")
	}
	if strings.TrimSpace(o.payloadPath) == "" {
		return fmt.Errorf("--payload is required")
	}
	return nil
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
		rpcEndpoint = strings.TrimSpace(opts.apiEndpoint)
		if rpcEndpoint == "" {
			rpcEndpoint = defaultCLIAPIEndpoint
		}
		var err error
		wsEndpoint, err = runCommandWebSocketEndpoint(rpcEndpoint)
		if err != nil {
			return opts, "", err
		}
	}
	opts.apiEndpoint = rpcEndpoint
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
	path := strings.TrimRight(parsed.Path, "/")
	if strings.HasSuffix(path, "/v1/rpc") {
		parsed.Path = strings.TrimSuffix(path, "/v1/rpc") + "/v1/ws"
	} else {
		return "", fmt.Errorf("derive /v1/ws endpoint: API endpoint must end in /v1/rpc")
	}
	return parsed.String(), nil
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
	serveOpts := defaultServeOptions()
	serveOpts.ContractsPath = opts.contractsPath
	serveOpts.PlatformSpecPath = opts.platformSpecPath
	if opts.apiPort > 0 {
		serveOpts.HealthAddr = ":" + strconv.Itoa(opts.apiPort)
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
	}
	if err := waitForRunCommandReady(ctx, opts, done); err != nil {
		stop()
		return nil, err
	}
	return stop, nil
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
		"bundle_ref": map[string]any{
			"fingerprint": health.Bundle.Fingerprint,
		},
		"event_name": strings.TrimSpace(opts.eventName),
		"payload":    payload,
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
		fmt.Fprintln(errOut, err)
		return commandExitError{code: runCommandErrorExitCode(err)}
	}
	runID := strings.TrimSpace(opts.reattachRunID)
	run, err := runCommandGet(ctx, client, runID)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: runCommandErrorExitCode(err)}
	}
	if runCommandTerminalStatus(run.Status) {
		writeRunCommandTerminalSummary(out, run)
		return runCommandTerminalExit(run.Status)
	}
	writeRunCommandReattached(out, run)
	return followRunCommand(ctx, out, errOut, client, opts, wsEndpoint, runID, false)
}

func followRunCommand(ctx context.Context, out, errOut io.Writer, client *cliAPIClient, opts runCommandOptions, wsEndpoint, runID string, stopOnInterrupt bool) error {
	sub, err := subscribeRunTrace(ctx, wsEndpoint, client.token, runID)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: runCommandErrorExitCode(err)}
	}
	defer sub.close()
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
					fmt.Fprintf(errOut, "interrupted; run.stop failed: %v\n", err)
					return commandExitError{code: 130}
				}
			}
			if stopOnInterrupt {
				fmt.Fprintln(errOut, "interrupted; requested run.stop")
			} else {
				fmt.Fprintln(errOut, "detached from run trace")
			}
			return commandExitError{code: 130}
		case row, ok := <-sub.rows:
			if !ok {
				continue
			}
			writeRunCommandTraceRow(out, row)
		case err := <-sub.errs:
			if err != nil {
				fmt.Fprintln(errOut, err)
				return commandExitError{code: runCommandErrorExitCode(err)}
			}
		case <-ticker.C:
			run, err := runCommandGet(ctx, client, runID)
			if err != nil {
				fmt.Fprintln(errOut, err)
				return commandExitError{code: runCommandErrorExitCode(err)}
			}
			if runCommandTerminalStatus(run.Status) {
				writeRunCommandTerminalSummary(out, run)
				return runCommandTerminalExit(run.Status)
			}
		}
	}
}

func subscribeRunTrace(ctx context.Context, wsEndpoint, token, runID string) (*runTraceSubscription, error) {
	header := http.Header{"Authorization": []string{"Bearer " + token}}
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsEndpoint, header)
	if err != nil {
		return nil, fmt.Errorf("v1 WS dial failed: %w", err)
	}
	requestID := "swarm-cli:" + runCommandMethodSubscribeTrace
	if err := conn.WriteJSON(jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      requestID,
		Method:  runCommandMethodSubscribeTrace,
		Params:  map[string]any{"run_id": runID},
	}); err != nil {
		conn.Close()
		return nil, fmt.Errorf("write run.subscribe_trace request: %w", err)
	}
	var envelope jsonRPCResponse
	if err := conn.ReadJSON(&envelope); err != nil {
		conn.Close()
		return nil, fmt.Errorf("read run.subscribe_trace response: %w", err)
	}
	if envelope.JSONRPC != "2.0" {
		conn.Close()
		return nil, fmt.Errorf("malformed run.subscribe_trace response: jsonrpc=%q", envelope.JSONRPC)
	}
	if id, ok := envelope.ID.(string); !ok || id != requestID {
		conn.Close()
		return nil, fmt.Errorf("malformed run.subscribe_trace response: id=%s, want %q", formatJSONRPCID(envelope.ID), requestID)
	}
	if envelope.Error != nil {
		conn.Close()
		return nil, envelope.Error
	}
	var result runTraceSubscriptionResult
	if err := json.Unmarshal(envelope.Result, &result); err != nil {
		conn.Close()
		return nil, fmt.Errorf("decode run.subscribe_trace result: %w", err)
	}
	if strings.TrimSpace(result.SubscriptionID) == "" {
		conn.Close()
		return nil, fmt.Errorf("malformed run.subscribe_trace result: subscription_id is required")
	}
	sub := &runTraceSubscription{
		conn:           conn,
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
		if err := s.conn.ReadJSON(&notification); err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) || strings.Contains(err.Error(), "use of closed network connection") {
				return
			}
			s.reportError(fmt.Errorf("read run.subscribe_trace notification: %w", err))
			return
		}
		if notification.JSONRPC != "2.0" || notification.Method != "rpc.subscription" {
			s.reportError(fmt.Errorf("malformed run.subscribe_trace notification"))
			return
		}
		if notification.Params.Subscription != s.subscriptionID {
			s.reportError(fmt.Errorf("malformed run.subscribe_trace notification: subscription mismatch"))
			return
		}
		row := notification.Params.Result
		if err := validateRunCommandTraceRow(row); err != nil {
			s.reportError(err)
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
	if err == nil {
		return 0
	}
	if strings.Contains(err.Error(), "SWARM_API_TOKEN is required") {
		return 4
	}
	var httpErr *cliAPIHTTPError
	if errors.As(err, &httpErr) {
		if httpErr.statusCode == http.StatusUnauthorized || httpErr.statusCode == http.StatusForbidden {
			return 4
		}
		return 3
	}
	var rpcErr *jsonRPCError
	if errors.As(err, &rpcErr) {
		switch applicationErrorCode(rpcErr.Data) {
		case "UNAUTHORIZED":
			return 4
		case "RUN_NOT_FOUND":
			return 5
		case "BUNDLE_MISMATCH", "UNSUPPORTED_BUNDLE_REF", "IDEMPOTENCY_CONFLICT":
			return 6
		default:
			return 3
		}
	}
	return 3
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
		fmt.Fprintf(out, "reattach: swarm run --connect %s --reattach %s\n", connect, runID)
		return
	}
	fmt.Fprintf(out, "reattach: swarm run --reattach %s\n", runID)
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
