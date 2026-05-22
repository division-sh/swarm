package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

const (
	traceFollowMaxReconnects    = 3
	traceFollowInitialBackoff   = 250 * time.Millisecond
	traceFollowMaximumBackoff   = time.Second
	traceFollowRetryableReadErr = "read run.subscribe_trace notification:"
)

type diagnosticRunListOptions struct {
	apiOptions rootCommandOptions
	output     cliOutputOptions
	logging    cliLoggingOptions
	status     string
	since      string
	until      string
	limit      int
	cursor     string
}

type diagnosticTraceOptions struct {
	apiOptions rootCommandOptions
	follow     bool
	noRetry    bool
	since      string
	limit      int
	cursor     string
	sinceSet   bool
	limitSet   bool
	cursorSet  bool
}

type diagnosticRunOptions struct {
	apiOptions rootCommandOptions
	output     cliOutputOptions
	logging    cliLoggingOptions
	noDiagnose bool
}

type diagnosticRunListResult struct {
	Runs       []diagnosticRunHeader `json:"runs"`
	NextCursor string                `json:"next_cursor,omitempty"`
}

type diagnosticRunGetResult struct {
	Run diagnosticRunHeader `json:"run"`
}

type diagnosticRunDiagnosisResult struct {
	Run              diagnosticRunHeader `json:"run"`
	OperationalState *string             `json:"operational_state"`
	BlockingLayer    *string             `json:"blocking_layer"`
	BlockingReason   *string             `json:"blocking_reason"`
	Heuristics       []string            `json:"heuristics"`
}

type diagnosticRunTraceResult struct {
	Trace      []diagnosticRunTraceRow `json:"trace"`
	NextCursor string                  `json:"next_cursor,omitempty"`
}

type diagnosticHealthCheckResult struct {
	Alive     *bool                    `json:"alive"`
	Ready     *bool                    `json:"ready"`
	DBOK      *bool                    `json:"db_ok"`
	RuntimeOK *bool                    `json:"runtime_ok"`
	Bundle    diagnosticBundleIdentity `json:"bundle"`
}

type diagnosticBundleIdentity struct {
	WorkflowName    *string `json:"workflow_name"`
	WorkflowVersion *string `json:"workflow_version"`
	Fingerprint     string  `json:"fingerprint"`
}

type diagnosticRunHeader struct {
	RunID            string `json:"run_id"`
	Status           string `json:"status"`
	TriggerEventType string `json:"trigger_event_type"`
	TriggerEventID   string `json:"trigger_event_id"`
	EntityCount      *int   `json:"entity_count"`
	EventCount       *int   `json:"event_count"`
	StartedAt        string `json:"started_at"`
	EndedAt          string `json:"ended_at,omitempty"`
	ForkedFromRunID  string `json:"forked_from_run_id,omitempty"`
	ErrorSummary     string `json:"error_summary,omitempty"`
}

type diagnosticRunTraceRow struct {
	EventID              string `json:"event_id"`
	EventName            string `json:"event_name"`
	EventCreatedAt       string `json:"event_created_at"`
	EntityID             string `json:"entity_id,omitempty"`
	DeliveryStatus       string `json:"delivery_status,omitempty"`
	SubscriberType       string `json:"subscriber_type,omitempty"`
	SubscriberID         string `json:"subscriber_id,omitempty"`
	SessionID            string `json:"session_id,omitempty"`
	TurnID               string `json:"turn_id,omitempty"`
	TurnTriggerEventType string `json:"turn_trigger_event_type,omitempty"`
}

var diagnosticValidRunStatuses = map[string]struct{}{
	"running":   {},
	"paused":    {},
	"completed": {},
	"failed":    {},
	"cancelled": {},
	"forked":    {},
}

var diagnosticValidOperationalStates = map[string]struct{}{
	"running":   {},
	"stalled":   {},
	"paused":    {},
	"completed": {},
	"failed":    {},
	"cancelled": {},
	"forked":    {},
}

func newRunsCommand(opts rootCommandOptions) *cobra.Command {
	runOpts := diagnosticRunListOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "runs",
		Short: "List runs through the v1 RPC read owner.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if cmd.Flags().Changed("limit") && runOpts.limit == 0 {
				return fmt.Errorf("--limit must be between 1 and 500")
			}
			if err := runOpts.logging.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			if err := runOpts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			return runDiagnosticRunListCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), runOpts)
		},
	}
	bindDiagnosticRunListFlags(cmd, &runOpts)
	bindCLIOutputFlags(cmd, &runOpts.output)
	bindCLILoggingFlags(cmd, &runOpts.logging)
	bindCLIAPIConnectionFlags(cmd, &runOpts.apiOptions)
	return cmd
}

func newHealthCommand(opts rootCommandOptions) *cobra.Command {
	apiOpts := opts
	outputOpts := cliOutputOptions{}
	loggingOpts := cliLoggingOptions{}
	cmd := &cobra.Command{
		Use:   "health",
		Short: "Print structured operator health through v1 RPC.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := loggingOpts.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			if err := outputOpts.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			return runDiagnosticHealthCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), apiOpts, outputOpts)
		},
	}
	bindCLIOutputFlags(cmd, &outputOpts)
	bindCLILoggingFlags(cmd, &loggingOpts)
	bindCLIAPIConnectionFlags(cmd, &apiOpts)
	return cmd
}

func newStatusCommand(opts rootCommandOptions) *cobra.Command {
	runOpts := diagnosticRunOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "status [run-id]",
		Short: "Diagnose one run through the v1 RPC read owner.",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runID := ""
			if len(args) == 1 {
				runID = args[0]
			}
			if err := runOpts.logging.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			if err := runOpts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			return runDiagnosticRunCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), runOpts, runID)
		},
	}
	cmd.Flags().BoolVar(&runOpts.noDiagnose, "no-diagnose", false, "Use run.get and print only the canonical run header")
	bindCLIOutputFlags(cmd, &runOpts.output)
	bindCLILoggingFlags(cmd, &runOpts.logging)
	bindCLIAPIConnectionFlags(cmd, &runOpts.apiOptions)
	return cmd
}

func newTraceCommand(opts rootCommandOptions) *cobra.Command {
	traceOpts := diagnosticTraceOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "trace [run-id]",
		Short: "Print or follow a run trace through v1 API owners.",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			traceOpts.sinceSet = cmd.Flags().Changed("since")
			traceOpts.limitSet = cmd.Flags().Changed("limit")
			traceOpts.cursorSet = cmd.Flags().Changed("cursor")
			runID := ""
			if len(args) == 1 {
				runID = args[0]
			}
			return runDiagnosticTraceCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), traceOpts, runID)
		},
	}
	cmd.Flags().BoolVarP(&traceOpts.follow, "follow", "f", false, "Follow live trace rows through /v1/ws run.subscribe_trace")
	cmd.Flags().BoolVar(&traceOpts.noRetry, "no-retry", false, "Disable trace follow reconnect/recovery retries")
	cmd.Flags().StringVar(&traceOpts.since, "since", "", "Snapshot-only RFC3339 trace materialization watermark")
	cmd.Flags().IntVar(&traceOpts.limit, "limit", 0, "Snapshot-only page size, 1-2000")
	cmd.Flags().StringVar(&traceOpts.cursor, "cursor", "", "Snapshot-only pagination cursor")
	bindCLIAPIConnectionFlags(cmd, &traceOpts.apiOptions)
	return cmd
}

func newInvestigateCommand(opts rootCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:                "investigate",
		Short:              "Retired legacy namespace; use swarm runs/status/trace/health.",
		Hidden:             true,
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			writeInvestigateRetiredMessage(cmd.ErrOrStderr())
			return commandExitError{code: 2}
		},
	}
	cmd.AddCommand(
		newInvestigateRunsCommand(opts),
		newInvestigateRunCommand(opts),
		newInvestigateTraceCommand(opts),
		newInvestigateHealthCommand(),
	)
	return cmd
}

func newInvestigateRunsCommand(opts rootCommandOptions) *cobra.Command {
	return &cobra.Command{
		Use:                "runs",
		Short:              "Retired legacy command; use swarm runs.",
		Hidden:             true,
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			writeInvestigateRunsRetiredMessage(cmd.ErrOrStderr())
			return commandExitError{code: 2}
		},
	}
}

func newInvestigateRunCommand(opts rootCommandOptions) *cobra.Command {
	return &cobra.Command{
		Use:                "run [run-id]",
		Short:              "Retired legacy command; use swarm status.",
		Hidden:             true,
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			writeInvestigateRunRetiredMessage(cmd.ErrOrStderr())
			return commandExitError{code: 2}
		},
	}
}

func newInvestigateTraceCommand(opts rootCommandOptions) *cobra.Command {
	return &cobra.Command{
		Use:                "trace [run-id]",
		Short:              "Retired legacy command; use swarm trace.",
		Hidden:             true,
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			writeInvestigateTraceRetiredMessage(cmd.ErrOrStderr())
			return commandExitError{code: 2}
		},
	}
}

func newInvestigateHealthCommand() *cobra.Command {
	return &cobra.Command{
		Use:                "health",
		Short:              "Retired legacy command; use swarm health.",
		Hidden:             true,
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			writeInvestigateHealthRetiredMessage(cmd.ErrOrStderr())
			return commandExitError{code: 2}
		},
	}
}

func writeInvestigateRetiredMessage(w io.Writer) {
	if w == nil {
		return
	}
	fmt.Fprintln(w, "ERROR: `swarm investigate` was retired in CLI v2.")
	fmt.Fprintln(w, "  Use `swarm runs` to list runs.")
	fmt.Fprintln(w, "  Use `swarm status [run-id]` to diagnose a run.")
	fmt.Fprintln(w, "  Use `swarm trace [run-id] [--follow]` for run traces.")
	fmt.Fprintln(w, "  Use `swarm health` for runtime health.")
}

func writeInvestigateRunsRetiredMessage(w io.Writer) {
	if w == nil {
		return
	}
	fmt.Fprintln(w, "ERROR: `swarm investigate runs` was retired in CLI v2.")
	fmt.Fprintln(w, "  Use `swarm runs`.")
}

func writeInvestigateHealthRetiredMessage(w io.Writer) {
	if w == nil {
		return
	}
	fmt.Fprintln(w, "ERROR: `swarm investigate health` was retired in CLI v2.")
	fmt.Fprintln(w, "  Use `swarm health`.")
}

func writeInvestigateRunRetiredMessage(w io.Writer) {
	if w == nil {
		return
	}
	fmt.Fprintln(w, "ERROR: `swarm investigate run` was retired in CLI v2.")
	fmt.Fprintln(w, "  Use `swarm status`.")
	fmt.Fprintln(w, "  Use `swarm status --no-diagnose` for the header-only run read.")
}

func writeInvestigateTraceRetiredMessage(w io.Writer) {
	if w == nil {
		return
	}
	fmt.Fprintln(w, "ERROR: `swarm investigate trace` was retired in CLI v2.")
	fmt.Fprintln(w, "  Use `swarm trace`.")
	fmt.Fprintln(w, "  Use `swarm trace --follow` for live trace streaming.")
}

func bindDiagnosticRunListFlags(cmd *cobra.Command, opts *diagnosticRunListOptions) {
	cmd.Flags().StringVar(&opts.status, "status", "", "Optional run status filter")
	cmd.Flags().StringVar(&opts.since, "since", "", "Optional RFC3339 lower started_at bound")
	cmd.Flags().StringVar(&opts.until, "until", "", "Optional RFC3339 upper started_at bound")
	cmd.Flags().IntVar(&opts.limit, "limit", 0, "Optional page size, 1-500")
	cmd.Flags().StringVar(&opts.cursor, "cursor", "", "Optional pagination cursor")
}

func runDiagnosticRunListCommand(ctx context.Context, out, errOut io.Writer, opts diagnosticRunListOptions) error {
	params, err := opts.params()
	if err != nil {
		return err
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, diagnosticRunAPIErrorClassifier())
	}
	result, err := fetchDiagnosticRunList(ctx, client, params)
	if err != nil {
		return returnCLIAPIError(errOut, err, diagnosticRunAPIErrorClassifier())
	}
	return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
		writeDiagnosticRunList(w, result)
	}, func() ([]string, error) {
		return quietDiagnosticRunList(result), nil
	})
}

func runDiagnosticRunCommand(ctx context.Context, out, errOut io.Writer, opts diagnosticRunOptions, runID string) error {
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, diagnosticRunAPIErrorClassifier())
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		runID, err = resolveActivePreferredRunID(ctx, client)
		if err != nil {
			return returnCLIAPIError(errOut, err, diagnosticRunAPIErrorClassifier())
		}
	}
	if opts.noDiagnose {
		var result diagnosticRunGetResult
		if err := client.call(ctx, "run.get", map[string]any{"run_id": runID}, &result); err != nil {
			return returnCLIAPIError(errOut, err, diagnosticRunAPIErrorClassifier())
		}
		if err := validateDiagnosticRunHeader("run", result.Run); err != nil {
			return returnCLIAPIError(errOut, err, diagnosticRunAPIErrorClassifier())
		}
		return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
			writeDiagnosticRunHeader(w, result.Run)
		}, func() ([]string, error) {
			return []string{fmt.Sprintf("%s %s", result.Run.RunID, result.Run.Status)}, nil
		})
	}
	var result diagnosticRunDiagnosisResult
	if err := client.call(ctx, "run.diagnose", map[string]any{"run_id": runID}, &result); err != nil {
		return returnCLIAPIError(errOut, err, diagnosticRunAPIErrorClassifier())
	}
	if err := validateDiagnosticRunDiagnosis(result); err != nil {
		return returnCLIAPIError(errOut, err, diagnosticRunAPIErrorClassifier())
	}
	return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
		writeDiagnosticRunDiagnosis(w, result)
	}, func() ([]string, error) {
		return []string{fmt.Sprintf("%s %s", result.Run.RunID, stringPointerValue(result.OperationalState))}, nil
	})
}

func runDiagnosticTraceCommand(ctx context.Context, out, errOut io.Writer, opts diagnosticTraceOptions, runID string) error {
	snapshotParams, err := opts.snapshotParams()
	if err != nil {
		return err
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, diagnosticRunAPIErrorClassifier())
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		runID, err = resolveActivePreferredRunID(ctx, client)
		if err != nil {
			return returnCLIAPIError(errOut, err, diagnosticRunAPIErrorClassifier())
		}
	}
	if opts.follow {
		return followDiagnosticTraceCommand(ctx, out, errOut, client, runID, opts)
	}
	snapshotParams["run_id"] = runID
	var result diagnosticRunTraceResult
	if err := client.call(ctx, "run.trace", snapshotParams, &result); err != nil {
		return returnCLIAPIError(errOut, err, diagnosticRunAPIErrorClassifier())
	}
	if err := validateDiagnosticRunTraceResult(result); err != nil {
		return returnCLIAPIError(errOut, err, diagnosticRunAPIErrorClassifier())
	}
	writeDiagnosticRunTrace(out, runID, result)
	return nil
}

func (o diagnosticTraceOptions) snapshotParams() (map[string]any, error) {
	if o.follow {
		for _, flag := range []struct {
			name string
			set  bool
		}{
			{name: "--since", set: o.sinceSet},
			{name: "--limit", set: o.limitSet},
			{name: "--cursor", set: o.cursorSet},
		} {
			if flag.set {
				return nil, fmt.Errorf("%s is not supported with --follow", flag.name)
			}
		}
		return map[string]any{}, nil
	}
	if o.noRetry {
		return nil, fmt.Errorf("--no-retry requires --follow")
	}
	params := map[string]any{}
	if o.limitSet {
		if o.limit < 1 || o.limit > 2000 {
			return nil, fmt.Errorf("--limit must be between 1 and 2000")
		}
		params["limit"] = o.limit
	}
	if o.cursorSet {
		cursor := strings.TrimSpace(o.cursor)
		if cursor == "" {
			return nil, fmt.Errorf("--cursor must not be empty")
		}
		params["cursor"] = cursor
	}
	if o.sinceSet {
		since := strings.TrimSpace(o.since)
		if err := validateRFC3339Flag("--since", since); err != nil {
			return nil, err
		}
		params["since"] = since
	}
	return params, nil
}

func followDiagnosticTraceCommand(ctx context.Context, out, errOut io.Writer, client *cliAPIClient, runID string, opts diagnosticTraceOptions) error {
	wsEndpoint, err := runCommandWebSocketEndpoint(client.endpoint)
	if err != nil {
		if errOut != nil {
			fmt.Fprintln(errOut, err)
		}
		return commandExitError{code: runCommandErrorExitCode(err)}
	}
	reconnectsRemaining := traceFollowMaxReconnects
	if opts.noRetry {
		reconnectsRemaining = 0
	}
	retryAttempt := 0
	var replaySince *time.Time
	for {
		sub, err := subscribeRunTrace(ctx, wsEndpoint, client.token, runID, replaySince)
		if err != nil {
			if ctx.Err() != nil {
				return traceFollowDetached(errOut)
			}
			if reconnectsRemaining == 0 || !traceFollowRetryableSubscribeError(err) {
				if errOut != nil {
					fmt.Fprintln(errOut, err)
				}
				return commandExitError{code: runCommandErrorExitCode(err)}
			}
			reconnectsRemaining--
			retryAttempt++
			if err := waitTraceFollowReconnect(ctx, errOut, retryAttempt); err != nil {
				return err
			}
			continue
		}
		streamErr, interrupted := consumeDiagnosticTraceSubscription(ctx, out, sub, &replaySince)
		sub.close()
		if interrupted {
			return traceFollowDetached(errOut)
		}
		if streamErr == nil {
			if reconnectsRemaining == 0 {
				return nil
			}
			reconnectsRemaining--
			retryAttempt++
			if err := waitTraceFollowReconnect(ctx, errOut, retryAttempt); err != nil {
				return err
			}
			continue
		}
		if reconnectsRemaining == 0 || !traceFollowRetryableStreamError(streamErr) {
			if errOut != nil {
				fmt.Fprintln(errOut, streamErr)
			}
			return commandExitError{code: runCommandErrorExitCode(streamErr)}
		}
		reconnectsRemaining--
		retryAttempt++
		if err := waitTraceFollowReconnect(ctx, errOut, retryAttempt); err != nil {
			return err
		}
	}
}

func consumeDiagnosticTraceSubscription(ctx context.Context, out io.Writer, sub *runTraceSubscription, replaySince **time.Time) (error, bool) {
	for {
		select {
		case <-ctx.Done():
			return nil, true
		case row, ok := <-sub.rows:
			if !ok {
				select {
				case err := <-sub.errs:
					if err != nil {
						return err, false
					}
				default:
				}
				return nil, false
			}
			nextReplaySince, err := time.Parse(time.RFC3339Nano, row.EventCreatedAt)
			if err != nil {
				return fmt.Errorf("malformed run.subscribe_trace notification: event_created_at is not RFC3339: %w", err), false
			}
			writeRunCommandTraceRow(out, row)
			nextReplaySince = nextReplaySince.UTC()
			*replaySince = &nextReplaySince
		case err := <-sub.errs:
			if err != nil {
				return err, false
			}
		}
	}
}

func waitTraceFollowReconnect(ctx context.Context, errOut io.Writer, retryAttempt int) error {
	timer := time.NewTimer(traceFollowBackoff(retryAttempt))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return traceFollowDetached(errOut)
	case <-timer.C:
		return nil
	}
}

func traceFollowBackoff(retryAttempt int) time.Duration {
	if retryAttempt <= 1 {
		return traceFollowInitialBackoff
	}
	backoff := traceFollowInitialBackoff
	for i := 1; i < retryAttempt; i++ {
		backoff *= 2
		if backoff >= traceFollowMaximumBackoff {
			return traceFollowMaximumBackoff
		}
	}
	return backoff
}

func traceFollowDetached(errOut io.Writer) error {
	if errOut != nil {
		fmt.Fprintln(errOut, "detached from run trace")
	}
	return commandExitError{code: 130}
}

func traceFollowRetryableSubscribeError(err error) bool {
	if err == nil {
		return false
	}
	var rpcErr *jsonRPCError
	if errors.As(err, &rpcErr) {
		return false
	}
	msg := err.Error()
	if strings.HasPrefix(msg, "v1 WS dial failed:") || strings.HasPrefix(msg, "write run.subscribe_trace request:") {
		return true
	}
	if strings.HasPrefix(msg, "read run.subscribe_trace response:") {
		return traceFollowTransportErrorText(msg)
	}
	return false
}

func traceFollowRetryableStreamError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if !strings.HasPrefix(msg, traceFollowRetryableReadErr) {
		return false
	}
	return traceFollowTransportErrorText(msg)
}

func traceFollowTransportErrorText(msg string) bool {
	msg = strings.ToLower(msg)
	if msg == "eof" || strings.HasSuffix(msg, ": eof") {
		return true
	}
	for _, fragment := range []string{
		"websocket: close",
		"unexpected eof",
		"connection reset",
		"connection refused",
		"broken pipe",
		"i/o timeout",
		"use of closed network connection",
	} {
		if strings.Contains(msg, fragment) {
			return true
		}
	}
	return false
}

func runDiagnosticHealthCommand(ctx context.Context, out, errOut io.Writer, opts rootCommandOptions, output cliOutputOptions) error {
	result, err := fetchDiagnosticHealthCheck(ctx, opts)
	if err != nil {
		return returnCLIAPIError(errOut, err, cliAPIErrorClassifier{})
	}
	return renderCLIOutput(out, errOut, output, result, func(w io.Writer) {
		writeDiagnosticHealth(w, result)
	}, func() ([]string, error) {
		return []string{quietDiagnosticHealth(result)}, nil
	})
}

func quietDiagnosticRunList(result diagnosticRunListResult) []string {
	out := make([]string, 0, len(result.Runs))
	for _, run := range result.Runs {
		out = append(out, run.RunID)
	}
	return out
}

func quietDiagnosticHealth(result diagnosticHealthCheckResult) string {
	if boolPointerValue(result.Alive) && boolPointerValue(result.Ready) && boolPointerValue(result.DBOK) && boolPointerValue(result.RuntimeOK) {
		return "healthy"
	}
	return "unhealthy"
}

func diagnosticRunAPIErrorClassifier() cliAPIErrorClassifier {
	return cliAPIErrorClassifier{notFoundCodes: []string{"RUN_NOT_FOUND"}}
}

func fetchDiagnosticHealthCheck(ctx context.Context, opts rootCommandOptions) (diagnosticHealthCheckResult, error) {
	client, err := newCLIAPIClient(opts)
	if err != nil {
		return diagnosticHealthCheckResult{}, err
	}
	var result diagnosticHealthCheckResult
	if err := client.call(ctx, "health.check", map[string]any{}, &result); err != nil {
		return diagnosticHealthCheckResult{}, err
	}
	if err := validateDiagnosticHealthCheck(result); err != nil {
		return diagnosticHealthCheckResult{}, err
	}
	return result, nil
}

func fetchDiagnosticRunList(ctx context.Context, client *cliAPIClient, params map[string]any) (diagnosticRunListResult, error) {
	var result diagnosticRunListResult
	if err := client.call(ctx, "run.list", params, &result); err != nil {
		return diagnosticRunListResult{}, err
	}
	if err := validateDiagnosticRunListResult(result); err != nil {
		return diagnosticRunListResult{}, err
	}
	return result, nil
}

func resolveActivePreferredRunID(ctx context.Context, client *cliAPIClient) (string, error) {
	for _, status := range []string{"running", "paused"} {
		result, err := fetchDiagnosticRunList(ctx, client, map[string]any{"status": status, "limit": 1})
		if err != nil {
			return "", err
		}
		if len(result.Runs) > 0 {
			return result.Runs[0].RunID, nil
		}
	}
	result, err := fetchDiagnosticRunList(ctx, client, map[string]any{"limit": 1})
	if err != nil {
		return "", err
	}
	if len(result.Runs) == 0 {
		return "", fmt.Errorf("no runs found; pass a run id explicitly")
	}
	return result.Runs[0].RunID, nil
}

func (o diagnosticRunListOptions) params() (map[string]any, error) {
	params := map[string]any{}
	if status := strings.ToLower(strings.TrimSpace(o.status)); status != "" {
		params["status"] = status
	}
	if cursor := strings.TrimSpace(o.cursor); cursor != "" {
		params["cursor"] = cursor
	}
	if o.limit != 0 {
		if o.limit < 1 || o.limit > 500 {
			return nil, fmt.Errorf("--limit must be between 1 and 500")
		}
		params["limit"] = o.limit
	}
	if since := strings.TrimSpace(o.since); since != "" {
		if err := validateRFC3339Flag("--since", since); err != nil {
			return nil, err
		}
		params["since"] = since
	}
	if until := strings.TrimSpace(o.until); until != "" {
		if err := validateRFC3339Flag("--until", until); err != nil {
			return nil, err
		}
		params["until"] = until
	}
	return params, nil
}

func validateRFC3339Flag(name, value string) error {
	if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
		return fmt.Errorf("%s must be an RFC3339 timestamp: %w", name, err)
	}
	return nil
}

func validateDiagnosticRunListResult(result diagnosticRunListResult) error {
	if result.Runs == nil {
		return fmt.Errorf("malformed run.list result: runs is required")
	}
	for i, run := range result.Runs {
		if err := validateDiagnosticRunHeader(fmt.Sprintf("runs[%d]", i), run); err != nil {
			return err
		}
	}
	return nil
}

func validateDiagnosticRunDiagnosis(result diagnosticRunDiagnosisResult) error {
	if err := validateDiagnosticRunHeader("run", result.Run); err != nil {
		return err
	}
	if result.OperationalState == nil {
		return fmt.Errorf("malformed run.diagnose result: operational_state is required")
	}
	operationalState := strings.TrimSpace(*result.OperationalState)
	if operationalState == "" {
		return fmt.Errorf("malformed run.diagnose result: operational_state is required")
	}
	if _, ok := diagnosticValidOperationalStates[operationalState]; !ok {
		return fmt.Errorf("malformed run.diagnose result: operational_state=%q is not a valid OperationalState", operationalState)
	}
	if result.BlockingLayer == nil {
		return fmt.Errorf("malformed run.diagnose result: blocking_layer is required")
	}
	if result.BlockingReason == nil {
		return fmt.Errorf("malformed run.diagnose result: blocking_reason is required")
	}
	if result.Heuristics == nil {
		return fmt.Errorf("malformed run.diagnose result: heuristics is required")
	}
	return nil
}

func validateDiagnosticRunTraceResult(result diagnosticRunTraceResult) error {
	if result.Trace == nil {
		return fmt.Errorf("malformed run.trace result: trace is required")
	}
	for i, row := range result.Trace {
		if strings.TrimSpace(row.EventID) == "" {
			return fmt.Errorf("malformed run.trace result: trace[%d].event_id is required", i)
		}
		if strings.TrimSpace(row.EventName) == "" {
			return fmt.Errorf("malformed run.trace result: trace[%d].event_name is required", i)
		}
		if err := validateRequiredTimestamp(fmt.Sprintf("trace[%d].event_created_at", i), row.EventCreatedAt); err != nil {
			return err
		}
	}
	return nil
}

func validateDiagnosticHealthCheck(result diagnosticHealthCheckResult) error {
	if result.Alive == nil {
		return fmt.Errorf("malformed health.check result: alive is required")
	}
	if result.Ready == nil {
		return fmt.Errorf("malformed health.check result: ready is required")
	}
	if result.DBOK == nil {
		return fmt.Errorf("malformed health.check result: db_ok is required")
	}
	if result.RuntimeOK == nil {
		return fmt.Errorf("malformed health.check result: runtime_ok is required")
	}
	if strings.TrimSpace(result.Bundle.Fingerprint) == "" {
		return fmt.Errorf("malformed health.check result: bundle.fingerprint is required")
	}
	if result.Bundle.WorkflowName == nil {
		return fmt.Errorf("malformed health.check result: bundle.workflow_name is required")
	}
	if result.Bundle.WorkflowVersion == nil {
		return fmt.Errorf("malformed health.check result: bundle.workflow_version is required")
	}
	return nil
}

func validateDiagnosticRunHeader(prefix string, run diagnosticRunHeader) error {
	if strings.TrimSpace(run.RunID) == "" {
		return fmt.Errorf("malformed run header: %s.run_id is required", prefix)
	}
	status := strings.TrimSpace(run.Status)
	if status == "" {
		return fmt.Errorf("malformed run header: %s.status is required", prefix)
	}
	if _, ok := diagnosticValidRunStatuses[status]; !ok {
		return fmt.Errorf("malformed run header: %s.status=%q is not a valid RunStatus", prefix, status)
	}
	if strings.TrimSpace(run.TriggerEventType) == "" {
		return fmt.Errorf("malformed run header: %s.trigger_event_type is required", prefix)
	}
	if strings.TrimSpace(run.TriggerEventID) == "" {
		return fmt.Errorf("malformed run header: %s.trigger_event_id is required", prefix)
	}
	if run.EntityCount == nil {
		return fmt.Errorf("malformed run header: %s.entity_count is required", prefix)
	}
	if *run.EntityCount < 0 {
		return fmt.Errorf("malformed run header: %s.entity_count must be non-negative", prefix)
	}
	if run.EventCount == nil {
		return fmt.Errorf("malformed run header: %s.event_count is required", prefix)
	}
	if *run.EventCount < 0 {
		return fmt.Errorf("malformed run header: %s.event_count must be non-negative", prefix)
	}
	if err := validateRequiredTimestamp(prefix+".started_at", run.StartedAt); err != nil {
		return err
	}
	if endedAt := strings.TrimSpace(run.EndedAt); endedAt != "" {
		if _, err := time.Parse(time.RFC3339Nano, endedAt); err != nil {
			return fmt.Errorf("malformed run header: %s.ended_at must be an RFC3339 timestamp: %w", prefix, err)
		}
	}
	return nil
}

func validateRequiredTimestamp(field, value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("malformed result: %s is required", field)
	}
	if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
		return fmt.Errorf("malformed result: %s must be an RFC3339 timestamp: %w", field, err)
	}
	return nil
}

func writeDiagnosticRunList(out io.Writer, result diagnosticRunListResult) {
	if out == nil {
		return
	}
	if len(result.Runs) == 0 {
		fmt.Fprintln(out, "no runs")
	} else {
		fmt.Fprintln(out, "RUN ID\tSTATUS\tSTARTED\tEVENTS\tENTITIES\tTRIGGER")
		for _, run := range result.Runs {
			fmt.Fprintf(out, "%s\t%s\t%s\t%d\t%d\t%s\n",
				run.RunID,
				run.Status,
				run.StartedAt,
				intPointerValue(run.EventCount),
				intPointerValue(run.EntityCount),
				run.TriggerEventType,
			)
		}
	}
	if result.NextCursor != "" {
		fmt.Fprintf(out, "next_cursor=%s\n", result.NextCursor)
	}
}

func writeDiagnosticRunHeader(out io.Writer, run diagnosticRunHeader) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "Run: %s\n", run.RunID)
	fmt.Fprintf(out, "status=%s started_at=%s events=%d entities=%d trigger=%s trigger_event_id=%s\n",
		run.Status,
		run.StartedAt,
		intPointerValue(run.EventCount),
		intPointerValue(run.EntityCount),
		run.TriggerEventType,
		run.TriggerEventID,
	)
	if run.EndedAt != "" {
		fmt.Fprintf(out, "ended_at=%s\n", run.EndedAt)
	}
	if run.ForkedFromRunID != "" {
		fmt.Fprintf(out, "forked_from_run_id=%s\n", run.ForkedFromRunID)
	}
	if run.ErrorSummary != "" {
		fmt.Fprintf(out, "error_summary=%s\n", run.ErrorSummary)
	}
}

func writeDiagnosticRunDiagnosis(out io.Writer, result diagnosticRunDiagnosisResult) {
	if out == nil {
		return
	}
	writeDiagnosticRunHeader(out, result.Run)
	fmt.Fprintf(out, "operational_state=%s blocking_layer=%s blocking_reason=%s\n",
		stringPointerValue(result.OperationalState),
		stringPointerValue(result.BlockingLayer),
		stringPointerValue(result.BlockingReason),
	)
	if len(result.Heuristics) == 0 {
		fmt.Fprintln(out, "heuristics=none")
		return
	}
	fmt.Fprintln(out, "heuristics:")
	for _, item := range result.Heuristics {
		fmt.Fprintf(out, "- %s\n", item)
	}
}

func writeDiagnosticRunTrace(out io.Writer, runID string, result diagnosticRunTraceResult) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "run trace: run_id=%s\n", runID)
	if len(result.Trace) == 0 {
		fmt.Fprintln(out, "no trace rows")
	} else {
		fmt.Fprintln(out, "EVENT AT\tEVENT\tEVENT ID\tDELIVERY\tSUBSCRIBER\tSESSION\tTURN")
		for _, row := range result.Trace {
			fmt.Fprintf(out, "%s\t%s\t%s\t%s\t%s/%s\t%s\t%s\n",
				row.EventCreatedAt,
				row.EventName,
				row.EventID,
				emptyDash(row.DeliveryStatus),
				emptyDash(row.SubscriberType),
				emptyDash(row.SubscriberID),
				emptyDash(row.SessionID),
				emptyDash(firstNonEmpty(row.TurnID, row.TurnTriggerEventType)),
			)
		}
	}
	if result.NextCursor != "" {
		fmt.Fprintf(out, "next_cursor=%s\n", result.NextCursor)
	}
}

func writeDiagnosticHealth(out io.Writer, result diagnosticHealthCheckResult) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "alive=%t ready=%t db_ok=%t runtime_ok=%t\n", boolPointerValue(result.Alive), boolPointerValue(result.Ready), boolPointerValue(result.DBOK), boolPointerValue(result.RuntimeOK))
	fmt.Fprintf(out, "bundle fingerprint=%s workflow_name=%s workflow_version=%s\n",
		result.Bundle.Fingerprint,
		emptyDash(stringPointerValue(result.Bundle.WorkflowName)),
		emptyDash(stringPointerValue(result.Bundle.WorkflowVersion)),
	)
}

func intPointerValue(value *int) int {
	if value == nil {
		return 0
	}
	return *value
}

func boolPointerValue(value *bool) bool {
	return value != nil && *value
}

func stringPointerValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func emptyDash(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "-"
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
