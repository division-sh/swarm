package cliapp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/cli/argcount"
	"github.com/division-sh/swarm/internal/cli/readwindow"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/spf13/cobra"
)

const (
	traceFollowMaxReconnects  = 3
	traceFollowInitialBackoff = 250 * time.Millisecond
	traceFollowMaximumBackoff = time.Second
)

var traceDeliverySummaryStatuses = []string{"pending", "in_progress", "delivered", "failed", "dead_letter"}

type diagnosticRunListOptions struct {
	apiOptions rootCommandOptions
	output     cliOutputOptions
	logging    cliLoggingOptions
	status     string
	since      string
	until      string
	sinceSet   bool
	untilSet   bool
	reference  time.Time
	limit      int
	cursor     string
}

type diagnosticTraceOptions struct {
	apiOptions       rootCommandOptions
	follow           bool
	verbose          bool
	noRetry          bool
	deliveryDetail   bool
	deliverySummary  bool
	since            string
	until            string
	limit            int
	cursor           string
	eventNames       []string
	entityIDs        []string
	deliveryStatuses []string
	subscriberIDs    []string
	subscriberTypes  []string
	sinceSet         bool
	untilSet         bool
	reference        time.Time
	limitSet         bool
	cursorSet        bool
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

type DiagnosticRunDiagnosisResult struct {
	Run              diagnosticRunHeader            `json:"run"`
	OperationalState *string                        `json:"operational_state"`
	BlockingLayer    *string                        `json:"blocking_layer"`
	BlockingReason   *string                        `json:"blocking_reason"`
	Heuristics       []string                       `json:"heuristics"`
	FailedDeliveries []diagnosticRunFailureDelivery `json:"failed_deliveries"`
	TestQuiescence   *diagnosticRunTestQuiescence   `json:"test_quiescence"`
}

type diagnosticRunTestQuiescence struct {
	Ready                   *bool `json:"ready"`
	ActiveDeliveries        *int  `json:"active_deliveries"`
	UnsettledPipelineEvents *int  `json:"unsettled_pipeline_events"`
	DueTimers               *int  `json:"due_timers"`
	ActiveSessionLeases     *int  `json:"active_session_leases"`
}

type diagnosticRunTraceResult struct {
	Trace      []diagnosticRunTraceRow `json:"trace"`
	NextCursor string                  `json:"next_cursor,omitempty"`
}

type diagnosticRunTraceSummaryResult struct {
	TraceRows       int
	DeliveryRows    int
	NonDeliveryRows int
	Groups          []diagnosticRunTraceSummaryGroup
}

type diagnosticRunTraceSummaryAccumulator struct {
	traceRows       int
	deliveryRows    int
	nonDeliveryRows int
	groups          map[string]*diagnosticRunTraceSummaryGroup
}

type diagnosticRunTraceSummaryGroup struct {
	SubscriberType   string
	SubscriberID     string
	StatusCounts     map[string]int
	QueueWait        diagnosticTraceDurationStats
	ExecutionTime    diagnosticTraceDurationStats
	UnavailableQueue int
	UnavailableExec  int
}

type diagnosticTraceDurationStats struct {
	Count int
	Sum   time.Duration
	Max   time.Duration
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
	BundleHash      string  `json:"bundle_hash,omitempty"`
}

type diagnosticRunHeader struct {
	RunID            string                    `json:"run_id"`
	Status           string                    `json:"status"`
	TriggerEventType string                    `json:"trigger_event_type"`
	TriggerEventID   string                    `json:"trigger_event_id"`
	EntityCount      *int                      `json:"entity_count"`
	EventCount       *int                      `json:"event_count"`
	StartedAt        string                    `json:"started_at"`
	EndedAt          string                    `json:"ended_at,omitempty"`
	ForkedFromRunID  string                    `json:"forked_from_run_id,omitempty"`
	ContinuedAsRunID string                    `json:"continued_as_run_id,omitempty"`
	Failure          *runtimefailures.Envelope `json:"failure,omitempty"`
	ControlReason    string                    `json:"control_reason,omitempty"`
}

type diagnosticRunTraceRow struct {
	EventID               string                    `json:"event_id"`
	EventName             string                    `json:"event_name"`
	EventCreatedAt        string                    `json:"event_created_at"`
	EntityID              string                    `json:"entity_id,omitempty"`
	DeliveryID            string                    `json:"delivery_id,omitempty"`
	DeliveryStatus        string                    `json:"delivery_status,omitempty"`
	DeliveryReasonCode    string                    `json:"delivery_reason_code,omitempty"`
	ReplyContextID        string                    `json:"reply_context_id,omitempty"`
	DeliveryFailure       *runtimefailures.Envelope `json:"delivery_failure,omitempty"`
	DeliveryRetryCount    int                       `json:"delivery_retry_count,omitempty"`
	DeliveryRetryEligible bool                      `json:"delivery_retry_eligible,omitempty"`
	DeliveryTerminal      bool                      `json:"delivery_terminal,omitempty"`
	DeliveryCreatedAt     string                    `json:"delivery_created_at,omitempty"`
	DeliveryStartedAt     string                    `json:"delivery_started_at,omitempty"`
	DeliveryDeliveredAt   string                    `json:"delivery_delivered_at,omitempty"`
	SubscriberType        string                    `json:"subscriber_type,omitempty"`
	SubscriberID          string                    `json:"subscriber_id,omitempty"`
	SessionID             string                    `json:"session_id,omitempty"`
	TurnID                string                    `json:"turn_id,omitempty"`
	TurnTriggerEventType  string                    `json:"turn_trigger_event_type,omitempty"`
}

type diagnosticRunFailureDelivery struct {
	EventID        string                    `json:"event_id"`
	EventName      string                    `json:"event_name"`
	EntityID       string                    `json:"entity_id,omitempty"`
	DeliveryID     string                    `json:"delivery_id"`
	SubscriberType string                    `json:"subscriber_type"`
	SubscriberID   string                    `json:"subscriber_id"`
	SessionID      string                    `json:"session_id,omitempty"`
	Status         string                    `json:"status"`
	ReasonCode     string                    `json:"reason_code,omitempty"`
	Failure        *runtimefailures.Envelope `json:"failure,omitempty"`
	RetryCount     int                       `json:"retry_count"`
	RetryEligible  bool                      `json:"retry_eligible"`
	Terminal       bool                      `json:"terminal"`
	CreatedAt      string                    `json:"created_at,omitempty"`
	StartedAt      string                    `json:"started_at,omitempty"`
	FinishedAt     string                    `json:"finished_at,omitempty"`
	DeadLetters    []eventDeadLetter         `json:"dead_letters,omitempty"`
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
		Use:   "list",
		Short: "List runs.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			runOpts.sinceSet = cmd.Flags().Changed("since")
			runOpts.untilSet = cmd.Flags().Changed("until")
			runOpts.reference = readwindow.Reference(runOpts.apiOptions.now)
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
		Short: "Show runtime health.",
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
		Short: "Diagnose one run (state, gates, stuck points).",
		Args:  argcount.MaximumNArgs(1),
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
	argcount.SetDiscoveryHint(cmd, "List run ids with `swarm run list`.")
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
		Short: "Print or follow a run's execution trace.",
		Example: `  swarm run trace <run-id>
  swarm run trace -f <run-id>    # follow live`,
		Args: argcount.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			traceOpts.sinceSet = cmd.Flags().Changed("since")
			traceOpts.untilSet = cmd.Flags().Changed("until")
			traceOpts.reference = readwindow.Reference(traceOpts.apiOptions.now)
			traceOpts.limitSet = cmd.Flags().Changed("limit")
			traceOpts.cursorSet = cmd.Flags().Changed("cursor")
			runID := ""
			if len(args) == 1 {
				runID = args[0]
			}
			return runDiagnosticTraceCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), traceOpts, runID)
		},
	}
	argcount.SetDiscoveryHint(cmd, "List run ids with `swarm run list`.")
	cmd.Flags().BoolVarP(&traceOpts.follow, "follow", "f", false, "Follow live trace rows as they stream")
	cmd.Flags().BoolVar(&traceOpts.verbose, "verbose", false, "Include internal trace rows such as platform.runtime_log")
	cmd.Flags().BoolVar(&traceOpts.noRetry, "no-retry", false, "Disable trace follow reconnect/recovery retries")
	cmd.Flags().BoolVar(&traceOpts.deliveryDetail, "delivery-detail", false, "Show snapshot delivery lifecycle fields from RunTraceRow")
	cmd.Flags().BoolVar(&traceOpts.deliverySummary, "delivery-summary", false, "Summarize snapshot delivery lifecycle fields from all RunTraceRow pages")
	cmd.Flags().StringVar(&traceOpts.since, "since", "", "Snapshot-only RFC3339 or relative trace materialization watermark")
	cmd.Flags().StringVar(&traceOpts.until, "until", "", "Snapshot-only inclusive RFC3339 or relative trace materialization upper bound")
	cmd.Flags().IntVar(&traceOpts.limit, "limit", 0, "Snapshot-only page size, 1-2000")
	cmd.Flags().StringVar(&traceOpts.cursor, "cursor", "", "Snapshot-only pagination cursor")
	cmd.Flags().StringArrayVar(&traceOpts.eventNames, "event-name", nil, "Event name filter; repeat to match any")
	cmd.Flags().StringArrayVar(&traceOpts.entityIDs, "entity-id", nil, "Entity id filter; repeat to match any")
	cmd.Flags().StringArrayVar(&traceOpts.deliveryStatuses, "delivery-status", nil, "Delivery status filter; repeat to match any")
	cmd.Flags().StringArrayVar(&traceOpts.subscriberIDs, "subscriber-id", nil, "Subscriber id filter; repeat to match any")
	cmd.Flags().StringArrayVar(&traceOpts.subscriberTypes, "subscriber-type", nil, "Subscriber type filter: node or agent; repeat to match any")
	bindCLIAPIConnectionFlags(cmd, &traceOpts.apiOptions)
	return cmd
}

func newInvestigateCommand(opts rootCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:                "investigate",
		Short:              "Retired legacy namespace; use swarm run list/status/trace and swarm health.",
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
		Short:              "Retired legacy command; use swarm run list.",
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
		Short:              "Retired legacy command; use swarm run status.",
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
		Short:              "Retired legacy command; use swarm run trace.",
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
	fmt.Fprintln(w, "  Use `swarm run list` to list runs.")
	fmt.Fprintln(w, "  Use `swarm run status [run-id]` to diagnose a run.")
	fmt.Fprintln(w, "  Use `swarm run trace [run-id] [--follow]` for run traces.")
	fmt.Fprintln(w, "  Use `swarm health` for runtime health.")
}

func writeInvestigateRunsRetiredMessage(w io.Writer) {
	if w == nil {
		return
	}
	fmt.Fprintln(w, "ERROR: `swarm investigate runs` was retired in CLI v2.")
	fmt.Fprintln(w, "  Use `swarm run list`.")
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
	fmt.Fprintln(w, "  Use `swarm run status`.")
	fmt.Fprintln(w, "  Use `swarm run status --no-diagnose` for the header-only run read.")
}

func writeInvestigateTraceRetiredMessage(w io.Writer) {
	if w == nil {
		return
	}
	fmt.Fprintln(w, "ERROR: `swarm investigate trace` was retired in CLI v2.")
	fmt.Fprintln(w, "  Use `swarm run trace`.")
	fmt.Fprintln(w, "  Use `swarm run trace --follow` for live trace streaming.")
}

func bindDiagnosticRunListFlags(cmd *cobra.Command, opts *diagnosticRunListOptions) {
	cmd.Flags().StringVar(&opts.status, "status", "", "Optional run status filter")
	cmd.Flags().StringVar(&opts.since, "since", "", "Optional RFC3339 or relative lower started_at bound")
	cmd.Flags().StringVar(&opts.until, "until", "", "Optional RFC3339 or relative upper started_at bound")
	cmd.Flags().IntVar(&opts.limit, "limit", 0, "Optional page size, 1-500")
	cmd.Flags().StringVar(&opts.cursor, "cursor", "", "Optional pagination cursor")
}

func runDiagnosticRunListCommand(ctx context.Context, out, errOut io.Writer, opts diagnosticRunListOptions) error {
	params, err := opts.params()
	if err != nil {
		return returnCLIValidationError(errOut, err)
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
	var result DiagnosticRunDiagnosisResult
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
	traceParams, err := opts.traceParams()
	if err != nil {
		return returnCLIValidationError(errOut, err)
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
		return followDiagnosticTraceCommand(ctx, out, errOut, client, runID, opts, traceParams)
	}
	traceParams["run_id"] = runID
	if opts.deliverySummary {
		if !opts.untilSet {
			traceParams["until"] = opts.reference.UTC().Format(time.RFC3339Nano)
		}
		result, err := fetchDiagnosticRunTraceSummary(ctx, client, traceParams)
		if err != nil {
			return returnCLIAPIError(errOut, err, diagnosticRunAPIErrorClassifier())
		}
		writeDiagnosticRunTraceDeliverySummary(out, runID, result)
		return nil
	}
	var result diagnosticRunTraceResult
	if err := client.call(ctx, "run.trace", traceParams, &result); err != nil {
		return returnCLIAPIError(errOut, err, diagnosticRunAPIErrorClassifier())
	}
	if err := validateDiagnosticRunTraceResult(result); err != nil {
		return returnCLIAPIError(errOut, err, diagnosticRunAPIErrorClassifier())
	}
	writeDiagnosticRunTrace(out, runID, result, opts.deliveryDetail, opts.verbose)
	return nil
}

func (o diagnosticTraceOptions) traceParams() (map[string]any, error) {
	if o.follow {
		return o.followParams()
	}
	return o.snapshotParams()
}

func (o diagnosticTraceOptions) snapshotParams() (map[string]any, error) {
	if o.noRetry {
		return nil, fmt.Errorf("--no-retry requires --follow")
	}
	if o.deliveryDetail && o.deliverySummary {
		return nil, fmt.Errorf("--delivery-detail and --delivery-summary are mutually exclusive")
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
	window, err := readwindow.Resolve(readwindow.Input{
		Since:        readwindow.BoundInput{Value: o.since, Set: o.sinceSet},
		Until:        readwindow.BoundInput{Value: o.until, Set: o.untilSet},
		ReferenceUTC: o.reference,
	})
	if err != nil {
		return nil, err
	}
	window.AddParams(params)
	filter, err := o.traceFilter()
	if err != nil {
		return nil, err
	}
	if len(filter) > 0 {
		params["filter"] = filter
	}
	if o.verbose {
		params["include_internal"] = true
	}
	return params, nil
}

func (o diagnosticTraceOptions) followParams() (map[string]any, error) {
	for _, flag := range []struct {
		name string
		set  bool
	}{
		{name: "--since", set: o.sinceSet},
		{name: "--until", set: o.untilSet},
		{name: "--limit", set: o.limitSet},
		{name: "--cursor", set: o.cursorSet},
		{name: "--delivery-detail", set: o.deliveryDetail},
		{name: "--delivery-summary", set: o.deliverySummary},
	} {
		if flag.set {
			return nil, fmt.Errorf("%s is not supported with --follow", flag.name)
		}
	}
	filter, err := o.traceFilter()
	if err != nil {
		return nil, err
	}
	params := map[string]any{}
	if len(filter) > 0 {
		params["filter"] = filter
	}
	if o.verbose {
		params["include_internal"] = true
	}
	return params, nil
}

func (o diagnosticTraceOptions) traceFilter() (map[string]any, error) {
	filter := map[string]any{}
	addStringList := func(name, flag string, values []string) error {
		values, err := traceStringList(flag, values)
		if err != nil {
			return err
		}
		if len(values) > 0 {
			filter[name] = values
		}
		return nil
	}
	if err := addStringList("event_name", "--event-name", o.eventNames); err != nil {
		return nil, err
	}
	entityIDs, err := traceStringList("--entity-id", o.entityIDs)
	if err != nil {
		return nil, err
	}
	for _, entityID := range entityIDs {
		if err := validateEntityOpaqueIDArg("--entity-id", entityID); err != nil {
			return nil, err
		}
	}
	if len(entityIDs) > 0 {
		filter["entity_id"] = entityIDs
	}
	deliveryStatuses, err := traceEnumList("--delivery-status", o.deliveryStatuses, eventObservationValidDeliveryStatuses, "pending, in_progress, delivered, failed, dead_letter")
	if err != nil {
		return nil, err
	}
	if len(deliveryStatuses) > 0 {
		filter["delivery_status"] = deliveryStatuses
	}
	if err := addStringList("subscriber_id", "--subscriber-id", o.subscriberIDs); err != nil {
		return nil, err
	}
	subscriberTypes, err := traceEnumList("--subscriber-type", o.subscriberTypes, eventObservationValidSubscriberTypes, "node, agent")
	if err != nil {
		return nil, err
	}
	if len(subscriberTypes) > 0 {
		filter["subscriber_type"] = subscriberTypes
	}
	return filter, nil
}

func fetchDiagnosticRunTraceSummary(ctx context.Context, client *cliAPIClient, params map[string]any) (diagnosticRunTraceSummaryResult, error) {
	pageParams := cloneDiagnosticTraceParams(params)
	seenCursors := map[string]struct{}{}
	if cursor, ok := pageParams["cursor"].(string); ok {
		cursor = strings.TrimSpace(cursor)
		if cursor != "" {
			seenCursors[cursor] = struct{}{}
		}
	}
	summary := newDiagnosticRunTraceSummaryAccumulator()
	for {
		var page diagnosticRunTraceResult
		if err := client.call(ctx, "run.trace", pageParams, &page); err != nil {
			return diagnosticRunTraceSummaryResult{}, err
		}
		if err := validateDiagnosticRunTraceResult(page); err != nil {
			return diagnosticRunTraceSummaryResult{}, err
		}
		if err := validateDiagnosticRunTraceSummaryRows(page.Trace); err != nil {
			return diagnosticRunTraceSummaryResult{}, err
		}
		summary.AddRows(page.Trace)
		if page.NextCursor == "" {
			break
		}
		nextCursor := strings.TrimSpace(page.NextCursor)
		if nextCursor == "" {
			return diagnosticRunTraceSummaryResult{}, fmt.Errorf("malformed run.trace result: next_cursor must not be empty")
		}
		if _, ok := seenCursors[nextCursor]; ok {
			return diagnosticRunTraceSummaryResult{}, fmt.Errorf("malformed run.trace result: repeated next_cursor %q", nextCursor)
		}
		seenCursors[nextCursor] = struct{}{}
		pageParams["cursor"] = nextCursor
	}
	return summary.Result(), nil
}

func cloneDiagnosticTraceParams(params map[string]any) map[string]any {
	out := make(map[string]any, len(params))
	for key, value := range params {
		out[key] = value
	}
	return out
}

func validateDiagnosticRunTraceSummaryRows(rows []diagnosticRunTraceRow) error {
	for i, row := range rows {
		if strings.TrimSpace(row.DeliveryID) == "" {
			continue
		}
		if strings.TrimSpace(row.SubscriberType) == "" {
			return fmt.Errorf("malformed run.trace result: trace[%d].subscriber_type is required when delivery_id is present", i)
		}
		if _, ok := eventObservationValidSubscriberTypes[strings.TrimSpace(row.SubscriberType)]; !ok {
			return fmt.Errorf("malformed run.trace result: trace[%d].subscriber_type=%q is not a valid SubscriberType", i, row.SubscriberType)
		}
		if strings.TrimSpace(row.SubscriberID) == "" {
			return fmt.Errorf("malformed run.trace result: trace[%d].subscriber_id is required when delivery_id is present", i)
		}
		if strings.TrimSpace(row.DeliveryStatus) == "" {
			return fmt.Errorf("malformed run.trace result: trace[%d].delivery_status is required when delivery_id is present", i)
		}
		if _, ok := eventObservationValidDeliveryStatuses[strings.TrimSpace(row.DeliveryStatus)]; !ok {
			return fmt.Errorf("malformed run.trace result: trace[%d].delivery_status=%q is not a valid DeliveryStatus", i, row.DeliveryStatus)
		}
	}
	return nil
}

func newDiagnosticRunTraceSummaryAccumulator() *diagnosticRunTraceSummaryAccumulator {
	return &diagnosticRunTraceSummaryAccumulator{groups: map[string]*diagnosticRunTraceSummaryGroup{}}
}

func (s *diagnosticRunTraceSummaryAccumulator) AddRows(rows []diagnosticRunTraceRow) {
	for _, row := range rows {
		s.traceRows++
		if strings.TrimSpace(row.DeliveryID) == "" {
			s.nonDeliveryRows++
			continue
		}
		s.deliveryRows++
		subscriberType := strings.TrimSpace(row.SubscriberType)
		subscriberID := strings.TrimSpace(row.SubscriberID)
		key := subscriberType + "\x00" + subscriberID
		group, ok := s.groups[key]
		if !ok {
			group = &diagnosticRunTraceSummaryGroup{
				SubscriberType: subscriberType,
				SubscriberID:   subscriberID,
				StatusCounts:   make(map[string]int, len(traceDeliverySummaryStatuses)),
			}
			s.groups[key] = group
		}
		group.StatusCounts[strings.TrimSpace(row.DeliveryStatus)]++
		if duration, ok := traceDurationValue(row.DeliveryCreatedAt, row.DeliveryStartedAt); ok {
			group.QueueWait.Add(duration)
		} else {
			group.UnavailableQueue++
		}
		if duration, ok := traceDurationValue(row.DeliveryStartedAt, row.DeliveryDeliveredAt); ok {
			group.ExecutionTime.Add(duration)
		} else {
			group.UnavailableExec++
		}
	}
}

func (s *diagnosticRunTraceSummaryAccumulator) Result() diagnosticRunTraceSummaryResult {
	result := diagnosticRunTraceSummaryResult{
		TraceRows:       s.traceRows,
		DeliveryRows:    s.deliveryRows,
		NonDeliveryRows: s.nonDeliveryRows,
		Groups:          make([]diagnosticRunTraceSummaryGroup, 0, len(s.groups)),
	}
	for _, group := range s.groups {
		result.Groups = append(result.Groups, *group)
	}
	sort.Slice(result.Groups, func(i, j int) bool {
		if result.Groups[i].SubscriberType == result.Groups[j].SubscriberType {
			return result.Groups[i].SubscriberID < result.Groups[j].SubscriberID
		}
		return result.Groups[i].SubscriberType < result.Groups[j].SubscriberType
	})
	return result
}

func (s *diagnosticTraceDurationStats) Add(value time.Duration) {
	s.Count++
	s.Sum += value
	if s.Count == 1 || value > s.Max {
		s.Max = value
	}
}

func traceStringList(flag string, values []string) ([]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, fmt.Errorf("%s must not be empty", flag)
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out, nil
}

func traceEnumList(flag string, values []string, valid map[string]struct{}, allowed string) ([]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		normalized := strings.ToLower(strings.TrimSpace(value))
		if normalized == "" {
			return nil, fmt.Errorf("%s must not be empty", flag)
		}
		if _, ok := valid[normalized]; !ok {
			return nil, fmt.Errorf("%s must be one of %s", flag, allowed)
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out, nil
}

func followDiagnosticTraceCommand(ctx context.Context, out, errOut io.Writer, client *cliAPIClient, runID string, opts diagnosticTraceOptions, params map[string]any) error {
	wsEndpoint, err := runCommandWebSocketEndpoint(client.endpoint)
	if err != nil {
		if errOut != nil {
			writeCLIAPIError(errOut, err)
		}
		return commandExitError{code: runCommandErrorExitCode(err)}
	}
	reconnectsRemaining := traceFollowMaxReconnects
	if opts.noRetry {
		reconnectsRemaining = 0
	}
	retryAttempt := 0
	var replaySince *time.Time
	traceWriter := &runTraceRowLineWriter{}
	for {
		sub, err := subscribeRunTrace(ctx, wsEndpoint, client.token, runID, replaySince, params)
		if err != nil {
			if ctx.Err() != nil {
				return traceFollowDetached(errOut)
			}
			if reconnectsRemaining == 0 || !traceFollowRetryableSubscribeError(err) {
				if errOut != nil {
					writeCLIAPIError(errOut, err)
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
		streamErr, interrupted := consumeDiagnosticTraceSubscription(ctx, out, sub, &replaySince, traceWriter)
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
			writeCLIAPIError(errOut, streamErr)
			return commandExitError{code: runCommandErrorExitCode(streamErr)}
		}
		reconnectsRemaining--
		retryAttempt++
		if err := waitTraceFollowReconnect(ctx, errOut, retryAttempt); err != nil {
			return err
		}
	}
}

func consumeDiagnosticTraceSubscription(ctx context.Context, out io.Writer, sub *runTraceSubscription, replaySince **time.Time, traceWriter *runTraceRowLineWriter) (error, bool) {
	if traceWriter == nil {
		traceWriter = &runTraceRowLineWriter{}
	}
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
			traceWriter.Write(out, row)
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
	if cliAPIIsTransportFailure(err) {
		return true
	}
	return false
}

func traceFollowRetryableStreamError(err error) bool {
	if err == nil {
		return false
	}
	if cliAPIIsTransportFailure(err) {
		return true
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
	if BoolPointerValue(result.Alive) && BoolPointerValue(result.Ready) && BoolPointerValue(result.DBOK) && BoolPointerValue(result.RuntimeOK) {
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
	window, err := readwindow.Resolve(readwindow.Input{
		Since:        readwindow.BoundInput{Value: o.since, Set: o.sinceSet},
		Until:        readwindow.BoundInput{Value: o.until, Set: o.untilSet},
		ReferenceUTC: o.reference,
	})
	if err != nil {
		return nil, err
	}
	window.AddParams(params)
	return params, nil
}

func validateRFC3339Flag(name, value string) error {
	_, err := parseRFC3339Flag(name, value)
	return err
}

func parseRFC3339Flag(name, value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s must be an RFC3339 timestamp: %w", name, err)
	}
	return parsed, nil
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

func validateDiagnosticRunDiagnosis(result DiagnosticRunDiagnosisResult) error {
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
	if result.TestQuiescence == nil {
		return fmt.Errorf("malformed run.diagnose result: test_quiescence is required")
	}
	if err := validateDiagnosticRunTestQuiescence(*result.TestQuiescence); err != nil {
		return err
	}
	for i, delivery := range result.FailedDeliveries {
		if err := validateDiagnosticRunFailureDelivery(fmt.Sprintf("failed_deliveries[%d]", i), delivery); err != nil {
			return err
		}
	}
	return nil
}

func validateDiagnosticRunTestQuiescence(result diagnosticRunTestQuiescence) error {
	fields := []struct {
		name  string
		value *int
	}{
		{name: "active_deliveries", value: result.ActiveDeliveries},
		{name: "unsettled_pipeline_events", value: result.UnsettledPipelineEvents},
		{name: "due_timers", value: result.DueTimers},
		{name: "active_session_leases", value: result.ActiveSessionLeases},
	}
	if result.Ready == nil {
		return fmt.Errorf("malformed run.diagnose result: test_quiescence.ready is required")
	}
	for _, field := range fields {
		if field.value == nil {
			return fmt.Errorf("malformed run.diagnose result: test_quiescence.%s is required", field.name)
		}
		if *field.value < 0 {
			return fmt.Errorf("malformed run.diagnose result: test_quiescence.%s must be non-negative", field.name)
		}
	}
	return nil
}

func validateDiagnosticRunFailureDelivery(prefix string, delivery diagnosticRunFailureDelivery) error {
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "event_id", value: delivery.EventID},
		{name: "event_name", value: delivery.EventName},
		{name: "delivery_id", value: delivery.DeliveryID},
		{name: "subscriber_type", value: delivery.SubscriberType},
		{name: "subscriber_id", value: delivery.SubscriberID},
		{name: "status", value: delivery.Status},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("malformed run.diagnose result: %s.%s is required", prefix, field.name)
		}
	}
	if _, ok := eventObservationValidSubscriberTypes[strings.TrimSpace(delivery.SubscriberType)]; !ok {
		return fmt.Errorf("malformed run.diagnose result: %s.subscriber_type=%q is not a valid SubscriberType", prefix, delivery.SubscriberType)
	}
	if _, ok := eventObservationValidDeliveryStatuses[strings.TrimSpace(delivery.Status)]; !ok {
		return fmt.Errorf("malformed run.diagnose result: %s.status=%q is not a valid DeliveryStatus", prefix, delivery.Status)
	}
	if delivery.RetryCount < 0 {
		return fmt.Errorf("malformed run.diagnose result: %s.retry_count must be >= 0", prefix)
	}
	if err := validateEventDeliveryFailure(prefix, delivery.Status, delivery.Failure); err != nil {
		return fmt.Errorf("malformed run.diagnose result: %w", err)
	}
	for i, deadLetter := range delivery.DeadLetters {
		if err := validateEventDeadLetter(fmt.Sprintf("%s.dead_letters[%d]", prefix, i), deadLetter); err != nil {
			return err
		}
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
		if row.DeliveryID != "" {
			if err := validateEventDeliveryFailure(fmt.Sprintf("trace[%d]", i), row.DeliveryStatus, row.DeliveryFailure); err != nil {
				return fmt.Errorf("malformed run.trace result: %w", err)
			}
		}
		if err := validateRequiredTimestamp(fmt.Sprintf("trace[%d].event_created_at", i), row.EventCreatedAt); err != nil {
			return err
		}
		for _, field := range []struct {
			name  string
			value string
		}{
			{name: "delivery_created_at", value: row.DeliveryCreatedAt},
			{name: "delivery_started_at", value: row.DeliveryStartedAt},
			{name: "delivery_delivered_at", value: row.DeliveryDeliveredAt},
		} {
			if err := validateOptionalTimestamp(fmt.Sprintf("trace[%d].%s", i, field.name), field.value); err != nil {
				return err
			}
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
	if hash := strings.TrimSpace(result.Bundle.BundleHash); hash != "" && !cliBundleHashPattern.MatchString(hash) {
		return fmt.Errorf("malformed health.check result: bundle.bundle_hash must be bundle-v1:sha256:<64 lowercase hex>")
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
	if status == "failed" {
		if run.Failure == nil {
			return fmt.Errorf("malformed run header: %s.failure is required for failed status", prefix)
		}
		if err := runtimefailures.ValidateEnvelope(*run.Failure); err != nil {
			return fmt.Errorf("malformed run header: %s.failure: %w", prefix, err)
		}
	} else if run.Failure != nil {
		return fmt.Errorf("malformed run header: %s.failure is forbidden for status %s", prefix, status)
	}
	if status == "cancelled" && strings.TrimSpace(run.ControlReason) == "" {
		return fmt.Errorf("malformed run header: %s.control_reason is required for cancelled status", prefix)
	}
	continuedAsRunID := strings.TrimSpace(run.ContinuedAsRunID)
	if status == "forked" && continuedAsRunID == "" {
		return fmt.Errorf("malformed run header: %s.continued_as_run_id is required for forked status", prefix)
	}
	if status != "forked" && continuedAsRunID != "" {
		return fmt.Errorf("malformed run header: %s.continued_as_run_id is forbidden for status %s", prefix, status)
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

func validateOptionalTimestamp(field, value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
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
	rows := make([][]string, 0, len(result.Runs))
	for _, run := range result.Runs {
		rows = append(rows, []string{
			run.RunID,
			diagnosticRunStatusLabel(run),
			run.StartedAt,
			fmt.Sprintf("%d", IntPointerValue(run.EventCount)),
			fmt.Sprintf("%d", IntPointerValue(run.EntityCount)),
			run.TriggerEventType,
		})
	}
	footers := []string{}
	if result.NextCursor != "" {
		footers = append(footers, fmt.Sprintf("next_cursor=%s", result.NextCursor))
	}
	writeCLITable(out, cliTable{
		Columns: []cliTableColumn{
			{Header: "RUN ID", KeyColumn: true, IdentifierFamily: cliIdentifierFamilyRun},
			{Header: "STATUS"},
			{Header: "STARTED"},
			{Header: "EVENTS"},
			{Header: "ENTITIES"},
			{Header: "TRIGGER"},
		},
		Rows:         rows,
		EmptyMessage: "No runs found. Start one: swarm run start --event <event>",
		FooterLines:  footers,
	})
}

func writeDiagnosticRunHeader(out io.Writer, run diagnosticRunHeader) {
	if out == nil {
		return
	}
	writeCLILabeledDetail(out, cliLabeledDetail{
		Title: fmt.Sprintf("Run %s  %s", run.RunID, diagnosticRunStatusLabel(run)),
		Rows:  diagnosticRunHeaderRows(run),
	})
}

func writeDiagnosticRunDiagnosis(out io.Writer, result DiagnosticRunDiagnosisResult) {
	if out == nil {
		return
	}
	rows := diagnosticRunHeaderRows(result.Run)
	state := stringPointerValue(result.OperationalState)
	if projectedRunStatus := diagnosticRunStatusLabel(result.Run); projectedRunStatus != formatCLIHumanCode(cliHumanCodeOperationalState, state) {
		rows = append(rows, cliLabeledDetailRow{Label: "run status", Value: projectedRunStatus})
	}
	if result.TestQuiescence != nil {
		settled := "not settled"
		if BoolPointerValue(result.TestQuiescence.Ready) {
			settled = "settled"
		}
		rows = append(rows, cliLabeledDetailRow{Label: "health", Value: fmt.Sprintf("%s, %s, %s, %s, %s, %s",
			settled,
			formatCLIHumanCount(IntPointerValue(result.TestQuiescence.ActiveDeliveries), "active delivery", "active deliveries"),
			formatCLIHumanCount(IntPointerValue(result.TestQuiescence.UnsettledPipelineEvents), "unsettled event", "unsettled events"),
			formatCLIHumanCount(IntPointerValue(result.TestQuiescence.DueTimers), "due timer", "due timers"),
			formatCLIHumanCount(IntPointerValue(result.TestQuiescence.ActiveSessionLeases), "active session lease", "active session leases"),
			formatCLIHumanCount(len(result.FailedDeliveries), "failed delivery", "failed deliveries"),
		)})
	}
	if layer := stringPointerValue(result.BlockingLayer); layer != "" {
		value := formatCLIHumanCode(cliHumanCodeRunBlockingLayer, layer)
		if reason := stringPointerValue(result.BlockingReason); reason != "" {
			value += ", " + formatCLIHumanCode(cliHumanCodeRunBlockingReason, reason)
		}
		rows = append(rows, cliLabeledDetailRow{Label: "blocker", Value: value})
	}
	failures := make([]string, 0, len(result.FailedDeliveries))
	for _, delivery := range result.FailedDeliveries {
		value := fmt.Sprintf("%s (%s), delivery %s, subscriber %s/%s, %s, %s, terminal %t, %s",
			delivery.EventName,
			delivery.EventID,
			delivery.DeliveryID,
			delivery.SubscriberType,
			delivery.SubscriberID,
			formatCLIHumanCode(cliHumanCodeDeliveryStatus, delivery.Status),
			formatCLIHumanCount(delivery.RetryCount, "retry", "retries"),
			delivery.Terminal,
			formatCLIHumanCount(len(delivery.DeadLetters), "dead letter", "dead letters"),
		)
		if strings.TrimSpace(delivery.ReasonCode) != "" {
			value += ", reason " + delivery.ReasonCode
		}
		if delivery.Failure != nil {
			value += ", failure " + eventObservationFailureSummary(delivery.Failure)
		}
		failures = append(failures, value)
	}
	titleState := formatCLIHumanCode(cliHumanCodeOperationalState, state)
	if strings.EqualFold(strings.TrimSpace(state), "forked") {
		titleState = diagnosticRunStatusLabel(result.Run)
	}
	writeCLILabeledDetail(out, cliLabeledDetail{
		Title: fmt.Sprintf("Run %s  %s", result.Run.RunID, titleState),
		Rows:  rows,
		Sections: []cliLabeledDetailSection{
			{Label: "notes", Items: result.Heuristics},
			{Label: "failed deliveries", Items: failures},
		},
	})
}

func diagnosticRunStatusLabel(run diagnosticRunHeader) string {
	status := formatCLIHumanCode(cliHumanCodeRunStatus, run.Status)
	if strings.EqualFold(strings.TrimSpace(run.Status), "forked") && strings.TrimSpace(run.ContinuedAsRunID) != "" {
		return fmt.Sprintf("%s - continued as run %s", status, strings.TrimSpace(run.ContinuedAsRunID))
	}
	return status
}

func diagnosticRunHeaderRows(run diagnosticRunHeader) []cliLabeledDetailRow {
	timing := "started " + run.StartedAt
	if run.EndedAt != "" {
		timing += ", ended " + run.EndedAt
	}
	rows := []cliLabeledDetailRow{
		{Label: "trigger", Value: fmt.Sprintf("%s (%s)", run.TriggerEventType, run.TriggerEventID)},
		{Label: "timing", Value: timing},
		{Label: "scale", Value: fmt.Sprintf("%s, %s",
			formatCLIHumanCount(IntPointerValue(run.EventCount), "event", "events"),
			formatCLIHumanCount(IntPointerValue(run.EntityCount), "entity", "entities"),
		)},
	}
	if run.ForkedFromRunID != "" {
		rows = append(rows, cliLabeledDetailRow{Label: "forked from", Value: run.ForkedFromRunID})
	}
	if run.ContinuedAsRunID != "" {
		rows = append(rows, cliLabeledDetailRow{Label: "continued as", Value: run.ContinuedAsRunID})
	}
	if run.Failure != nil {
		rows = append(rows,
			cliLabeledDetailRow{Label: "failure", Value: eventObservationFailureSummary(run.Failure)},
			cliLabeledDetailRow{Label: "problem", Value: run.Failure.Message},
			cliLabeledDetailRow{Label: "remediation", Value: run.Failure.Remediation},
		)
	}
	if run.ControlReason != "" {
		rows = append(rows, cliLabeledDetailRow{Label: "control reason", Value: run.ControlReason})
	}
	return rows
}

func writeDiagnosticRunTrace(out io.Writer, runID string, result diagnosticRunTraceResult, deliveryDetail, verbose bool) {
	if out == nil {
		return
	}
	startedAt, hasStart := traceRowsStart(result.Trace)
	fmt.Fprintf(out, "run trace %s", runID)
	if hasStart {
		fmt.Fprintf(out, " started %s", formatTraceWallClock(startedAt))
	}
	fmt.Fprintf(out, " rows=%d\n", len(result.Trace))
	if deliveryDetail {
		writeDiagnosticRunTraceDeliveryDetail(out, result.Trace)
	} else {
		rows := make([][]string, 0, len(result.Trace))
		includeSessionTurn := verbose || traceRowsHaveSessionOrTurn(result.Trace)
		for _, row := range result.Trace {
			tableRow := []string{
				formatTraceRelativeFrom(startedAt, hasStart, row.EventCreatedAt),
				row.EventName,
				row.EventID,
				emptyDash(formatCLIHumanCode(cliHumanCodeDeliveryStatus, row.DeliveryStatus)),
				formatTraceSubscriber(row),
			}
			if includeSessionTurn {
				tableRow = append(tableRow,
					emptyDash(row.SessionID),
					emptyDash(firstNonEmpty(row.TurnID, row.TurnTriggerEventType)),
				)
			}
			rows = append(rows, tableRow)
		}
		columns := []cliTableColumn{
			{Header: "TIME"},
			{Header: "EVENT"},
			{Header: "ID", KeyColumn: true, IdentifierFamily: cliIdentifierFamilyEvent},
			{Header: "DELIVERY"},
			{Header: "SUBSCRIBER", IdentifierFamily: cliIdentifierFamilySubscriber},
		}
		if includeSessionTurn {
			columns = append(columns,
				cliTableColumn{Header: "SESSION", IdentifierFamily: cliIdentifierFamilySession},
				cliTableColumn{Header: "TURN"},
			)
		}
		writeCLITable(out, cliTable{
			Columns:      columns,
			Rows:         rows,
			EmptyMessage: "No trace rows found for this run.",
		})
	}
	if result.NextCursor != "" {
		fmt.Fprintf(out, "next_cursor=%s\n", result.NextCursor)
	}
}

func traceRowsStart(rows []diagnosticRunTraceRow) (time.Time, bool) {
	if len(rows) == 0 {
		return time.Time{}, false
	}
	startedAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(rows[0].EventCreatedAt))
	if err != nil {
		return time.Time{}, false
	}
	return startedAt.UTC(), true
}

func traceRowsHaveSessionOrTurn(rows []diagnosticRunTraceRow) bool {
	for _, row := range rows {
		if strings.TrimSpace(row.SessionID) != "" || strings.TrimSpace(row.TurnID) != "" || strings.TrimSpace(row.TurnTriggerEventType) != "" {
			return true
		}
	}
	return false
}

func formatTraceRelativeFrom(startedAt time.Time, hasStart bool, raw string) string {
	at, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(raw))
	if err != nil || !hasStart {
		return raw
	}
	return formatTraceOffset(at.UTC().Sub(startedAt))
}

func formatTraceOffset(duration time.Duration) string {
	duration = duration.Round(time.Millisecond)
	if duration == 0 {
		return "+0ms"
	}
	sign := "+"
	if duration < 0 {
		sign = "-"
		duration = -duration
	}
	return sign + duration.String()
}

func formatTraceWallClock(at time.Time) string {
	return at.UTC().Format("15:04:05.000")
}

func formatTraceSubscriber(row diagnosticRunTraceRow) string {
	subscriber := strings.Trim(strings.TrimSpace(row.SubscriberType)+"/"+strings.TrimSpace(row.SubscriberID), "/")
	return emptyDash(subscriber)
}

func writeDiagnosticRunTraceDeliverySummary(out io.Writer, runID string, result diagnosticRunTraceSummaryResult) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "run trace delivery summary: run_id=%s snapshot=point-in-time trace_rows=%d delivery_rows=%d non_delivery_rows=%d\n",
		runID,
		result.TraceRows,
		result.DeliveryRows,
		result.NonDeliveryRows,
	)
	if len(result.Groups) == 0 {
		writeCLIEmptyState(out, "No delivery rows found for this trace.")
		return
	}
	rows := make([][]string, 0, len(result.Groups))
	for _, group := range result.Groups {
		rows = append(rows, []string{
			group.SubscriberType + "/" + group.SubscriberID,
			fmt.Sprintf("%d", group.StatusCounts["pending"]),
			fmt.Sprintf("%d", group.StatusCounts["in_progress"]),
			fmt.Sprintf("%d", group.StatusCounts["delivered"]),
			fmt.Sprintf("%d", group.StatusCounts["failed"]),
			fmt.Sprintf("%d", group.StatusCounts["dead_letter"]),
			traceDurationAverage(group.QueueWait),
			traceDurationMax(group.QueueWait),
			traceDurationAverage(group.ExecutionTime),
			traceDurationMax(group.ExecutionTime),
			fmt.Sprintf("%d", group.UnavailableQueue),
			fmt.Sprintf("%d", group.UnavailableExec),
		})
	}
	writeCLITable(out, cliTable{
		Columns: []cliTableColumn{
			{Header: "SUBSCRIBER", IdentifierFamily: cliIdentifierFamilySubscriber},
			{Header: strings.ToUpper(formatCLIHumanCode(cliHumanCodeDeliveryStatus, "pending"))},
			{Header: strings.ToUpper(formatCLIHumanCode(cliHumanCodeDeliveryStatus, "in_progress"))},
			{Header: strings.ToUpper(formatCLIHumanCode(cliHumanCodeDeliveryStatus, "delivered"))},
			{Header: strings.ToUpper(formatCLIHumanCode(cliHumanCodeDeliveryStatus, "failed"))},
			{Header: strings.ToUpper(formatCLIHumanCode(cliHumanCodeDeliveryStatus, "dead_letter"))},
			{Header: "AVG_QUEUE_WAIT"},
			{Header: "MAX_QUEUE_WAIT"},
			{Header: "AVG_EXECUTION_TIME"},
			{Header: "MAX_EXECUTION_TIME"},
			{Header: "QUEUE_UNAVAILABLE"},
			{Header: "EXECUTION_UNAVAILABLE"},
		},
		Rows: rows,
	})
}

func writeDiagnosticRunTraceDeliveryDetail(out io.Writer, rows []diagnosticRunTraceRow) {
	tableRows := make([][]string, 0, len(rows))
	for _, row := range rows {
		tableRows = append(tableRows, []string{
			row.EventCreatedAt,
			row.EventName,
			row.EventID,
			emptyDash(row.DeliveryID),
			emptyDash(row.SubscriberType) + "/" + emptyDash(row.SubscriberID),
			emptyDash(formatCLIHumanCode(cliHumanCodeDeliveryStatus, row.DeliveryStatus)),
			emptyDash(row.DeliveryReasonCode),
			emptyDash(row.ReplyContextID),
			emptyDash(row.DeliveryCreatedAt),
			emptyDash(row.DeliveryStartedAt),
			emptyDash(row.DeliveryDeliveredAt),
			traceDuration(row.DeliveryCreatedAt, row.DeliveryStartedAt),
			traceDuration(row.DeliveryStartedAt, row.DeliveryDeliveredAt),
			emptyDash(row.SessionID),
			emptyDash(firstNonEmpty(row.TurnID, row.TurnTriggerEventType)),
		})
	}
	writeCLITable(out, cliTable{
		Columns: []cliTableColumn{
			{Header: "EVENT AT"},
			{Header: "EVENT"},
			{Header: "EVENT ID", KeyColumn: true, IdentifierFamily: cliIdentifierFamilyEvent},
			{Header: "DELIVERY ID", KeyColumn: true},
			{Header: "SUBSCRIBER", IdentifierFamily: cliIdentifierFamilySubscriber},
			{Header: "STATUS"},
			{Header: "REASON"},
			{Header: "REPLY CONTEXT", KeyColumn: true},
			{Header: "DELIVERY CREATED"},
			{Header: "DELIVERY STARTED"},
			{Header: "DELIVERY DELIVERED"},
			{Header: "QUEUE WAIT"},
			{Header: "EXECUTION TIME"},
			{Header: "SESSION", IdentifierFamily: cliIdentifierFamilySession},
			{Header: "TURN"},
		},
		Rows:         tableRows,
		EmptyMessage: "No delivery rows found for this trace.",
	})
}

func traceDurationAverage(stats diagnosticTraceDurationStats) string {
	if stats.Count == 0 {
		return "-"
	}
	return (stats.Sum / time.Duration(stats.Count)).Round(time.Millisecond).String()
}

func traceDurationMax(stats diagnosticTraceDurationStats) string {
	if stats.Count == 0 {
		return "-"
	}
	return stats.Max.Round(time.Millisecond).String()
}

func traceDuration(start, end string) string {
	duration, ok := traceDurationValue(start, end)
	if !ok {
		return "-"
	}
	return duration.Round(time.Millisecond).String()
}

func traceDurationValue(start, end string) (time.Duration, bool) {
	start = strings.TrimSpace(start)
	end = strings.TrimSpace(end)
	if start == "" || end == "" {
		return 0, false
	}
	startAt, err := time.Parse(time.RFC3339Nano, start)
	if err != nil {
		return 0, false
	}
	endAt, err := time.Parse(time.RFC3339Nano, end)
	if err != nil {
		return 0, false
	}
	return endAt.Sub(startAt), true
}

func writeDiagnosticHealth(out io.Writer, result diagnosticHealthCheckResult) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "alive=%t ready=%t db_ok=%t runtime_ok=%t\n", BoolPointerValue(result.Alive), BoolPointerValue(result.Ready), BoolPointerValue(result.DBOK), BoolPointerValue(result.RuntimeOK))
	fmt.Fprintf(out, "bundle fingerprint=%s workflow_name=%s workflow_version=%s\n",
		result.Bundle.Fingerprint,
		emptyDash(stringPointerValue(result.Bundle.WorkflowName)),
		emptyDash(stringPointerValue(result.Bundle.WorkflowVersion)),
	)
	if hash := strings.TrimSpace(result.Bundle.BundleHash); hash != "" {
		fmt.Fprintf(out, "bundle_hash=%s\n", hash)
	}
}

func IntPointerValue(value *int) int {
	if value == nil {
		return 0
	}
	return *value
}

func BoolPointerValue(value *bool) bool {
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
