package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

type diagnosticRunListOptions struct {
	apiOptions rootCommandOptions
	status     string
	since      string
	until      string
	limit      int
	cursor     string
}

type diagnosticTraceOptions struct {
	apiOptions rootCommandOptions
	limit      int
	cursor     string
}

type diagnosticRunOptions struct {
	apiOptions rootCommandOptions
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
	OperationalState string              `json:"operational_state"`
	BlockingLayer    string              `json:"blocking_layer"`
	BlockingReason   string              `json:"blocking_reason"`
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
	WorkflowName    string `json:"workflow_name"`
	WorkflowVersion string `json:"workflow_version"`
	Fingerprint     string `json:"fingerprint"`
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
			return runDiagnosticRunListCommand(cmd.Context(), cmd.OutOrStdout(), runOpts)
		},
	}
	bindDiagnosticRunListFlags(cmd, &runOpts)
	return cmd
}

func newInvestigateCommand(opts rootCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "investigate",
		Short: "Inspect runtime state through v1 RPC read owners.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(
		newInvestigateRunsCommand(opts),
		newInvestigateRunCommand(opts),
		newInvestigateTraceCommand(opts),
		newInvestigateHealthCommand(opts),
	)
	return cmd
}

func newInvestigateRunsCommand(opts rootCommandOptions) *cobra.Command {
	runOpts := diagnosticRunListOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "runs",
		Short: "List runs through the same v1 RPC path as swarm runs.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if cmd.Flags().Changed("limit") && runOpts.limit == 0 {
				return fmt.Errorf("--limit must be between 1 and 500")
			}
			return runDiagnosticRunListCommand(cmd.Context(), cmd.OutOrStdout(), runOpts)
		},
	}
	bindDiagnosticRunListFlags(cmd, &runOpts)
	return cmd
}

func newInvestigateRunCommand(opts rootCommandOptions) *cobra.Command {
	runOpts := diagnosticRunOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "run [run-id]",
		Short: "Diagnose one run through v1 RPC.",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runID := ""
			if len(args) == 1 {
				runID = args[0]
			}
			return runDiagnosticRunCommand(cmd.Context(), cmd.OutOrStdout(), runOpts, runID)
		},
	}
	cmd.Flags().BoolVar(&runOpts.noDiagnose, "no-diagnose", false, "Use run.get and print only the canonical run header")
	return cmd
}

func newInvestigateTraceCommand(opts rootCommandOptions) *cobra.Command {
	traceOpts := diagnosticTraceOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "trace [run-id]",
		Short: "Print a snapshot run trace through v1 RPC.",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if cmd.Flags().Changed("limit") && traceOpts.limit == 0 {
				return fmt.Errorf("--limit must be between 1 and 2000")
			}
			runID := ""
			if len(args) == 1 {
				runID = args[0]
			}
			return runDiagnosticTraceCommand(cmd.Context(), cmd.OutOrStdout(), traceOpts, runID)
		},
	}
	cmd.Flags().IntVar(&traceOpts.limit, "limit", 0, "Optional page size, 1-2000")
	cmd.Flags().StringVar(&traceOpts.cursor, "cursor", "", "Optional pagination cursor")
	return cmd
}

func newInvestigateHealthCommand(opts rootCommandOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "health",
		Short: "Print structured operator health through v1 RPC.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDiagnosticHealthCommand(cmd.Context(), cmd.OutOrStdout(), opts)
		},
	}
}

func bindDiagnosticRunListFlags(cmd *cobra.Command, opts *diagnosticRunListOptions) {
	cmd.Flags().StringVar(&opts.status, "status", "", "Optional run status filter")
	cmd.Flags().StringVar(&opts.since, "since", "", "Optional RFC3339 lower started_at bound")
	cmd.Flags().StringVar(&opts.until, "until", "", "Optional RFC3339 upper started_at bound")
	cmd.Flags().IntVar(&opts.limit, "limit", 0, "Optional page size, 1-500")
	cmd.Flags().StringVar(&opts.cursor, "cursor", "", "Optional pagination cursor")
}

func runDiagnosticRunListCommand(ctx context.Context, out io.Writer, opts diagnosticRunListOptions) error {
	params, err := opts.params()
	if err != nil {
		return err
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return err
	}
	result, err := fetchDiagnosticRunList(ctx, client, params)
	if err != nil {
		return err
	}
	writeDiagnosticRunList(out, result)
	return nil
}

func runDiagnosticRunCommand(ctx context.Context, out io.Writer, opts diagnosticRunOptions, runID string) error {
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return err
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		runID, err = latestDiagnosticRunID(ctx, client)
		if err != nil {
			return err
		}
	}
	if opts.noDiagnose {
		var result diagnosticRunGetResult
		if err := client.call(ctx, "run.get", map[string]any{"run_id": runID}, &result); err != nil {
			return err
		}
		if err := validateDiagnosticRunHeader("run", result.Run); err != nil {
			return err
		}
		writeDiagnosticRunHeader(out, result.Run)
		return nil
	}
	var result diagnosticRunDiagnosisResult
	if err := client.call(ctx, "run.diagnose", map[string]any{"run_id": runID}, &result); err != nil {
		return err
	}
	if err := validateDiagnosticRunDiagnosis(result); err != nil {
		return err
	}
	writeDiagnosticRunDiagnosis(out, result)
	return nil
}

func runDiagnosticTraceCommand(ctx context.Context, out io.Writer, opts diagnosticTraceOptions, runID string) error {
	params, err := opts.params()
	if err != nil {
		return err
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return err
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		runID, err = latestDiagnosticRunID(ctx, client)
		if err != nil {
			return err
		}
	}
	params["run_id"] = runID
	var result diagnosticRunTraceResult
	if err := client.call(ctx, "run.trace", params, &result); err != nil {
		return err
	}
	if err := validateDiagnosticRunTraceResult(result); err != nil {
		return err
	}
	writeDiagnosticRunTrace(out, runID, result)
	return nil
}

func runDiagnosticHealthCommand(ctx context.Context, out io.Writer, opts rootCommandOptions) error {
	client, err := newCLIAPIClient(opts)
	if err != nil {
		return err
	}
	var result diagnosticHealthCheckResult
	if err := client.call(ctx, "health.check", map[string]any{}, &result); err != nil {
		return err
	}
	if err := validateDiagnosticHealthCheck(result); err != nil {
		return err
	}
	writeDiagnosticHealth(out, result)
	return nil
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

func latestDiagnosticRunID(ctx context.Context, client *cliAPIClient) (string, error) {
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

func (o diagnosticTraceOptions) params() (map[string]any, error) {
	params := map[string]any{}
	if cursor := strings.TrimSpace(o.cursor); cursor != "" {
		params["cursor"] = cursor
	}
	if o.limit != 0 {
		if o.limit < 1 || o.limit > 2000 {
			return nil, fmt.Errorf("--limit must be between 1 and 2000")
		}
		params["limit"] = o.limit
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
	if strings.TrimSpace(result.OperationalState) == "" {
		return fmt.Errorf("malformed run.diagnose result: operational_state is required")
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
	return nil
}

func validateDiagnosticRunHeader(prefix string, run diagnosticRunHeader) error {
	if strings.TrimSpace(run.RunID) == "" {
		return fmt.Errorf("malformed run header: %s.run_id is required", prefix)
	}
	if strings.TrimSpace(run.Status) == "" {
		return fmt.Errorf("malformed run header: %s.status is required", prefix)
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
		result.OperationalState,
		result.BlockingLayer,
		result.BlockingReason,
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
		emptyDash(result.Bundle.WorkflowName),
		emptyDash(result.Bundle.WorkflowVersion),
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
