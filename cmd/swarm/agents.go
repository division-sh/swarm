package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

type agentListCommandOptions struct {
	apiOptions rootCommandOptions
	flow       string
	role       string
}

type agentDiagnoseCommandOptions struct {
	apiOptions  rootCommandOptions
	queueLimit  int
	queueCursor string
	asJSON      bool

	queueLimitSet  bool
	queueCursorSet bool
}

type agentDeliveriesCommandOptions struct {
	apiOptions       rootCommandOptions
	runID            string
	deliveryStatuses []string
	limit            int
	cursor           string
	asJSON           bool

	runIDSet  bool
	limitSet  bool
	cursorSet bool
}

type agentListResult struct {
	Agents []agentSummary `json:"agents"`
}

type agentDetailResult struct {
	Agent             agentSummary     `json:"agent"`
	CurrentSessionRef *agentSessionRef `json:"current_session_ref,omitempty"`
	LastTurnRef       *agentTurnRef    `json:"last_turn_ref,omitempty"`
}

type agentDiagnosisResult struct {
	AgentID           string                           `json:"agent_id"`
	Status            string                           `json:"status"`
	CurrentSessionRef *agentSessionRef                 `json:"current_session_ref,omitempty"`
	LastTurnRef       *agentTurnRef                    `json:"last_turn_ref,omitempty"`
	Queue             agentDiagnosisQueue              `json:"queue"`
	DeliveryLifecycle *agentDiagnosisDeliveryLifecycle `json:"delivery_lifecycle,omitempty"`
	RuntimeState      *agentDiagnosisRuntimeState      `json:"runtime_state,omitempty"`
	Active            *agentDiagnosisActive            `json:"active,omitempty"`
	LastToolOutcome   *agentDiagnosisLastToolOutcome   `json:"last_tool_outcome,omitempty"`
}

type agentDeliveryLifecycleListResult struct {
	AgentID    string                      `json:"agent_id"`
	Deliveries []agentDeliveryLifecycleRow `json:"deliveries"`
	NextCursor *string                     `json:"next_cursor,omitempty"`
}

type agentDeliveryLifecycleRow struct {
	DeliveryID          string  `json:"delivery_id"`
	EventID             string  `json:"event_id"`
	EventName           string  `json:"event_name"`
	RunID               *string `json:"run_id,omitempty"`
	EntityID            *string `json:"entity_id,omitempty"`
	Status              string  `json:"status"`
	RetryCount          *int    `json:"retry_count"`
	ReasonCode          string  `json:"reason_code,omitempty"`
	LastError           string  `json:"last_error,omitempty"`
	DeliveryCreatedAt   string  `json:"delivery_created_at"`
	DeliveryStartedAt   *string `json:"delivery_started_at,omitempty"`
	DeliveryDeliveredAt *string `json:"delivery_delivered_at,omitempty"`
}

type agentDiagnosisQueue struct {
	PendingCount            int                             `json:"pending_count"`
	OldestPendingAgeSeconds int                             `json:"oldest_pending_age_seconds"`
	PendingDeliveries       []agentDiagnosisPendingDelivery `json:"pending_deliveries"`
	NextCursor              *string                         `json:"next_cursor,omitempty"`
}

type agentDiagnosisPendingDelivery struct {
	EventID    string `json:"event_id"`
	EventName  string `json:"event_name"`
	EnqueuedAt string `json:"enqueued_at"`
	Attempts   int    `json:"attempts"`
}

type agentDiagnosisDeliveryLifecycle struct {
	State         string `json:"state"`
	BlockingLayer string `json:"blocking_layer"`
}

type agentDiagnosisRuntimeState struct {
	Watchdog *agentDiagnosisWatchdog `json:"watchdog"`
}

type agentDiagnosisWatchdog struct {
	State         string  `json:"state"`
	BlockingLayer string  `json:"blocking_layer"`
	Action        string  `json:"action"`
	Outcome       string  `json:"outcome"`
	LastOutputAt  *string `json:"last_output_at,omitempty"`
	RecordedAt    string  `json:"recorded_at"`
}

type agentDiagnosisActive struct {
	TurnID   string  `json:"turn_id"`
	TaskID   *string `json:"task_id,omitempty"`
	EntityID *string `json:"entity_id,omitempty"`
}

type agentDiagnosisLastToolOutcome struct {
	TurnID    string          `json:"turn_id"`
	ToolName  string          `json:"tool_name"`
	ToolUseID *string         `json:"tool_use_id,omitempty"`
	OK        *bool           `json:"ok"`
	Result    json.RawMessage `json:"result,omitempty"`
}

type agentSummary struct {
	AgentID      string `json:"agent_id"`
	Role         string `json:"role"`
	Type         string `json:"type"`
	Model        string `json:"model"`
	Mode         string `json:"mode"`
	SessionScope string `json:"session_scope,omitempty"`
	Status       string `json:"status"`
}

type agentSessionRef struct {
	SessionID string `json:"session_id"`
	StartedAt string `json:"started_at"`
}

type agentTurnRef struct {
	TurnID      string `json:"turn_id"`
	CompletedAt string `json:"completed_at"`
	ParseOK     *bool  `json:"parse_ok"`
	Error       string `json:"error,omitempty"`
}

var agentValidStatuses = map[string]struct{}{
	"idle":       {},
	"running":    {},
	"paused":     {},
	"failed":     {},
	"terminated": {},
}

var agentValidConversationModes = map[string]struct{}{
	"task":               {},
	"session":            {},
	"session_per_entity": {},
}

var agentValidSessionScopes = map[string]struct{}{
	"global": {},
	"flow":   {},
	"entity": {},
}

func newAgentsCommand(opts rootCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "agents",
		Short: "List agents and their current state.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(newAgentsListCommand(opts))
	return cmd
}

func newAgentCommand(opts rootCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "agent",
		Short: "List agents; inspect, direct, restart, or replay a single agent.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(
		newAgentsListCommand(opts),
		newAgentViewCommand(opts),
		newAgentDiagnoseCommand(opts),
		newAgentDeliveriesCommand(opts),
		newAgentRestartCommand(opts),
		newAgentReplayCommand(opts),
		newAgentReplayBacklogCommand(opts),
		newAgentDirectiveCommand(opts),
	)
	return cmd
}

func newAgentDeliveriesCommand(opts rootCommandOptions) *cobra.Command {
	deliveryOpts := agentDeliveriesCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "deliveries <agent-id>",
		Short: "List one agent's event delivery history.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			deliveryOpts.runIDSet = cmd.Flags().Changed("run-id")
			deliveryOpts.limitSet = cmd.Flags().Changed("limit")
			deliveryOpts.cursorSet = cmd.Flags().Changed("cursor")
			return runAgentDeliveriesCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), deliveryOpts, args[0])
		},
	}
	cmd.Flags().StringVar(&deliveryOpts.runID, "run-id", "", "Filter by run id")
	cmd.Flags().StringArrayVar(&deliveryOpts.deliveryStatuses, "delivery-status", nil, "Delivery status filter; repeat to match any")
	cmd.Flags().IntVar(&deliveryOpts.limit, "limit", 0, "Max lifecycle rows to return (1-200)")
	cmd.Flags().StringVar(&deliveryOpts.cursor, "cursor", "", "Opaque cursor returned by the previous lifecycle result")
	cmd.Flags().BoolVar(&deliveryOpts.asJSON, "json", false, cliOutputJSONFlagHelp)
	bindCLIAPIConnectionFlags(cmd, &deliveryOpts.apiOptions)
	return cmd
}

func newAgentDiagnoseCommand(opts rootCommandOptions) *cobra.Command {
	diagnoseOpts := agentDiagnoseCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "diagnose <agent-id>",
		Short: "Diagnose why an agent is stuck or failing.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			diagnoseOpts.queueLimitSet = cmd.Flags().Changed("queue-limit")
			diagnoseOpts.queueCursorSet = cmd.Flags().Changed("queue-cursor")
			return runAgentDiagnoseCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), diagnoseOpts, args[0])
		},
	}
	cmd.Flags().IntVar(&diagnoseOpts.queueLimit, "queue-limit", 0, "Max pending-delivery detail rows to return (1-200)")
	cmd.Flags().StringVar(&diagnoseOpts.queueCursor, "queue-cursor", "", "Opaque queue cursor returned by the previous diagnosis result")
	cmd.Flags().BoolVar(&diagnoseOpts.asJSON, "json", false, cliOutputJSONFlagHelp)
	bindCLIAPIConnectionFlags(cmd, &diagnoseOpts.apiOptions)
	return cmd
}

func newAgentsListCommand(opts rootCommandOptions) *cobra.Command {
	listOpts := agentListCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List declared agents and their status.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAgentListCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), listOpts)
		},
	}
	cmd.Flags().StringVar(&listOpts.flow, "flow", "", "Filter by canonical flow path")
	cmd.Flags().StringVar(&listOpts.role, "role", "", "Filter by agent role")
	bindCLIAPIConnectionFlags(cmd, &listOpts.apiOptions)
	return cmd
}

func newAgentViewCommand(opts rootCommandOptions) *cobra.Command {
	apiOpts := opts
	cmd := &cobra.Command{
		Use:   "view <agent-id>",
		Short: "View one agent's configuration and state.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAgentViewCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), apiOpts, args[0])
		},
	}
	bindCLIAPIConnectionFlags(cmd, &apiOpts)
	return cmd
}

func runAgentListCommand(ctx context.Context, out, errOut io.Writer, opts agentListCommandOptions) error {
	params := opts.params()
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, agentListAPIErrorClassifier())
	}
	var result agentListResult
	if err := client.call(ctx, "agent.list", params, &result); err != nil {
		return returnCLIAPIError(errOut, err, agentListAPIErrorClassifier())
	}
	if err := validateAgentListResult(result); err != nil {
		return returnCLIAPIError(errOut, err, agentListAPIErrorClassifier())
	}
	writeAgentListResult(out, result)
	return nil
}

func runAgentViewCommand(ctx context.Context, out, errOut io.Writer, opts rootCommandOptions, agentID string) error {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return returnCLIValidationError(errOut, fmt.Errorf("agent id is required"))
	}
	client, err := newCLIAPIClient(opts)
	if err != nil {
		return returnCLIAPIError(errOut, err, agentViewAPIErrorClassifier())
	}
	var result agentDetailResult
	if err := client.call(ctx, "agent.get", map[string]any{"agent_id": agentID}, &result); err != nil {
		return returnCLIAPIError(errOut, err, agentViewAPIErrorClassifier())
	}
	if err := validateAgentDetailResult(result); err != nil {
		return returnCLIAPIError(errOut, err, agentViewAPIErrorClassifier())
	}
	writeAgentDetailResult(out, result)
	return nil
}

func runAgentDeliveriesCommand(ctx context.Context, out, errOut io.Writer, opts agentDeliveriesCommandOptions, agentID string) error {
	params, err := opts.params(agentID)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, agentDeliveryLifecycleAPIErrorClassifier())
	}
	var result agentDeliveryLifecycleListResult
	if err := client.call(ctx, "agent.delivery_lifecycle", params, &result); err != nil {
		return returnCLIAPIError(errOut, err, agentDeliveryLifecycleAPIErrorClassifier())
	}
	if err := validateAgentDeliveryLifecycleListResult(result); err != nil {
		return returnCLIAPIError(errOut, err, agentDeliveryLifecycleAPIErrorClassifier())
	}
	if opts.asJSON {
		if err := json.NewEncoder(out).Encode(result); err != nil {
			return returnCLIValidationError(errOut, fmt.Errorf("render json output: %w", err))
		}
		return nil
	}
	writeAgentDeliveryLifecycleListResult(out, result)
	return nil
}

func runAgentDiagnoseCommand(ctx context.Context, out, errOut io.Writer, opts agentDiagnoseCommandOptions, agentID string) error {
	params, err := opts.params(agentID)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, agentDiagnoseAPIErrorClassifier())
	}
	var result agentDiagnosisResult
	if err := client.call(ctx, "agent.diagnose", params, &result); err != nil {
		return returnCLIAPIError(errOut, err, agentDiagnoseAPIErrorClassifier())
	}
	if err := validateAgentDiagnosisResult(result); err != nil {
		return returnCLIAPIError(errOut, err, agentDiagnoseAPIErrorClassifier())
	}
	if opts.asJSON {
		if err := json.NewEncoder(out).Encode(result); err != nil {
			return returnCLIValidationError(errOut, fmt.Errorf("render json output: %w", err))
		}
		return nil
	}
	writeAgentDiagnosisResult(out, result)
	return nil
}

func agentListAPIErrorClassifier() cliAPIErrorClassifier {
	return cliAPIErrorClassifier{}
}

func agentViewAPIErrorClassifier() cliAPIErrorClassifier {
	return cliAPIErrorClassifier{notFoundCodes: []string{"AGENT_NOT_FOUND"}}
}

func agentDiagnoseAPIErrorClassifier() cliAPIErrorClassifier {
	return cliAPIErrorClassifier{notFoundCodes: []string{"AGENT_NOT_FOUND"}}
}

func agentDeliveryLifecycleAPIErrorClassifier() cliAPIErrorClassifier {
	return cliAPIErrorClassifier{notFoundCodes: []string{"AGENT_NOT_FOUND"}}
}

func (opts agentListCommandOptions) params() map[string]any {
	params := map[string]any{}
	if flow := strings.TrimSpace(opts.flow); flow != "" {
		params["flow"] = flow
	}
	if role := strings.TrimSpace(opts.role); role != "" {
		params["role"] = role
	}
	return params
}

func (opts agentDiagnoseCommandOptions) params(agentID string) (map[string]any, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return nil, fmt.Errorf("agent id is required")
	}
	params := map[string]any{"agent_id": agentID}
	if opts.queueLimitSet {
		if opts.queueLimit < 1 || opts.queueLimit > 200 {
			return nil, fmt.Errorf("--queue-limit must be between 1 and 200")
		}
		params["queue_limit"] = opts.queueLimit
	}
	if opts.queueCursorSet {
		cursor := strings.TrimSpace(opts.queueCursor)
		if cursor == "" {
			return nil, fmt.Errorf("--queue-cursor is required when provided")
		}
		params["queue_cursor"] = cursor
	}
	return params, nil
}

func (opts agentDeliveriesCommandOptions) params(agentID string) (map[string]any, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return nil, fmt.Errorf("agent id is required")
	}
	if err := validateEntityOpaqueIDArg("agent id", agentID); err != nil {
		return nil, err
	}
	params := map[string]any{"agent_id": agentID}
	if opts.runIDSet {
		runID := strings.TrimSpace(opts.runID)
		if runID == "" {
			return nil, fmt.Errorf("--run-id is required when provided")
		}
		if err := validateEntityOpaqueIDArg("--run-id", runID); err != nil {
			return nil, err
		}
		params["run_id"] = runID
	}
	statuses, err := traceEnumList("--delivery-status", opts.deliveryStatuses, eventObservationValidDeliveryStatuses, "pending, in_progress, delivered, failed, dead_letter")
	if err != nil {
		return nil, err
	}
	if len(statuses) > 0 {
		params["delivery_status"] = statuses
	}
	if opts.limitSet {
		if opts.limit < 1 || opts.limit > 200 {
			return nil, fmt.Errorf("--limit must be between 1 and 200")
		}
		params["limit"] = opts.limit
	}
	if opts.cursorSet {
		cursor := strings.TrimSpace(opts.cursor)
		if cursor == "" {
			return nil, fmt.Errorf("--cursor is required when provided")
		}
		params["cursor"] = cursor
	}
	return params, nil
}

func validateAgentListResult(result agentListResult) error {
	if result.Agents == nil {
		return fmt.Errorf("malformed agent.list result: agents is required")
	}
	for i, agent := range result.Agents {
		if err := validateAgentSummary(agent); err != nil {
			return fmt.Errorf("malformed agent.list result: agents[%d]: %w", i, err)
		}
	}
	return nil
}

func validateAgentDetailResult(result agentDetailResult) error {
	if err := validateAgentSummary(result.Agent); err != nil {
		return fmt.Errorf("malformed agent.get result: agent: %w", err)
	}
	if ref := result.CurrentSessionRef; ref != nil {
		if strings.TrimSpace(ref.SessionID) == "" {
			return fmt.Errorf("malformed agent.get result: current_session_ref.session_id is required")
		}
		if err := validateAgentTimestamp("current_session_ref.started_at", ref.StartedAt); err != nil {
			return err
		}
	}
	if ref := result.LastTurnRef; ref != nil {
		if strings.TrimSpace(ref.TurnID) == "" {
			return fmt.Errorf("malformed agent.get result: last_turn_ref.turn_id is required")
		}
		if err := validateAgentTimestamp("last_turn_ref.completed_at", ref.CompletedAt); err != nil {
			return err
		}
		if ref.ParseOK == nil {
			return fmt.Errorf("malformed agent.get result: last_turn_ref.parse_ok is required")
		}
	}
	return nil
}

func validateAgentDiagnosisResult(result agentDiagnosisResult) error {
	if strings.TrimSpace(result.AgentID) == "" {
		return fmt.Errorf("malformed agent.diagnose result: agent_id is required")
	}
	if _, ok := agentValidStatuses[strings.TrimSpace(result.Status)]; !ok {
		return fmt.Errorf("malformed agent.diagnose result: status=%q is not a valid AgentStatus", result.Status)
	}
	if ref := result.CurrentSessionRef; ref != nil {
		if strings.TrimSpace(ref.SessionID) == "" {
			return fmt.Errorf("malformed agent.diagnose result: current_session_ref.session_id is required")
		}
		if err := validateAgentDiagnosisTimestamp("current_session_ref.started_at", ref.StartedAt); err != nil {
			return err
		}
	}
	if ref := result.LastTurnRef; ref != nil {
		if strings.TrimSpace(ref.TurnID) == "" {
			return fmt.Errorf("malformed agent.diagnose result: last_turn_ref.turn_id is required")
		}
		if err := validateAgentDiagnosisTimestamp("last_turn_ref.completed_at", ref.CompletedAt); err != nil {
			return err
		}
		if ref.ParseOK == nil {
			return fmt.Errorf("malformed agent.diagnose result: last_turn_ref.parse_ok is required")
		}
	}
	if result.Queue.PendingCount < 0 {
		return fmt.Errorf("malformed agent.diagnose result: queue.pending_count must be non-negative")
	}
	if result.Queue.OldestPendingAgeSeconds < 0 {
		return fmt.Errorf("malformed agent.diagnose result: queue.oldest_pending_age_seconds must be non-negative")
	}
	if result.Queue.PendingDeliveries == nil {
		return fmt.Errorf("malformed agent.diagnose result: queue.pending_deliveries is required")
	}
	if result.Queue.NextCursor != nil && strings.TrimSpace(*result.Queue.NextCursor) == "" {
		return fmt.Errorf("malformed agent.diagnose result: queue.next_cursor is empty")
	}
	for i, delivery := range result.Queue.PendingDeliveries {
		if strings.TrimSpace(delivery.EventID) == "" {
			return fmt.Errorf("malformed agent.diagnose result: queue.pending_deliveries[%d].event_id is required", i)
		}
		if strings.TrimSpace(delivery.EventName) == "" {
			return fmt.Errorf("malformed agent.diagnose result: queue.pending_deliveries[%d].event_name is required", i)
		}
		if err := validateAgentDiagnosisTimestamp(fmt.Sprintf("queue.pending_deliveries[%d].enqueued_at", i), delivery.EnqueuedAt); err != nil {
			return err
		}
		if delivery.Attempts < 0 {
			return fmt.Errorf("malformed agent.diagnose result: queue.pending_deliveries[%d].attempts must be non-negative", i)
		}
	}
	if lifecycle := result.DeliveryLifecycle; lifecycle != nil {
		if _, ok := agentDiagnosisLifecycleStates[strings.TrimSpace(lifecycle.State)]; !ok {
			return fmt.Errorf("malformed agent.diagnose result: delivery_lifecycle.state=%q is not valid", lifecycle.State)
		}
		if strings.TrimSpace(lifecycle.BlockingLayer) == "" {
			return fmt.Errorf("malformed agent.diagnose result: delivery_lifecycle.blocking_layer is required")
		}
	}
	if state := result.RuntimeState; state != nil {
		if err := validateAgentDiagnosisRuntimeState(state); err != nil {
			return err
		}
	}
	if active := result.Active; active != nil {
		if err := validateAgentDiagnosisActive(active); err != nil {
			return err
		}
	}
	if outcome := result.LastToolOutcome; outcome != nil {
		if err := validateAgentDiagnosisLastToolOutcome(outcome); err != nil {
			return err
		}
		if result.Active == nil {
			return fmt.Errorf("malformed agent.diagnose result: last_tool_outcome requires active selected-turn evidence")
		}
		if strings.TrimSpace(outcome.TurnID) != strings.TrimSpace(result.Active.TurnID) {
			return fmt.Errorf("malformed agent.diagnose result: last_tool_outcome.turn_id %q must match active.turn_id %q", outcome.TurnID, result.Active.TurnID)
		}
	}
	return nil
}

func validateAgentDeliveryLifecycleListResult(result agentDeliveryLifecycleListResult) error {
	if strings.TrimSpace(result.AgentID) == "" {
		return fmt.Errorf("malformed agent.delivery_lifecycle result: agent_id is required")
	}
	if err := validateEntityOpaqueIDArg("agent.delivery_lifecycle result.agent_id", result.AgentID); err != nil {
		return fmt.Errorf("malformed agent.delivery_lifecycle result: %w", err)
	}
	if result.Deliveries == nil {
		return fmt.Errorf("malformed agent.delivery_lifecycle result: deliveries is required")
	}
	if result.NextCursor != nil && strings.TrimSpace(*result.NextCursor) == "" {
		return fmt.Errorf("malformed agent.delivery_lifecycle result: next_cursor is empty")
	}
	for i, delivery := range result.Deliveries {
		if err := validateAgentDeliveryLifecycleRow(delivery, i); err != nil {
			return err
		}
	}
	return nil
}

func validateAgentDeliveryLifecycleRow(row agentDeliveryLifecycleRow, index int) error {
	prefix := fmt.Sprintf("deliveries[%d]", index)
	requiredOpaque := []struct {
		field string
		value string
	}{
		{field: "delivery_id", value: row.DeliveryID},
		{field: "event_id", value: row.EventID},
	}
	for _, item := range requiredOpaque {
		if strings.TrimSpace(item.value) == "" {
			return fmt.Errorf("malformed agent.delivery_lifecycle result: %s.%s is required", prefix, item.field)
		}
		if err := validateEntityOpaqueIDArg("agent.delivery_lifecycle result."+prefix+"."+item.field, item.value); err != nil {
			return fmt.Errorf("malformed agent.delivery_lifecycle result: %w", err)
		}
	}
	if strings.TrimSpace(row.EventName) == "" {
		return fmt.Errorf("malformed agent.delivery_lifecycle result: %s.event_name is required", prefix)
	}
	if optional := row.RunID; optional != nil {
		if strings.TrimSpace(*optional) == "" {
			return fmt.Errorf("malformed agent.delivery_lifecycle result: %s.run_id is empty", prefix)
		}
		if err := validateEntityOpaqueIDArg("agent.delivery_lifecycle result."+prefix+".run_id", *optional); err != nil {
			return fmt.Errorf("malformed agent.delivery_lifecycle result: %w", err)
		}
	}
	if optional := row.EntityID; optional != nil {
		if strings.TrimSpace(*optional) == "" {
			return fmt.Errorf("malformed agent.delivery_lifecycle result: %s.entity_id is empty", prefix)
		}
		if err := validateEntityOpaqueIDArg("agent.delivery_lifecycle result."+prefix+".entity_id", *optional); err != nil {
			return fmt.Errorf("malformed agent.delivery_lifecycle result: %w", err)
		}
	}
	if _, ok := eventObservationValidDeliveryStatuses[strings.TrimSpace(row.Status)]; !ok {
		return fmt.Errorf("malformed agent.delivery_lifecycle result: %s.status=%q is not a valid DeliveryStatus", prefix, row.Status)
	}
	if row.RetryCount == nil {
		return fmt.Errorf("malformed agent.delivery_lifecycle result: %s.retry_count is required", prefix)
	}
	if *row.RetryCount < 0 {
		return fmt.Errorf("malformed agent.delivery_lifecycle result: %s.retry_count must be non-negative", prefix)
	}
	if err := validateAgentDeliveryLifecycleTimestamp(prefix+".delivery_created_at", row.DeliveryCreatedAt); err != nil {
		return err
	}
	if timestamp := row.DeliveryStartedAt; timestamp != nil {
		if err := validateAgentDeliveryLifecycleTimestamp(prefix+".delivery_started_at", *timestamp); err != nil {
			return err
		}
	}
	if timestamp := row.DeliveryDeliveredAt; timestamp != nil {
		if err := validateAgentDeliveryLifecycleTimestamp(prefix+".delivery_delivered_at", *timestamp); err != nil {
			return err
		}
	}
	return nil
}

var agentDiagnosisLifecycleStates = map[string]struct{}{
	"queued":    {},
	"launching": {},
	"active":    {},
	"retrying":  {},
	"exhausted": {},
}

var agentDiagnosisWatchdogStates = map[string]struct{}{
	"healthy_long_running": {},
	"no_output":            {},
}

var agentDiagnosisWatchdogBlockingLayers = map[string]struct{}{
	"session_execution": {},
}

var agentDiagnosisWatchdogActions = map[string]struct{}{
	"turn_long_running": {},
	"session_no_output": {},
}

var agentDiagnosisWatchdogOutcomes = map[string]struct{}{
	"observed":        {},
	"warning_emitted": {},
}

func validateAgentDiagnosisRuntimeState(state *agentDiagnosisRuntimeState) error {
	if state.Watchdog == nil {
		return fmt.Errorf("malformed agent.diagnose result: runtime_state.watchdog is required")
	}
	watchdog := state.Watchdog
	if _, ok := agentDiagnosisWatchdogStates[strings.TrimSpace(watchdog.State)]; !ok {
		return fmt.Errorf("malformed agent.diagnose result: runtime_state.watchdog.state=%q is not valid", watchdog.State)
	}
	if _, ok := agentDiagnosisWatchdogBlockingLayers[strings.TrimSpace(watchdog.BlockingLayer)]; !ok {
		return fmt.Errorf("malformed agent.diagnose result: runtime_state.watchdog.blocking_layer=%q is not valid", watchdog.BlockingLayer)
	}
	if _, ok := agentDiagnosisWatchdogActions[strings.TrimSpace(watchdog.Action)]; !ok {
		return fmt.Errorf("malformed agent.diagnose result: runtime_state.watchdog.action=%q is not valid", watchdog.Action)
	}
	if _, ok := agentDiagnosisWatchdogOutcomes[strings.TrimSpace(watchdog.Outcome)]; !ok {
		return fmt.Errorf("malformed agent.diagnose result: runtime_state.watchdog.outcome=%q is not valid", watchdog.Outcome)
	}
	if strings.TrimSpace(watchdog.State) == "healthy_long_running" && watchdog.LastOutputAt == nil {
		return fmt.Errorf("malformed agent.diagnose result: runtime_state.watchdog.last_output_at is required for healthy_long_running state")
	}
	if watchdog.LastOutputAt != nil {
		if err := validateAgentDiagnosisTimestamp("runtime_state.watchdog.last_output_at", *watchdog.LastOutputAt); err != nil {
			return err
		}
	}
	if err := validateAgentDiagnosisTimestamp("runtime_state.watchdog.recorded_at", watchdog.RecordedAt); err != nil {
		return err
	}
	return nil
}

func validateAgentDiagnosisActive(active *agentDiagnosisActive) error {
	if strings.TrimSpace(active.TurnID) == "" {
		return fmt.Errorf("malformed agent.diagnose result: active.turn_id is required")
	}
	if active.TaskID != nil && strings.TrimSpace(*active.TaskID) == "" {
		return fmt.Errorf("malformed agent.diagnose result: active.task_id is empty")
	}
	if active.EntityID != nil && strings.TrimSpace(*active.EntityID) == "" {
		return fmt.Errorf("malformed agent.diagnose result: active.entity_id is empty")
	}
	return nil
}

func validateAgentDiagnosisLastToolOutcome(outcome *agentDiagnosisLastToolOutcome) error {
	if strings.TrimSpace(outcome.TurnID) == "" {
		return fmt.Errorf("malformed agent.diagnose result: last_tool_outcome.turn_id is required")
	}
	if strings.TrimSpace(outcome.ToolName) == "" {
		return fmt.Errorf("malformed agent.diagnose result: last_tool_outcome.tool_name is required")
	}
	if outcome.ToolUseID != nil && strings.TrimSpace(*outcome.ToolUseID) == "" {
		return fmt.Errorf("malformed agent.diagnose result: last_tool_outcome.tool_use_id is empty")
	}
	if outcome.OK == nil {
		return fmt.Errorf("malformed agent.diagnose result: last_tool_outcome.ok is required")
	}
	if len(outcome.Result) > 0 && !jsonRawMessageIsObject(outcome.Result) {
		return fmt.Errorf("malformed agent.diagnose result: last_tool_outcome.result must be a JSON object")
	}
	return nil
}

func jsonRawMessageIsObject(raw json.RawMessage) bool {
	var decoded map[string]any
	return json.Unmarshal(raw, &decoded) == nil && decoded != nil
}

func validateAgentSummary(agent agentSummary) error {
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "agent_id", value: agent.AgentID},
		{name: "role", value: agent.Role},
		{name: "type", value: agent.Type},
		{name: "model", value: agent.Model},
		{name: "mode", value: agent.Mode},
		{name: "status", value: agent.Status},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("%s is required", field.name)
		}
	}
	if _, ok := agentValidStatuses[strings.TrimSpace(agent.Status)]; !ok {
		return fmt.Errorf("status=%q is not a valid AgentStatus", agent.Status)
	}
	if _, ok := agentValidConversationModes[strings.TrimSpace(agent.Mode)]; !ok {
		return fmt.Errorf("mode=%q is not a valid AgentMode", agent.Mode)
	}
	if strings.TrimSpace(agent.Mode) != "task" {
		if _, ok := agentValidSessionScopes[strings.TrimSpace(agent.SessionScope)]; !ok {
			return fmt.Errorf("session_scope=%q is not a valid SessionScope", agent.SessionScope)
		}
	}
	return nil
}

func validateAgentDeliveryLifecycleTimestamp(field, value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("malformed agent.delivery_lifecycle result: %s is required", field)
	}
	if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
		return fmt.Errorf("malformed agent.delivery_lifecycle result: %s must be an RFC3339 timestamp: %w", field, err)
	}
	return nil
}

func validateAgentDiagnosisTimestamp(field, value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("malformed agent.diagnose result: %s is required", field)
	}
	if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
		return fmt.Errorf("malformed agent.diagnose result: %s must be an RFC3339 timestamp: %w", field, err)
	}
	return nil
}

func validateAgentTimestamp(field, value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("malformed agent.get result: %s is required", field)
	}
	if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
		return fmt.Errorf("malformed agent.get result: %s must be an RFC3339 timestamp: %w", field, err)
	}
	return nil
}

func writeAgentListResult(out io.Writer, result agentListResult) {
	if out == nil {
		return
	}
	if len(result.Agents) == 0 {
		fmt.Fprintln(out, "No agents match the filter.")
		return
	}
	fmt.Fprintln(out, "AGENT_ID\tROLE\tTYPE\tSTATUS\tMODEL\tMODE\tSCOPE")
	for _, agent := range result.Agents {
		fmt.Fprintf(out, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			agent.AgentID,
			agent.Role,
			agent.Type,
			agent.Status,
			agent.Model,
			agent.Mode,
			agent.SessionScope,
		)
	}
}

func writeAgentDeliveryLifecycleListResult(out io.Writer, result agentDeliveryLifecycleListResult) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "Agent %s deliveries\n", result.AgentID)
	if len(result.Deliveries) == 0 {
		fmt.Fprintln(out, "No deliveries match the filter.")
	} else {
		for _, delivery := range result.Deliveries {
			fmt.Fprintf(out, "delivery: delivery_id=%s event_name=%s event_id=%s run_id=%s entity_id=%s status=%s delivery_created_at=%s delivery_started_at=%s delivery_delivered_at=%s retry_count=%d reason_code=%s last_error=%s\n",
				delivery.DeliveryID,
				delivery.EventName,
				delivery.EventID,
				agentOptionalStringDash(delivery.RunID),
				agentOptionalStringDash(delivery.EntityID),
				delivery.Status,
				delivery.DeliveryCreatedAt,
				agentOptionalStringDash(delivery.DeliveryStartedAt),
				agentOptionalStringDash(delivery.DeliveryDeliveredAt),
				*delivery.RetryCount,
				agentDash(delivery.ReasonCode),
				agentDash(delivery.LastError),
			)
		}
	}
	if result.NextCursor != nil {
		fmt.Fprintf(out, "next_cursor=%s\n", strings.TrimSpace(*result.NextCursor))
	}
}

func writeAgentDiagnosisResult(out io.Writer, result agentDiagnosisResult) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "Agent %s\n", result.AgentID)
	fmt.Fprintf(out, "status=%s\n", result.Status)
	if ref := result.CurrentSessionRef; ref != nil {
		fmt.Fprintf(out, "current_session_ref: session_id=%s started_at=%s\n", ref.SessionID, ref.StartedAt)
	}
	if ref := result.LastTurnRef; ref != nil {
		fmt.Fprintf(out, "last_turn_ref: turn_id=%s completed_at=%s parse_ok=%t error=%s\n",
			ref.TurnID,
			ref.CompletedAt,
			*ref.ParseOK,
			agentDash(ref.Error),
		)
	}
	queue := result.Queue
	fmt.Fprintf(out, "queue: pending_count=%d oldest_pending_age_seconds=%d next_cursor=%s\n",
		queue.PendingCount,
		queue.OldestPendingAgeSeconds,
		agentOptionalStringDash(queue.NextCursor),
	)
	for _, delivery := range queue.PendingDeliveries {
		fmt.Fprintf(out, "pending_delivery: event_id=%s event_name=%s enqueued_at=%s attempts=%d\n",
			delivery.EventID,
			delivery.EventName,
			delivery.EnqueuedAt,
			delivery.Attempts,
		)
	}
	if lifecycle := result.DeliveryLifecycle; lifecycle != nil {
		fmt.Fprintf(out, "delivery_lifecycle: state=%s blocking_layer=%s\n", lifecycle.State, lifecycle.BlockingLayer)
	}
	if state := result.RuntimeState; state != nil && state.Watchdog != nil {
		watchdog := state.Watchdog
		fmt.Fprintf(out, "runtime_state.watchdog: state=%s blocking_layer=%s action=%s outcome=%s last_output_at=%s recorded_at=%s\n",
			watchdog.State,
			watchdog.BlockingLayer,
			watchdog.Action,
			watchdog.Outcome,
			agentOptionalStringDash(watchdog.LastOutputAt),
			watchdog.RecordedAt,
		)
	}
	if active := result.Active; active != nil {
		fmt.Fprintf(out, "active: turn_id=%s task_id=%s entity_id=%s\n",
			active.TurnID,
			agentOptionalStringDash(active.TaskID),
			agentOptionalStringDash(active.EntityID),
		)
	}
	if outcome := result.LastToolOutcome; outcome != nil {
		fmt.Fprintf(out, "last_tool_outcome: turn_id=%s tool_name=%s tool_use_id=%s ok=%t result=%s\n",
			outcome.TurnID,
			outcome.ToolName,
			agentOptionalStringDash(outcome.ToolUseID),
			*outcome.OK,
			agentJSONRawMessageDash(outcome.Result),
		)
	}
}

func writeAgentDetailResult(out io.Writer, result agentDetailResult) {
	if out == nil {
		return
	}
	agent := result.Agent
	fmt.Fprintf(out, "Agent %s\n", agent.AgentID)
	fmt.Fprintf(out, "role=%s type=%s status=%s model=%s mode=%s session_scope=%s\n",
		agent.Role,
		agent.Type,
		agent.Status,
		agent.Model,
		agent.Mode,
		agent.SessionScope,
	)
	if ref := result.CurrentSessionRef; ref != nil {
		fmt.Fprintf(out, "current_session_ref: session_id=%s started_at=%s\n", ref.SessionID, ref.StartedAt)
	}
	if ref := result.LastTurnRef; ref != nil {
		fmt.Fprintf(out, "last_turn_ref: turn_id=%s completed_at=%s parse_ok=%t error=%s\n",
			ref.TurnID,
			ref.CompletedAt,
			*ref.ParseOK,
			agentDash(ref.Error),
		)
	}
}

func agentOptionalStringDash(value *string) string {
	if value == nil {
		return "-"
	}
	return agentDash(*value)
}

func agentJSONRawMessageDash(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "-"
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		return string(raw)
	}
	return compact.String()
}

func agentDash(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "-"
	}
	return value
}
