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
	AgentID          string `json:"agent_id"`
	Role             string `json:"role"`
	Type             string `json:"type"`
	ModelTier        string `json:"model_tier"`
	ConversationMode string `json:"conversation_mode"`
	SessionScope     string `json:"session_scope"`
	Status           string `json:"status"`
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
		Short: "List agents through v1 RPC.",
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
		Short: "View or direct one agent through v1 RPC.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(
		newAgentViewCommand(opts),
		newAgentDiagnoseCommand(opts),
		newAgentRestartCommand(opts),
		newAgentReplayCommand(opts),
		newAgentReplayBacklogCommand(opts),
		newAgentDirectiveCommand(opts),
	)
	return cmd
}

func newAgentDiagnoseCommand(opts rootCommandOptions) *cobra.Command {
	diagnoseOpts := agentDiagnoseCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "diagnose <agent-id>",
		Short: "Diagnose one agent through v1 RPC.",
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
		Short: "List declared agents through v1 RPC.",
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
		Short: "View one agent through v1 RPC.",
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
		{name: "model_tier", value: agent.ModelTier},
		{name: "conversation_mode", value: agent.ConversationMode},
		{name: "session_scope", value: agent.SessionScope},
		{name: "status", value: agent.Status},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("%s is required", field.name)
		}
	}
	if _, ok := agentValidStatuses[strings.TrimSpace(agent.Status)]; !ok {
		return fmt.Errorf("status=%q is not a valid AgentStatus", agent.Status)
	}
	if _, ok := agentValidConversationModes[strings.TrimSpace(agent.ConversationMode)]; !ok {
		return fmt.Errorf("conversation_mode=%q is not a valid ConversationMode", agent.ConversationMode)
	}
	if _, ok := agentValidSessionScopes[strings.TrimSpace(agent.SessionScope)]; !ok {
		return fmt.Errorf("session_scope=%q is not a valid SessionScope", agent.SessionScope)
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
	fmt.Fprintln(out, "AGENT_ID\tROLE\tTYPE\tSTATUS\tMODEL_TIER\tMODE\tSCOPE")
	for _, agent := range result.Agents {
		fmt.Fprintf(out, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			agent.AgentID,
			agent.Role,
			agent.Type,
			agent.Status,
			agent.ModelTier,
			agent.ConversationMode,
			agent.SessionScope,
		)
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
	fmt.Fprintf(out, "role=%s type=%s status=%s model_tier=%s conversation_mode=%s session_scope=%s\n",
		agent.Role,
		agent.Type,
		agent.Status,
		agent.ModelTier,
		agent.ConversationMode,
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
