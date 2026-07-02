package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

const (
	conversationListMethod    = "conversation.list"
	conversationGetMethod     = "conversation.get"
	conversationGetTurnMethod = "conversation.get_turn"
)

type conversationListCommandOptions struct {
	apiOptions rootCommandOptions
	output     cliOutputOptions
	logging    cliLoggingOptions

	agentID string
	runID   string
	limit   int
	cursor  string

	agentIDSet bool
	runIDSet   bool
	limitSet   bool
	cursorSet  bool
}

type conversationViewCommandOptions struct {
	apiOptions rootCommandOptions
	output     cliOutputOptions
	logging    cliLoggingOptions
}

type conversationTurnCommandOptions struct {
	apiOptions rootCommandOptions
	output     cliOutputOptions
	logging    cliLoggingOptions
}

type conversationListResult struct {
	Conversations []conversationSummary `json:"conversations"`
	NextCursor    string                `json:"next_cursor,omitempty"`
}

type conversationSummary struct {
	SessionID    string `json:"session_id"`
	AgentID      string `json:"agent_id"`
	RunID        string `json:"run_id,omitempty"`
	StartedAt    string `json:"started_at"`
	EndedAt      string `json:"ended_at,omitempty"`
	TurnCount    int    `json:"turn_count"`
	MessageCount int    `json:"message_count"`
	Status       string `json:"status"`
}

type conversationDetail struct {
	Conversation conversationSummary `json:"conversation"`
	Turns        []conversationTurn  `json:"turns"`
}

type conversationTurn struct {
	TurnIndex        int              `json:"turn_index"`
	TurnID           string           `json:"turn_id"`
	TriggerEventID   string           `json:"trigger_event_id"`
	TriggerEventType string           `json:"trigger_event_type"`
	RequestPayload   map[string]any   `json:"request_payload,omitempty"`
	ResponsePayload  map[string]any   `json:"response_payload,omitempty"`
	ToolCalls        []map[string]any `json:"tool_calls,omitempty"`
	TurnBlocks       []map[string]any `json:"turn_blocks,omitempty"`
	ParseOK          *bool            `json:"parse_ok"`
	LatencyMS        *int             `json:"latency_ms"`
	Error            string           `json:"error,omitempty"`
}

type conversationTurnDetail struct {
	Session       conversationSummary  `json:"session"`
	Turn          conversationDeepTurn `json:"turn"`
	TurnBlocksRaw []map[string]any     `json:"turn_blocks_raw"`
}

type conversationDeepTurn struct {
	TurnIndex                   int                           `json:"turn_index"`
	TurnID                      string                        `json:"turn_id"`
	Scope                       string                        `json:"scope,omitempty"`
	StartedAt                   string                        `json:"started_at"`
	CompletedAt                 string                        `json:"completed_at"`
	DurationMS                  *int                          `json:"duration_ms"`
	Outcome                     string                        `json:"outcome,omitempty"`
	ParseOK                     *bool                         `json:"parse_ok"`
	Error                       string                        `json:"error,omitempty"`
	RetryCount                  int                           `json:"retry_count,omitempty"`
	DispatchMetadata            *conversationDispatchMetadata `json:"dispatch_metadata"`
	AdvertisedTools             []string                      `json:"advertised_tools"`
	MCPToolsListed              []string                      `json:"mcp_tools_listed,omitempty"`
	MCPToolsVisible             []string                      `json:"mcp_tools_visible,omitempty"`
	ReasoningBlocks             []string                      `json:"reasoning_blocks,omitempty"`
	ProgressUpdates             []string                      `json:"progress_updates,omitempty"`
	ToolCalls                   []map[string]any              `json:"tool_calls,omitempty"`
	ToolResults                 []map[string]any              `json:"tool_results,omitempty"`
	EmittedEvents               []string                      `json:"emitted_events,omitempty"`
	RuntimeLogEntries           []runtimeLogEntry             `json:"runtime_log_entries"`
	ProviderMetadata            *conversationProviderMetadata `json:"provider_metadata"`
	RequestPayload              map[string]any                `json:"request_payload,omitempty"`
	ResponsePayload             map[string]any                `json:"response_payload,omitempty"`
	FullPromptContext           json.RawMessage               `json:"full_prompt_context"`
	FullPromptContextV2Reserved *bool                         `json:"full_prompt_context_v2_reserved"`
	RawLLMResponse              json.RawMessage               `json:"raw_llm_response"`
	RawLLMResponseV2Reserved    *bool                         `json:"raw_llm_response_v2_reserved"`
	AssistantVisibleOutput      string                        `json:"assistant_visible_output,omitempty"`
}

type conversationDispatchMetadata struct {
	TriggerEventID   string `json:"trigger_event_id,omitempty"`
	TriggerEventType string `json:"trigger_event_type,omitempty"`
	EntityID         string `json:"entity_id,omitempty"`
	TaskID           string `json:"task_id,omitempty"`
	RunID            string `json:"run_id,omitempty"`
}

type conversationProviderMetadata struct {
	LatencyMS *int `json:"latency_ms"`
}

var conversationOpaqueIDPattern = regexp.MustCompile(`^[A-Za-z0-9_:.-]+$`)

func newConversationsCommand(opts rootCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "conversations",
		Short: "List conversations through v1 RPC.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(newConversationsListCommand(opts))
	return cmd
}

func newConversationCommand(opts rootCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "conversation",
		Short: "View conversation details and turns through v1 RPC.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(
		newConversationViewCommand(opts),
		newConversationTurnCommand(opts),
	)
	return cmd
}

func newConversationsListCommand(opts rootCommandOptions) *cobra.Command {
	listOpts := conversationListCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List conversations through /v1/rpc conversation.list.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			listOpts.agentIDSet = cmd.Flags().Changed("agent-id")
			listOpts.runIDSet = cmd.Flags().Changed("run-id")
			listOpts.limitSet = cmd.Flags().Changed("limit")
			listOpts.cursorSet = cmd.Flags().Changed("cursor")
			if err := listOpts.logging.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			if err := listOpts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			return runConversationsListCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), listOpts)
		},
	}
	cmd.Flags().StringVar(&listOpts.agentID, "agent-id", "", "Filter by agent id")
	cmd.Flags().StringVar(&listOpts.runID, "run-id", "", "Filter by run id")
	cmd.Flags().IntVar(&listOpts.limit, "limit", 0, "Optional page size, 1-500")
	cmd.Flags().StringVar(&listOpts.cursor, "cursor", "", "Pagination cursor")
	bindCLIAPIConnectionFlags(cmd, &listOpts.apiOptions)
	bindCLIOutputFlags(cmd, &listOpts.output)
	bindCLILoggingFlags(cmd, &listOpts.logging)
	return cmd
}

func newConversationViewCommand(opts rootCommandOptions) *cobra.Command {
	viewOpts := conversationViewCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "view <session-id>",
		Short: "View one conversation through /v1/rpc conversation.get.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := viewOpts.logging.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			if err := viewOpts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			return runConversationViewCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), viewOpts, args[0])
		},
	}
	bindCLIAPIConnectionFlags(cmd, &viewOpts.apiOptions)
	bindCLIOutputFlags(cmd, &viewOpts.output)
	bindCLILoggingFlags(cmd, &viewOpts.logging)
	return cmd
}

func newConversationTurnCommand(opts rootCommandOptions) *cobra.Command {
	turnOpts := conversationTurnCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "turn <session-id> <turn-index>",
		Short: "View one conversation turn through /v1/rpc conversation.get_turn.",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := turnOpts.logging.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			if err := turnOpts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			return runConversationTurnCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), turnOpts, args[0], args[1])
		},
	}
	bindCLIAPIConnectionFlags(cmd, &turnOpts.apiOptions)
	bindCLIOutputFlags(cmd, &turnOpts.output)
	bindCLILoggingFlags(cmd, &turnOpts.logging)
	return cmd
}

func runConversationsListCommand(ctx context.Context, out, errOut io.Writer, opts conversationListCommandOptions) error {
	params, err := opts.params()
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, conversationListAPIErrorClassifier())
	}
	var result conversationListResult
	if err := client.call(ctx, conversationListMethod, params, &result); err != nil {
		return returnCLIAPIError(errOut, err, conversationListAPIErrorClassifier())
	}
	if err := validateConversationListResult(result); err != nil {
		return returnCLIAPIError(errOut, err, conversationListAPIErrorClassifier())
	}
	return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
		writeConversationListResult(w, result)
	}, func() ([]string, error) {
		return quietConversationList(result), nil
	})
}

func runConversationViewCommand(ctx context.Context, out, errOut io.Writer, opts conversationViewCommandOptions, sessionID string) error {
	sessionID = strings.TrimSpace(sessionID)
	if err := validateConversationOpaqueIDArg("session id", sessionID); err != nil {
		return returnCLIValidationError(errOut, err)
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, conversationViewAPIErrorClassifier())
	}
	var result conversationDetail
	if err := client.call(ctx, conversationGetMethod, map[string]any{"session_id": sessionID}, &result); err != nil {
		return returnCLIAPIError(errOut, err, conversationViewAPIErrorClassifier())
	}
	if err := validateConversationDetailResult("conversation.get result", result); err != nil {
		return returnCLIAPIError(errOut, err, conversationViewAPIErrorClassifier())
	}
	return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
		writeConversationDetailResult(w, result)
	}, func() ([]string, error) {
		return []string{result.Conversation.SessionID}, nil
	})
}

func runConversationTurnCommand(ctx context.Context, out, errOut io.Writer, opts conversationTurnCommandOptions, sessionID, turnIndexRaw string) error {
	sessionID = strings.TrimSpace(sessionID)
	if err := validateConversationOpaqueIDArg("session id", sessionID); err != nil {
		return returnCLIValidationError(errOut, err)
	}
	turnIndex, err := parseConversationTurnIndex(turnIndexRaw)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, conversationTurnAPIErrorClassifier())
	}
	params := map[string]any{"session_id": sessionID, "turn_index": turnIndex}
	var result conversationTurnDetail
	if err := client.call(ctx, conversationGetTurnMethod, params, &result); err != nil {
		return returnCLIAPIError(errOut, err, conversationTurnAPIErrorClassifier())
	}
	if err := validateConversationTurnDetailResult(result); err != nil {
		return returnCLIAPIError(errOut, err, conversationTurnAPIErrorClassifier())
	}
	return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
		writeConversationTurnDetailResult(w, result)
	}, func() ([]string, error) {
		return []string{fmt.Sprintf("%s %d %s", result.Session.SessionID, result.Turn.TurnIndex, firstNonEmpty(result.Turn.Outcome, result.Session.Status))}, nil
	})
}

func quietConversationList(result conversationListResult) []string {
	out := make([]string, 0, len(result.Conversations))
	for _, conversation := range result.Conversations {
		out = append(out, conversation.SessionID)
	}
	return out
}

func (opts conversationListCommandOptions) params() (map[string]any, error) {
	params := map[string]any{}
	if opts.agentIDSet {
		agentID, err := conversationNonEmptyFlag("--agent-id", opts.agentID)
		if err != nil {
			return nil, err
		}
		if err := validateConversationOpaqueIDArg("--agent-id", agentID); err != nil {
			return nil, err
		}
		params["agent_id"] = agentID
	}
	if opts.runIDSet {
		runID, err := conversationNonEmptyFlag("--run-id", opts.runID)
		if err != nil {
			return nil, err
		}
		if err := validateConversationOpaqueIDArg("--run-id", runID); err != nil {
			return nil, err
		}
		params["run_id"] = runID
	}
	if opts.limitSet {
		if opts.limit < 1 || opts.limit > 500 {
			return nil, fmt.Errorf("--limit must be between 1 and 500")
		}
		params["limit"] = opts.limit
	}
	if opts.cursorSet {
		cursor, err := conversationNonEmptyFlag("--cursor", opts.cursor)
		if err != nil {
			return nil, err
		}
		params["cursor"] = cursor
	}
	return params, nil
}

func parseConversationTurnIndex(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("turn index is required")
	}
	index, err := strconv.Atoi(raw)
	if err != nil || index < 1 || index > 1000000 {
		return 0, fmt.Errorf("turn index must be an integer from 1 to 1000000")
	}
	return index, nil
}

func conversationNonEmptyFlag(name, value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("%s must not be empty", name)
	}
	return value, nil
}

func validateConversationOpaqueIDArg(name, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", name)
	}
	if len(value) > 256 {
		return fmt.Errorf("%s must be at most 256 characters", name)
	}
	if !conversationOpaqueIDPattern.MatchString(value) {
		return fmt.Errorf("%s must match OpaqueId pattern", name)
	}
	return nil
}

func validateConversationListResult(result conversationListResult) error {
	if result.Conversations == nil {
		return fmt.Errorf("malformed conversation.list result: conversations is required")
	}
	for i, item := range result.Conversations {
		if err := validateConversationSummary(fmt.Sprintf("conversation.list result: conversations[%d]", i), item); err != nil {
			return err
		}
	}
	return nil
}

func validateConversationDetailResult(prefix string, result conversationDetail) error {
	if err := validateConversationSummary(prefix+".conversation", result.Conversation); err != nil {
		return err
	}
	if result.Turns == nil {
		return fmt.Errorf("malformed %s: turns is required", prefix)
	}
	for i, turn := range result.Turns {
		if err := validateConversationTurn(fmt.Sprintf("%s.turns[%d]", prefix, i), turn); err != nil {
			return err
		}
	}
	return nil
}

func validateConversationTurnDetailResult(result conversationTurnDetail) error {
	if err := validateConversationSummary("conversation.get_turn result.session", result.Session); err != nil {
		return err
	}
	if err := validateConversationDeepTurn("conversation.get_turn result.turn", result.Turn); err != nil {
		return err
	}
	if result.TurnBlocksRaw == nil {
		return fmt.Errorf("malformed conversation.get_turn result: turn_blocks_raw is required")
	}
	return nil
}

func validateConversationSummary(prefix string, item conversationSummary) error {
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "session_id", value: item.SessionID},
		{name: "agent_id", value: item.AgentID},
		{name: "started_at", value: item.StartedAt},
		{name: "status", value: item.Status},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("malformed %s: %s is required", prefix, field.name)
		}
	}
	if err := validateConversationOpaqueIDArg(prefix+".session_id", item.SessionID); err != nil {
		return fmt.Errorf("malformed %s: %w", prefix, err)
	}
	if err := validateConversationOpaqueIDArg(prefix+".agent_id", item.AgentID); err != nil {
		return fmt.Errorf("malformed %s: %w", prefix, err)
	}
	if strings.TrimSpace(item.RunID) != "" {
		if err := validateConversationOpaqueIDArg(prefix+".run_id", item.RunID); err != nil {
			return fmt.Errorf("malformed %s: %w", prefix, err)
		}
	}
	if item.TurnCount < 0 {
		return fmt.Errorf("malformed %s: turn_count must be non-negative", prefix)
	}
	if item.MessageCount < 0 {
		return fmt.Errorf("malformed %s: message_count must be non-negative", prefix)
	}
	switch strings.TrimSpace(item.Status) {
	case "active", "terminated":
	default:
		return fmt.Errorf("malformed %s: status=%q is not a valid conversation status", prefix, item.Status)
	}
	if err := validateRequiredTimestamp(prefix+".started_at", item.StartedAt); err != nil {
		return err
	}
	if endedAt := strings.TrimSpace(item.EndedAt); endedAt != "" {
		if err := validateRequiredTimestamp(prefix+".ended_at", endedAt); err != nil {
			return err
		}
	}
	return nil
}

func validateConversationTurn(prefix string, turn conversationTurn) error {
	if turn.TurnIndex < 1 {
		return fmt.Errorf("malformed %s: turn_index must be at least 1", prefix)
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "turn_id", value: turn.TurnID},
		{name: "trigger_event_id", value: turn.TriggerEventID},
		{name: "trigger_event_type", value: turn.TriggerEventType},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("malformed %s: %s is required", prefix, field.name)
		}
	}
	if err := validateConversationOpaqueIDArg(prefix+".turn_id", turn.TurnID); err != nil {
		return fmt.Errorf("malformed %s: %w", prefix, err)
	}
	if err := validateConversationOpaqueIDArg(prefix+".trigger_event_id", turn.TriggerEventID); err != nil {
		return fmt.Errorf("malformed %s: %w", prefix, err)
	}
	if turn.ParseOK == nil {
		return fmt.Errorf("malformed %s: parse_ok is required", prefix)
	}
	if turn.LatencyMS == nil {
		return fmt.Errorf("malformed %s: latency_ms is required", prefix)
	}
	if *turn.LatencyMS < 0 {
		return fmt.Errorf("malformed %s: latency_ms must be non-negative", prefix)
	}
	return nil
}

func validateConversationDeepTurn(prefix string, turn conversationDeepTurn) error {
	if turn.TurnIndex < 1 {
		return fmt.Errorf("malformed %s: turn_index must be at least 1", prefix)
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "turn_id", value: turn.TurnID},
		{name: "started_at", value: turn.StartedAt},
		{name: "completed_at", value: turn.CompletedAt},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("malformed %s: %s is required", prefix, field.name)
		}
	}
	if err := validateConversationOpaqueIDArg(prefix+".turn_id", turn.TurnID); err != nil {
		return fmt.Errorf("malformed %s: %w", prefix, err)
	}
	if err := validateRequiredTimestamp(prefix+".started_at", turn.StartedAt); err != nil {
		return err
	}
	if err := validateRequiredTimestamp(prefix+".completed_at", turn.CompletedAt); err != nil {
		return err
	}
	if turn.DurationMS == nil {
		return fmt.Errorf("malformed %s: duration_ms is required", prefix)
	}
	if *turn.DurationMS < 0 {
		return fmt.Errorf("malformed %s: duration_ms must be non-negative", prefix)
	}
	if turn.ParseOK == nil {
		return fmt.Errorf("malformed %s: parse_ok is required", prefix)
	}
	if turn.RetryCount < 0 {
		return fmt.Errorf("malformed %s: retry_count must be non-negative", prefix)
	}
	if turn.DispatchMetadata == nil {
		return fmt.Errorf("malformed %s: dispatch_metadata is required", prefix)
	}
	if strings.TrimSpace(turn.DispatchMetadata.TriggerEventID) != "" {
		if err := validateConversationOpaqueIDArg(prefix+".dispatch_metadata.trigger_event_id", turn.DispatchMetadata.TriggerEventID); err != nil {
			return fmt.Errorf("malformed %s: %w", prefix, err)
		}
	}
	if strings.TrimSpace(turn.DispatchMetadata.EntityID) != "" {
		if err := validateConversationOpaqueIDArg(prefix+".dispatch_metadata.entity_id", turn.DispatchMetadata.EntityID); err != nil {
			return fmt.Errorf("malformed %s: %w", prefix, err)
		}
	}
	if strings.TrimSpace(turn.DispatchMetadata.RunID) != "" {
		if err := validateConversationOpaqueIDArg(prefix+".dispatch_metadata.run_id", turn.DispatchMetadata.RunID); err != nil {
			return fmt.Errorf("malformed %s: %w", prefix, err)
		}
	}
	if turn.AdvertisedTools == nil {
		return fmt.Errorf("malformed %s: advertised_tools is required", prefix)
	}
	if turn.RuntimeLogEntries == nil {
		return fmt.Errorf("malformed %s: runtime_log_entries is required", prefix)
	}
	for i, log := range turn.RuntimeLogEntries {
		if err := validateRuntimeLogEntry(fmt.Sprintf("%s.runtime_log_entries[%d]", prefix, i), log); err != nil {
			return err
		}
	}
	if turn.ProviderMetadata == nil {
		return fmt.Errorf("malformed %s: provider_metadata is required", prefix)
	}
	if turn.ProviderMetadata.LatencyMS == nil {
		return fmt.Errorf("malformed %s: provider_metadata.latency_ms is required", prefix)
	}
	if *turn.ProviderMetadata.LatencyMS < 0 {
		return fmt.Errorf("malformed %s: provider_metadata.latency_ms must be non-negative", prefix)
	}
	if turn.FullPromptContext == nil {
		return fmt.Errorf("malformed %s: full_prompt_context is required", prefix)
	}
	if turn.FullPromptContextV2Reserved == nil || !*turn.FullPromptContextV2Reserved {
		return fmt.Errorf("malformed %s: full_prompt_context_v2_reserved must be true", prefix)
	}
	if turn.RawLLMResponse == nil {
		return fmt.Errorf("malformed %s: raw_llm_response is required", prefix)
	}
	if turn.RawLLMResponseV2Reserved == nil || !*turn.RawLLMResponseV2Reserved {
		return fmt.Errorf("malformed %s: raw_llm_response_v2_reserved must be true", prefix)
	}
	return nil
}

func writeConversationListResult(out io.Writer, result conversationListResult) {
	if out == nil {
		return
	}
	if len(result.Conversations) == 0 {
		fmt.Fprintln(out, "No conversations match the filter.")
		return
	}
	fmt.Fprintln(out, "SESSION_ID\tAGENT\tRUN\tSTATUS\tTURNS\tMESSAGES\tSTARTED\tENDED")
	for _, item := range result.Conversations {
		fmt.Fprintf(out, "%s\t%s\t%s\t%s\t%d\t%d\t%s\t%s\n",
			item.SessionID,
			item.AgentID,
			conversationDash(item.RunID),
			item.Status,
			item.TurnCount,
			item.MessageCount,
			item.StartedAt,
			conversationDash(item.EndedAt),
		)
	}
	if strings.TrimSpace(result.NextCursor) != "" {
		fmt.Fprintf(out, "next_cursor=%s\n", result.NextCursor)
	}
}

func writeConversationDetailResult(out io.Writer, result conversationDetail) {
	if out == nil {
		return
	}
	item := result.Conversation
	fmt.Fprintf(out, "Conversation %s\n", item.SessionID)
	fmt.Fprintf(out, "agent_id=%s run_id=%s status=%s turns=%d messages=%d started_at=%s ended_at=%s\n",
		item.AgentID,
		conversationDash(item.RunID),
		item.Status,
		item.TurnCount,
		item.MessageCount,
		item.StartedAt,
		conversationDash(item.EndedAt),
	)
	if len(result.Turns) == 0 {
		fmt.Fprintln(out, "No turns recorded.")
		return
	}
	fmt.Fprintln(out, "TURN\tTURN_ID\tEVENT_ID\tEVENT_TYPE\tPARSE_OK\tLATENCY_MS\tERROR")
	for _, turn := range result.Turns {
		fmt.Fprintf(out, "%d\t%s\t%s\t%s\t%t\t%d\t%s\n",
			turn.TurnIndex,
			turn.TurnID,
			turn.TriggerEventID,
			turn.TriggerEventType,
			*turn.ParseOK,
			*turn.LatencyMS,
			conversationDash(turn.Error),
		)
	}
}

func writeConversationTurnDetailResult(out io.Writer, result conversationTurnDetail) {
	if out == nil {
		return
	}
	session := result.Session
	turn := result.Turn
	fmt.Fprintf(out, "Conversation %s turn %d\n", session.SessionID, turn.TurnIndex)
	fmt.Fprintf(out, "turn_id=%s agent_id=%s run_id=%s status=%s started_at=%s completed_at=%s duration_ms=%d parse_ok=%t outcome=%s error=%s\n",
		turn.TurnID,
		session.AgentID,
		conversationDash(session.RunID),
		session.Status,
		turn.StartedAt,
		turn.CompletedAt,
		*turn.DurationMS,
		*turn.ParseOK,
		conversationDash(turn.Outcome),
		conversationDash(turn.Error),
	)
	dispatch := turn.DispatchMetadata
	fmt.Fprintf(out, "dispatch trigger_event_id=%s trigger_event_type=%s entity_id=%s task_id=%s run_id=%s\n",
		conversationDash(dispatch.TriggerEventID),
		conversationDash(dispatch.TriggerEventType),
		conversationDash(dispatch.EntityID),
		conversationDash(dispatch.TaskID),
		conversationDash(dispatch.RunID),
	)
	fmt.Fprintf(out, "advertised_tools=%s runtime_log_entries=%d turn_blocks_raw=%s\n",
		conversationListDash(turn.AdvertisedTools),
		len(turn.RuntimeLogEntries),
		conversationCompactJSON(result.TurnBlocksRaw),
	)
	if strings.TrimSpace(turn.AssistantVisibleOutput) != "" {
		fmt.Fprintf(out, "assistant_visible_output=%s\n", strings.TrimSpace(turn.AssistantVisibleOutput))
	}
	if len(turn.RuntimeLogEntries) > 0 {
		fmt.Fprintln(out, "Runtime logs:")
		for _, log := range turn.RuntimeLogEntries {
			fmt.Fprintf(out, "log_id=%s ts=%s level=%s component=%s source=%s message=%s\n",
				log.LogID,
				log.TS,
				log.Level,
				log.Component,
				log.Source,
				runtimeLogMessage(log),
			)
		}
	}
}

func conversationListAPIErrorClassifier() cliAPIErrorClassifier {
	return cliAPIErrorClassifier{}
}

func conversationViewAPIErrorClassifier() cliAPIErrorClassifier {
	return cliAPIErrorClassifier{notFoundCodes: []string{"SESSION_NOT_FOUND"}}
}

func conversationTurnAPIErrorClassifier() cliAPIErrorClassifier {
	return cliAPIErrorClassifier{notFoundCodes: []string{"SESSION_NOT_FOUND", "TURN_NOT_FOUND"}}
}

func conversationDash(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "-"
	}
	return value
}

func conversationListDash(values []string) string {
	if len(values) == 0 {
		return "-"
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			out = append(out, strings.TrimSpace(value))
		}
	}
	if len(out) == 0 {
		return "-"
	}
	return strings.Join(out, ",")
}

func conversationCompactJSON(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return "[]"
	}
	return string(raw)
}
