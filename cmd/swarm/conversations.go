package main

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/spf13/cobra"
)

const (
	conversationListMethod      = "conversation.list"
	conversationListTurnsMethod = "conversation.list_turns"
	conversationGetTurnMethod   = "conversation.get_turn"
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
	limit      int
	cursor     string
	limitSet   bool
	cursorSet  bool
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

type conversationTurnPage struct {
	Conversation conversationSummary        `json:"conversation"`
	Turns        []conversationTurnListItem `json:"turns"`
	NextCursor   string                     `json:"next_cursor,omitempty"`
}

type conversationTurnListItem struct {
	Ordinal          int                        `json:"ordinal"`
	TurnID           string                     `json:"turn_id"`
	CompletedAt      string                     `json:"completed_at"`
	DurationMS       *int                       `json:"duration_ms"`
	TriggerEventID   string                     `json:"trigger_event_id,omitempty"`
	TriggerEventType string                     `json:"trigger_event_type,omitempty"`
	ActivityCounts   conversationActivityCounts `json:"activity_counts"`
	Tokens           *conversationTokenUsage    `json:"tokens,omitempty"`
	Outcome          string                     `json:"outcome,omitempty"`
	ParseOK          *bool                      `json:"parse_ok"`
	Failure          *runtimefailures.Envelope  `json:"failure,omitempty"`
}

type conversationActivityCounts struct {
	Dispatch   int `json:"dispatch"`
	Tool       int `json:"tool"`
	ToolResult int `json:"tool_result"`
	Publish    int `json:"publish"`
	Output     int `json:"output"`
	Failure    int `json:"failure"`
}

type conversationTurn struct {
	Ordinal                int                       `json:"ordinal"`
	TurnID                 string                    `json:"turn_id"`
	CompletedAt            string                    `json:"completed_at"`
	DurationMS             *int                      `json:"duration_ms"`
	TriggerEventID         string                    `json:"trigger_event_id,omitempty"`
	TriggerEventType       string                    `json:"trigger_event_type,omitempty"`
	EntityID               string                    `json:"entity_id,omitempty"`
	TaskID                 string                    `json:"task_id,omitempty"`
	Activity               []conversationActivity    `json:"activity"`
	Tokens                 *conversationTokenUsage   `json:"tokens,omitempty"`
	Outcome                string                    `json:"outcome,omitempty"`
	ParseOK                *bool                     `json:"parse_ok"`
	Failure                *runtimefailures.Envelope `json:"failure,omitempty"`
	AssistantVisibleOutput string                    `json:"assistant_visible_output,omitempty"`
	RetryCount             int                       `json:"retry_count,omitempty"`
}

type conversationTurnDetail struct {
	Session conversationSummary `json:"session"`
	Turn    conversationTurn    `json:"turn"`
}

type conversationActivity struct {
	Kind      string `json:"kind"`
	ToolName  string `json:"tool_name,omitempty"`
	ToolUseID string `json:"tool_use_id,omitempty"`
	EventID   string `json:"event_id,omitempty"`
	EventType string `json:"event_type,omitempty"`
	Text      string `json:"text,omitempty"`
	OK        *bool  `json:"ok,omitempty"`
}

type conversationTokenUsage struct {
	Input     int64  `json:"input"`
	Output    int64  `json:"output"`
	Exactness string `json:"exactness"`
}

var conversationOpaqueIDPattern = regexp.MustCompile(`^[A-Za-z0-9_:.-]+$`)

func newConversationsCommand(opts rootCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "conversations",
		Short: "List agent conversations.",
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
		Short: "List conversations, or view one conversation and its turns.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(
		newConversationsListCommand(opts),
		newConversationViewCommand(opts),
		newConversationTurnCommand(opts),
	)
	return cmd
}

func newConversationsListCommand(opts rootCommandOptions) *cobra.Command {
	listOpts := conversationListCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List conversations.",
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
		Short: "View one conversation.",
		Args:  cliExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			viewOpts.limitSet = cmd.Flags().Changed("limit")
			viewOpts.cursorSet = cmd.Flags().Changed("cursor")
			if err := viewOpts.logging.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			if err := viewOpts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			return runConversationViewCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), viewOpts, args[0])
		},
	}
	setCLIArgDiscoveryHint(cmd, "List conversation session ids with `swarm conversation list`.")
	cmd.Flags().IntVar(&viewOpts.limit, "limit", 0, "Optional turn page size, 1-500")
	cmd.Flags().StringVar(&viewOpts.cursor, "cursor", "", "Turn pagination cursor")
	bindCLIAPIConnectionFlags(cmd, &viewOpts.apiOptions)
	bindCLIOutputFlags(cmd, &viewOpts.output)
	bindCLILoggingFlags(cmd, &viewOpts.logging)
	return cmd
}

func newConversationTurnCommand(opts rootCommandOptions) *cobra.Command {
	turnOpts := conversationTurnCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "turn <session-id> <turn-id-or-prefix>",
		Short: "View one conversation turn.",
		Args:  cliExactArgs(2),
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
	setCLIArgDiscoveryHint(cmd, "List conversation session ids with `swarm conversation list`.")
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
	params := map[string]any{"session_id": sessionID}
	if opts.limitSet {
		if opts.limit < 1 || opts.limit > 500 {
			return returnCLIValidationError(errOut, fmt.Errorf("--limit must be between 1 and 500"))
		}
		params["limit"] = opts.limit
	}
	if opts.cursorSet {
		cursor, err := conversationNonEmptyFlag("--cursor", opts.cursor)
		if err != nil {
			return returnCLIValidationError(errOut, err)
		}
		params["cursor"] = cursor
	}
	var result conversationTurnPage
	if err := client.call(ctx, conversationListTurnsMethod, params, &result); err != nil {
		return returnCLIAPIError(errOut, err, conversationViewAPIErrorClassifier())
	}
	if err := validateConversationTurnPageResult("conversation.list_turns result", result); err != nil {
		return returnCLIAPIError(errOut, err, conversationViewAPIErrorClassifier())
	}
	return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
		writeConversationDetailResult(w, result)
	}, func() ([]string, error) {
		return []string{result.Conversation.SessionID}, nil
	})
}

func runConversationTurnCommand(ctx context.Context, out, errOut io.Writer, opts conversationTurnCommandOptions, sessionID, turnID string) error {
	sessionID = strings.TrimSpace(sessionID)
	if err := validateConversationOpaqueIDArg("session id", sessionID); err != nil {
		return returnCLIValidationError(errOut, err)
	}
	turnID = strings.TrimSpace(turnID)
	if err := validateConversationOpaqueIDArg("turn id or prefix", turnID); err != nil {
		return returnCLIValidationError(errOut, err)
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, conversationTurnAPIErrorClassifier())
	}
	params := map[string]any{"session_id": sessionID, "turn_id": turnID}
	var result conversationTurnDetail
	if err := client.call(ctx, conversationGetTurnMethod, params, &result); err != nil {
		resolved, resolveErr := resolveCLIIdentifierAfterNotFound(ctx, client, cliIdentifierResolveRequest{
			Command: "swarm conversation turn", Selector: "arg:turn-id-or-prefix", Value: turnID,
			Scope: map[string]string{"session_id": sessionID},
		}, err, "TURN_NOT_FOUND")
		if resolveErr != nil {
			return returnCLIAPIError(errOut, resolveErr, conversationTurnAPIErrorClassifier())
		}
		params["turn_id"] = resolved
		if err := client.call(ctx, conversationGetTurnMethod, params, &result); err != nil {
			return returnCLIAPIError(errOut, err, conversationTurnAPIErrorClassifier())
		}
	}
	if err := validateConversationTurnDetailResult(result); err != nil {
		return returnCLIAPIError(errOut, err, conversationTurnAPIErrorClassifier())
	}
	return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
		writeConversationTurnDetailResult(w, result)
	}, func() ([]string, error) {
		return []string{fmt.Sprintf("%s %s %s", result.Session.SessionID, result.Turn.TurnID, firstNonEmpty(result.Turn.Outcome, result.Session.Status))}, nil
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

func validateConversationTurnPageResult(prefix string, result conversationTurnPage) error {
	if err := validateConversationSummary(prefix+".conversation", result.Conversation); err != nil {
		return err
	}
	if result.Turns == nil {
		return fmt.Errorf("malformed %s: turns is required", prefix)
	}
	for i, turn := range result.Turns {
		if err := validateConversationTurnListItem(fmt.Sprintf("%s.turns[%d]", prefix, i), turn); err != nil {
			return err
		}
	}
	return nil
}

func validateConversationTurnDetailResult(result conversationTurnDetail) error {
	if err := validateConversationSummary("conversation.get_turn result.session", result.Session); err != nil {
		return err
	}
	return validateConversationTurn("conversation.get_turn result.turn", result.Turn)
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
	if turn.Ordinal < 1 {
		return fmt.Errorf("malformed %s: ordinal must be at least 1", prefix)
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "turn_id", value: turn.TurnID},
		{name: "completed_at", value: turn.CompletedAt},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("malformed %s: %s is required", prefix, field.name)
		}
	}
	if err := validateConversationOpaqueIDArg(prefix+".turn_id", turn.TurnID); err != nil {
		return fmt.Errorf("malformed %s: %w", prefix, err)
	}
	if strings.TrimSpace(turn.TriggerEventID) != "" {
		if err := validateConversationOpaqueIDArg(prefix+".trigger_event_id", turn.TriggerEventID); err != nil {
			return fmt.Errorf("malformed %s: %w", prefix, err)
		}
	}
	if turn.ParseOK == nil {
		return fmt.Errorf("malformed %s: parse_ok is required", prefix)
	}
	if turn.DurationMS == nil {
		return fmt.Errorf("malformed %s: duration_ms is required", prefix)
	}
	if *turn.DurationMS < 0 {
		return fmt.Errorf("malformed %s: duration_ms must be non-negative", prefix)
	}
	if err := validateRequiredTimestamp(prefix+".completed_at", turn.CompletedAt); err != nil {
		return err
	}
	if turn.RetryCount < 0 {
		return fmt.Errorf("malformed %s: retry_count must be non-negative", prefix)
	}
	if turn.Activity == nil {
		return fmt.Errorf("malformed %s: activity is required", prefix)
	}
	if turn.Tokens != nil {
		if turn.Tokens.Input < 0 || turn.Tokens.Output < 0 {
			return fmt.Errorf("malformed %s: token totals must be non-negative", prefix)
		}
		switch turn.Tokens.Exactness {
		case "exact", "estimated":
		default:
			return fmt.Errorf("malformed %s: token exactness is invalid", prefix)
		}
	}
	return nil
}

func validateConversationTurnListItem(prefix string, turn conversationTurnListItem) error {
	if turn.Ordinal < 1 {
		return fmt.Errorf("malformed %s: ordinal must be at least 1", prefix)
	}
	if err := validateConversationOpaqueIDArg(prefix+".turn_id", turn.TurnID); err != nil {
		return fmt.Errorf("malformed %s: %w", prefix, err)
	}
	if err := validateRequiredTimestamp(prefix+".completed_at", turn.CompletedAt); err != nil {
		return err
	}
	if turn.DurationMS == nil || *turn.DurationMS < 0 {
		return fmt.Errorf("malformed %s: duration_ms must be non-negative", prefix)
	}
	if turn.ParseOK == nil {
		return fmt.Errorf("malformed %s: parse_ok is required", prefix)
	}
	if strings.TrimSpace(turn.TriggerEventID) != "" {
		if err := validateConversationOpaqueIDArg(prefix+".trigger_event_id", turn.TriggerEventID); err != nil {
			return fmt.Errorf("malformed %s: %w", prefix, err)
		}
	}
	for name, count := range map[string]int{
		"dispatch": turn.ActivityCounts.Dispatch, "tool": turn.ActivityCounts.Tool,
		"tool_result": turn.ActivityCounts.ToolResult, "publish": turn.ActivityCounts.Publish,
		"output": turn.ActivityCounts.Output, "failure": turn.ActivityCounts.Failure,
	} {
		if count < 0 {
			return fmt.Errorf("malformed %s: activity_counts.%s must be non-negative", prefix, name)
		}
	}
	if turn.Tokens != nil {
		if turn.Tokens.Input < 0 || turn.Tokens.Output < 0 {
			return fmt.Errorf("malformed %s: token totals must be non-negative", prefix)
		}
		switch turn.Tokens.Exactness {
		case "exact", "estimated":
		default:
			return fmt.Errorf("malformed %s: token exactness is invalid", prefix)
		}
	}
	return nil
}

func writeConversationListResult(out io.Writer, result conversationListResult) {
	if out == nil {
		return
	}
	rows := make([][]string, 0, len(result.Conversations))
	for _, item := range result.Conversations {
		rows = append(rows, []string{
			item.SessionID,
			item.AgentID,
			conversationDash(item.RunID),
			item.Status,
			fmt.Sprintf("%d", item.TurnCount),
			fmt.Sprintf("%d", item.MessageCount),
			item.StartedAt,
			conversationDash(item.EndedAt),
		})
	}
	footers := []string{}
	if strings.TrimSpace(result.NextCursor) != "" {
		footers = append(footers, fmt.Sprintf("next_cursor=%s", result.NextCursor))
	}
	writeCLITable(out, cliTable{
		Columns: []cliTableColumn{
			{Header: "SESSION_ID", KeyColumn: true, IdentifierFamily: cliIdentifierFamilySession},
			{Header: "AGENT", IdentifierFamily: cliIdentifierFamilyAgent},
			{Header: "RUN", IdentifierFamily: cliIdentifierFamilyRun},
			{Header: "STATUS"},
			{Header: "TURNS"},
			{Header: "MESSAGES"},
			{Header: "STARTED"},
			{Header: "ENDED"},
		},
		Rows:         rows,
		EmptyMessage: "No conversations match the current filters.",
		FooterLines:  footers,
	})
}

func writeConversationDetailResult(out io.Writer, result conversationTurnPage) {
	if out == nil {
		return
	}
	item := result.Conversation
	writeCLITitle(out, fmt.Sprintf("Conversation %s", item.SessionID))
	writeCLIFieldLine(out,
		cliDetailField{Key: "agent_id", Value: item.AgentID},
		cliDetailField{Key: "run_id", Value: conversationDash(item.RunID)},
		cliDetailField{Key: "status", Value: item.Status},
		cliDetailField{Key: "turns", Value: fmt.Sprintf("%d", item.TurnCount)},
		cliDetailField{Key: "messages", Value: fmt.Sprintf("%d", item.MessageCount)},
		cliDetailField{Key: "started_at", Value: item.StartedAt},
		cliDetailField{Key: "ended_at", Value: conversationDash(item.EndedAt)},
	)
	rows := make([][]string, 0, len(result.Turns))
	for _, turn := range result.Turns {
		rows = append(rows, []string{
			turn.TurnID,
			fmt.Sprintf("%d", turn.Ordinal),
			turn.CompletedAt,
			conversationListTriggerSummary(turn),
			conversationActivityCountsSummary(turn.ActivityCounts),
			conversationTokenSummary(turn.Tokens),
			fmt.Sprintf("%dms", *turn.DurationMS),
			conversationListOutcomeSummary(turn),
		})
	}
	footers := []string{}
	if strings.TrimSpace(result.NextCursor) != "" {
		footers = append(footers, fmt.Sprintf("next_cursor=%s", result.NextCursor))
	}
	writeCLITable(out, cliTable{
		Columns: []cliTableColumn{
			{Header: "TURN", KeyColumn: true, IdentifierFamily: cliIdentifierFamilyTurn},
			{Header: "#"},
			{Header: "TIME"},
			{Header: "TRIGGER", Truncatable: true},
			{Header: "ACTIVITY", Truncatable: true},
			{Header: "TOKENS"},
			{Header: "DURATION"},
			{Header: "OUTCOME", Truncatable: true},
		},
		Rows:         rows,
		EmptyMessage: "No turns recorded.",
		FooterLines:  footers,
	})
}

func writeConversationTurnDetailResult(out io.Writer, result conversationTurnDetail) {
	if out == nil {
		return
	}
	session := result.Session
	turn := result.Turn
	activity := make([]string, 0, len(turn.Activity))
	for _, item := range turn.Activity {
		activity = append(activity, conversationActivityDetail(item))
	}
	sections := []cliLabeledDetailSection{{Label: "ACTIVITY", Items: activity}}
	if output := strings.TrimSpace(turn.AssistantVisibleOutput); output != "" {
		sections = append(sections, cliLabeledDetailSection{Label: "ASSISTANT OUTPUT", Items: []string{output}})
	}
	writeCLILabeledDetail(out, cliLabeledDetail{
		Title: fmt.Sprintf("Conversation %s turn #%d", session.SessionID, turn.Ordinal),
		Rows: []cliLabeledDetailRow{
			{Label: "Turn", Value: turn.TurnID},
			{Label: "Agent", Value: session.AgentID},
			{Label: "Run", Value: session.RunID},
			{Label: "Completed", Value: turn.CompletedAt},
			{Label: "Duration", Value: fmt.Sprintf("%dms", *turn.DurationMS)},
			{Label: "Trigger", Value: conversationTurnTriggerSummary(turn)},
			{Label: "Entity", Value: turn.EntityID},
			{Label: "Task", Value: turn.TaskID},
			{Label: "Tokens", Value: conversationTokenSummary(turn.Tokens)},
			{Label: "Outcome", Value: conversationOutcomeSummary(turn)},
			{Label: "Retries", Value: fmt.Sprintf("%d", turn.RetryCount)},
		},
		Sections: sections,
	})
}

func conversationTurnTriggerSummary(turn conversationTurn) string {
	return firstNonEmpty(strings.TrimSpace(turn.TriggerEventType), strings.TrimSpace(turn.TriggerEventID))
}

func conversationListTriggerSummary(turn conversationTurnListItem) string {
	return firstNonEmpty(strings.TrimSpace(turn.TriggerEventType), strings.TrimSpace(turn.TriggerEventID))
}

func conversationActivityCountsSummary(counts conversationActivityCounts) string {
	parts := make([]string, 0, 6)
	for _, item := range []struct {
		count int
		label string
	}{
		{counts.Dispatch, "dispatch"},
		{counts.Tool, "tool"},
		{counts.ToolResult, "result"},
		{counts.Publish, "emit"},
		{counts.Output, "output"},
		{counts.Failure, "failure"},
	} {
		if item.count > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", item.count, item.label))
		}
	}
	return strings.Join(parts, " · ")
}

func conversationActivityDetail(item conversationActivity) string {
	fields := []string{strings.ReplaceAll(strings.TrimSpace(item.Kind), "_", " ")}
	for _, value := range []string{item.ToolName, item.ToolUseID, item.EventType, item.EventID, item.Text} {
		if value = strings.TrimSpace(value); value != "" {
			fields = append(fields, value)
		}
	}
	if item.OK != nil {
		fields = append(fields, fmt.Sprintf("ok=%t", *item.OK))
	}
	return strings.Join(fields, " · ")
}

func conversationTokenSummary(tokens *conversationTokenUsage) string {
	if tokens == nil {
		return ""
	}
	suffix := ""
	if tokens.Exactness == "estimated" {
		suffix = " (est)"
	}
	return fmt.Sprintf("%d→%d%s", tokens.Input, tokens.Output, suffix)
}

func conversationOutcomeSummary(turn conversationTurn) string {
	if turn.Failure != nil {
		return conversationFailureSummary(turn.Failure)
	}
	if strings.TrimSpace(turn.Outcome) != "" {
		return strings.TrimSpace(turn.Outcome)
	}
	if turn.ParseOK != nil && !*turn.ParseOK {
		return "invalid response"
	}
	return ""
}

func conversationListOutcomeSummary(turn conversationTurnListItem) string {
	if turn.Failure != nil {
		return conversationFailureSummary(turn.Failure)
	}
	if strings.TrimSpace(turn.Outcome) != "" {
		return strings.TrimSpace(turn.Outcome)
	}
	if turn.ParseOK != nil && !*turn.ParseOK {
		return "invalid response"
	}
	return ""
}

func conversationFailureSummary(failure *runtimefailures.Envelope) string {
	if failure == nil {
		return "-"
	}
	return string(failure.Class) + "/" + strings.TrimSpace(failure.Detail.Code)
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
