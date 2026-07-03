package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

const (
	forkChatCreateMethod = "conversation.fork"
	forkChatChatMethod   = "conversation.fork_chat"
	forkChatListMethod   = "conversation.fork_list"
	forkChatViewMethod   = "conversation.fork_view"
	forkChatDeleteMethod = "conversation.fork_delete"
)

type forkChatNewCommandOptions struct {
	apiOptions rootCommandOptions
	output     cliOutputOptions
	logging    cliLoggingOptions

	turnIndex      int
	eventID        string
	at             string
	message        string
	idempotencyKey string

	turnIndexSet      bool
	eventIDSet        bool
	atSet             bool
	messageSet        bool
	idempotencyKeySet bool
}

type forkChatResumeCommandOptions struct {
	apiOptions rootCommandOptions
	output     cliOutputOptions
	logging    cliLoggingOptions

	message        string
	idempotencyKey string

	messageSet        bool
	idempotencyKeySet bool
}

type forkChatListCommandOptions struct {
	apiOptions rootCommandOptions
	output     cliOutputOptions
	logging    cliLoggingOptions

	sourceSessionID string
	limit           int
	cursor          string

	sourceSessionIDSet bool
	limitSet           bool
	cursorSet          bool
}

type forkChatViewCommandOptions struct {
	apiOptions rootCommandOptions
	output     cliOutputOptions
	logging    cliLoggingOptions
}

type forkChatDeleteCommandOptions struct {
	apiOptions rootCommandOptions
	output     cliOutputOptions
	logging    cliLoggingOptions

	idempotencyKey    string
	idempotencyKeySet bool
}

type forkChatCreateResult struct {
	Fork                forkChatSession `json:"fork"`
	IdempotencyReplayed *bool           `json:"idempotency_replayed"`
}

type forkChatNewResult struct {
	Fork                forkChatSession     `json:"fork"`
	IdempotencyReplayed *bool               `json:"idempotency_replayed"`
	Chat                *forkChatChatResult `json:"chat,omitempty"`
}

type forkChatListResult struct {
	Forks      []forkChatSession `json:"forks"`
	NextCursor string            `json:"next_cursor,omitempty"`
}

type forkChatDeleteResult struct {
	OK                  *bool  `json:"ok"`
	ForkID              string `json:"fork_id"`
	Deleted             *bool  `json:"deleted"`
	AlreadyDeleted      *bool  `json:"already_deleted"`
	IdempotencyReplayed *bool  `json:"idempotency_replayed"`
}

type forkChatChatResult struct {
	ForkID              string                `json:"fork_id"`
	Turn                forkChatTurn          `json:"turn"`
	Snapshot            forkChatSnapshot      `json:"snapshot"`
	SandboxPolicy       forkChatSandboxPolicy `json:"sandbox_policy"`
	IdempotencyReplayed *bool                 `json:"idempotency_replayed"`
}

type forkChatSession struct {
	ForkID          string            `json:"fork_id"`
	SourceSessionID string            `json:"source_session_id"`
	SourceRunID     string            `json:"source_run_id,omitempty"`
	SourceAgentID   string            `json:"source_agent_id"`
	ForkPoint       forkChatForkPoint `json:"fork_point"`
	CreatedBy       string            `json:"created_by"`
	CreatedAt       string            `json:"created_at"`
	ExpiresAt       string            `json:"expires_at"`
	DeletedAt       string            `json:"deleted_at,omitempty"`
	State           string            `json:"state"`
	Turns           []forkChatTurn    `json:"turns"`
}

type forkChatForkPoint struct {
	Kind       string `json:"kind"`
	TurnIndex  int    `json:"turn_index"`
	TurnID     string `json:"turn_id"`
	EventID    string `json:"event_id,omitempty"`
	At         string `json:"at,omitempty"`
	SelectedAt string `json:"selected_at"`
}

type forkChatTurn struct {
	TurnIndex        int              `json:"turn_index"`
	TurnID           string           `json:"turn_id"`
	TriggerEventID   string           `json:"trigger_event_id,omitempty"`
	TriggerEventType string           `json:"trigger_event_type,omitempty"`
	RequestPayload   map[string]any   `json:"request_payload,omitempty"`
	ResponsePayload  map[string]any   `json:"response_payload,omitempty"`
	ToolCalls        []map[string]any `json:"tool_calls,omitempty"`
	TurnBlocks       []map[string]any `json:"turn_blocks,omitempty"`
	ParseOK          *bool            `json:"parse_ok"`
	LatencyMS        *int             `json:"latency_ms"`
	Error            string           `json:"error,omitempty"`
}

type forkChatSnapshot struct {
	ForkID          string                   `json:"fork_id"`
	SourceSessionID string                   `json:"source_session_id"`
	SourceRunID     string                   `json:"source_run_id,omitempty"`
	SourceAgentID   string                   `json:"source_agent_id"`
	SourceTurn      map[string]any           `json:"source_turn"`
	EntitySnapshot  []forkChatEntitySnapshot `json:"entity_snapshot"`
	SnapshotOwner   string                   `json:"snapshot_owner"`
	CreatedAt       string                   `json:"created_at"`
}

type forkChatEntitySnapshot struct {
	EntityID       string         `json:"entity_id"`
	CurrentState   string         `json:"current_state,omitempty"`
	EnteredStateAt string         `json:"entered_state_at,omitempty"`
	Fields         map[string]any `json:"fields,omitempty"`
	Gates          map[string]any `json:"gates,omitempty"`
	Accumulator    map[string]any `json:"accumulator,omitempty"`
}

type forkChatSandboxPolicy struct {
	Owner              string   `json:"owner"`
	ReadPolicy         string   `json:"read_policy"`
	WritePolicy        string   `json:"write_policy"`
	SideEffectingTools []string `json:"side_effecting_tools"`
	StubbedTools       []string `json:"stubbed_tools"`
}

func newForkChatCommand(opts rootCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "forkchat",
		Short: "Open sandboxed chat sessions against forked runs.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(
		newForkChatNewCommand(opts),
		newForkChatResumeCommand(opts),
		newForkChatListCommand(opts),
		newForkChatViewCommand(opts),
		newForkChatDeleteCommand(opts),
	)
	return cmd
}

func newForkChatNewCommand(opts rootCommandOptions) *cobra.Command {
	newOpts := forkChatNewCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "new <source-session-id>",
		Short: "Create a sandboxed forkchat session.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			newOpts.turnIndexSet = cmd.Flags().Changed("turn-index")
			newOpts.eventIDSet = cmd.Flags().Changed("event-id")
			newOpts.atSet = cmd.Flags().Changed("at")
			newOpts.messageSet = cmd.Flags().Changed("message")
			newOpts.idempotencyKeySet = cmd.Flags().Changed("idempotency-key")
			if err := newOpts.logging.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			if err := newOpts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			return runForkChatNewCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), newOpts, args[0])
		},
	}
	cmd.Flags().IntVar(&newOpts.turnIndex, "turn-index", 0, "Fork at this 1-based source conversation turn index")
	cmd.Flags().StringVar(&newOpts.eventID, "event-id", "", "Fork at the source turn triggered by this event id")
	cmd.Flags().StringVar(&newOpts.at, "at", "", "Fork at the latest source turn at or before this RFC3339 timestamp")
	cmd.Flags().StringVarP(&newOpts.message, "message", "m", "", "Optional first sandboxed message after fork creation")
	cmd.Flags().StringVar(&newOpts.idempotencyKey, "idempotency-key", "", "Optional idempotency key for safe retries (advanced)")
	_ = cmd.Flags().MarkHidden("idempotency-key")
	bindCLIOutputFlags(cmd, &newOpts.output)
	bindCLILoggingFlags(cmd, &newOpts.logging)
	bindCLIAPIConnectionFlagsWithClass(cmd, &newOpts.apiOptions, cliAPICommandClassMutating, "swarm forkchat new")
	return cmd
}

func newForkChatResumeCommand(opts rootCommandOptions) *cobra.Command {
	resumeOpts := forkChatResumeCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "resume <fork-id>",
		Short: "Send a message in a forkchat session.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resumeOpts.messageSet = cmd.Flags().Changed("message")
			resumeOpts.idempotencyKeySet = cmd.Flags().Changed("idempotency-key")
			if err := resumeOpts.logging.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			if err := resumeOpts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			return runForkChatResumeCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), resumeOpts, args[0])
		},
	}
	cmd.Flags().StringVarP(&resumeOpts.message, "message", "m", "", "Required sandboxed message to send")
	cmd.Flags().StringVar(&resumeOpts.idempotencyKey, "idempotency-key", "", "Optional idempotency key for safe retries (advanced)")
	_ = cmd.Flags().MarkHidden("idempotency-key")
	bindCLIOutputFlags(cmd, &resumeOpts.output)
	bindCLILoggingFlags(cmd, &resumeOpts.logging)
	bindCLIAPIConnectionFlagsWithClass(cmd, &resumeOpts.apiOptions, cliAPICommandClassMutating, "swarm forkchat resume")
	return cmd
}

func newForkChatListCommand(opts rootCommandOptions) *cobra.Command {
	listOpts := forkChatListCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List forkchat sessions.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			listOpts.sourceSessionIDSet = cmd.Flags().Changed("source-session-id")
			listOpts.limitSet = cmd.Flags().Changed("limit")
			listOpts.cursorSet = cmd.Flags().Changed("cursor")
			if err := listOpts.logging.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			if err := listOpts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			return runForkChatListCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), listOpts)
		},
	}
	cmd.Flags().StringVar(&listOpts.sourceSessionID, "source-session-id", "", "Filter by source conversation session id")
	cmd.Flags().IntVar(&listOpts.limit, "limit", 0, "Optional page size, 1-500")
	cmd.Flags().StringVar(&listOpts.cursor, "cursor", "", "Pagination cursor")
	bindCLIOutputFlags(cmd, &listOpts.output)
	bindCLILoggingFlags(cmd, &listOpts.logging)
	bindCLIAPIConnectionFlags(cmd, &listOpts.apiOptions)
	return cmd
}

func newForkChatViewCommand(opts rootCommandOptions) *cobra.Command {
	viewOpts := forkChatViewCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "view <fork-id>",
		Short: "View one forkchat session.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := viewOpts.logging.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			if err := viewOpts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			return runForkChatViewCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), viewOpts, args[0])
		},
	}
	bindCLIOutputFlags(cmd, &viewOpts.output)
	bindCLILoggingFlags(cmd, &viewOpts.logging)
	bindCLIAPIConnectionFlags(cmd, &viewOpts.apiOptions)
	return cmd
}

func newForkChatDeleteCommand(opts rootCommandOptions) *cobra.Command {
	deleteOpts := forkChatDeleteCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "delete <fork-id>",
		Short: "Delete one forkchat session.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			deleteOpts.idempotencyKeySet = cmd.Flags().Changed("idempotency-key")
			if err := deleteOpts.logging.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			if err := deleteOpts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			return runForkChatDeleteCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), deleteOpts, args[0])
		},
	}
	cmd.Flags().StringVar(&deleteOpts.idempotencyKey, "idempotency-key", "", "Optional idempotency key for safe retries (advanced)")
	_ = cmd.Flags().MarkHidden("idempotency-key")
	bindCLIOutputFlags(cmd, &deleteOpts.output)
	bindCLILoggingFlags(cmd, &deleteOpts.logging)
	bindCLIAPIConnectionFlagsWithClass(cmd, &deleteOpts.apiOptions, cliAPICommandClassMutating, "swarm forkchat delete")
	return cmd
}

func runForkChatNewCommand(ctx context.Context, out, errOut io.Writer, opts forkChatNewCommandOptions, sourceSessionID string) error {
	sourceSessionID = strings.TrimSpace(sourceSessionID)
	if err := validateConversationOpaqueIDArg("source session id", sourceSessionID); err != nil {
		return returnCLIValidationError(errOut, err)
	}
	params, err := opts.createParams(sourceSessionID)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	message, err := optionalNonEmptyFlag("--message", opts.message, opts.messageSet)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, forkChatAPIErrorClassifier())
	}
	var createResult forkChatCreateResult
	if err := client.call(ctx, forkChatCreateMethod, params, &createResult); err != nil {
		return returnCLIAPIError(errOut, err, forkChatAPIErrorClassifier())
	}
	if err := validateForkChatCreateResult(createResult); err != nil {
		return returnCLIAPIError(errOut, err, forkChatAPIErrorClassifier())
	}
	result := forkChatNewResult{Fork: createResult.Fork, IdempotencyReplayed: createResult.IdempotencyReplayed}
	if opts.messageSet {
		chatParams, err := forkChatChatParams(createResult.Fork.ForkID, message, opts.idempotencyKey, opts.idempotencyKeySet)
		if err != nil {
			return returnCLIValidationError(errOut, err)
		}
		var chatResult forkChatChatResult
		if err := client.call(ctx, forkChatChatMethod, chatParams, &chatResult); err != nil {
			return returnCLIAPIError(errOut, err, forkChatAPIErrorClassifier())
		}
		if err := validateForkChatChatResult(chatResult); err != nil {
			return returnCLIAPIError(errOut, err, forkChatAPIErrorClassifier())
		}
		result.Chat = &chatResult
	}
	return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
		writeForkChatNewResult(w, result)
	}, func() ([]string, error) {
		return []string{result.Fork.ForkID}, nil
	})
}

func runForkChatResumeCommand(ctx context.Context, out, errOut io.Writer, opts forkChatResumeCommandOptions, forkID string) error {
	if !opts.messageSet {
		return returnCLIValidationError(errOut, fmt.Errorf("--message is required"))
	}
	params, err := forkChatChatParams(forkID, opts.message, opts.idempotencyKey, opts.idempotencyKeySet)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, forkChatAPIErrorClassifier())
	}
	var result forkChatChatResult
	if err := client.call(ctx, forkChatChatMethod, params, &result); err != nil {
		return returnCLIAPIError(errOut, err, forkChatAPIErrorClassifier())
	}
	if err := validateForkChatChatResult(result); err != nil {
		return returnCLIAPIError(errOut, err, forkChatAPIErrorClassifier())
	}
	return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
		writeForkChatChatResult(w, result)
	}, func() ([]string, error) {
		return []string{result.Turn.TurnID}, nil
	})
}

func runForkChatListCommand(ctx context.Context, out, errOut io.Writer, opts forkChatListCommandOptions) error {
	params, err := opts.params()
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, forkChatAPIErrorClassifier())
	}
	var result forkChatListResult
	if err := client.call(ctx, forkChatListMethod, params, &result); err != nil {
		return returnCLIAPIError(errOut, err, forkChatAPIErrorClassifier())
	}
	if err := validateForkChatListResult(result); err != nil {
		return returnCLIAPIError(errOut, err, forkChatAPIErrorClassifier())
	}
	return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
		writeForkChatListResult(w, result)
	}, func() ([]string, error) {
		return quietForkChatList(result), nil
	})
}

func runForkChatViewCommand(ctx context.Context, out, errOut io.Writer, opts forkChatViewCommandOptions, forkID string) error {
	forkID = strings.TrimSpace(forkID)
	if err := validateConversationOpaqueIDArg("fork id", forkID); err != nil {
		return returnCLIValidationError(errOut, err)
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, forkChatAPIErrorClassifier())
	}
	var result forkChatSession
	if err := client.call(ctx, forkChatViewMethod, map[string]any{"fork_id": forkID}, &result); err != nil {
		return returnCLIAPIError(errOut, err, forkChatAPIErrorClassifier())
	}
	if err := validateForkChatSession("conversation.fork_view result", result); err != nil {
		return returnCLIAPIError(errOut, err, forkChatAPIErrorClassifier())
	}
	return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
		writeForkChatSessionDetail(w, result)
	}, func() ([]string, error) {
		return []string{result.ForkID}, nil
	})
}

func runForkChatDeleteCommand(ctx context.Context, out, errOut io.Writer, opts forkChatDeleteCommandOptions, forkID string) error {
	forkID = strings.TrimSpace(forkID)
	if err := validateConversationOpaqueIDArg("fork id", forkID); err != nil {
		return returnCLIValidationError(errOut, err)
	}
	idempotencyKey, err := optionalNonEmptyFlag("--idempotency-key", opts.idempotencyKey, opts.idempotencyKeySet)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	params := map[string]any{"fork_id": forkID}
	if idempotencyKey != "" {
		params["idempotency_key"] = idempotencyKey
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, forkChatAPIErrorClassifier())
	}
	var result forkChatDeleteResult
	if err := client.call(ctx, forkChatDeleteMethod, params, &result); err != nil {
		return returnCLIAPIError(errOut, err, forkChatAPIErrorClassifier())
	}
	if err := validateForkChatDeleteResult(result); err != nil {
		return returnCLIAPIError(errOut, err, forkChatAPIErrorClassifier())
	}
	return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
		writeForkChatDeleteResult(w, result)
	}, func() ([]string, error) {
		return []string{result.ForkID}, nil
	})
}

func (opts forkChatNewCommandOptions) createParams(sourceSessionID string) (map[string]any, error) {
	forkPoint, err := opts.forkPoint()
	if err != nil {
		return nil, err
	}
	idempotencyKey, err := optionalNonEmptyFlag("--idempotency-key", opts.idempotencyKey, opts.idempotencyKeySet)
	if err != nil {
		return nil, err
	}
	params := map[string]any{
		"source_session_id": sourceSessionID,
		"fork_point":        forkPoint,
	}
	if idempotencyKey != "" {
		params["idempotency_key"] = idempotencyKey
	}
	return params, nil
}

func (opts forkChatNewCommandOptions) forkPoint() (map[string]any, error) {
	count := 0
	for _, set := range []bool{opts.turnIndexSet, opts.eventIDSet, opts.atSet} {
		if set {
			count++
		}
	}
	if count != 1 {
		return nil, fmt.Errorf("exactly one fork point selector is required: --turn-index, --event-id, or --at")
	}
	if opts.turnIndexSet {
		if opts.turnIndex < 1 || opts.turnIndex > 1000000 {
			return nil, fmt.Errorf("--turn-index must be an integer from 1 to 1000000")
		}
		return map[string]any{"kind": "turn", "turn_index": opts.turnIndex}, nil
	}
	if opts.eventIDSet {
		eventID, err := optionalNonEmptyFlag("--event-id", opts.eventID, opts.eventIDSet)
		if err != nil {
			return nil, err
		}
		if err := validateConversationOpaqueIDArg("--event-id", eventID); err != nil {
			return nil, err
		}
		return map[string]any{"kind": "event", "event_id": eventID}, nil
	}
	at, err := optionalNonEmptyFlag("--at", opts.at, opts.atSet)
	if err != nil {
		return nil, err
	}
	parsed, err := parseRFC3339Flag("--at", at)
	if err != nil {
		return nil, err
	}
	return map[string]any{"kind": "time", "at": parsed.UTC().Format(time.RFC3339Nano)}, nil
}

func forkChatChatParams(forkID, message, idempotencyKey string, idempotencyKeySet bool) (map[string]any, error) {
	forkID = strings.TrimSpace(forkID)
	if err := validateConversationOpaqueIDArg("fork id", forkID); err != nil {
		return nil, err
	}
	message = strings.TrimSpace(message)
	if message == "" {
		return nil, fmt.Errorf("--message must be non-empty")
	}
	key, err := optionalNonEmptyFlag("--idempotency-key", idempotencyKey, idempotencyKeySet)
	if err != nil {
		return nil, err
	}
	params := map[string]any{"fork_id": forkID, "message": message}
	if key != "" {
		params["idempotency_key"] = key
	}
	return params, nil
}

func (opts forkChatListCommandOptions) params() (map[string]any, error) {
	params := map[string]any{}
	if opts.sourceSessionIDSet {
		sourceSessionID, err := optionalNonEmptyFlag("--source-session-id", opts.sourceSessionID, opts.sourceSessionIDSet)
		if err != nil {
			return nil, err
		}
		if err := validateConversationOpaqueIDArg("--source-session-id", sourceSessionID); err != nil {
			return nil, err
		}
		params["source_session_id"] = sourceSessionID
	}
	if opts.limitSet {
		if opts.limit < 1 || opts.limit > 500 {
			return nil, fmt.Errorf("--limit must be between 1 and 500")
		}
		params["limit"] = opts.limit
	}
	if opts.cursorSet {
		cursor, err := optionalNonEmptyFlag("--cursor", opts.cursor, opts.cursorSet)
		if err != nil {
			return nil, err
		}
		params["cursor"] = cursor
	}
	return params, nil
}

func validateForkChatCreateResult(result forkChatCreateResult) error {
	if err := validateForkChatSession("conversation.fork result.fork", result.Fork); err != nil {
		return err
	}
	if result.IdempotencyReplayed == nil {
		return fmt.Errorf("malformed conversation.fork result: idempotency_replayed is required")
	}
	return nil
}

func validateForkChatListResult(result forkChatListResult) error {
	if result.Forks == nil {
		return fmt.Errorf("malformed conversation.fork_list result: forks is required")
	}
	for i, fork := range result.Forks {
		if err := validateForkChatSession(fmt.Sprintf("conversation.fork_list result.forks[%d]", i), fork); err != nil {
			return err
		}
	}
	return nil
}

func validateForkChatSession(prefix string, fork forkChatSession) error {
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "fork_id", value: fork.ForkID},
		{name: "source_session_id", value: fork.SourceSessionID},
		{name: "source_agent_id", value: fork.SourceAgentID},
		{name: "created_by", value: fork.CreatedBy},
		{name: "created_at", value: fork.CreatedAt},
		{name: "expires_at", value: fork.ExpiresAt},
		{name: "state", value: fork.State},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("malformed %s: %s is required", prefix, field.name)
		}
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "fork_id", value: fork.ForkID},
		{name: "source_session_id", value: fork.SourceSessionID},
		{name: "source_agent_id", value: fork.SourceAgentID},
	} {
		if err := validateConversationOpaqueIDArg(prefix+"."+field.name, field.value); err != nil {
			return fmt.Errorf("malformed %s: %w", prefix, err)
		}
	}
	if strings.TrimSpace(fork.SourceRunID) != "" {
		if err := validateConversationOpaqueIDArg(prefix+".source_run_id", fork.SourceRunID); err != nil {
			return fmt.Errorf("malformed %s: %w", prefix, err)
		}
	}
	switch strings.TrimSpace(fork.State) {
	case "active", "expired", "deleted":
	default:
		return fmt.Errorf("malformed %s: state=%q is not valid", prefix, fork.State)
	}
	if err := validateRequiredTimestamp(prefix+".created_at", fork.CreatedAt); err != nil {
		return err
	}
	if err := validateRequiredTimestamp(prefix+".expires_at", fork.ExpiresAt); err != nil {
		return err
	}
	if strings.TrimSpace(fork.DeletedAt) != "" {
		if err := validateRequiredTimestamp(prefix+".deleted_at", fork.DeletedAt); err != nil {
			return err
		}
	}
	if err := validateForkChatForkPoint(prefix+".fork_point", fork.ForkPoint); err != nil {
		return err
	}
	if fork.Turns == nil {
		return fmt.Errorf("malformed %s: turns is required", prefix)
	}
	for i, turn := range fork.Turns {
		if err := validateForkChatTurn(fmt.Sprintf("%s.turns[%d]", prefix, i), turn); err != nil {
			return err
		}
	}
	return nil
}

func validateForkChatForkPoint(prefix string, point forkChatForkPoint) error {
	if point.TurnIndex < 1 {
		return fmt.Errorf("malformed %s: turn_index must be at least 1", prefix)
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "kind", value: point.Kind},
		{name: "turn_id", value: point.TurnID},
		{name: "selected_at", value: point.SelectedAt},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("malformed %s: %s is required", prefix, field.name)
		}
	}
	switch strings.TrimSpace(point.Kind) {
	case "turn":
	case "event":
		if strings.TrimSpace(point.EventID) == "" {
			return fmt.Errorf("malformed %s: event_id is required for event fork point", prefix)
		}
	case "time":
		if strings.TrimSpace(point.At) == "" {
			return fmt.Errorf("malformed %s: at is required for time fork point", prefix)
		}
	default:
		return fmt.Errorf("malformed %s: kind=%q is not valid", prefix, point.Kind)
	}
	if err := validateConversationOpaqueIDArg(prefix+".turn_id", point.TurnID); err != nil {
		return fmt.Errorf("malformed %s: %w", prefix, err)
	}
	if strings.TrimSpace(point.EventID) != "" {
		if err := validateConversationOpaqueIDArg(prefix+".event_id", point.EventID); err != nil {
			return fmt.Errorf("malformed %s: %w", prefix, err)
		}
	}
	if strings.TrimSpace(point.At) != "" {
		if err := validateRequiredTimestamp(prefix+".at", point.At); err != nil {
			return err
		}
	}
	return validateRequiredTimestamp(prefix+".selected_at", point.SelectedAt)
}

func validateForkChatTurn(prefix string, turn forkChatTurn) error {
	if turn.TurnIndex < 1 {
		return fmt.Errorf("malformed %s: turn_index must be at least 1", prefix)
	}
	if strings.TrimSpace(turn.TurnID) == "" {
		return fmt.Errorf("malformed %s: turn_id is required", prefix)
	}
	if err := validateConversationOpaqueIDArg(prefix+".turn_id", turn.TurnID); err != nil {
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

func validateForkChatChatResult(result forkChatChatResult) error {
	if strings.TrimSpace(result.ForkID) == "" {
		return fmt.Errorf("malformed conversation.fork_chat result: fork_id is required")
	}
	if err := validateConversationOpaqueIDArg("conversation.fork_chat result.fork_id", result.ForkID); err != nil {
		return fmt.Errorf("malformed conversation.fork_chat result: %w", err)
	}
	if err := validateForkChatTurn("conversation.fork_chat result.turn", result.Turn); err != nil {
		return err
	}
	if err := validateForkChatSnapshot(result.Snapshot); err != nil {
		return err
	}
	if err := validateForkChatSandboxPolicy(result.SandboxPolicy); err != nil {
		return err
	}
	if result.IdempotencyReplayed == nil {
		return fmt.Errorf("malformed conversation.fork_chat result: idempotency_replayed is required")
	}
	return nil
}

func validateForkChatSnapshot(snapshot forkChatSnapshot) error {
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "fork_id", value: snapshot.ForkID},
		{name: "source_session_id", value: snapshot.SourceSessionID},
		{name: "source_agent_id", value: snapshot.SourceAgentID},
		{name: "snapshot_owner", value: snapshot.SnapshotOwner},
		{name: "created_at", value: snapshot.CreatedAt},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("malformed conversation.fork_chat result.snapshot: %s is required", field.name)
		}
	}
	if snapshot.SnapshotOwner != "conversation.fork_chat.snapshot.v1" {
		return fmt.Errorf("malformed conversation.fork_chat result.snapshot: snapshot_owner=%q is not valid", snapshot.SnapshotOwner)
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "fork_id", value: snapshot.ForkID},
		{name: "source_session_id", value: snapshot.SourceSessionID},
		{name: "source_agent_id", value: snapshot.SourceAgentID},
	} {
		if err := validateConversationOpaqueIDArg("conversation.fork_chat result.snapshot."+field.name, field.value); err != nil {
			return fmt.Errorf("malformed conversation.fork_chat result.snapshot: %w", err)
		}
	}
	if strings.TrimSpace(snapshot.SourceRunID) != "" {
		if err := validateConversationOpaqueIDArg("conversation.fork_chat result.snapshot.source_run_id", snapshot.SourceRunID); err != nil {
			return fmt.Errorf("malformed conversation.fork_chat result.snapshot: %w", err)
		}
	}
	if snapshot.SourceTurn == nil {
		return fmt.Errorf("malformed conversation.fork_chat result.snapshot: source_turn is required")
	}
	if snapshot.EntitySnapshot == nil {
		return fmt.Errorf("malformed conversation.fork_chat result.snapshot: entity_snapshot is required")
	}
	for i, entity := range snapshot.EntitySnapshot {
		if strings.TrimSpace(entity.EntityID) == "" {
			return fmt.Errorf("malformed conversation.fork_chat result.snapshot.entity_snapshot[%d]: entity_id is required", i)
		}
		if err := validateConversationOpaqueIDArg(fmt.Sprintf("conversation.fork_chat result.snapshot.entity_snapshot[%d].entity_id", i), entity.EntityID); err != nil {
			return fmt.Errorf("malformed conversation.fork_chat result.snapshot: %w", err)
		}
	}
	return validateRequiredTimestamp("conversation.fork_chat result.snapshot.created_at", snapshot.CreatedAt)
}

func validateForkChatSandboxPolicy(policy forkChatSandboxPolicy) error {
	if policy.Owner != "conversation.fork_chat.sandbox.v1" {
		return fmt.Errorf("malformed conversation.fork_chat result.sandbox_policy: owner=%q is not valid", policy.Owner)
	}
	if policy.ReadPolicy != "fork_snapshot_only" {
		return fmt.Errorf("malformed conversation.fork_chat result.sandbox_policy: read_policy=%q is not valid", policy.ReadPolicy)
	}
	if policy.WritePolicy != "stub_record_only_no_live_mutation" {
		return fmt.Errorf("malformed conversation.fork_chat result.sandbox_policy: write_policy=%q is not valid", policy.WritePolicy)
	}
	if policy.SideEffectingTools == nil {
		return fmt.Errorf("malformed conversation.fork_chat result.sandbox_policy: side_effecting_tools is required")
	}
	if policy.StubbedTools == nil {
		return fmt.Errorf("malformed conversation.fork_chat result.sandbox_policy: stubbed_tools is required")
	}
	return nil
}

func validateForkChatDeleteResult(result forkChatDeleteResult) error {
	if strings.TrimSpace(result.ForkID) == "" {
		return fmt.Errorf("malformed conversation.fork_delete result: fork_id is required")
	}
	if err := validateConversationOpaqueIDArg("conversation.fork_delete result.fork_id", result.ForkID); err != nil {
		return fmt.Errorf("malformed conversation.fork_delete result: %w", err)
	}
	if result.OK == nil {
		return fmt.Errorf("malformed conversation.fork_delete result: ok is required")
	}
	if !*result.OK {
		return fmt.Errorf("malformed conversation.fork_delete result: ok must be true")
	}
	if result.Deleted == nil {
		return fmt.Errorf("malformed conversation.fork_delete result: deleted is required")
	}
	if result.AlreadyDeleted == nil {
		return fmt.Errorf("malformed conversation.fork_delete result: already_deleted is required")
	}
	if result.IdempotencyReplayed == nil {
		return fmt.Errorf("malformed conversation.fork_delete result: idempotency_replayed is required")
	}
	return nil
}

func writeForkChatNewResult(out io.Writer, result forkChatNewResult) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "Fork %s\n", result.Fork.ForkID)
	writeForkChatSessionHeader(out, result.Fork)
	if result.Chat != nil {
		fmt.Fprintln(out, "Initial chat:")
		writeForkChatChatSummary(out, *result.Chat)
	}
}

func writeForkChatListResult(out io.Writer, result forkChatListResult) {
	if out == nil {
		return
	}
	if len(result.Forks) == 0 {
		fmt.Fprintln(out, "No forkchat sessions match the filter.")
		return
	}
	fmt.Fprintln(out, "FORK_ID\tSOURCE_SESSION\tSOURCE_AGENT\tSTATE\tTURNS\tEXPIRES_AT")
	for _, fork := range result.Forks {
		fmt.Fprintf(out, "%s\t%s\t%s\t%s\t%d\t%s\n",
			fork.ForkID,
			fork.SourceSessionID,
			fork.SourceAgentID,
			fork.State,
			len(fork.Turns),
			fork.ExpiresAt,
		)
	}
	if strings.TrimSpace(result.NextCursor) != "" {
		fmt.Fprintf(out, "next_cursor=%s\n", result.NextCursor)
	}
}

func writeForkChatSessionDetail(out io.Writer, fork forkChatSession) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "Fork %s\n", fork.ForkID)
	writeForkChatSessionHeader(out, fork)
	if len(fork.Turns) == 0 {
		fmt.Fprintln(out, "No forkchat turns recorded.")
		return
	}
	fmt.Fprintln(out, "TURN\tTURN_ID\tPARSE_OK\tLATENCY_MS\tTOOL_CALLS\tASSISTANT\tERROR")
	for _, turn := range fork.Turns {
		fmt.Fprintf(out, "%d\t%s\t%t\t%d\t%d\t%s\t%s\n",
			turn.TurnIndex,
			turn.TurnID,
			*turn.ParseOK,
			*turn.LatencyMS,
			len(turn.ToolCalls),
			conversationDash(forkChatAssistantMessage(turn)),
			conversationDash(turn.Error),
		)
	}
}

func writeForkChatChatResult(out io.Writer, result forkChatChatResult) {
	if out == nil {
		return
	}
	writeForkChatChatSummary(out, result)
}

func writeForkChatDeleteResult(out io.Writer, result forkChatDeleteResult) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "Fork %s deleted=%t already_deleted=%t idempotency_replayed=%t\n",
		result.ForkID,
		boolValue(result.Deleted),
		boolValue(result.AlreadyDeleted),
		boolValue(result.IdempotencyReplayed),
	)
}

func writeForkChatSessionHeader(out io.Writer, fork forkChatSession) {
	fmt.Fprintf(out, "source_session_id=%s source_agent_id=%s source_run_id=%s state=%s created_by=%s expires_at=%s\n",
		fork.SourceSessionID,
		fork.SourceAgentID,
		conversationDash(fork.SourceRunID),
		fork.State,
		fork.CreatedBy,
		fork.ExpiresAt,
	)
	point := fork.ForkPoint
	fmt.Fprintf(out, "fork_point kind=%s turn_index=%d turn_id=%s event_id=%s at=%s selected_at=%s\n",
		point.Kind,
		point.TurnIndex,
		point.TurnID,
		conversationDash(point.EventID),
		conversationDash(point.At),
		point.SelectedAt,
	)
}

func writeForkChatChatSummary(out io.Writer, result forkChatChatResult) {
	turn := result.Turn
	fmt.Fprintf(out, "Fork %s turn %d\n", result.ForkID, turn.TurnIndex)
	fmt.Fprintf(out, "turn_id=%s parse_ok=%t latency_ms=%d tool_calls=%d snapshot_owner=%s sandbox_owner=%s idempotency_replayed=%t error=%s\n",
		turn.TurnID,
		*turn.ParseOK,
		*turn.LatencyMS,
		len(turn.ToolCalls),
		result.Snapshot.SnapshotOwner,
		result.SandboxPolicy.Owner,
		boolValue(result.IdempotencyReplayed),
		conversationDash(turn.Error),
	)
	if message := forkChatAssistantMessage(turn); message != "" {
		fmt.Fprintf(out, "assistant=%s\n", message)
	}
}

func quietForkChatList(result forkChatListResult) []string {
	out := make([]string, 0, len(result.Forks))
	for _, fork := range result.Forks {
		out = append(out, fork.ForkID)
	}
	return out
}

func forkChatAssistantMessage(turn forkChatTurn) string {
	if turn.ResponsePayload == nil {
		return ""
	}
	message, _ := turn.ResponsePayload["message"].(string)
	return strings.TrimSpace(message)
}

func boolValue(value *bool) bool {
	return value != nil && *value
}

func forkChatAPIErrorClassifier() cliAPIErrorClassifier {
	return cliAPIErrorClassifier{
		notFoundCodes: []string{"FORK_NOT_FOUND", "SESSION_NOT_FOUND", "TURN_NOT_FOUND", "EVENT_NOT_FOUND"},
		conflictCodes: []string{"IDEMPOTENCY_CONFLICT"},
	}
}
