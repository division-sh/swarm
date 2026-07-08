package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

type mailboxDecisionResult struct {
	OK                         bool      `json:"ok"`
	MailboxDecisionID          string    `json:"mailbox_decision_id"`
	DownstreamEventID          string    `json:"downstream_event_id,omitempty"`
	DownstreamEventName        string    `json:"downstream_event_name,omitempty"`
	DownstreamSubscribers      *[]string `json:"downstream_subscribers,omitempty"`
	DownstreamSubscriberSource string    `json:"downstream_subscriber_source,omitempty"`
	Status                     string    `json:"status"`
	IdempotencyReplayed        *bool     `json:"idempotency_replayed"`
}

type mailboxItem struct {
	MailboxID      string         `json:"mailbox_id"`
	Type           string         `json:"type"`
	Status         string         `json:"status"`
	Priority       string         `json:"priority"`
	SourceEventID  string         `json:"source_event_id"`
	SourceFlow     string         `json:"source_flow"`
	SourceEntityID string         `json:"source_entity_id,omitempty"`
	Payload        map[string]any `json:"payload"`
	CreatedAt      string         `json:"created_at"`
	DecidedAt      string         `json:"decided_at,omitempty"`
	Decision       string         `json:"decision,omitempty"`
}

type mailboxHistoryEntry struct {
	Action          string         `json:"action"`
	ActorTokenID    string         `json:"actor_token_id"`
	TS              string         `json:"ts"`
	DecisionPayload map[string]any `json:"decision_payload,omitempty"`
	Reason          string         `json:"reason,omitempty"`
}

type mailboxListResult struct {
	Items      []mailboxItem `json:"items"`
	NextCursor string        `json:"next_cursor,omitempty"`
}

type mailboxDetailResult struct {
	Item          mailboxItem           `json:"item"`
	Payload       map[string]any        `json:"payload"`
	History       []mailboxHistoryEntry `json:"history"`
	DecisionSheet *mailboxDecisionSheet `json:"decision_sheet"`
}

type mailboxDecisionSheet struct {
	EntityContext     mailboxEntityContext     `json:"entity_context"`
	DownstreamPreview mailboxDownstreamPreview `json:"downstream_preview"`
}

type mailboxEntityContext struct {
	Available bool        `json:"available"`
	Reason    string      `json:"reason,omitempty"`
	Entity    *entityFull `json:"entity,omitempty"`
}

type mailboxDownstreamPreview struct {
	Available        bool     `json:"available"`
	Reason           string   `json:"reason,omitempty"`
	EventName        string   `json:"event_name,omitempty"`
	Subscribers      []string `json:"subscribers"`
	SubscriberSource string   `json:"subscriber_source"`
}

type mailboxListCommandOptions struct {
	apiOptions rootCommandOptions
	status     string
	all        bool
	runID      string
	entityID   string
	itemType   string
	priority   string
	limit      int
	limitSet   bool
	cursor     string
}

type mailboxDecisionCommandOptions struct {
	apiOptions             rootCommandOptions
	action                 string
	method                 string
	reason                 string
	until                  string
	idempotencyKey         string
	decisionPayloadJSON    string
	decisionPayloadJSONSet bool
	decisionPayloadFile    string
	decisionPayloadFileSet bool
}

func newControlCommand(opts rootCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "control",
		Short: "Pause, continue, stop, or reset runs (operator actions).",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(
		newRetiredControlMailboxCommand(),
		newControlPauseCommand(opts),
		newControlContinueCommand(opts),
		newControlStopCommand(opts),
		newControlNukeCommand(opts),
	)
	return cmd
}

func newMailboxCommand(opts rootCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mailbox",
		Short: "List, view, and decide pending human decisions.",
		Example: `  swarm mailbox list
  swarm mailbox approve <mailbox-item-id>`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(
		newMailboxListCommand(opts),
		newMailboxViewCommand(opts),
		newMailboxApproveCommand(opts),
		newMailboxRejectCommand(opts),
		newMailboxDeferCommand(opts),
	)
	return cmd
}

func newRetiredControlMailboxCommand() *cobra.Command {
	return &cobra.Command{
		Use:                "mailbox",
		Short:              "Removed v2 command; use swarm mailbox.",
		Hidden:             true,
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			writeControlMailboxRetiredMessage(cmd.ErrOrStderr())
			return commandExitError{code: 2}
		},
	}
}

func writeControlMailboxRetiredMessage(w io.Writer) {
	if w == nil {
		return
	}
	fmt.Fprintln(w, "ERROR: `swarm control mailbox` was removed in CLI v2.")
	fmt.Fprintln(w, "  Mailbox decisions are resource-noun-first commands now:")
	fmt.Fprintln(w, "  `swarm mailbox approve <mailbox-item-id>`")
	fmt.Fprintln(w, "  `swarm mailbox reject <mailbox-item-id> --reason <text>`")
	fmt.Fprintln(w, "  `swarm mailbox defer <mailbox-item-id> --until <RFC3339>`")
}

func newMailboxListCommand(opts rootCommandOptions) *cobra.Command {
	listOpts := mailboxListCommandOptions{
		apiOptions: opts,
		status:     "pending",
	}
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List mailbox items.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			listOpts.limitSet = cmd.Flags().Changed("limit")
			return runMailboxListCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), listOpts)
		},
	}
	cmd.Flags().StringVar(&listOpts.status, "status", listOpts.status, "Mailbox status: pending, decided, expired, or deferred")
	cmd.Flags().BoolVar(&listOpts.all, "all", false, "List all mailbox statuses instead of only pending")
	cmd.Flags().StringVar(&listOpts.runID, "run-id", "", "Filter to one run")
	cmd.Flags().StringVar(&listOpts.entityID, "entity-id", "", "Filter to one entity")
	cmd.Flags().StringVar(&listOpts.itemType, "type", "", "Filter by mailbox item type")
	cmd.Flags().StringVar(&listOpts.priority, "priority", "", "Mailbox priority: normal, high, or critical")
	cmd.Flags().IntVar(&listOpts.limit, "limit", 0, "Page size from 1 to 200; omitted uses the API default")
	cmd.Flags().StringVar(&listOpts.cursor, "cursor", "", "Continuation cursor")
	bindCLIAPIConnectionFlags(cmd, &listOpts.apiOptions)
	return cmd
}

func newMailboxViewCommand(opts rootCommandOptions) *cobra.Command {
	apiOpts := opts
	cmd := &cobra.Command{
		Use:   "view <mailbox-item-id>",
		Short: "View one mailbox item.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMailboxViewCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), apiOpts, args[0])
		},
	}
	bindCLIAPIConnectionFlags(cmd, &apiOpts)
	return cmd
}

func newMailboxApproveCommand(opts rootCommandOptions) *cobra.Command {
	actionOpts := mailboxDecisionCommandOptions{
		apiOptions: opts,
		action:     "approve",
		method:     "mailbox.approve",
	}
	cmd := &cobra.Command{
		Use:   "approve <mailbox-item-id>",
		Short: "Approve a pending mailbox item.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runOpts := actionOpts
			runOpts.decisionPayloadFileSet = cmd.Flags().Changed("decision-payload")
			runOpts.decisionPayloadJSONSet = cmd.Flags().Changed("decision-payload-json")
			return runMailboxDecisionCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), runOpts, args[0])
		},
	}
	cmd.Flags().StringVar(&actionOpts.idempotencyKey, "idempotency-key", "", "Optional idempotency key for safe retries (advanced)")
	_ = cmd.Flags().MarkHidden("idempotency-key")
	cmd.Flags().StringVar(&actionOpts.decisionPayloadFile, "decision-payload", "", "Read optional terminal decision event JSON object from file")
	cmd.Flags().StringVar(&actionOpts.decisionPayloadJSON, "decision-payload-json", "", "Optional JSON object attached to the terminal decision event")
	bindCLIAPIConnectionFlagsWithClass(cmd, &actionOpts.apiOptions, cliAPICommandClassMutating, "swarm mailbox approve")
	return cmd
}

func newMailboxRejectCommand(opts rootCommandOptions) *cobra.Command {
	actionOpts := mailboxDecisionCommandOptions{
		apiOptions: opts,
		action:     "reject",
		method:     "mailbox.reject",
	}
	cmd := &cobra.Command{
		Use:   "reject <mailbox-item-id>",
		Short: "Reject a pending mailbox item.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMailboxDecisionCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), actionOpts, args[0])
		},
	}
	cmd.Flags().StringVar(&actionOpts.reason, "reason", "", "Required rejection reason")
	cmd.Flags().StringVar(&actionOpts.idempotencyKey, "idempotency-key", "", "Optional idempotency key for safe retries (advanced)")
	_ = cmd.Flags().MarkHidden("idempotency-key")
	bindCLIAPIConnectionFlagsWithClass(cmd, &actionOpts.apiOptions, cliAPICommandClassMutating, "swarm mailbox reject")
	return cmd
}

func newMailboxDeferCommand(opts rootCommandOptions) *cobra.Command {
	actionOpts := mailboxDecisionCommandOptions{
		apiOptions: opts,
		action:     "defer",
		method:     "mailbox.defer",
	}
	cmd := &cobra.Command{
		Use:   "defer <mailbox-item-id>",
		Short: "Defer a pending mailbox item.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMailboxDecisionCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), actionOpts, args[0])
		},
	}
	cmd.Flags().StringVar(&actionOpts.until, "until", "", "Required RFC3339 timestamp")
	cmd.Flags().StringVar(&actionOpts.idempotencyKey, "idempotency-key", "", "Optional idempotency key for safe retries (advanced)")
	_ = cmd.Flags().MarkHidden("idempotency-key")
	bindCLIAPIConnectionFlagsWithClass(cmd, &actionOpts.apiOptions, cliAPICommandClassMutating, "swarm mailbox defer")
	return cmd
}

func runMailboxListCommand(ctx context.Context, out, errOut io.Writer, opts mailboxListCommandOptions) error {
	params, err := opts.params()
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, mailboxListAPIErrorClassifier())
	}
	var result mailboxListResult
	if err := client.call(ctx, "mailbox.list", params, &result); err != nil {
		return returnCLIAPIError(errOut, err, mailboxListAPIErrorClassifier())
	}
	if err := validateMailboxListResult(result); err != nil {
		return returnCLIAPIError(errOut, err, mailboxListAPIErrorClassifier())
	}
	writeMailboxListResult(out, result)
	return nil
}

func runMailboxViewCommand(ctx context.Context, out, errOut io.Writer, opts rootCommandOptions, mailboxID string) error {
	mailboxID = strings.TrimSpace(mailboxID)
	if mailboxID == "" {
		return returnCLIValidationError(errOut, fmt.Errorf("mailbox item id is required"))
	}
	client, err := newCLIAPIClient(opts)
	if err != nil {
		return returnCLIAPIError(errOut, err, mailboxReadAPIErrorClassifier())
	}
	var result mailboxDetailResult
	if err := client.call(ctx, "mailbox.get", map[string]any{"mailbox_id": mailboxID}, &result); err != nil {
		return returnCLIAPIError(errOut, err, mailboxReadAPIErrorClassifier())
	}
	if err := validateMailboxDetailResult(result); err != nil {
		return returnCLIAPIError(errOut, err, mailboxReadAPIErrorClassifier())
	}
	writeMailboxDetailResult(out, result)
	return nil
}

func runMailboxDecisionCommand(ctx context.Context, out, errOut io.Writer, opts mailboxDecisionCommandOptions, mailboxID string) error {
	mailboxID = strings.TrimSpace(mailboxID)
	if mailboxID == "" {
		return returnCLIValidationError(errOut, fmt.Errorf("mailbox item id is required"))
	}
	params, err := opts.params(mailboxID)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, mailboxDecisionAPIErrorClassifier())
	}
	var result mailboxDecisionResult
	if err := client.call(ctx, opts.method, params, &result); err != nil {
		return returnCLIAPIError(errOut, err, mailboxDecisionAPIErrorClassifier())
	}
	if err := validateMailboxDecisionResult(opts.action, result); err != nil {
		return returnCLIAPIError(errOut, err, mailboxDecisionAPIErrorClassifier())
	}
	writeMailboxDecisionResult(out, opts.action, mailboxID, result)
	return nil
}

func mailboxListAPIErrorClassifier() cliAPIErrorClassifier {
	return cliAPIErrorClassifier{}
}

func mailboxReadAPIErrorClassifier() cliAPIErrorClassifier {
	return cliAPIErrorClassifier{notFoundCodes: []string{"MAILBOX_NOT_FOUND"}}
}

func mailboxDecisionAPIErrorClassifier() cliAPIErrorClassifier {
	return cliAPIErrorClassifier{
		notFoundCodes: []string{"MAILBOX_NOT_FOUND"},
		conflictCodes: []string{"MAILBOX_ALREADY_DECIDED", "MAILBOX_DECISION_EVENT_UNCONFIGURED", "IDEMPOTENCY_CONFLICT"},
	}
}

func validateMailboxListResult(result mailboxListResult) error {
	if result.Items == nil {
		return fmt.Errorf("malformed mailbox.list result: items is required")
	}
	for i, item := range result.Items {
		if err := validateMailboxItem(item); err != nil {
			return fmt.Errorf("malformed mailbox.list result: items[%d]: %w", i, err)
		}
	}
	return nil
}

func validateMailboxDetailResult(result mailboxDetailResult) error {
	if err := validateMailboxItem(result.Item); err != nil {
		return fmt.Errorf("malformed mailbox.get result: item: %w", err)
	}
	if result.Payload == nil {
		return fmt.Errorf("malformed mailbox.get result: payload is required")
	}
	if result.History == nil {
		return fmt.Errorf("malformed mailbox.get result: history is required")
	}
	if result.DecisionSheet == nil {
		return fmt.Errorf("malformed mailbox.get result: decision_sheet is required")
	}
	if err := validateMailboxDecisionSheet(*result.DecisionSheet); err != nil {
		return err
	}
	for i, entry := range result.History {
		if strings.TrimSpace(entry.Action) == "" {
			return fmt.Errorf("malformed mailbox.get result: history[%d].action is required", i)
		}
		if strings.TrimSpace(entry.ActorTokenID) == "" {
			return fmt.Errorf("malformed mailbox.get result: history[%d].actor_token_id is required", i)
		}
		if strings.TrimSpace(entry.TS) == "" {
			return fmt.Errorf("malformed mailbox.get result: history[%d].ts is required", i)
		}
	}
	return nil
}

func validateMailboxDecisionSheet(sheet mailboxDecisionSheet) error {
	entityCtx := sheet.EntityContext
	if entityCtx.Available {
		if entityCtx.Entity == nil {
			return fmt.Errorf("malformed mailbox.get result: decision_sheet.entity_context.entity is required when available")
		}
		if err := validateEntityFullResult("mailbox.get result.decision_sheet.entity_context", *entityCtx.Entity); err != nil {
			return err
		}
	} else if strings.TrimSpace(entityCtx.Reason) == "" {
		return fmt.Errorf("malformed mailbox.get result: decision_sheet.entity_context.reason is required when unavailable")
	}
	preview := sheet.DownstreamPreview
	if preview.Subscribers == nil {
		return fmt.Errorf("malformed mailbox.get result: decision_sheet.downstream_preview.subscribers is required")
	}
	if strings.TrimSpace(preview.SubscriberSource) == "" {
		return fmt.Errorf("malformed mailbox.get result: decision_sheet.downstream_preview.subscriber_source is required")
	}
	if preview.Available {
		if strings.TrimSpace(preview.EventName) == "" {
			return fmt.Errorf("malformed mailbox.get result: decision_sheet.downstream_preview.event_name is required when available")
		}
	} else if strings.TrimSpace(preview.Reason) == "" {
		return fmt.Errorf("malformed mailbox.get result: decision_sheet.downstream_preview.reason is required when unavailable")
	}
	return nil
}

func validateMailboxItem(item mailboxItem) error {
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "mailbox_id", value: item.MailboxID},
		{name: "type", value: item.Type},
		{name: "status", value: item.Status},
		{name: "priority", value: item.Priority},
		{name: "source_flow", value: item.SourceFlow},
		{name: "created_at", value: item.CreatedAt},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("%s is required", field.name)
		}
	}
	if item.Payload == nil {
		return fmt.Errorf("payload is required")
	}
	return nil
}

func validateMailboxDecisionResult(action string, result mailboxDecisionResult) error {
	if !result.OK {
		return fmt.Errorf("malformed mailbox decision result: ok must be true")
	}
	if strings.TrimSpace(result.MailboxDecisionID) == "" {
		return fmt.Errorf("malformed mailbox decision result: mailbox_decision_id is required")
	}
	if strings.TrimSpace(result.Status) == "" {
		return fmt.Errorf("malformed mailbox decision result: status is required")
	}
	if result.IdempotencyReplayed == nil {
		return fmt.Errorf("malformed mailbox decision result: idempotency_replayed is required")
	}
	expectedStatus, err := expectedMailboxDecisionStatus(action)
	if err != nil {
		return err
	}
	if result.Status != expectedStatus {
		return fmt.Errorf("malformed mailbox decision result: status=%q, want %q for mailbox %s", result.Status, expectedStatus, action)
	}
	if result.DownstreamEventID != "" {
		if strings.TrimSpace(result.DownstreamEventName) == "" {
			return fmt.Errorf("malformed mailbox decision result: downstream_event_name is required when downstream_event_id is present")
		}
		if result.DownstreamSubscribers == nil {
			return fmt.Errorf("malformed mailbox decision result: downstream_subscribers is required when downstream_event_id is present")
		}
		if !validMailboxSubscriberSource(result.DownstreamSubscriberSource) {
			return fmt.Errorf("malformed mailbox decision result: downstream_subscriber_source must be event_catalog, unavailable, or none")
		}
	} else if strings.TrimSpace(result.DownstreamEventName) != "" || result.DownstreamSubscribers != nil || strings.TrimSpace(result.DownstreamSubscriberSource) != "" {
		return fmt.Errorf("malformed mailbox decision result: downstream_event_id is required when downstream detail is present")
	}
	return nil
}

func validMailboxSubscriberSource(value string) bool {
	switch strings.TrimSpace(value) {
	case "event_catalog", "unavailable", "none":
		return true
	default:
		return false
	}
}

func expectedMailboxDecisionStatus(action string) (string, error) {
	switch action {
	case "approve", "reject":
		return "decided", nil
	case "defer":
		return "deferred", nil
	default:
		return "", fmt.Errorf("unsupported mailbox action %q", action)
	}
}

func (o mailboxListCommandOptions) params() (map[string]any, error) {
	params := map[string]any{}
	if !o.all {
		status := strings.TrimSpace(strings.ToLower(o.status))
		if status == "" {
			status = "pending"
		}
		if !validMailboxStatus(status) {
			return nil, fmt.Errorf("--status must be one of pending, decided, expired, deferred")
		}
		params["status"] = status
	}
	if runID := strings.TrimSpace(o.runID); runID != "" {
		params["run_id"] = runID
	}
	if entityID := strings.TrimSpace(o.entityID); entityID != "" {
		params["entity_id"] = entityID
	}
	if itemType := strings.TrimSpace(o.itemType); itemType != "" {
		params["type"] = itemType
	}
	if priority := strings.TrimSpace(strings.ToLower(o.priority)); priority != "" {
		if !validMailboxPriority(priority) {
			return nil, fmt.Errorf("--priority must be one of normal, high, critical")
		}
		params["priority"] = priority
	}
	if (o.limitSet && o.limit <= 0) || o.limit > 200 {
		return nil, fmt.Errorf("--limit must be an integer from 1 to 200")
	}
	if o.limit > 0 {
		params["limit"] = o.limit
	}
	if cursor := strings.TrimSpace(o.cursor); cursor != "" {
		params["cursor"] = cursor
	}
	return params, nil
}

func validMailboxStatus(status string) bool {
	switch status {
	case "pending", "decided", "expired", "deferred":
		return true
	default:
		return false
	}
}

func validMailboxPriority(priority string) bool {
	switch priority {
	case "normal", "high", "critical":
		return true
	default:
		return false
	}
}

func (o mailboxDecisionCommandOptions) params(mailboxID string) (map[string]any, error) {
	params := map[string]any{"mailbox_id": mailboxID}
	if key := strings.TrimSpace(o.idempotencyKey); key != "" {
		params["idempotency_key"] = key
	}
	switch o.action {
	case "approve":
		payload, ok, err := parseOptionalDecisionPayload(o.decisionPayloadJSON, o.decisionPayloadJSONSet, o.decisionPayloadFile, o.decisionPayloadFileSet)
		if err != nil {
			return nil, err
		}
		if ok {
			params["decision_payload"] = payload
		}
	case "reject":
		reason := strings.TrimSpace(o.reason)
		if reason == "" {
			return nil, fmt.Errorf("--reason is required for mailbox reject")
		}
		params["reason"] = reason
	case "defer":
		until := strings.TrimSpace(o.until)
		if until == "" {
			return nil, fmt.Errorf("--until is required for mailbox defer")
		}
		parsed, err := time.Parse(time.RFC3339Nano, until)
		if err != nil {
			return nil, fmt.Errorf("--until must be an RFC3339 timestamp: %w", err)
		}
		params["until"] = parsed.UTC().Format(time.RFC3339Nano)
	default:
		return nil, fmt.Errorf("unsupported mailbox action %q", o.action)
	}
	return params, nil
}

func parseOptionalDecisionPayload(inlineJSON string, inlineSet bool, filePath string, fileSet bool) (map[string]any, bool, error) {
	inlineJSON = strings.TrimSpace(inlineJSON)
	filePath = strings.TrimSpace(filePath)
	if inlineSet && fileSet {
		return nil, false, fmt.Errorf("--decision-payload and --decision-payload-json are mutually exclusive")
	}
	if fileSet {
		if filePath == "" {
			return nil, false, fmt.Errorf("--decision-payload requires a file path")
		}
		content, err := os.ReadFile(filePath)
		if err != nil {
			return nil, false, fmt.Errorf("--decision-payload could not be read: %w", err)
		}
		payload, err := parseDecisionPayloadObject("--decision-payload", string(content))
		if err != nil {
			return nil, false, err
		}
		return payload, true, nil
	}
	if !inlineSet {
		return nil, false, nil
	}
	payload, err := parseDecisionPayloadObject("--decision-payload-json", inlineJSON)
	if err != nil {
		return nil, false, err
	}
	return payload, true, nil
}

func parseDecisionPayloadObject(flagName, raw string) (map[string]any, error) {
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil, fmt.Errorf("%s must be a JSON object: %w", flagName, err)
	}
	if payload == nil {
		return nil, fmt.Errorf("%s must be a JSON object", flagName)
	}
	return payload, nil
}

func writeMailboxListResult(out io.Writer, result mailboxListResult) {
	if out == nil {
		return
	}
	if len(result.Items) == 0 {
		fmt.Fprintln(out, "No mailbox items match the filter.")
		return
	}
	fmt.Fprintln(out, "MAILBOX_ID\tSTATUS\tPRIORITY\tTYPE\tSOURCE_EVENT\tENTITY\tCREATED\tDECISION")
	for _, item := range result.Items {
		fmt.Fprintf(out, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			item.MailboxID,
			item.Status,
			item.Priority,
			item.Type,
			mailboxDash(item.SourceEventID),
			mailboxDash(item.SourceEntityID),
			item.CreatedAt,
			mailboxDash(item.Decision),
		)
	}
	if result.NextCursor != "" {
		fmt.Fprintf(out, "next_cursor=%s\n", result.NextCursor)
	}
}

func writeMailboxDetailResult(out io.Writer, result mailboxDetailResult) {
	if out == nil {
		return
	}
	item := result.Item
	fmt.Fprintf(out, "Mailbox %s\n", item.MailboxID)
	fmt.Fprintf(out, "status=%s priority=%s type=%s\n", item.Status, item.Priority, item.Type)
	fmt.Fprintf(out, "source_event_id=%s source_flow=%s source_entity_id=%s created_at=%s\n",
		mailboxDash(item.SourceEventID),
		item.SourceFlow,
		mailboxDash(item.SourceEntityID),
		item.CreatedAt,
	)
	if item.Decision != "" || item.DecidedAt != "" {
		fmt.Fprintf(out, "decision=%s decided_at=%s\n", mailboxDash(item.Decision), mailboxDash(item.DecidedAt))
	}
	writeMailboxDecisionSheet(out, *result.DecisionSheet)
	writeMailboxObject(out, "payload", result.Payload)
	if len(result.History) > 0 {
		fmt.Fprintln(out, "history:")
		for _, entry := range result.History {
			fmt.Fprintf(out, "- action=%s actor=%s ts=%s", entry.Action, entry.ActorTokenID, entry.TS)
			if entry.Reason != "" {
				fmt.Fprintf(out, " reason=%q", entry.Reason)
			}
			if len(entry.DecisionPayload) > 0 {
				fmt.Fprint(out, " decision_payload=")
				writeCompactJSON(out, entry.DecisionPayload)
			}
			fmt.Fprintln(out)
		}
	}
}

func writeMailboxDecisionSheet(out io.Writer, sheet mailboxDecisionSheet) {
	fmt.Fprintln(out, "decision_sheet:")
	if sheet.EntityContext.Available && sheet.EntityContext.Entity != nil {
		entity := sheet.EntityContext.Entity.Entity
		fmt.Fprintf(out, "  entity_context: entity_id=%s run_id=%s flow=%s type=%s state=%s revision=%d\n",
			entity.EntityID,
			entity.RunID,
			entity.FlowInstance,
			entity.EntityType,
			entity.CurrentState,
			entity.Revision,
		)
		fmt.Fprintf(out, "  entity_fields=%s\n", entityCompactJSON(sheet.EntityContext.Entity.Fields))
		fmt.Fprintf(out, "  entity_gates=%s\n", entityCompactJSON(sheet.EntityContext.Entity.Gates))
		fmt.Fprintf(out, "  entity_accumulated=%s\n", entityCompactJSON(sheet.EntityContext.Entity.Accumulated))
	} else {
		fmt.Fprintf(out, "  entity_context: unavailable reason=%s\n", mailboxDash(sheet.EntityContext.Reason))
	}
	if sheet.DownstreamPreview.Available {
		fmt.Fprintf(out, "  downstream_preview: event_name=%s subscribers=%s subscriber_source=%s\n",
			sheet.DownstreamPreview.EventName,
			mailboxStringList(sheet.DownstreamPreview.Subscribers),
			mailboxDash(sheet.DownstreamPreview.SubscriberSource),
		)
		return
	}
	fmt.Fprintf(out, "  downstream_preview: unavailable reason=%s subscriber_source=%s\n",
		mailboxDash(sheet.DownstreamPreview.Reason),
		mailboxDash(sheet.DownstreamPreview.SubscriberSource),
	)
}

func writeMailboxDecisionResult(out io.Writer, action, mailboxID string, result mailboxDecisionResult) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "mailbox %s ok: mailbox_id=%s status=%s decision_id=%s", action, mailboxID, result.Status, result.MailboxDecisionID)
	if result.IdempotencyReplayed != nil {
		fmt.Fprintf(out, " idempotency_replayed=%t", *result.IdempotencyReplayed)
	}
	if result.DownstreamEventID != "" {
		fmt.Fprintf(out, " downstream_event_id=%s", result.DownstreamEventID)
		fmt.Fprintf(out, " downstream_event_name=%s", result.DownstreamEventName)
		fmt.Fprintf(out, " downstream_subscribers=%s", mailboxStringList(*result.DownstreamSubscribers))
		fmt.Fprintf(out, " downstream_subscriber_source=%s", result.DownstreamSubscriberSource)
	}
	fmt.Fprintln(out)
}

func writeMailboxObject(out io.Writer, label string, value map[string]any) {
	if len(value) == 0 {
		fmt.Fprintf(out, "%s={}\n", label)
		return
	}
	fmt.Fprintf(out, "%s=", label)
	writeCompactJSON(out, value)
	fmt.Fprintln(out)
}

func writeCompactJSON(out io.Writer, value map[string]any) {
	raw, err := json.Marshal(value)
	if err != nil {
		fmt.Fprint(out, "{}")
		return
	}
	fmt.Fprint(out, string(raw))
}

func mailboxDash(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}
	return value
}

func mailboxStringList(values []string) string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	if len(out) == 0 {
		return "-"
	}
	return strings.Join(out, ",")
}
