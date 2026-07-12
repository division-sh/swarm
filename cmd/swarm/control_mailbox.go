package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

type mailboxProjection struct {
	Kind         string                      `json:"kind"`
	Notice       *mailboxItem                `json:"notice,omitempty"`
	DecisionCard *mailboxDecisionCardSummary `json:"decision_card,omitempty"`
}

type mailboxDetailProjection struct {
	Kind         string               `json:"kind"`
	Notice       *mailboxNoticeDetail `json:"notice,omitempty"`
	DecisionCard *mailboxDecisionCard `json:"decision_card,omitempty"`
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
}

type mailboxNoticeDetail struct {
	Item    mailboxItem    `json:"item"`
	Payload map[string]any `json:"payload"`
}

type mailboxDecisionCardSummary struct {
	CardID        string `json:"card_id"`
	RunID         string `json:"run_id"`
	FlowInstance  string `json:"flow_instance"`
	EntityID      string `json:"entity_id"`
	Stage         string `json:"stage"`
	DecisionID    string `json:"decision_id"`
	Title         string `json:"title"`
	Status        string `json:"status"`
	DeferredUntil string `json:"deferred_until,omitempty"`
	CreatedAt     string `json:"created_at"`
	UpdatedAt     string `json:"updated_at"`
}

type mailboxDecisionCard struct {
	mailboxDecisionCardSummary
	CardContentHash string         `json:"card_content_hash"`
	Snapshot        map[string]any `json:"snapshot"`
	Verdict         string         `json:"verdict,omitempty"`
	Fields          map[string]any `json:"fields,omitempty"`
}

type mailboxListResult struct {
	Items      []mailboxProjection `json:"items"`
	NextCursor string              `json:"next_cursor,omitempty"`
}

type mailboxMutationResult struct {
	OK                  bool   `json:"ok"`
	CardID              string `json:"card_id"`
	Status              string `json:"status"`
	ChangeID            int64  `json:"change_id"`
	IdempotencyReplayed *bool  `json:"idempotency_replayed"`
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

type mailboxDeferCommandOptions struct {
	apiOptions     rootCommandOptions
	until          string
	idempotencyKey string
}

func newControlCommand(opts rootCommandOptions) *cobra.Command {
	cmd := &cobra.Command{Use: "control", Short: "Pause, continue, stop, or reset runs (operator actions).", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error { return cmd.Help() }}
	cmd.AddCommand(newRetiredControlMailboxCommand(), newControlPauseCommand(opts), newControlContinueCommand(opts), newControlStopCommand(opts), newControlNukeCommand(opts))
	return cmd
}

func newMailboxCommand(opts rootCommandOptions) *cobra.Command {
	cmd := &cobra.Command{Use: "mailbox", Short: "Inspect notices and typed decision cards.", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error { return cmd.Help() }}
	cmd.AddCommand(newMailboxListCommand(opts), newMailboxViewCommand(opts), newMailboxDeferCommand(opts))
	return cmd
}

func newRetiredControlMailboxCommand() *cobra.Command {
	return &cobra.Command{Use: "mailbox", Hidden: true, DisableFlagParsing: true, RunE: func(cmd *cobra.Command, _ []string) error {
		fmt.Fprintln(cmd.ErrOrStderr(), "ERROR: `swarm control mailbox` was removed; use `swarm mailbox`.")
		return commandExitError{code: 2}
	}}
}

func newMailboxListCommand(opts rootCommandOptions) *cobra.Command {
	listOpts := mailboxListCommandOptions{apiOptions: opts, status: "pending"}
	cmd := &cobra.Command{Use: "list", Short: "List mailbox notices and decision cards.", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		listOpts.limitSet = cmd.Flags().Changed("limit")
		return runMailboxListCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), listOpts)
	}}
	cmd.Flags().StringVar(&listOpts.status, "status", listOpts.status, "Mailbox status")
	cmd.Flags().BoolVar(&listOpts.all, "all", false, "List all mailbox statuses")
	cmd.Flags().StringVar(&listOpts.runID, "run-id", "", "Filter to one run")
	cmd.Flags().StringVar(&listOpts.entityID, "entity-id", "", "Filter to one entity")
	cmd.Flags().StringVar(&listOpts.itemType, "type", "", "Filter notice type")
	cmd.Flags().StringVar(&listOpts.priority, "priority", "", "Filter notice priority")
	cmd.Flags().IntVar(&listOpts.limit, "limit", 0, "Page size from 1 to 200")
	cmd.Flags().StringVar(&listOpts.cursor, "cursor", "", "Continuation cursor")
	bindCLIAPIConnectionFlags(cmd, &listOpts.apiOptions)
	return cmd
}

func newMailboxViewCommand(opts rootCommandOptions) *cobra.Command {
	apiOpts := opts
	cmd := &cobra.Command{Use: "view <mailbox-id>", Short: "View one notice or decision card.", Args: cliExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return runMailboxViewCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), apiOpts, args[0])
	}}
	setCLIArgDiscoveryHint(cmd, "List mailbox ids with `swarm mailbox list`.")
	bindCLIAPIConnectionFlags(cmd, &apiOpts)
	return cmd
}

func newMailboxDeferCommand(opts rootCommandOptions) *cobra.Command {
	actionOpts := mailboxDeferCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{Use: "defer <card-id>", Short: "Defer a pending decision card.", Args: cliExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return runMailboxDeferCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), actionOpts, args[0])
	}}
	setCLIArgDiscoveryHint(cmd, "List decision card ids with `swarm mailbox list`.")
	cmd.Flags().StringVar(&actionOpts.until, "until", "", "Required RFC3339 timestamp")
	cmd.Flags().StringVar(&actionOpts.idempotencyKey, "idempotency-key", "", "Optional idempotency key")
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

func runMailboxViewCommand(ctx context.Context, out, errOut io.Writer, opts rootCommandOptions, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return returnCLIValidationError(errOut, fmt.Errorf("mailbox id is required"))
	}
	client, err := newCLIAPIClient(opts)
	if err != nil {
		return returnCLIAPIError(errOut, err, mailboxReadAPIErrorClassifier())
	}
	var result mailboxDetailProjection
	if err := client.call(ctx, "mailbox.get", map[string]any{"mailbox_id": id}, &result); err != nil {
		return returnCLIAPIError(errOut, err, mailboxReadAPIErrorClassifier())
	}
	if err := validateMailboxDetailResult(result); err != nil {
		return returnCLIAPIError(errOut, err, mailboxReadAPIErrorClassifier())
	}
	writeMailboxDetailResult(out, result)
	return nil
}

func runMailboxDeferCommand(ctx context.Context, out, errOut io.Writer, opts mailboxDeferCommandOptions, cardID string) error {
	cardID = strings.TrimSpace(cardID)
	if cardID == "" {
		return returnCLIValidationError(errOut, fmt.Errorf("decision card id is required"))
	}
	until, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(opts.until))
	if err != nil {
		return returnCLIValidationError(errOut, fmt.Errorf("--until must be an RFC3339 timestamp: %w", err))
	}
	params := map[string]any{"card_id": cardID, "until": until.UTC().Format(time.RFC3339Nano)}
	if key := strings.TrimSpace(opts.idempotencyKey); key != "" {
		params["idempotency_key"] = key
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, mailboxDecisionAPIErrorClassifier())
	}
	var result mailboxMutationResult
	if err := client.call(ctx, "mailbox.defer", params, &result); err != nil {
		return returnCLIAPIError(errOut, err, mailboxDecisionAPIErrorClassifier())
	}
	if !result.OK || result.CardID != cardID || result.Status != "pending" || result.ChangeID <= 0 || result.IdempotencyReplayed == nil {
		return returnCLIAPIError(errOut, fmt.Errorf("malformed mailbox.defer result"), mailboxDecisionAPIErrorClassifier())
	}
	fmt.Fprintf(out, "mailbox defer ok: card_id=%s status=%s change_id=%d idempotency_replayed=%t\n", result.CardID, result.Status, result.ChangeID, *result.IdempotencyReplayed)
	return nil
}

func mailboxListAPIErrorClassifier() cliAPIErrorClassifier { return cliAPIErrorClassifier{} }
func mailboxReadAPIErrorClassifier() cliAPIErrorClassifier {
	return cliAPIErrorClassifier{notFoundCodes: []string{"MAILBOX_NOT_FOUND"}}
}
func mailboxDecisionAPIErrorClassifier() cliAPIErrorClassifier {
	return cliAPIErrorClassifier{notFoundCodes: []string{"MAILBOX_NOT_FOUND"}, conflictCodes: []string{"MAILBOX_ALREADY_DECIDED", "MAILBOX_CARD_SUPERSEDED", "IDEMPOTENCY_CONFLICT"}}
}

func (o mailboxListCommandOptions) params() (map[string]any, error) {
	params := map[string]any{}
	if !o.all {
		status := strings.TrimSpace(strings.ToLower(o.status))
		if status == "" {
			status = "pending"
		}
		switch status {
		case "pending", "decided", "superseded", "expired", "deferred":
		default:
			return nil, fmt.Errorf("--status is invalid")
		}
		params["status"] = status
	}
	if value := strings.TrimSpace(o.runID); value != "" {
		params["run_id"] = value
	}
	if value := strings.TrimSpace(o.entityID); value != "" {
		params["entity_id"] = value
	}
	if value := strings.TrimSpace(o.itemType); value != "" {
		params["type"] = value
	}
	if value := strings.TrimSpace(o.priority); value != "" {
		params["priority"] = value
	}
	if (o.limitSet && o.limit <= 0) || o.limit > 200 {
		return nil, fmt.Errorf("--limit must be an integer from 1 to 200")
	}
	if o.limit > 0 {
		params["limit"] = o.limit
	}
	if value := strings.TrimSpace(o.cursor); value != "" {
		params["cursor"] = value
	}
	return params, nil
}

func validateMailboxListResult(result mailboxListResult) error {
	if result.Items == nil {
		return fmt.Errorf("malformed mailbox.list result: items is required")
	}
	for i, item := range result.Items {
		if err := validateMailboxProjection(item); err != nil {
			return fmt.Errorf("malformed mailbox.list result: items[%d]: %w", i, err)
		}
	}
	return nil
}

func validateMailboxProjection(item mailboxProjection) error {
	switch item.Kind {
	case "notice":
		if item.Notice == nil || strings.TrimSpace(item.Notice.MailboxID) == "" {
			return fmt.Errorf("notice is required")
		}
	case "decision_card":
		if item.DecisionCard == nil || strings.TrimSpace(item.DecisionCard.CardID) == "" || strings.TrimSpace(item.DecisionCard.DecisionID) == "" {
			return fmt.Errorf("decision_card is required")
		}
	default:
		return fmt.Errorf("kind must be notice or decision_card")
	}
	return nil
}

func validateMailboxDetailResult(result mailboxDetailProjection) error {
	switch result.Kind {
	case "notice":
		if result.Notice == nil || strings.TrimSpace(result.Notice.Item.MailboxID) == "" {
			return fmt.Errorf("malformed mailbox.get notice")
		}
	case "decision_card":
		if result.DecisionCard == nil || strings.TrimSpace(result.DecisionCard.CardID) == "" || strings.TrimSpace(result.DecisionCard.CardContentHash) == "" || result.DecisionCard.Snapshot == nil {
			return fmt.Errorf("malformed mailbox.get decision card")
		}
	default:
		return fmt.Errorf("malformed mailbox.get result: kind must be notice or decision_card")
	}
	return nil
}

func writeMailboxListResult(out io.Writer, result mailboxListResult) {
	if out == nil {
		return
	}
	rows := make([][]string, 0, len(result.Items))
	for _, projection := range result.Items {
		if projection.Kind == "notice" {
			item := projection.Notice
			rows = append(rows, []string{item.MailboxID, "notice", item.Status, item.Type, item.SourceFlow, item.CreatedAt})
			continue
		}
		card := projection.DecisionCard
		rows = append(rows, []string{card.CardID, "decision_card", card.Status, card.DecisionID, card.FlowInstance, card.CreatedAt})
	}
	footers := []string{}
	if result.NextCursor != "" {
		footers = append(footers, "next_cursor="+result.NextCursor)
	}
	writeCLITable(out, cliTable{Columns: []cliTableColumn{{Header: "MAILBOX_ID", KeyColumn: true, IdentifierFamily: cliIdentifierFamilyMailbox}, {Header: "KIND"}, {Header: "STATUS"}, {Header: "TYPE/DECISION"}, {Header: "FLOW"}, {Header: "CREATED"}}, Rows: rows, EmptyMessage: "No mailbox items match the current filters.", FooterLines: footers})
}

func writeMailboxDetailResult(out io.Writer, result mailboxDetailProjection) {
	if out == nil {
		return
	}
	if result.Kind == "notice" {
		item := result.Notice.Item
		writeCLITitle(out, "Mailbox notice "+item.MailboxID)
		writeCLIFieldLine(out, cliDetailField{Key: "status", Value: item.Status}, cliDetailField{Key: "type", Value: item.Type}, cliDetailField{Key: "priority", Value: item.Priority})
		writeMailboxObject(out, "payload", result.Notice.Payload)
		return
	}
	card := result.DecisionCard
	writeCLITitle(out, "Decision card "+card.CardID)
	writeCLIFieldLine(out, cliDetailField{Key: "status", Value: card.Status}, cliDetailField{Key: "decision", Value: card.DecisionID}, cliDetailField{Key: "stage", Value: card.Stage})
	fmt.Fprintf(out, "card_content_hash=%s\n", card.CardContentHash)
	writeMailboxObject(out, "snapshot", card.Snapshot)
}

func writeMailboxObject(out io.Writer, label string, value map[string]any) {
	raw, _ := json.Marshal(value)
	if len(raw) == 0 {
		raw = []byte("{}")
	}
	fmt.Fprintf(out, "%s=%s\n", label, raw)
}
