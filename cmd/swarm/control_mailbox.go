package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/spf13/cobra"
)

type mailboxProjection struct {
	Kind         string                      `json:"kind"`
	Notice       *mailboxItem                `json:"notice,omitempty"`
	DecisionCard *mailboxDecisionCardSummary `json:"decision_card,omitempty"`
	Effect       *mailboxEffectState         `json:"effect,omitempty"`
}

type mailboxDetailProjection struct {
	Kind         string               `json:"kind"`
	Notice       *mailboxNoticeDetail `json:"notice,omitempty"`
	DecisionCard *mailboxDecisionCard `json:"decision_card,omitempty"`
	Effect       *mailboxEffectState  `json:"effect,omitempty"`
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
	CardID        string                    `json:"card_id"`
	RunID         string                    `json:"run_id"`
	AnchorKind    string                    `json:"anchor_kind"`
	Anchor        mailboxDecisionCardAnchor `json:"anchor"`
	Scope         mailboxDecisionCardScope  `json:"scope"`
	Decision      string                    `json:"decision,omitempty"`
	Category      string                    `json:"category,omitempty"`
	Title         string                    `json:"title"`
	Status        string                    `json:"status"`
	DeferredUntil string                    `json:"deferred_until,omitempty"`
	CreatedAt     string                    `json:"created_at"`
	UpdatedAt     string                    `json:"updated_at"`
}

type mailboxDecisionCardScope struct {
	Kind         string `json:"kind"`
	FlowInstance string `json:"flow_instance,omitempty"`
	EntityID     string `json:"entity_id,omitempty"`
}

type mailboxDecisionCardAnchor struct {
	FlowInstance      string                   `json:"flow_instance,omitempty"`
	FlowID            string                   `json:"flow_id,omitempty"`
	EntityID          string                   `json:"entity_id,omitempty"`
	Stage             string                   `json:"stage,omitempty"`
	StageActivationID string                   `json:"stage_activation_id,omitempty"`
	RequesterAgentID  string                   `json:"requester_agent_id,omitempty"`
	OperationID       string                   `json:"operation_id,omitempty"`
	Category          string                   `json:"category,omitempty"`
	RequestEventID    string                   `json:"request_event_id,omitempty"`
	ActivityID        string                   `json:"activity_id,omitempty"`
	Decision          string                   `json:"decision,omitempty"`
	Scope             mailboxDecisionCardScope `json:"scope,omitempty"`
}

type mailboxEffectState struct {
	ContinuationState string `json:"continuation_state"`
	DispatchState     string `json:"dispatch_state"`
	RequestEventID    string `json:"request_event_id"`
	ActivityID        string `json:"activity_id"`
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
		if item.DecisionCard == nil || strings.TrimSpace(item.DecisionCard.CardID) == "" {
			return fmt.Errorf("decision_card is required")
		}
		if err := validateMailboxDecisionCardAnchor(*item.DecisionCard); err != nil {
			return err
		}
		if item.DecisionCard.AnchorKind == string(decisioncard.AnchorKindProposedEffect) {
			if item.Effect == nil || item.Effect.RequestEventID == "" || item.Effect.ActivityID == "" || item.Effect.DispatchState == "" {
				return fmt.Errorf("proposed_effect dispatch state is required")
			}
		} else if item.Effect != nil {
			return fmt.Errorf("effect state is valid only for proposed_effect cards")
		}
	default:
		return fmt.Errorf("kind must be notice or decision_card")
	}
	return nil
}

func validateMailboxDecisionCardAnchor(card mailboxDecisionCardSummary) error {
	switch strings.TrimSpace(card.AnchorKind) {
	case string(decisioncard.AnchorKindStageGate):
		if card.Anchor.FlowInstance == "" || card.Anchor.EntityID == "" || card.Anchor.Stage == "" || card.Anchor.StageActivationID == "" || card.Decision == "" {
			return fmt.Errorf("stage_gate anchor detail is incomplete")
		}
		if card.Category != "" || card.Anchor.RequesterAgentID != "" || card.Anchor.OperationID != "" {
			return fmt.Errorf("stage_gate card carries human_task selector fields")
		}
	case string(decisioncard.AnchorKindHumanTask):
		if card.Anchor.RequesterAgentID == "" || card.Anchor.OperationID == "" || card.Anchor.Category == "" || card.Category == "" {
			return fmt.Errorf("human_task anchor detail is incomplete")
		}
		if card.Category != card.Anchor.Category || card.Decision != "" || card.Anchor.Stage != "" || card.Anchor.StageActivationID != "" {
			return fmt.Errorf("human_task card carries conflicting anchor detail")
		}
	case string(decisioncard.AnchorKindProposedEffect):
		if card.Anchor.RequestEventID == "" || card.Anchor.ActivityID == "" || card.Anchor.Decision == "" || card.Decision == "" {
			return fmt.Errorf("proposed_effect anchor detail is incomplete")
		}
		if card.Decision != card.Anchor.Decision || card.Anchor.Stage != "" || card.Anchor.RequesterAgentID != "" {
			return fmt.Errorf("proposed_effect card carries conflicting anchor detail")
		}
	default:
		return fmt.Errorf("anchor_kind must be one of: %s", decisioncard.RegisteredAnchorKindDescription())
	}
	if card.Scope.Kind == "" {
		return fmt.Errorf("decision-card scope is required")
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
		if err := validateMailboxDecisionCardAnchor(result.DecisionCard.mailboxDecisionCardSummary); err != nil {
			return err
		}
		if result.DecisionCard.AnchorKind == string(decisioncard.AnchorKindProposedEffect) {
			if result.Effect == nil || result.Effect.RequestEventID == "" || result.Effect.ActivityID == "" || result.Effect.DispatchState == "" {
				return fmt.Errorf("malformed mailbox.get proposed_effect dispatch state")
			}
		} else if result.Effect != nil {
			return fmt.Errorf("mailbox.get effect state is valid only for proposed_effect cards")
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
		label, scope := mailboxDecisionCardListLabels(*card)
		rows = append(rows, []string{card.CardID, "decision_card", card.Status, label, scope, card.CreatedAt})
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
	writeCLIFieldLine(out, cliDetailField{Key: "status", Value: card.Status}, cliDetailField{Key: "anchor_kind", Value: card.AnchorKind}, cliDetailField{Key: "scope", Value: mailboxDecisionCardScopeLabel(card.Scope)})
	if card.AnchorKind == "stage_gate" {
		writeCLIFieldLine(out, cliDetailField{Key: "decision", Value: card.Decision}, cliDetailField{Key: "stage", Value: card.Anchor.Stage})
	} else if card.AnchorKind == "human_task" {
		writeCLIFieldLine(out, cliDetailField{Key: "category", Value: card.Category}, cliDetailField{Key: "requested_by", Value: card.Anchor.RequesterAgentID})
	} else {
		writeCLIFieldLine(out, cliDetailField{Key: "decision", Value: card.Decision}, cliDetailField{Key: "activity", Value: card.Anchor.ActivityID})
		writeCLIFieldLine(out, cliDetailField{Key: "authorization", Value: card.Status}, cliDetailField{Key: "dispatch", Value: result.Effect.DispatchState})
		fmt.Fprintf(out, "activity_request_id=%s\n", result.Effect.RequestEventID)
	}
	fmt.Fprintf(out, "card_content_hash=%s\n", card.CardContentHash)
	writeMailboxObject(out, "snapshot", card.Snapshot)
}

func mailboxDecisionCardListLabels(card mailboxDecisionCardSummary) (string, string) {
	if card.AnchorKind == "stage_gate" {
		return "stage_gate:" + card.Decision, mailboxDecisionCardScopeLabel(card.Scope)
	}
	if card.AnchorKind == "human_task" {
		return "human_task:" + card.Category, mailboxDecisionCardScopeLabel(card.Scope)
	}
	return "proposed_effect:" + card.Decision, mailboxDecisionCardScopeLabel(card.Scope)
}

func mailboxDecisionCardScopeLabel(scope mailboxDecisionCardScope) string {
	switch scope.Kind {
	case "entity":
		return scope.FlowInstance + "/" + scope.EntityID
	case "flow":
		return scope.FlowInstance
	case "global":
		return "global"
	default:
		return ""
	}
}

func writeMailboxObject(out io.Writer, label string, value map[string]any) {
	raw, _ := json.Marshal(value)
	if len(raw) == 0 {
		raw = []byte("{}")
	}
	fmt.Fprintf(out, "%s=%s\n", label, raw)
}
