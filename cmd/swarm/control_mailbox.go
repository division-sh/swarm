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

type mailboxDecisionResult struct {
	OK                bool   `json:"ok"`
	MailboxDecisionID string `json:"mailbox_decision_id"`
	DownstreamEventID string `json:"downstream_event_id,omitempty"`
	Status            string `json:"status"`
}

type mailboxDecisionCommandOptions struct {
	apiOptions          rootCommandOptions
	action              string
	method              string
	reason              string
	until               string
	idempotencyKey      string
	decisionPayloadJSON string
}

func newControlCommand(opts rootCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "control",
		Short: "Run supported v1 operator control actions.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(
		newControlMailboxCommand(opts),
		newControlNukeCommand(opts),
	)
	return cmd
}

func newControlMailboxCommand(opts rootCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mailbox",
		Short: "Approve, reject, or defer mailbox items through v1 RPC.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(
		newControlMailboxApproveCommand(opts),
		newControlMailboxRejectCommand(opts),
		newControlMailboxDeferCommand(opts),
	)
	return cmd
}

func newControlMailboxApproveCommand(opts rootCommandOptions) *cobra.Command {
	actionOpts := mailboxDecisionCommandOptions{
		apiOptions: opts,
		action:     "approve",
		method:     "mailbox.approve",
	}
	cmd := &cobra.Command{
		Use:   "approve <mailbox-item-id>",
		Short: "Approve a pending mailbox item through v1 RPC.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMailboxDecisionCommand(cmd.Context(), cmd.OutOrStdout(), actionOpts, args[0])
		},
	}
	cmd.Flags().StringVar(&actionOpts.idempotencyKey, "idempotency-key", "", "Optional v1 API idempotency key")
	cmd.Flags().StringVar(&actionOpts.decisionPayloadJSON, "decision-payload-json", "", "Optional JSON object attached to the approval event")
	return cmd
}

func newControlMailboxRejectCommand(opts rootCommandOptions) *cobra.Command {
	actionOpts := mailboxDecisionCommandOptions{
		apiOptions: opts,
		action:     "reject",
		method:     "mailbox.reject",
	}
	cmd := &cobra.Command{
		Use:   "reject <mailbox-item-id>",
		Short: "Reject a pending mailbox item through v1 RPC.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMailboxDecisionCommand(cmd.Context(), cmd.OutOrStdout(), actionOpts, args[0])
		},
	}
	cmd.Flags().StringVar(&actionOpts.reason, "reason", "", "Required rejection reason")
	cmd.Flags().StringVar(&actionOpts.idempotencyKey, "idempotency-key", "", "Optional v1 API idempotency key")
	return cmd
}

func newControlMailboxDeferCommand(opts rootCommandOptions) *cobra.Command {
	actionOpts := mailboxDecisionCommandOptions{
		apiOptions: opts,
		action:     "defer",
		method:     "mailbox.defer",
	}
	cmd := &cobra.Command{
		Use:   "defer <mailbox-item-id>",
		Short: "Defer a pending mailbox item through v1 RPC.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMailboxDecisionCommand(cmd.Context(), cmd.OutOrStdout(), actionOpts, args[0])
		},
	}
	cmd.Flags().StringVar(&actionOpts.until, "until", "", "Required RFC3339 timestamp")
	cmd.Flags().StringVar(&actionOpts.idempotencyKey, "idempotency-key", "", "Optional v1 API idempotency key")
	return cmd
}

func runMailboxDecisionCommand(ctx context.Context, out io.Writer, opts mailboxDecisionCommandOptions, mailboxID string) error {
	mailboxID = strings.TrimSpace(mailboxID)
	if mailboxID == "" {
		return fmt.Errorf("mailbox item id is required")
	}
	params, err := opts.params(mailboxID)
	if err != nil {
		return err
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return err
	}
	var result mailboxDecisionResult
	if err := client.call(ctx, opts.method, params, &result); err != nil {
		return err
	}
	if err := validateMailboxDecisionResult(opts.action, result); err != nil {
		return err
	}
	writeMailboxDecisionResult(out, opts.action, mailboxID, result)
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
	expectedStatus, err := expectedMailboxDecisionStatus(action)
	if err != nil {
		return err
	}
	if result.Status != expectedStatus {
		return fmt.Errorf("malformed mailbox decision result: status=%q, want %q for mailbox %s", result.Status, expectedStatus, action)
	}
	return nil
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

func (o mailboxDecisionCommandOptions) params(mailboxID string) (map[string]any, error) {
	params := map[string]any{"mailbox_id": mailboxID}
	if key := strings.TrimSpace(o.idempotencyKey); key != "" {
		params["idempotency_key"] = key
	}
	switch o.action {
	case "approve":
		payload, ok, err := parseOptionalDecisionPayload(o.decisionPayloadJSON)
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

func parseOptionalDecisionPayload(raw string) (map[string]any, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, false, nil
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil, false, fmt.Errorf("--decision-payload-json must be a JSON object: %w", err)
	}
	if payload == nil {
		return nil, false, fmt.Errorf("--decision-payload-json must be a JSON object")
	}
	return payload, true, nil
}

func writeMailboxDecisionResult(out io.Writer, action, mailboxID string, result mailboxDecisionResult) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "mailbox %s ok: mailbox_id=%s status=%s decision_id=%s", action, mailboxID, result.Status, result.MailboxDecisionID)
	if result.DownstreamEventID != "" {
		fmt.Fprintf(out, " downstream_event_id=%s", result.DownstreamEventID)
	}
	fmt.Fprintln(out)
}
