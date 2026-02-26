package mailbox

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"empireai/internal/runtime"
)

type Status struct {
	Pending  int
	Critical int
}

type ListOptions struct {
	Limit        int
	CriticalOnly bool
	ReviewsOnly  bool
}

type DecisionOutcome struct {
	Status   string
	Decision string
}

func GetStatus(ctx context.Context, store runtime.MailboxPersistence) (Status, error) {
	if store == nil {
		return Status{}, fmt.Errorf("mailbox store is required")
	}
	pending, err := store.CountMailboxItems(ctx, "pending")
	if err != nil {
		return Status{}, err
	}
	criticalItems, err := store.ListMailboxItems(ctx, "pending", 200)
	if err != nil {
		return Status{}, err
	}
	critical := 0
	for _, item := range criticalItems {
		if item.Priority == "critical" {
			critical++
		}
	}
	return Status{
		Pending:  pending,
		Critical: critical,
	}, nil
}

func PrintStatus(ctx context.Context, store runtime.MailboxPersistence, out io.Writer) error {
	if out == nil {
		out = os.Stdout
	}
	st, err := GetStatus(ctx, store)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "mailbox: pending=%d critical=%d\n", st.Pending, st.Critical)
	return err
}

func PrintPending(ctx context.Context, store runtime.MailboxPersistence, out io.Writer, limit int) error {
	return PrintPendingWithOptions(ctx, store, out, ListOptions{Limit: limit})
}

func PrintPendingWithOptions(ctx context.Context, store runtime.MailboxPersistence, out io.Writer, opts ListOptions) error {
	if store == nil {
		return fmt.Errorf("mailbox store is required")
	}
	if out == nil {
		out = os.Stdout
	}
	if opts.Limit <= 0 {
		opts.Limit = 20
	}
	items, err := store.ListMailboxItems(ctx, "pending", opts.Limit*3)
	if err != nil {
		return err
	}
	items = filterPending(items, opts)
	if len(items) == 0 {
		_, err := fmt.Fprintln(out, "mailbox: no pending items")
		return err
	}
	if len(items) > opts.Limit {
		items = items[:opts.Limit]
	}
	for _, it := range items {
		timeout := ""
		if !it.TimeoutAt.IsZero() {
			timeout = it.TimeoutAt.UTC().Format(time.RFC3339)
		}
		summary := strings.TrimSpace(it.Summary)
		if summary == "" {
			summary = "(no summary)"
		}
		if len(summary) > 140 {
			summary = summary[:140] + "..."
		}
		if _, err := fmt.Fprintf(out, "- id=%s type=%s priority=%s from=%s vertical=%s timeout=%s summary=%s\n",
			it.ID, it.Type, it.Priority, it.FromAgent, it.VerticalID, timeout, summary); err != nil {
			return err
		}
	}
	return nil
}

func PrintItem(ctx context.Context, store runtime.MailboxPersistence, out io.Writer, id string) error {
	if store == nil {
		return fmt.Errorf("mailbox store is required")
	}
	if out == nil {
		out = os.Stdout
	}
	item, err := store.GetMailboxItem(ctx, id)
	if err != nil {
		return err
	}
	timeout := ""
	if !item.TimeoutAt.IsZero() {
		timeout = item.TimeoutAt.UTC().Format(time.RFC3339)
	}
	_, err = fmt.Fprintf(out,
		"mailbox item\nid: %s\ntype: %s\npriority: %s\nstatus: %s\nfrom: %s\nvertical: %s\ntimeout_at: %s\ndecision: %s\nnotes: %s\nsummary: %s\n",
		item.ID, item.Type, item.Priority, item.Status, item.FromAgent, item.VerticalID, timeout, item.Decision, item.DecisionNotes, strings.TrimSpace(item.Summary),
	)
	return err
}

func Decide(ctx context.Context, store runtime.MailboxPersistence, id, action, notes string) (DecisionOutcome, error) {
	if store == nil {
		return DecisionOutcome{}, fmt.Errorf("mailbox store is required")
	}
	outcome, err := NormalizeDecisionAction(action)
	if err != nil {
		return DecisionOutcome{}, err
	}
	if err := store.DecideMailboxItem(ctx, id, outcome.Status, outcome.Decision, notes); err != nil {
		return DecisionOutcome{}, err
	}
	return outcome, nil
}

func NormalizeDecisionAction(action string) (DecisionOutcome, error) {
	a := strings.ToLower(strings.TrimSpace(action))
	a = strings.ReplaceAll(a, "_", "-")
	switch a {
	case "approve", "approved", "approve-spend", "respond":
		return DecisionOutcome{Status: "approved", Decision: "approve"}, nil
	case "reject", "rejected", "reject-spend":
		return DecisionOutcome{Status: "rejected", Decision: "reject"}, nil
	case "kill":
		return DecisionOutcome{Status: "rejected", Decision: "kill"}, nil
	case "revise":
		return DecisionOutcome{Status: "rejected", Decision: "revise"}, nil
	case "more-data", "defer":
		return DecisionOutcome{Status: "more_data", Decision: "more_data"}, nil
	case "skip", "timed-out", "timeout":
		return DecisionOutcome{Status: "timed_out", Decision: "timed_out"}, nil
	default:
		return DecisionOutcome{}, fmt.Errorf("invalid mailbox decision action: %s", action)
	}
}

func filterPending(items []runtime.MailboxItem, opts ListOptions) []runtime.MailboxItem {
	out := make([]runtime.MailboxItem, 0, len(items))
	for _, item := range items {
		if opts.CriticalOnly && strings.TrimSpace(item.Priority) != "critical" {
			continue
		}
		if opts.ReviewsOnly && !isReviewType(item.Type) {
			continue
		}
		out = append(out, item)
	}
	return out
}

func isReviewType(t string) bool {
	switch strings.TrimSpace(t) {
	case "product_spec_review", "deploy_review":
		return true
	default:
		return false
	}
}
