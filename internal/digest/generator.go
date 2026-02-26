package digest

import (
	"context"
	"fmt"
	"strings"

	"empireai/internal/runtime"
)

type Snapshot struct {
	ActiveVerticals int
	MailboxPending  int
	MailboxCritical int
	TopVerticals    []runtime.VerticalDigestRow
}

func BuildSnapshot(ctx context.Context, portfolio runtime.DigestPersistence, mailbox runtime.MailboxPersistence, topN int) (Snapshot, error) {
	if portfolio == nil {
		return Snapshot{}, fmt.Errorf("portfolio digest source is required")
	}
	if mailbox == nil {
		return Snapshot{}, fmt.Errorf("mailbox source is required")
	}
	active, err := portfolio.CountActiveVerticals(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	rows, err := portfolio.ListVerticalDigestRows(ctx, topN)
	if err != nil {
		return Snapshot{}, err
	}
	pending, err := mailbox.CountMailboxItems(ctx, "pending")
	if err != nil {
		return Snapshot{}, err
	}
	pendingItems, err := mailbox.ListMailboxItems(ctx, "pending", 300)
	if err != nil {
		return Snapshot{}, err
	}
	critical := 0
	for _, item := range pendingItems {
		if item.Priority == "critical" {
			critical++
		}
	}

	return Snapshot{
		ActiveVerticals: active,
		MailboxPending:  pending,
		MailboxCritical: critical,
		TopVerticals:    rows,
	}, nil
}

func RenderText(s Snapshot) string {
	var b strings.Builder
	b.WriteString("portfolio_digest\n")
	b.WriteString(fmt.Sprintf("active_verticals: %d\n", s.ActiveVerticals))
	b.WriteString(fmt.Sprintf("mailbox_pending: %d\n", s.MailboxPending))
	b.WriteString(fmt.Sprintf("mailbox_critical: %d\n", s.MailboxCritical))
	b.WriteString("top_verticals:\n")
	for _, v := range s.TopVerticals {
		b.WriteString(fmt.Sprintf("- %s (%s): users=%d mrr=$%.2f spend_30d=$%.2f\n",
			v.Name,
			v.Stage,
			v.UsersTotal,
			float64(v.MRRCents)/100.0,
			float64(v.SpendCents30d)/100.0,
		))
	}
	return b.String()
}
