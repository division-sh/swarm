package digest

import (
	"context"
	"fmt"
	"strings"

	"swarm/internal/runtime"
	runtimetools "swarm/internal/runtime/tools"
)

type Snapshot struct {
	ActiveInstances int
	MailboxPending  int
	MailboxCritical int
	TopInstances    []runtime.InstanceDigestRow
}

func BuildSnapshot(ctx context.Context, source runtime.DigestPersistence, mailbox runtimetools.MailboxPersistence, topN int) (Snapshot, error) {
	if source == nil {
		return Snapshot{}, fmt.Errorf("digest source is required")
	}
	if mailbox == nil {
		return Snapshot{}, fmt.Errorf("mailbox source is required")
	}
	active, err := source.CountActiveInstances(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	rows, err := source.ListInstanceDigestRows(ctx, topN)
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
		ActiveInstances: active,
		MailboxPending:  pending,
		MailboxCritical: critical,
		TopInstances:    rows,
	}, nil
}

func RenderText(s Snapshot) string {
	var b strings.Builder
	b.WriteString("instance_digest\n")
	b.WriteString(fmt.Sprintf("active_instances: %d\n", s.ActiveInstances))
	b.WriteString(fmt.Sprintf("mailbox_pending: %d\n", s.MailboxPending))
	b.WriteString(fmt.Sprintf("mailbox_critical: %d\n", s.MailboxCritical))
	b.WriteString("top_instances:\n")
	for _, v := range s.TopInstances {
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
