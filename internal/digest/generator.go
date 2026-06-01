package digest

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
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
		line := fmt.Sprintf("- %s (%s)", v.Name, v.Stage)
		if !v.UpdatedAt.IsZero() {
			line += fmt.Sprintf(": updated=%s", v.UpdatedAt.UTC().Format("2006-01-02T15:04:05Z"))
		}
		b.WriteString(line + "\n")
	}
	return b.String()
}
