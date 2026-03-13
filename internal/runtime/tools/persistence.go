package tools

import (
	"context"

	corestate "empireai/internal/runtime/core/state"
)

type MailboxItem = corestate.MailboxItem

type MailboxPersistence interface {
	InsertMailboxItem(ctx context.Context, item MailboxItem) (string, error)
	ListMailboxItems(ctx context.Context, status string, limit int) ([]MailboxItem, error)
	CountMailboxItems(ctx context.Context, status string) (int, error)
	GetMailboxItem(ctx context.Context, id string) (MailboxItem, error)
	DecideMailboxItem(ctx context.Context, id, status, decision, notes string) error
	ExpireMailboxItems(ctx context.Context, limit int) ([]MailboxItem, error)
	ListUnnotifiedCriticalMailboxItems(ctx context.Context, limit int) ([]MailboxItem, error)
	MarkMailboxItemNotified(ctx context.Context, id string) error
}
