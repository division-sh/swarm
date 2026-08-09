package runtimepersistence

import (
	"time"

	storemailbox "github.com/division-sh/swarm/internal/store/internal/mailboxpersistence"
)

type MailboxV1ListOptions = storemailbox.MailboxV1ListOptions
type MailboxStore = storemailbox.MailboxStore
type MailboxV1Item = storemailbox.MailboxV1Item
type MailboxV1HistoryEntry = storemailbox.MailboxV1HistoryEntry
type MailboxV1ItemDetail = storemailbox.MailboxV1ItemDetail
type MailboxV1DecisionSheet = storemailbox.MailboxV1DecisionSheet
type MailboxV1EntityContext = storemailbox.MailboxV1EntityContext
type MailboxV1DownstreamPreview = storemailbox.MailboxV1DownstreamPreview
type mailboxV1Cursor = storemailbox.Cursor

var ErrMailboxV1NotFound = storemailbox.ErrMailboxV1NotFound
var ErrMailboxV1InvalidCursor = storemailbox.ErrMailboxV1InvalidCursor

func EncodeMailboxV1Cursor(createdAt time.Time, mailboxID string) string {
	return storemailbox.EncodeMailboxV1Cursor(createdAt, mailboxID)
}

func encodeMailboxV1Cursor(createdAt time.Time, mailboxID string) string {
	return storemailbox.EncodeMailboxV1Cursor(createdAt, mailboxID)
}

func decodeMailboxV1Cursor(raw string) (mailboxV1Cursor, error) {
	return storemailbox.DecodeMailboxV1Cursor(raw)
}

func sqliteNullTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC()
}
