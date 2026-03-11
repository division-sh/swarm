package store

import runtimetools "empireai/internal/runtime/tools"

// MailboxStore preserves the mailbox persistence surface after the Empire
// implementation moved under internal/empire/store.
type MailboxStore interface {
	runtimetools.MailboxPersistence
}
