// Package mailboxpersistence owns mailbox writes, lifecycle transitions, and
// bounded V1 projections for both selected-store backends.
package mailboxpersistence

import (
	"fmt"

	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"

	storeapiidempotency "github.com/division-sh/swarm/internal/store/internal/apiidempotency"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

type MailboxPostgresOwner struct {
	backend        *postgresbackend.Backend
	schemaGuard    func() error
	idempotency    *storeapiidempotency.PostgresOwner
	channelChanges *render.ReconcileSignal
}

func NewPostgres(backend *postgresbackend.Backend, schemaGuard func() error, idempotency *storeapiidempotency.PostgresOwner) (*MailboxPostgresOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("mailbox postgres backend is required")
	}
	if schemaGuard == nil {
		return nil, fmt.Errorf("mailbox postgres schema guard is required")
	}
	if idempotency == nil {
		return nil, fmt.Errorf("mailbox completion owner is required")
	}
	return &MailboxPostgresOwner{backend: backend, schemaGuard: schemaGuard, idempotency: idempotency}, nil
}

func (o *MailboxPostgresOwner) requireCurrentSchema() error {
	if o == nil || o.schemaGuard == nil {
		return fmt.Errorf("mailbox postgres schema guard is required")
	}
	return o.schemaGuard()
}

type MailboxSQLiteOwner struct {
	backend        *sqlitebackend.Backend
	schemaGuard    func() error
	idempotency    *storeapiidempotency.SQLiteOwner
	channelChanges *render.ReconcileSignal
}

func (s *MailboxPostgresOwner) BindChannelReconciliation(signal *render.ReconcileSignal) error {
	if s == nil || signal == nil || s.channelChanges != nil {
		return fmt.Errorf("postgres mailbox reconciliation requires one composition binding")
	}
	s.channelChanges = signal
	return nil
}

func (s *MailboxSQLiteOwner) BindChannelReconciliation(signal *render.ReconcileSignal) error {
	if s == nil || signal == nil || s.channelChanges != nil {
		return fmt.Errorf("sqlite mailbox reconciliation requires one composition binding")
	}
	s.channelChanges = signal
	return nil
}

func NewSQLite(backend *sqlitebackend.Backend, schemaGuard func() error, idempotency *storeapiidempotency.SQLiteOwner) (*MailboxSQLiteOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("mailbox sqlite backend is required")
	}
	if schemaGuard == nil {
		return nil, fmt.Errorf("mailbox sqlite schema guard is required")
	}
	if idempotency == nil {
		return nil, fmt.Errorf("mailbox completion owner is required")
	}
	return &MailboxSQLiteOwner{backend: backend, schemaGuard: schemaGuard, idempotency: idempotency}, nil
}

func (o *MailboxSQLiteOwner) requireCurrentSchema() error {
	if o == nil || o.schemaGuard == nil {
		return fmt.Errorf("mailbox sqlite schema guard is required")
	}
	return o.schemaGuard()
}
