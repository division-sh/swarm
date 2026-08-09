// Package mailboxpersistence owns mailbox writes, lifecycle transitions, and
// bounded V1 projections for both selected-store backends.
package mailboxpersistence

import (
	"fmt"

	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

type MailboxPostgresOwner struct {
	backend     *postgresbackend.Backend
	schemaGuard func() error
}

func NewPostgres(backend *postgresbackend.Backend, schemaGuard func() error) (*MailboxPostgresOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("mailbox postgres backend is required")
	}
	if schemaGuard == nil {
		return nil, fmt.Errorf("mailbox postgres schema guard is required")
	}
	return &MailboxPostgresOwner{backend: backend, schemaGuard: schemaGuard}, nil
}

func (o *MailboxPostgresOwner) requireCurrentSchema() error {
	if o == nil || o.schemaGuard == nil {
		return fmt.Errorf("mailbox postgres schema guard is required")
	}
	return o.schemaGuard()
}

type MailboxSQLiteOwner struct {
	backend     *sqlitebackend.Backend
	schemaGuard func() error
}

func NewSQLite(backend *sqlitebackend.Backend, schemaGuard func() error) (*MailboxSQLiteOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("mailbox sqlite backend is required")
	}
	if schemaGuard == nil {
		return nil, fmt.Errorf("mailbox sqlite schema guard is required")
	}
	return &MailboxSQLiteOwner{backend: backend, schemaGuard: schemaGuard}, nil
}

func (o *MailboxSQLiteOwner) requireCurrentSchema() error {
	if o == nil || o.schemaGuard == nil {
		return fmt.Errorf("mailbox sqlite schema guard is required")
	}
	return o.schemaGuard()
}
