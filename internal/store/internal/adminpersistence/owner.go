package adminpersistence

import (
	"fmt"
	"strings"

	deliveryadapter "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/google/uuid"
)

type BundleDeletePostgresOwner struct {
	backend     *postgresbackend.Backend
	schemaGuard func() error
	deliveries  *deliveryadapter.Adapter
}

type DestructiveResetPostgresOwner struct {
	backend     *postgresbackend.Backend
	schemaGuard func() error
	deliveries  *deliveryadapter.Adapter
}

func NewBundleDeletePostgres(backend *postgresbackend.Backend, schemaGuard func() error) (*BundleDeletePostgresOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("bundle delete postgres backend is required")
	}
	deliveries, err := deliveryadapter.NewAdapter(deliveryadapter.DialectPostgres)
	if err != nil {
		return nil, err
	}
	return &BundleDeletePostgresOwner{backend: backend, schemaGuard: schemaGuard, deliveries: deliveries}, nil
}

func NewDestructiveResetPostgres(backend *postgresbackend.Backend, schemaGuard func() error) (*DestructiveResetPostgresOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("destructive reset postgres backend is required")
	}
	deliveries, err := deliveryadapter.NewAdapter(deliveryadapter.DialectPostgres)
	if err != nil {
		return nil, err
	}
	return &DestructiveResetPostgresOwner{backend: backend, schemaGuard: schemaGuard, deliveries: deliveries}, nil
}

func (s *BundleDeletePostgresOwner) requireCurrentSchema() error {
	if s == nil || s.schemaGuard == nil {
		return fmt.Errorf("bundle delete postgres schema guard is required")
	}
	return s.schemaGuard()
}

func quoteIdent(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func nullUUIDString(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if _, err := uuid.Parse(raw); err != nil {
		return ""
	}
	return raw
}
