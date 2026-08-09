package adminpersistence

import (
	"fmt"
	"strings"

	deliveryadapter "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	storeerrors "github.com/division-sh/swarm/internal/store/internal/storeerrors"
	"github.com/google/uuid"
)

var ErrBundleNotFound = storeerrors.ErrBundleNotFound

type AdminPostgresOwner struct {
	backend     *postgresbackend.Backend
	schemaGuard func() error
	deliveries  *deliveryadapter.Adapter
}

func NewPostgres(backend *postgresbackend.Backend, schemaGuard func() error) (*AdminPostgresOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("admin postgres backend is required")
	}
	deliveries, err := deliveryadapter.NewAdapter(deliveryadapter.DialectPostgres)
	if err != nil {
		return nil, err
	}
	return &AdminPostgresOwner{backend: backend, schemaGuard: schemaGuard, deliveries: deliveries}, nil
}

func (s *AdminPostgresOwner) requireCurrentSchema() error {
	if s == nil || s.schemaGuard == nil {
		return fmt.Errorf("admin postgres schema guard is required")
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
