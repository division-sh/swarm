package adminpersistence

import (
	"fmt"
	"strings"

	runtimechanneldelivery "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	deliveryadapter "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/google/uuid"
)

type DestructiveResetPostgresOwner struct {
	backend        *postgresbackend.Backend
	schemaGuard    func() error
	deliveries     *deliveryadapter.Adapter
	channelChanges *runtimechanneldelivery.ReconcileSignal
}

func (s *DestructiveResetPostgresOwner) BindChannelReconciliation(signal *runtimechanneldelivery.ReconcileSignal) error {
	if s == nil || signal == nil || s.channelChanges != nil {
		return fmt.Errorf("destructive reset PostgreSQL channel reconciliation must be bound exactly once")
	}
	s.channelChanges = signal
	return nil
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
