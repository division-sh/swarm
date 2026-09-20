package runtimepersistence

import (
	"context"
	"time"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

// Diagnostic-only forwarders; production adoption requires exact design review.
func (s *PostgresStore) AdmitInlineClaim(ctx context.Context, claim deliverylifecycle.Claim) (time.Duration, error) {
	return s.deliveryPostgresOwner.AdmitInlineClaim(ctx, claim)
}

func (s *SQLiteRuntimeStore) AdmitInlineClaim(ctx context.Context, claim deliverylifecycle.Claim) (time.Duration, error) {
	return s.deliverySQLiteOwner.AdmitInlineClaim(ctx, claim)
}
