package manager

import (
	"context"
	"time"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func (s *managerDeliveryTestStore) AdmitInlineClaim(ctx context.Context, claim runtimedelivery.Claim) (time.Duration, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	remaining, err := s.adapter.AdmitInlineClaim(ctx, tx, claim)
	if err != nil {
		return 0, err
	}
	return remaining, tx.Commit()
}
