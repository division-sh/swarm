package delivery

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func (s *DeliveryPostgresOwner) ExpireExactDeliveryClaimFaultForTest(ctx context.Context, claim deliverylifecycle.Claim) error {
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return ExpireExactDeliveryClaimFaultTx(ctx, tx, claim)
	})
}

func (s *DeliverySQLiteOwner) ExpireExactDeliveryClaimFaultForTest(ctx context.Context, claim deliverylifecycle.Claim) error {
	return s.backend.RunTransaction(ctx, "unrevisioned event fixture", func(ctx context.Context, tx *sql.Tx) error {
		return ExpireExactDeliveryClaimFaultTx(ctx, tx, claim)
	})
}

func (s *DeliveryPostgresOwner) TieExactPendingPublicationCreatedAtFaultForTest(ctx context.Context, eventID, firstID, secondID string) error {
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return TieExactPendingPublicationCreatedAtFaultTx(ctx, tx, DialectPostgres, eventID, firstID, secondID)
	})
}

func (s *DeliverySQLiteOwner) TieExactPendingPublicationCreatedAtFaultForTest(ctx context.Context, eventID, firstID, secondID string) error {
	return s.backend.RunTransaction(ctx, "unrevisioned event fixture", func(ctx context.Context, tx *sql.Tx) error {
		return TieExactPendingPublicationCreatedAtFaultTx(ctx, tx, DialectSQLite, eventID, firstID, secondID)
	})
}
