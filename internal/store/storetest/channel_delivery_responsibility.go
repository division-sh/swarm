package storetest

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
)

// CountUnsettledChannelDeliveryResponsibilities is a test-only read projection
// of the production owner. Accepted work remains visible after source loss.
func CountUnsettledChannelDeliveryResponsibilities(ctx context.Context, db *sql.DB, postgres bool) (int, error) {
	query := `SELECT COUNT(*) FROM channel_delivery_plans p WHERE
		(p.state IN ('planned','rendered') AND ` + channeldelivery.SendEligibilityPredicate + `)
		OR ` + channeldelivery.AcceptedEffectPredicate(postgres)
	var count int
	err := db.QueryRowContext(ctx, query).Scan(&count)
	return count, err
}
