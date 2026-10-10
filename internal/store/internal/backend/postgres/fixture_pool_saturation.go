package postgres

import (
	"context"
	"fmt"
)

// LimitPublicationFixturePool fixes the original saturation cut before any
// dedicated claim session is retained. It never resizes an active reservation.
func (b *Backend) LimitPublicationFixturePool(ctx context.Context) error {
	if err := b.Ping(ctx); err != nil {
		return err
	}
	b.capacityMu.Lock()
	defer b.capacityMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.capacityReservations != 0 {
		return fmt.Errorf("publication pool fixture requires no retained claim sessions")
	}
	b.db.SetMaxOpenConns(4)
	b.db.SetMaxIdleConns(4)
	b.baseOpenConnections = 4
	return nil
}
