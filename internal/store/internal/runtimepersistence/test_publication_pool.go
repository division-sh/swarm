package runtimepersistence

import "context"

func LimitPostgresPublicationFixturePoolForTest(ctx context.Context, selected *PostgresStore) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	return selected.backend.LimitPublicationFixturePool(ctx)
}
