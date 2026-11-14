package runtimepersistence

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventpersistence"
)

type SelectedSourceOutcomeFixture = eventpersistence.SelectedSourceOutcomeFixture

// These two exact predecessor facts deliberately must not suppress fork work.
// Caller SQL and transaction authority never cross the fixture boundary.
func SeedSelectedSourceOutcomeForTest(ctx context.Context, selected any, fixture SelectedSourceOutcomeFixture) error {
	for _, identity := range []string{fixture.RunID, fixture.EventID, fixture.EntityID} {
		if err := validateSelectedForkStorageIdentity(identity); err != nil {
			return err
		}
	}
	source, ok := correlation.SourceArtifactFactFromContext(ctx)
	if !ok || source.BundleHash() == "" || fixture.CreatedAt.IsZero() {
		return fmt.Errorf("source outcome fixture requires exact source and time")
	}
	failure, err := json.Marshal(fixture.Failure)
	if err != nil {
		return err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("postgres event fixture store is required")
		}
		return store.eventPostgresOwner.SeedSelectedSourceOutcomeForTest(ctx, fixture, source.BundleHash(), string(failure))
	case *SQLiteRuntimeStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("sqlite event fixture store is required")
		}
		return store.eventSQLiteOwner.SeedSelectedSourceOutcomeForTest(ctx, fixture, source.BundleHash(), string(failure))
	default:
		return fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}
