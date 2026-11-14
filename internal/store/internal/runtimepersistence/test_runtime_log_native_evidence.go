package runtimepersistence

import (
	"context"
	"fmt"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func ReadRuntimeLogRunSnapshotForTest(ctx context.Context, selected any, runID string) (runlifecycle.Snapshot, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return runlifecycle.Snapshot{}, err
	}
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return runlifecycle.Snapshot{}, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.runLifecyclePostgresOwner.ReadRuntimeLogRunSnapshotForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return owner.runLifecycleSQLiteOwner.ReadRuntimeLogRunSnapshotForTest(ctx, runID)
	default:
		return runlifecycle.Snapshot{}, fmt.Errorf("runtime log snapshot requires an original native read owner, got %T", selected)
	}
}

func RemoveRuntimeLogFixtureSourceArtifactForTest(ctx context.Context, selected any, hash string) (int64, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	if err := runtimeidentity.ValidateCanonicalBundleHash(hash); err != nil {
		return 0, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.sourceArtifactOwner.RemoveRuntimeLogFixtureSourceArtifactForTest(ctx, hash)
	case *SQLiteRuntimeStore:
		return owner.sourceArtifactOwner.RemoveRuntimeLogFixtureSourceArtifactForTest(ctx, hash)
	default:
		return 0, fmt.Errorf("runtime log source-artifact fixture requires an original native owner, got %T", selected)
	}
}

// The fixed AFTER INSERT cut aborts the original diagnostic transaction after
// its event append. It never replaces the selected writer or grants SQL access.
func SetRuntimeLogFixtureAppendFaultForTest(ctx context.Context, selected any, runID string, enabled bool) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.eventPostgresOwner.SetRuntimeLogFixtureAppendFaultForTest(ctx, runID, enabled)
	case *SQLiteRuntimeStore:
		return owner.eventSQLiteOwner.SetRuntimeLogFixtureAppendFaultForTest(ctx, runID, enabled)
	default:
		return fmt.Errorf("runtime log append fault requires an original native owner, got %T", selected)
	}
}
