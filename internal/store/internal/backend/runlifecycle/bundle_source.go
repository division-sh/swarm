package runlifecycle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func MarkPostgresDeletedPersistedBundleSourcesTx(ctx context.Context, tx *sql.Tx, bundleHash string) (int64, error) {
	if tx == nil || strings.TrimSpace(bundleHash) == "" {
		return 0, errors.New("bundle source lifecycle transition requires transaction and bundle_hash")
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE runs
		SET bundle_source = $2
		WHERE bundle_hash = $1
		  AND bundle_source = $3
		  AND status IN ($4, $5, $6, $7)
	`,
		strings.TrimSpace(bundleHash),
		runtimerunlifecycle.BundleSourceDeleted,
		runtimerunlifecycle.BundleSourcePersisted,
		string(runtimerunlifecycle.StateCompleted),
		string(runtimerunlifecycle.StateFailed),
		string(runtimerunlifecycle.StateCancelled),
		string(runtimerunlifecycle.StateForked),
	)
	if err != nil {
		return 0, fmt.Errorf("mark deleted persisted bundle run sources: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count deleted persisted bundle run sources: %w", err)
	}
	return updated, nil
}
