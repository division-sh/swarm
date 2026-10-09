package sourceartifactstore

import (
	"context"
	"database/sql"
	"fmt"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

// DeleteSourceArtifactForFixtureRefusalTx injects missing-artifact evidence
// inside the caller's native fixture transaction, without clearing run admission.
func DeleteSourceArtifactForFixtureRefusalTx(ctx context.Context, tx *sql.Tx, dialect string, source runtimecorrelation.SourceArtifactFact) error {
	if tx == nil {
		return fmt.Errorf("source artifact fixture transaction is required")
	}
	if err := source.Validate(); err != nil {
		return err
	}
	var query string
	switch dialect {
	case "postgres":
		query = `DELETE FROM source_artifacts WHERE bundle_hash = $1`
	case "sqlite":
		query = `DELETE FROM source_artifacts WHERE bundle_hash = ?`
	default:
		return fmt.Errorf("source artifact fixture dialect %q is unsupported", dialect)
	}
	_, err := tx.ExecContext(ctx, query, source.BundleHash())
	return err
}
