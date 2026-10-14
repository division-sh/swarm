package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

func CorruptRootReadyImportShapeFaultForTest(ctx context.Context, selected any, bundleHash string) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	if err := sourceartifact.ValidateHash(bundleHash); err != nil {
		return err
	}
	return runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE resource_bundle_import_shapes SET shape_json=$1 WHERE bundle_hash=$2 AND flow_path='.' AND event_name='root.ready'`, []byte(`{"bad":true}`), bundleHash)
		return err
	})
}
