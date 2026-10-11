package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

// Removes one validated native receipt to exercise fail-closed admission; it
// never manufactures historical authority or changes the receiver header.
func RemoveReceiverConstructionReceiptForTest(ctx context.Context, attempt *mutationprotocol.Attempt, owner flowidentity.RunScopedFlowInstance, entityID string) error {
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if _, err := ReadFlowConstructionPublicationTx(ctx, tx, owner, entityID); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `DELETE FROM workflow_instance_initial_materializations WHERE run_id=$1 AND instance_path=$2 AND entity_id=$3`, owner.RunID, owner.Route.InstancePath, entityID)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("receipt fault changed %d rows, want exactly one", count)
		}
		return nil
	})
}
