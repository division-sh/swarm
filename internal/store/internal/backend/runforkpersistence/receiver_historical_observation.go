package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

// ReadConstructedEntityAtRevisionTx delegates fact selection, tombstones and
// reconstruction to the same fixed-snapshot owner as fork planning and replay.
func ReadConstructedEntityAtRevisionTx(ctx context.Context, tx *sql.Tx, owner flowidentity.RunScopedFlowInstance, entityID string, revision int64) (runfork.RunForkEntityState, error) {
	if tx == nil {
		return runfork.RunForkEntityState{}, fmt.Errorf("historical evidence requires the selected read transaction")
	}
	if err := owner.Validate(); err != nil {
		return runfork.RunForkEntityState{}, err
	}
	id, err := uuid.Parse(entityID)
	if err != nil || id == uuid.Nil || id.String() != entityID || revision <= 0 {
		return runfork.RunForkEntityState{}, fmt.Errorf("historical evidence requires an exact entity and positive revision")
	}
	var recorded int64
	if err := tx.QueryRowContext(ctx, `SELECT revision FROM run_fork_revisions WHERE run_id=$1 AND revision=$2`, owner.RunID, revision).Scan(&recorded); err != nil {
		return runfork.RunForkEntityState{}, fmt.Errorf("read exact historical cut: %w", err)
	}
	snapshot, err := loadRunForkRevisionSnapshot(ctx, tx, owner.RunID, recorded)
	if err != nil {
		return runfork.RunForkEntityState{}, err
	}
	state, err := loadRunForkConstructedEntityState(snapshot, entityID)
	if err != nil {
		return runfork.RunForkEntityState{}, err
	}
	metadata := state.MaterializationMetadata
	if metadata.FlowInstance != owner.Route.InstancePath || metadata.FlowTemplate != owner.Route.ScopeKey {
		return runfork.RunForkEntityState{}, fmt.Errorf("historical evidence contradicts exact receiver route/template")
	}
	return state, nil
}
