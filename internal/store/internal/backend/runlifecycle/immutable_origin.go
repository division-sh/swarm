package runlifecycle

import (
	"context"
	"database/sql"
	"fmt"

	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

// ReadImmutableRunOriginTx reads only the creation-owned lineage coordinate.
// Mutable status, headers and current entity state are not historical evidence.
func ReadImmutableRunOriginTx(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.RunOrigin, error) {
	if err := ctx.Err(); err != nil {
		return runtimerunlifecycle.RunOrigin{}, err
	}
	id, err := uuid.Parse(runID)
	if tx == nil || err != nil || id == uuid.Nil || id.String() != runID {
		return runtimerunlifecycle.RunOrigin{}, fmt.Errorf("immutable origin requires its exact run and original transaction")
	}
	var kind, eventID, eventType, serviceID, sourceRunID, pointKind, sourceEventID string
	var generation, revision int64
	err = tx.QueryRowContext(ctx, `SELECT origin_kind,
		COALESCE(CAST(trigger_event_id AS TEXT), ''), COALESCE(trigger_event_type, ''),
		COALESCE(CAST(origin_service_id AS TEXT), ''), COALESCE(origin_generation, 0),
		COALESCE(CAST(forked_from_run_id AS TEXT), ''), COALESCE(forked_from_point_kind, ''),
		COALESCE(forked_from_revision, 0), COALESCE(CAST(forked_from_event_id AS TEXT), '')
		FROM runs WHERE run_id=$1`, runID).Scan(&kind, &eventID, &eventType, &serviceID, &generation, &sourceRunID, &pointKind, &revision, &sourceEventID)
	if err != nil {
		return runtimerunlifecycle.RunOrigin{}, fmt.Errorf("read immutable construction origin: %w", err)
	}
	return runtimerunlifecycle.DecodeRunOrigin(kind, eventID, eventType, serviceID, generation, sourceRunID, pointKind, revision, sourceEventID)
}
