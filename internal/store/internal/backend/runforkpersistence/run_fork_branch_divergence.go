package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

func normalizeSelectedForkBranchDivergence(value runfork.RunForkSelectedContractBranchDivergence) (runfork.RunForkSelectedContractBranchDivergence, error) {
	if uuid.Validate(value.ForkRunID) != nil || uuid.Validate(value.SourceRunID) != nil || value.ForkRunID == value.SourceRunID {
		return value, fmt.Errorf("selected branch divergence requires distinct exact source and child run identities")
	}
	if err := value.ForkPoint.Validate(); err != nil {
		return value, fmt.Errorf("selected branch divergence point: %w", err)
	}
	if value.ForkEventID != value.ForkPoint.EventID {
		return value, fmt.Errorf("selected branch divergence event projection contradicts its typed point")
	}
	if value.Owner != runfork.RunForkSelectedContractBranchDivergenceOwner || value.Policy != runfork.RunForkSelectedContractSourceAdvancedBranchPolicy ||
		value.SourceFrozen || value.SourceRunStatusAtActivation != value.SourceRunStatusAfterActivation ||
		!runForkSelectedContractBranchSourceStatusSupported(value.SourceRunStatusAtActivation) {
		return value, fmt.Errorf("selected branch divergence requires unchanged supported source lifecycle and branch authority")
	}
	value.SourceAdvancedFacts = uniqueNonEmptyStrings(value.SourceAdvancedFacts)
	if len(value.SourceAdvancedFacts) == 0 {
		return value, fmt.Errorf("selected branch divergence requires source advancement evidence")
	}
	value.ForkPoint = runfork.RunForkPoint{Kind: value.ForkPoint.Kind, Revision: value.ForkPoint.Revision, EventID: value.ForkPoint.EventID}
	if value.CreatedAt.IsZero() {
		value.CreatedAt = time.Now().UTC()
	}
	value.CreatedAt = value.CreatedAt.UTC().Round(time.Microsecond)
	return value, nil
}

func validateSelectedForkBranchDivergenceTx(ctx context.Context, tx *sql.Tx, value runfork.RunForkSelectedContractBranchDivergence) (runfork.RunForkSelectedContractBranchDivergence, error) {
	value, err := normalizeSelectedForkBranchDivergence(value)
	if err != nil {
		return value, err
	}
	binding, err := loadRunForkSelectedContractBinding(ctx, tx, value.ForkRunID)
	if err != nil {
		return value, fmt.Errorf("load selected branch divergence binding: %w", err)
	}
	if binding.SourceRunID != value.SourceRunID || binding.ForkPoint != value.ForkPoint {
		return value, fmt.Errorf("selected branch divergence differs from selected binding")
	}
	var originKind, sourceID, pointKind, sourceStatus string
	var revision int64
	var eventID sql.NullString
	err = tx.QueryRowContext(ctx, `
		SELECT child.origin_kind, CAST(child.forked_from_run_id AS TEXT), child.forked_from_point_kind,
		       child.forked_from_revision, CAST(child.forked_from_event_id AS TEXT), source.status
		FROM runs child JOIN runs source ON source.run_id=child.forked_from_run_id
		WHERE child.run_id=$1
	`, value.ForkRunID).Scan(&originKind, &sourceID, &pointKind, &revision, &eventID, &sourceStatus)
	if err != nil {
		return value, fmt.Errorf("load selected branch divergence child origin: %w", err)
	}
	origin, err := runtimerunlifecycle.DecodeRunOrigin(originKind, "", "", "", 0, sourceID, pointKind, revision, eventID.String)
	if err != nil || origin.Kind() != runtimerunlifecycle.OriginForkMaterialization ||
		origin.SourceRunID() != value.SourceRunID || origin.ForkPointKind() != value.ForkPoint.Kind ||
		origin.ForkRevision() != value.ForkPoint.Revision || origin.SourceEventID() != value.ForkPoint.EventID ||
		sourceStatus != value.SourceRunStatusAtActivation {
		return value, fmt.Errorf("selected branch divergence differs from exact child origin or source lifecycle")
	}
	if _, err := resolveFixedRunForkRevisionPoint(ctx, tx, value.SourceRunID, value.ForkPoint, resolveRunForkRevisionPoint); err != nil {
		return value, fmt.Errorf("selected branch divergence source point: %w", err)
	}
	return value, nil
}

func requireSelectedForkBranchDivergenceRecorded(result sql.Result) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read selected branch divergence result: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("selected branch divergence conflicts with first committed evidence")
	}
	return nil
}
