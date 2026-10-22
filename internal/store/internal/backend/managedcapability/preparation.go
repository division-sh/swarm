package managedcapabilitystore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

// ProveSelectedPreparationReceiptsTx consumes the original surfaces in the
// transaction issuing or using selected authority. Failed/narrowed evidence is
// not repaired and a current grant cannot keep using a stale successful digest.
func ProveSelectedPreparationReceiptsTx(ctx context.Context, tx *sql.Tx, binding runfork.SelectedForkPreparation, sqlite bool) error {
	if tx == nil {
		return fmt.Errorf("selected preparation receipt proof requires a transaction")
	}
	if err := binding.Validate(); err != nil {
		return err
	}
	query := `SELECT surface,execution_kind,execution_authority_id,run_id,selected_execution_id,selected_fork_run_id FROM managed_agent_capability_surfaces WHERE surface_id=$1`
	if !sqlite {
		query += ` FOR SHARE`
	}
	for _, actor := range binding.Actors {
		if !actor.RequiresProbe() {
			continue
		}
		var raw []byte
		var stored StoredProjection
		if err := tx.QueryRowContext(ctx, query, actor.SurfaceID).Scan(&raw, &stored.ExecutionKind, &stored.ExecutionAuthorityID, &stored.RunID, &stored.SelectedExecutionID, &stored.SelectedForkRunID); err != nil {
			return fmt.Errorf("load selected preparation receipt: %w", err)
		}
		surface, err := DecodeStored(raw, stored)
		if err != nil {
			return err
		}
		if err := binding.ValidateSurface(actor, surface); err != nil {
			return err
		}
	}
	return nil
}

// Evidence-only reset inspection does not re-admit a formerly callable surface.
// The current canonical observation may have narrowed since initial admission.
func InspectSelectedPreparationHistoryTx(ctx context.Context, tx *sql.Tx, binding runfork.SelectedForkPreparation) error {
	if ctx == nil || tx == nil {
		return fmt.Errorf("selected preparation history requires its retained transaction")
	}
	if err := binding.Validate(); err != nil {
		return err
	}
	for _, actor := range binding.Actors {
		if !actor.RequiresProbe() {
			continue
		}
		var raw []byte
		var stored StoredProjection
		if err := tx.QueryRowContext(ctx, `SELECT surface,execution_kind,execution_authority_id,run_id,selected_execution_id,selected_fork_run_id
			FROM managed_agent_capability_surfaces WHERE surface_id=$1`, actor.SurfaceID).
			Scan(&raw, &stored.ExecutionKind, &stored.ExecutionAuthorityID, &stored.RunID, &stored.SelectedExecutionID, &stored.SelectedForkRunID); err != nil {
			return fmt.Errorf("read exact selected preparation history: %w", err)
		}
		surface, err := DecodeStored(raw, stored)
		if err != nil {
			return err
		}
		if err := binding.ValidateHistoricalSurface(actor, surface); err != nil {
			return err
		}
	}
	return ctx.Err()
}
