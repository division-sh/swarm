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
	query := `SELECT surface FROM managed_agent_capability_surfaces WHERE surface_id=$1`
	if !sqlite {
		query += ` FOR SHARE`
	}
	for _, actor := range binding.Actors {
		if !actor.RequiresProbe() {
			continue
		}
		var raw []byte
		if err := tx.QueryRowContext(ctx, query, actor.SurfaceID).Scan(&raw); err != nil {
			return fmt.Errorf("load selected preparation receipt: %w", err)
		}
		surface, err := Decode(raw)
		if err != nil {
			return err
		}
		if err := binding.ValidateSurface(actor, surface); err != nil {
			return err
		}
	}
	return nil
}
