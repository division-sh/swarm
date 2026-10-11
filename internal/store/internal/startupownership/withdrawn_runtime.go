package startupownership

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	"github.com/google/uuid"
)

// RuntimeAuthorityWithdrawnTx proves irreversible loss of the original process
// authority, not cache absence, a missed heartbeat, or an expired caller wait.
func RuntimeAuthorityWithdrawnTx(ctx context.Context, tx *sql.Tx, runtimeID string, sqlite bool) (bool, error) {
	if uuid.Validate(runtimeID) != nil {
		return false, fmt.Errorf("runtime withdrawal requires an exact runtime identity")
	}
	if err := generationauthority.FenceMutation(ctx, tx, sqlite); err != nil {
		return false, err
	}
	head, found, err := loadAuthorityHeadRecord(ctx, tx, sqlite, true)
	if err != nil || !found {
		return false, err
	}
	backend := "postgres_retained_session"
	query := `SELECT CAST(authority_id AS TEXT) FROM runtime_startup_authority_facts WHERE runtime_instance_id=$1::uuid ORDER BY authority_generation DESC,transition_ordinal DESC LIMIT 1`
	if sqlite {
		backend = "sqlite_retained_owner"
		query = `SELECT authority_id FROM runtime_startup_authority_facts WHERE runtime_instance_id=? ORDER BY authority_generation DESC,transition_ordinal DESC LIMIT 1`
	}
	if _, err := validateAuthorityLineage(ctx, tx, head, backend, sqlite, make(map[string]struct{})); err != nil {
		return false, err
	}
	var authorityID string
	if err := tx.QueryRowContext(ctx, query, runtimeID).Scan(&authorityID); errors.Is(err, sql.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	record, found, err := loadAuthorityRecord(ctx, tx, authorityID, nil, sqlite)
	if err != nil || !found {
		return false, err
	}
	original, err := validateAuthorityLineage(ctx, tx, record, backend, sqlite, make(map[string]struct{}))
	if err != nil {
		return false, err
	}
	return original.RuntimeInstanceID == runtimeID &&
		(original.State == runtimeownership.StateReleased || original.State == runtimeownership.StateSuperseded), ctx.Err()
}
