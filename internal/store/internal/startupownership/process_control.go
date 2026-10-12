package startupownership

import (
	"context"
	"database/sql"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
)

// ProcessAuthorityCurrent checks the exact current stamp. Mutation callers must
// use the canonical generation fence; retained data is never lock possession.
func ProcessAuthorityCurrent(ctx context.Context, q authorityQueryer, expected processbinding.LeaseStamp, sqlite, lock bool) (bool, error) {
	if err := expected.Validate(); err != nil {
		return false, err
	}
	if lock {
		tx, ok := q.(*sql.Tx)
		if !ok {
			return false, errors.New("process mutation proof requires its transaction")
		}
		return generationauthority.ProcessGenerationCurrent(ctx, tx, expected, sqlite)
	}
	current, err := generationauthority.ReadProcessGeneration(ctx, q, sqlite)
	return err == nil && current == expected, err
}

// GenerationGrantProcessCurrent observes the exact current grant's process,
// without conflating grant state or source permission with the physical lease.
func GenerationGrantProcessCurrent(ctx context.Context, q authorityQueryer, grantID string, sqlite, lock bool) (bool, error) {
	var tx *sql.Tx
	if lock {
		var ok bool
		tx, ok = q.(*sql.Tx)
		if !ok {
			return false, errors.New("grant process mutation proof requires its transaction")
		}
		if err := generationauthority.FenceMutation(ctx, tx, sqlite); err != nil {
			return false, err
		}
	}
	var raw []byte
	if err := q.QueryRowContext(ctx, `SELECT snapshot FROM runtime_generation_grants WHERE grant_id=$1 ORDER BY state_version DESC LIMIT 1`, grantID).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	var grant runtimeownership.GrantEvidence
	if err := canonicaljson.DecodeInto(raw, &grant); err != nil {
		return false, err
	}
	if err := grant.Validate(); err != nil {
		return false, err
	}
	if grant.GrantID != grantID {
		return false, errors.New("generation grant snapshot identity differs")
	}
	if grant.State == runtimeownership.GrantRetired {
		return false, nil
	}
	if lock {
		return generationauthority.ProcessCoordinatesCurrentTx(ctx, tx, grant.ProcessCoordinates(), sqlite)
	}
	return generationauthority.ProcessCoordinatesCurrent(ctx, q, grant.ProcessCoordinates(), sqlite)
}
