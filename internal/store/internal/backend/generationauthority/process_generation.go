package generationauthority

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"

	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
)

var ErrProcessGenerationMissing = errors.New("initialized store has no process-generation singleton")

type processGenerationReader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func ReadProcessGeneration(ctx context.Context, q processGenerationReader, sqlite bool) (processbinding.LeaseStamp, error) {
	return readProcessGeneration(ctx, q, sqlite, false)
}

func readProcessGeneration(ctx context.Context, q processGenerationReader, sqlite, lock bool) (processbinding.LeaseStamp, error) {
	if ctx == nil || q == nil {
		return processbinding.LeaseStamp{}, errors.New("process-generation context and reader are required")
	}
	if err := ctx.Err(); err != nil {
		return processbinding.LeaseStamp{}, err
	}
	query := `SELECT generation, CAST(lease_id AS TEXT), owner_id, CAST(boot_id AS TEXT),
		CAST(runtime_instance_id AS TEXT), backend
		FROM runtime_process_generation WHERE singleton_id = 1`
	if lock {
		query += " FOR UPDATE"
	}
	var generation int64
	var lease, owner, boot, runtime, backend sql.NullString
	err := q.QueryRowContext(ctx, query).Scan(&generation, &lease, &owner, &boot, &runtime, &backend)
	if errors.Is(err, sql.ErrNoRows) {
		return processbinding.LeaseStamp{}, ErrProcessGenerationMissing
	}
	if err != nil {
		return processbinding.LeaseStamp{}, fmt.Errorf("read process generation: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return processbinding.LeaseStamp{}, err
	}
	if generation == 0 && !lease.Valid && !owner.Valid && !boot.Valid && !runtime.Valid && !backend.Valid {
		return processbinding.LeaseStamp{}, nil
	}
	if generation <= 0 || !lease.Valid || !owner.Valid || !boot.Valid || !runtime.Valid || !backend.Valid {
		return processbinding.LeaseStamp{}, errors.New("process-generation singleton is malformed")
	}
	stamp := processbinding.LeaseStamp{
		LeaseIdentity: processbinding.LeaseIdentity{
			LeaseID: lease.String, OwnerID: owner.String, BootID: boot.String,
			RuntimeInstanceID: runtime.String, Backend: backend.String,
		},
		Generation: uint64(generation),
	}
	if err := stamp.Validate(); err != nil {
		return processbinding.LeaseStamp{}, fmt.Errorf("decode process generation: %w", err)
	}
	if stamp.Backend != processGenerationBackend(sqlite) {
		return processbinding.LeaseStamp{}, errors.New("process-generation backend differs from selected adapter")
	}
	return stamp, nil
}

// AdvanceProcessGeneration runs only inside the retained native lease's owning
// transaction. It does not acquire possession or acknowledge a transaction.
func AdvanceProcessGeneration(ctx context.Context, tx *sql.Tx, identity processbinding.LeaseIdentity, sqlite bool) (processbinding.LeaseStamp, error) {
	if err := identity.Validate(); err != nil {
		return processbinding.LeaseStamp{}, err
	}
	if identity.Backend != processGenerationBackend(sqlite) {
		return processbinding.LeaseStamp{}, errors.New("process lease backend differs from selected adapter")
	}
	if ctx == nil || tx == nil {
		return processbinding.LeaseStamp{}, errors.New("process-generation context and transaction are required")
	}
	if err := FenceMutation(ctx, tx, sqlite); err != nil {
		return processbinding.LeaseStamp{}, err
	}
	current, err := readProcessGeneration(ctx, tx, sqlite, !sqlite)
	if err != nil {
		return processbinding.LeaseStamp{}, err
	}
	if current.LeaseID == identity.LeaseID {
		if current.LeaseIdentity != identity {
			return processbinding.LeaseStamp{}, errors.New("process lease replay conflicts with current generation")
		}
		return current, nil
	}
	if current.Generation >= math.MaxInt64 {
		return processbinding.LeaseStamp{}, errors.New("process generation is exhausted")
	}
	next := processbinding.LeaseStamp{LeaseIdentity: identity, Generation: current.Generation + 1}
	result, err := tx.ExecContext(ctx, `UPDATE runtime_process_generation
		SET generation = $1, lease_id = $2, owner_id = $3, boot_id = $4, runtime_instance_id = $5, backend = $6
		WHERE singleton_id = 1 AND generation = $7`,
		int64(next.Generation), identity.LeaseID, identity.OwnerID, identity.BootID, identity.RuntimeInstanceID, identity.Backend, int64(current.Generation))
	if err != nil {
		return processbinding.LeaseStamp{}, fmt.Errorf("advance process generation: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return processbinding.LeaseStamp{}, err
	}
	if changed != 1 {
		return processbinding.LeaseStamp{}, errors.New("process-generation predecessor changed")
	}
	return next, nil
}

func ProcessGenerationCurrent(ctx context.Context, tx *sql.Tx, expected processbinding.LeaseStamp, sqlite bool) (bool, error) {
	if err := expected.Validate(); err != nil {
		return false, err
	}
	if expected.Backend != processGenerationBackend(sqlite) {
		return false, errors.New("process lease backend differs from selected adapter")
	}
	if ctx == nil || tx == nil {
		return false, errors.New("process-generation context and transaction are required")
	}
	if err := FenceMutation(ctx, tx, sqlite); err != nil {
		return false, err
	}
	current, err := readProcessGeneration(ctx, tx, sqlite, !sqlite)
	if err != nil {
		return false, err
	}
	return current == expected, nil
}

func processGenerationBackend(sqlite bool) string {
	if sqlite {
		return processbinding.SQLiteLeaseBackend
	}
	return processbinding.PostgresLeaseBackend
}
