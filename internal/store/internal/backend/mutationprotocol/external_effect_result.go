package mutationprotocol

import (
	"context"
	"database/sql"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/effects"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	privateactivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/runhandoff"
)

// ExternalEffectResultAccepted reports only the exact journal read made by this
// active native attempt. It cannot authorize a launch or another transaction.
func ExternalEffectResultAccepted(ctx context.Context, tx *sql.Tx, result effects.Settlement) bool {
	if ctx == nil || tx == nil {
		return false
	}
	if active, err := runAdmissionAttempt(ctx, tx); err != nil || active == nil {
		return false
	}
	return generationauthority.ExternalEffectResultAccepted(ctx, tx, result)
}

func RunPostgresExternalEffectResult[T any](ctx context.Context, db *postgresbackend.Backend, result effects.Settlement, evidence Evidence, candidates *runhandoff.CandidateCoordinator, write func(context.Context, *Attempt) (T, error)) Result[T] {
	if db == nil {
		return failed[T](errors.New("PostgreSQL mutation backend is required"))
	}
	return runExternalEffectResult(ctx, privateactivity.DialectPostgres, result, evidence, candidates, db.RunTransactionOutcome, write)
}

func RunSQLiteExternalEffectResult[T any](ctx context.Context, db *sqlitebackend.Backend, label string, result effects.Settlement, evidence Evidence, candidates *runhandoff.CandidateCoordinator, write func(context.Context, *Attempt) (T, error)) Result[T] {
	if db == nil {
		return failed[T](errors.New("SQLite mutation backend is required"))
	}
	return runExternalEffectResult(ctx, privateactivity.DialectSQLite, result, evidence, candidates, func(ctx context.Context, fn func(context.Context, *sql.Tx) error) (bool, error) {
		return db.RunTransactionOutcome(ctx, label, fn)
	}, write)
}

func runExternalEffectResult[T any](ctx context.Context, dialect privateactivity.Dialect, result effects.Settlement, evidence Evidence, candidates *runhandoff.CandidateCoordinator, native nativeRunner, write func(context.Context, *Attempt) (T, error)) Result[T] {
	if !result.Authority.Valid() {
		return runRuntime(ctx, dialect, evidence, Ordinary, nil, candidates, native, write)
	}
	resultCtx, err := runtimeownership.ExternalEffectResultContext(ctx, result)
	if err != nil {
		return failed[T](err)
	}
	original, originalErr := runtimeownership.CaptureMutationAuthority(ctx)
	resultCtx, err = generationauthority.PrepareExternalEffectResult(resultCtx, result, original, originalErr)
	if err != nil {
		return failed[T](err)
	}
	candidateCtx := resultCtx
	if originalErr == nil {
		candidateCtx = runtimeownership.CarryMutationAuthority(resultCtx, ctx)
	}
	return run(candidateCtx, dialect, evidence, Ordinary, nil, candidates, func(nativeCtx context.Context, fn func(context.Context, *sql.Tx) error) (bool, error) {
		nativeCtx, err := runtimeownership.ExternalEffectResultContext(nativeCtx, result)
		if err != nil {
			return false, err
		}
		return native(nativeCtx, fn)
	}, write)
}
