package mutationprotocol

import (
	"context"
	"database/sql"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/effects"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	privateactivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/runhandoff"
)

type externalEffectResultKey struct{}

type externalEffectResult struct {
	tx                     *sql.Tx
	operationID, attemptID string
	authority              []byte
	accepted               bool
}

// ExternalEffectResultAccepted reports only the exact journal read made by this
// active native attempt. It cannot authorize a launch or another transaction.
func ExternalEffectResultAccepted(ctx context.Context, tx *sql.Tx, result effects.Settlement) bool {
	if ctx == nil || tx == nil {
		return false
	}
	observed, ok := ctx.Value(externalEffectResultKey{}).(externalEffectResult)
	if !ok || observed.tx != tx || !observed.accepted || observed.operationID != result.OperationID || observed.attemptID != result.AttemptID {
		return false
	}
	if active, err := runAdmissionAttempt(ctx, tx); err != nil || active == nil {
		return false
	}
	want, err := canonicaljson.Bytes(result.Authority.Evidence())
	return err == nil && string(observed.authority) == string(want)
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
	authority, err := canonicaljson.Bytes(result.Authority.Evidence())
	if err != nil {
		return failed[T](err)
	}
	original, originalErr := runtimeownership.CaptureMutationAuthority(ctx)
	return run(resultCtx, dialect, evidence, Ordinary, nil, candidates, func(nativeCtx context.Context, fn func(context.Context, *sql.Tx) error) (bool, error) {
		return native(nativeCtx, func(txctx context.Context, tx *sql.Tx) error {
			sqlite := dialect == privateactivity.DialectSQLite
			if err := generationauthority.FenceMutation(txctx, tx, sqlite); err != nil {
				return err
			}
			accepted, err := readExternalEffectResultTx(txctx, tx, result, authority, sqlite)
			if err != nil {
				return err
			}
			if !accepted {
				if originalErr != nil {
					return originalErr
				}
				if err := generationauthority.RequireCurrentRuntimeMutationTx(txctx, tx, original, sqlite); err != nil {
					return err
				}
			}
			observed := externalEffectResult{tx: tx, operationID: result.OperationID, attemptID: result.AttemptID, authority: authority, accepted: accepted}
			return fn(context.WithValue(txctx, externalEffectResultKey{}, observed), tx)
		})
	}, write)
}

func readExternalEffectResultTx(ctx context.Context, tx *sql.Tx, result effects.Settlement, authority []byte, sqlite bool) (bool, error) {
	query := `SELECT a.authority_evidence, a.launched_at IS NOT NULL, a.state,
		(o.authority_kind=$3 AND o.authority_id=$4 AND o.execution_mode=$5
		AND a.execution_mode=$5 AND a.execution_owner=$6 AND a.fence_generation=$7)
		FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
		WHERE a.operation_id=$1::uuid AND a.attempt_id=$2::uuid`
	args := []any{result.OperationID, result.AttemptID, string(result.Authority.Kind), result.Authority.ID,
		string(result.Authority.ExecutionMode), result.Authority.ExecutionOwner, result.Authority.FenceGeneration}
	if sqlite {
		query = `SELECT a.authority_evidence, a.launched_at IS NOT NULL, a.state,
			(o.authority_kind=?3 AND o.authority_id=?4 AND o.execution_mode=?5
			AND a.execution_mode=?5 AND a.execution_owner=?6 AND a.fence_generation=?7)
			FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
			WHERE a.operation_id=?1 AND a.attempt_id=?2`
	}
	var raw []byte
	var accepted bool
	var matches bool
	var state effects.State
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&raw, &accepted, &state, &matches); errors.Is(err, sql.ErrNoRows) {
		// No journal proof means ordinary original custody and all existing
		// domain checks remain mandatory; this is not accepted-result admission.
		return false, nil
	} else if err != nil {
		return false, err
	}
	stored, err := canonicaljson.Decode(raw)
	if err != nil {
		return false, err
	}
	want, err := canonicaljson.Decode(authority)
	if err != nil {
		return false, err
	}
	if !matches || !stored.Equal(want) {
		return false, errors.New("external effect result differs from its original attempt authority")
	}
	if accepted && state != effects.StateLaunched && state != effects.StateResponseObserved && state != effects.StateSettled && state != effects.StateTerminalFailure && state != effects.StateOutcomeUncertain {
		return false, errors.New("external effect result has contradictory launch evidence")
	}
	if !accepted && (state == effects.StateLaunched || state == effects.StateResponseObserved) {
		return false, errors.New("external effect result is missing its launch evidence")
	}
	return accepted, nil
}
