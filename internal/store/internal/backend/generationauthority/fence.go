// Package generationauthority owns the transaction fence shared by generation
// grant transitions and the mutations they authorize.
package generationauthority

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

type lockMode uint8

const (
	unlocked lockMode = iota
	shared
	exclusive
)

type transactionKey struct{}

type originalProcessKey struct{}

type originalProcess struct {
	stamp processbinding.LeaseStamp
	mode  lockMode
}

// AdmissionFailure identifies the pre-domain boundary for diagnostics. It is
// not rollback or retry evidence; only native settlement can prove those.
type AdmissionFailure struct{ Cause error }

func (e *AdmissionFailure) Error() string { return e.Cause.Error() }
func (e *AdmissionFailure) Unwrap() error { return e.Cause }

// MutationContext carries immutable attribution from the original retained
// owner. It grants nothing until AdmitTransaction proves it under native locks.
func MutationContext(ctx context.Context, stamp processbinding.LeaseStamp, changingGeneration bool) (context.Context, error) {
	if ctx == nil {
		return nil, errors.New("original process mutation context is required")
	}
	if err := stamp.Validate(); err != nil {
		return nil, err
	}
	previous, present := ctx.Value(originalProcessKey{}).(originalProcess)
	if present && previous.stamp != stamp {
		return nil, errors.New("mutation context cannot replace its original process")
	}
	mode := shared
	if changingGeneration {
		mode = exclusive
	}
	if present && previous.mode > mode {
		mode = previous.mode
	}
	return context.WithValue(ctx, originalProcessKey{}, originalProcess{stamp: stamp, mode: mode}), nil
}

// AdmitTransaction is the single pre-domain process check. Missing attribution
// is a refusal, never a request to adopt the store's latest process generation.
func AdmitTransaction(ctx context.Context, tx *sql.Tx, sqlite bool) (err error) {
	defer func() {
		if err != nil {
			err = &AdmissionFailure{Cause: err}
		}
	}()
	if ctx == nil {
		return errors.New("original process mutation context is required")
	}
	original, present := ctx.Value(originalProcessKey{}).(originalProcess)
	if process, found := worklifetime.ProcessFromContext(ctx); found {
		if stamp, bound := process.OriginalProcess(); bound {
			if present && original.stamp != stamp {
				return errors.New("mutation context and work owner have different original processes")
			}
			if !present {
				original, present = originalProcess{stamp: stamp, mode: shared}, true
			}
		}
	}
	if !present {
		return errors.New("mutation requires its original process attribution")
	}
	if err := original.stamp.Validate(); err != nil {
		return err
	}
	current, err := readCurrentTx(ctx, tx, sqlite, original.mode)
	if err != nil {
		return err
	}
	if current != original.stamp {
		return errors.New("mutation original process generation is no longer current")
	}
	return nil
}

type transaction struct {
	sync.Mutex
	tx       *sql.Tx
	caller   context.Context
	sqlite   bool
	readOnly bool
	active   bool
	mode     lockMode
	stamp    processbinding.LeaseStamp
	loaded   bool
}

// BindTransaction is called only by native settlement owners. The record is
// possession of one SQL transaction, never proof of current process authority.
func BindTransaction(caller, ctx context.Context, tx *sql.Tx, sqlite, readOnly bool) (context.Context, func()) {
	if caller == nil {
		caller = context.Background()
	}
	scope := &transaction{tx: tx, caller: caller, sqlite: sqlite, readOnly: readOnly, active: true}
	return context.WithValue(ctx, transactionKey{}, scope), func() {
		scope.Lock()
		defer scope.Unlock()
		scope.active = false
		scope.tx = nil
	}
}

func readCurrentTx(ctx context.Context, tx *sql.Tx, sqlite bool, mode lockMode) (processbinding.LeaseStamp, error) {
	if err := fence(ctx, tx, sqlite, mode); err != nil {
		return processbinding.LeaseStamp{}, err
	}
	scope := ctx.Value(transactionKey{}).(*transaction)
	scope.Lock()
	defer scope.Unlock()
	if !scope.active {
		return processbinding.LeaseStamp{}, sql.ErrTxDone
	}
	if !scope.loaded {
		stamp, err := readProcessGeneration(scope.caller, tx, sqlite, mode)
		if err != nil {
			return processbinding.LeaseStamp{}, err
		}
		scope.stamp, scope.loaded = stamp, true
	}
	return scope.stamp, nil
}

func recordAdvance(ctx context.Context, next processbinding.LeaseStamp) {
	scope := ctx.Value(transactionKey{}).(*transaction)
	scope.Lock()
	defer scope.Unlock()
	scope.stamp, scope.loaded = next, true
}

// FenceMutation holds ordinary admission through native settlement. Callers
// still prove their ORIGINAL process/grant against facts read after this fence.
func FenceMutation(ctx context.Context, tx *sql.Tx, sqlite bool) error {
	return fence(ctx, tx, sqlite, shared)
}

// FenceGenerationChange must precede all reads in a generation-changing
// operation. A shared mutation cannot upgrade after taking domain locks.
func FenceGenerationChange(ctx context.Context, tx *sql.Tx, sqlite bool) error {
	return fence(ctx, tx, sqlite, exclusive)
}

func fence(ctx context.Context, tx *sql.Tx, sqlite bool, requested lockMode) error {
	if ctx == nil || tx == nil {
		return errors.New("generation fence requires its context and transaction")
	}
	scope, ok := ctx.Value(transactionKey{}).(*transaction)
	if !ok || scope == nil {
		return errors.New("generation fence requires its native transaction scope")
	}
	scope.Lock()
	defer scope.Unlock()
	if !scope.active {
		return sql.ErrTxDone
	}
	if scope.tx != tx || scope.sqlite != sqlite || scope.readOnly {
		return errors.New("generation fence differs from its native mutation transaction")
	}
	if err := errors.Join(ctx.Err(), scope.caller.Err()); err != nil {
		return err
	}
	if scope.mode >= requested {
		return nil
	}
	if scope.mode != unlocked {
		return errors.New("generation mutation cannot upgrade shared admission")
	}
	if !sqlite {
		query := `SELECT pg_advisory_xact_lock_shared(1398231629, 1)`
		if requested == exclusive {
			query = `SELECT pg_advisory_xact_lock(1398231629, 1)`
		}
		if _, err := tx.ExecContext(scope.caller, query); err != nil {
			return fmt.Errorf("acquire generation transaction fence: %w", err)
		}
		if requested == exclusive {
			// Grant/source retirement can leave the stamp unchanged. Touch its
			// native version so an older repeatable-read snapshot cannot admit
			// work from before that retirement after waiting for the fence.
			result, err := tx.ExecContext(scope.caller, `UPDATE runtime_process_generation SET generation = generation WHERE singleton_id = 1`)
			if err != nil {
				return fmt.Errorf("fence generation snapshot: %w", err)
			}
			rows, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("read generation snapshot fence: %w", err)
			}
			if rows != 1 {
				return ErrProcessGenerationMissing
			}
		}
	}
	// SQLite already holds its native mutation/writer possession.
	scope.mode = requested
	return nil
}
