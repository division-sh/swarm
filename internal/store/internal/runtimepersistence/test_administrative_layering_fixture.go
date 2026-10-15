package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
)

func administrativePostgresFixtureOwner(selected any) (*PostgresStore, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	owner, ok := selected.(*PostgresStore)
	if !ok {
		return nil, fmt.Errorf("administrative capacity fixture requires the original postgres owner")
	}
	return owner, nil
}

func LimitPostgresAdministrativeFixturePoolForTest(ctx context.Context, selected any) error {
	owner, err := administrativePostgresFixtureOwner(selected)
	if err != nil {
		return err
	}
	return owner.backend.LimitAdministrativeFixturePool(ctx)
}

func ReadPostgresAdministrativeUnrelatedProgressForTest(ctx context.Context, selected any) (int, error) {
	owner, err := administrativePostgresFixtureOwner(selected)
	if err != nil {
		return 0, err
	}
	value, err := owner.backend.ReadAdministrativeUnrelatedProgress(ctx)
	if err != nil {
		return 0, err
	}
	return value, nil
}

// One original-coordinator transaction stays open across the real lease
// acquisition/release. Its only query and joined rollback are fixed here.
type PostgresAdministrativeSurroundingRead struct {
	ctx                            context.Context
	verify, checked, release, done chan struct{}
	verifyOnce, closeOnce          sync.Once
	closed                         atomic.Bool
	value                          int
	readErr, closeErr              error
}

func (r *PostgresAdministrativeSurroundingRead) ReadAfterLeaseRelease(ctx context.Context) (int, error) {
	if r == nil || r.ctx == nil || r.verify == nil || ctx == nil {
		return 0, fmt.Errorf("administrative surrounding read is required")
	}
	if r.closed.Load() {
		return 0, fmt.Errorf("administrative surrounding read is closed")
	}
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	r.verifyOnce.Do(func() { close(r.verify) })
	select {
	case <-r.checked:
		if r.closed.Load() {
			return 0, fmt.Errorf("administrative surrounding read is closed")
		}
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
		if r.readErr != nil {
			return 0, r.readErr
		}
		return r.value, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (r *PostgresAdministrativeSurroundingRead) Close() error {
	if r == nil {
		return nil
	}
	if r.release == nil || r.done == nil {
		return fmt.Errorf("administrative surrounding read is uninitialized")
	}
	r.closeOnce.Do(func() {
		r.closed.Store(true)
		close(r.release)
	})
	<-r.done
	return r.closeErr
}

func HoldPostgresAdministrativeSurroundingReadForTest(ctx context.Context, selected any) (*PostgresAdministrativeSurroundingRead, error) {
	if ctx == nil {
		return nil, fmt.Errorf("administrative surrounding read requires a context")
	}
	owner, err := administrativePostgresFixtureOwner(selected)
	if err != nil {
		return nil, err
	}
	read := &PostgresAdministrativeSurroundingRead{
		ctx:    ctx,
		verify: make(chan struct{}), checked: make(chan struct{}), release: make(chan struct{}), done: make(chan struct{}),
	}
	ready := make(chan struct{})
	rollback := errors.New("administrative surrounding read released")
	go func() {
		err := owner.backend.RunTransaction(ctx, func(_ context.Context, tx *sql.Tx) error {
			close(ready)
			select {
			case <-read.verify:
				read.readErr = ctx.Err()
				if read.readErr == nil {
					read.value, read.readErr = postgresbackend.ReadAdministrativeSurroundingTransactionTx(ctx, tx)
				}
			case <-read.release:
				read.readErr = fmt.Errorf("administrative surrounding read closed before verification")
			case <-ctx.Done():
				read.readErr = ctx.Err()
			}
			close(read.checked)
			select {
			case <-read.release:
			case <-ctx.Done():
			}
			return rollback
		})
		read.closeErr = administrativeSurroundingCloseError(err, rollback, ctx.Err())
		// Begin can fail before the callback owns the verification channel.
		select {
		case <-read.checked:
		default:
			read.readErr = err
			close(read.checked)
		}
		close(read.done)
	}()
	select {
	case <-ready:
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(err, read.Close())
		}
		return read, nil
	case <-read.done:
		return nil, read.closeErr
	case <-ctx.Done():
		return nil, errors.Join(ctx.Err(), read.Close())
	}
}

// Remove only our own rollback marker, including the native owner's exact
// cancellation pair. Any additional cleanup/connection failure stays visible.
func administrativeSurroundingCloseError(err, rollback, cancelled error) error {
	if err == rollback {
		return nil
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok && cancelled != nil {
		causes := joined.Unwrap()
		if len(causes) == 2 && causes[0] == cancelled && causes[1] == rollback {
			return cancelled
		}
	}
	return err
}
