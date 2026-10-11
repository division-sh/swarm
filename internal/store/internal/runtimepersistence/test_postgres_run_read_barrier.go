package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
)

// The opaque barrier owns one original-coordinator transaction and joins its
// rollback before Close returns. No transaction or SQL callback escapes.
type PostgresRunTableReadBarrier struct {
	release, done chan struct{}
	once          sync.Once
	err           error
}

func (b *PostgresRunTableReadBarrier) Close() error {
	if b == nil {
		return nil
	}
	b.once.Do(func() { close(b.release) })
	<-b.done
	return b.err
}

func HoldPostgresRunTableReadBarrierForTest(ctx context.Context, selected any) (*PostgresRunTableReadBarrier, error) {
	if ctx == nil {
		return nil, errors.New("run read barrier requires a context")
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	owner, ok := selected.(*PostgresStore)
	if !ok {
		return nil, fmt.Errorf("run read lock barrier is PostgreSQL-scoped")
	}
	barrier := &PostgresRunTableReadBarrier{release: make(chan struct{}), done: make(chan struct{})}
	ready := make(chan struct{})
	rollback := errors.New("run read barrier released")
	go func() {
		err := owner.backend.RunTransaction(ctx, func(sqlctx context.Context, tx *sql.Tx) error {
			if err := runlifecycle.HoldPostgresRunTableReadBarrierTx(sqlctx, tx); err != nil {
				return err
			}
			close(ready)
			select {
			case <-barrier.release:
			case <-ctx.Done():
			}
			return rollback
		})
		// Do not discard a rollback/connection cleanup error joined to the marker.
		if err != rollback {
			barrier.err = err
		}
		close(barrier.done)
	}()
	select {
	case <-ready:
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(err, barrier.Close())
		}
		return barrier, nil
	case <-barrier.done:
		return nil, barrier.err
	case <-ctx.Done():
		return nil, errors.Join(ctx.Err(), barrier.Close())
	}
}

func ReadPostgresRunOriginLockCountForTest(ctx context.Context, selected any) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	if _, ok := selected.(*PostgresStore); !ok {
		return 0, fmt.Errorf("origin lock observation is PostgreSQL-scoped")
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = runlifecycle.ReadPostgresRunOriginLockCountTx(ctx, tx)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func ReadPostgresDatabaseLockCountForTest(ctx context.Context, selected any) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	if _, ok := selected.(*PostgresStore); !ok {
		return 0, fmt.Errorf("database lock observation is PostgreSQL-scoped")
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = runlifecycle.ReadPostgresDatabaseLockCountTx(ctx, tx)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}
