package runtimepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	modernsqlite "modernc.org/sqlite"
)

type operatorSnapshotReadBarrier struct {
	entered, release chan struct{}
	once             sync.Once
}

var operatorSnapshotBarriers sync.Map
var operatorSnapshotBarrierID atomic.Int64
var operatorSnapshotBarrierRegistration sync.Once
var operatorSnapshotBarrierRegistrationError error

func registerOperatorSnapshotReadBarrier() error {
	operatorSnapshotBarrierRegistration.Do(func() {
		operatorSnapshotBarrierRegistrationError = modernsqlite.RegisterScalarFunction("agent_snapshot_barrier", 1, func(_ *modernsqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			id, ok := args[0].(int64)
			if !ok {
				return nil, fmt.Errorf("operator snapshot barrier id has type %T", args[0])
			}
			value, ok := operatorSnapshotBarriers.Load(id)
			if !ok {
				return nil, errors.New("operator snapshot barrier is not registered")
			}
			barrier := value.(*operatorSnapshotReadBarrier)
			barrier.once.Do(func() { close(barrier.entered) })
			<-barrier.release
			return int64(1), nil
		})
	})
	return operatorSnapshotBarrierRegistrationError
}

func InitializeOperatorSnapshotReadBarrierForTest() error {
	return registerOperatorSnapshotReadBarrier()
}

func PrepareOperatorSnapshotReadsForTest(ctx context.Context, selected any) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	if owner, ok := selected.(*SQLiteRuntimeStore); ok {
		return owner.backend.EnableOperatorSnapshotWALForTest(ctx)
	}
	return nil
}

// The callback executes the unchanged production reader only. Mutation,
// admission, schema changes and release/join are owned here and by the fixed
// domain operations; no transaction or executor crosses the fixture boundary.
func ReadOperatorSnapshotAcrossCutForTest(ctx context.Context, selected any, cut OperatorSnapshotCut, read func(context.Context) error) error {
	if err := requireOperatorSnapshotReadArguments(ctx, selected, cut, read); err != nil {
		return err
	}
	if _, postgres := selected.(*PostgresStore); postgres {
		return readOperatorSnapshotPostgresCut(ctx, selected, cut, read)
	}
	return readOperatorSnapshotSQLiteCut(ctx, selected, cut, read)
}

func readOperatorSnapshotSQLiteCut(ctx context.Context, selected any, cut OperatorSnapshotCut, read func(context.Context) error) (err error) {
	if err := registerOperatorSnapshotReadBarrier(); err != nil {
		return err
	}
	id := operatorSnapshotBarrierID.Add(1)
	barrier := &operatorSnapshotReadBarrier{entered: make(chan struct{}), release: make(chan struct{})}
	operatorSnapshotBarriers.Store(id, barrier)
	defer operatorSnapshotBarriers.Delete(id)
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	started, finished, prepared, released := false, false, false, false
	defer func() {
		if !released {
			close(barrier.release)
		}
		if started && !finished {
			cancel()
			err = errors.Join(err, <-done)
		}
		if prepared {
			cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			err = errors.Join(err, runWorkflowProjectionFault(cleanup, selected, cut.restore))
		}
	}()
	if err := runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		if err := cut.hold(ctx, tx, false, id); err != nil {
			return err
		}
		prepared = true
		return nil
	}); err != nil {
		return err
	}
	started = true
	go func() { done <- read(readCtx) }()
	select {
	case <-barrier.entered:
	case result := <-done:
		finished = true
		return errors.Join(errors.New("operator reader completed without entering its SQLite snapshot boundary"), result)
	case <-readCtx.Done():
		return readCtx.Err()
	}
	if err := runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error { return cut.apply(ctx, tx, false) }); err != nil {
		return err
	}
	close(barrier.release)
	released = true
	err = <-done
	finished = true
	return err
}

func readOperatorSnapshotPostgresCut(ctx context.Context, selected any, cut OperatorSnapshotCut, read func(context.Context) error) (err error) {
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	started, finished := false, false
	defer func() {
		// RunTransaction has already released the physical lock on every exit.
		if started && !finished {
			cancel()
			err = errors.Join(err, <-done)
		}
	}()
	err = runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		if err := cut.hold(ctx, tx, true, 0); err != nil {
			return err
		}
		var blocker int
		if err := tx.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&blocker); err != nil {
			return err
		}
		started = true
		go func() { done <- read(readCtx) }()
		for {
			var blocked bool
			err := readServedDeliveryObservation(readCtx, selected, func(ctx context.Context, reader *sql.Tx) error {
				return reader.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND $1=ANY(pg_blocking_pids(pid)))`, blocker).Scan(&blocked)
			})
			if err != nil {
				return err
			}
			if blocked {
				return cut.apply(ctx, tx, true)
			}
			select {
			case result := <-done:
				finished = true
				return errors.Join(errors.New("operator reader completed without waiting on the selected PostgreSQL barrier"), result)
			case <-readCtx.Done():
				return readCtx.Err()
			default:
				runtime.Gosched()
			}
		}
	})
	if err != nil {
		return err
	}
	err = <-done
	finished = true
	return err
}
