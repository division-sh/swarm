package runlifecycle

import (
	"context"
	"database/sql"
	"errors"
	"sync"
)

// The barrier owns one original-coordinator transaction and joins its rollback.
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

func (s *RunLifecyclePostgresOwner) HoldPostgresRunTableReadBarrierForTest(ctx context.Context) (*PostgresRunTableReadBarrier, error) {
	barrier := &PostgresRunTableReadBarrier{release: make(chan struct{}), done: make(chan struct{})}
	ready := make(chan struct{})
	rollback := errors.New("run read barrier released")
	go func() {
		err := s.backend.RunTransaction(ctx, func(sqlctx context.Context, tx *sql.Tx) error {
			if _, err := tx.ExecContext(sqlctx, `SET LOCAL lock_timeout='5s'`); err != nil {
				return err
			}
			if _, err := tx.ExecContext(sqlctx, `LOCK TABLE runs IN ACCESS EXCLUSIVE MODE`); err != nil {
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

func (s *RunLifecyclePostgresOwner) ReadPostgresRunOriginLockCountForTest(ctx context.Context) (int, error) {
	var count int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `
SELECT count(*) FROM pg_stat_activity
WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock'
  AND btrim(regexp_replace(query, '[[:space:]]+', ' ', 'g'))
      LIKE 'SELECT r.run_id::text, lower(r.status), r.bundle_hash, r.source_root_directory, r.origin_kind,%'
`).Scan(&count)
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *RunLifecyclePostgresOwner) ReadPostgresDatabaseLockCountForTest(ctx context.Context) (int, error) {
	var count int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock'`).Scan(&count)
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}
