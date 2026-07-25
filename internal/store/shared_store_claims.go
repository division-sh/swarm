package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

const (
	pipelineReplayClaimNamespace = "swarm:pipeline-replay:"
	scheduleClaimNamespace       = "swarm:schedule:"
)

type advisoryLockLease interface {
	Release(context.Context) error
}

type sqlAdvisoryLockLease struct {
	mu              sync.Mutex
	session         *postgresSessionAuthority
	lockKey         string
	releaseSession  func() error
	releaseCapacity func()
	unlocked        bool
	released        bool
	testUnlock      func(context.Context, *postgresSessionAuthority, string) (bool, error)
}

type postgresSessionAuthorityContextKey struct{}

type postgresSessionAuthority struct {
	mu             sync.Mutex
	conn           *sql.Conn
	activeTx       *sql.Tx
	refs           int
	closed         bool
	discardPending bool
}

func newPostgresSessionAuthority(conn *sql.Conn) *postgresSessionAuthority {
	return &postgresSessionAuthority{conn: conn, refs: 1}
}

func (a *postgresSessionAuthority) bindContext(ctx context.Context) context.Context {
	if a == nil {
		return ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	conn, err := a.connection()
	if err != nil {
		return ctx
	}
	ctx = runtimepipeline.WithPipelineSQLConnContext(ctx, conn)
	return context.WithValue(ctx, postgresSessionAuthorityContextKey{}, a)
}

func postgresSessionAuthorityFromContext(ctx context.Context) (*postgresSessionAuthority, bool, error) {
	if ctx == nil {
		return nil, false, nil
	}
	authority, ok := ctx.Value(postgresSessionAuthorityContextKey{}).(*postgresSessionAuthority)
	if !ok || authority == nil {
		if _, borrowed := runtimepipeline.PipelineSQLConnFromContext(ctx); borrowed {
			return nil, false, errors.New("borrowed PostgreSQL connection lacks private session authority")
		}
		if _, borrowed := runtimepipeline.PipelineSQLTxFromContext(ctx); borrowed {
			return nil, false, errors.New("borrowed PostgreSQL transaction lacks private session authority")
		}
		return nil, false, nil
	}
	borrowedConn, hasConn := runtimepipeline.PipelineSQLConnFromContext(ctx)
	borrowedTx, hasTx := runtimepipeline.PipelineSQLTxFromContext(ctx)
	if !hasConn && !hasTx {
		// Detached work contexts deliberately strip public SQL capabilities.
		// A private token left in the value chain cannot revive that authority.
		return nil, false, nil
	}
	if !hasConn {
		return nil, false, errors.New("borrowed PostgreSQL transaction lacks its exact private session connection")
	}
	conn, err := authority.connection()
	if err != nil {
		return nil, false, err
	}
	if borrowedConn != conn {
		return nil, false, errors.New("borrowed PostgreSQL connection does not match private session authority")
	}
	if hasTx {
		authority.mu.Lock()
		exact := authority.activeTx == borrowedTx
		authority.mu.Unlock()
		if !exact {
			return nil, false, errors.New("borrowed PostgreSQL transaction does not match private session authority")
		}
	}
	return authority, true, nil
}

func (a *postgresSessionAuthority) connection() (*sql.Conn, error) {
	if a == nil {
		return nil, errors.New("PostgreSQL session authority is missing")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.conn == nil {
		return nil, errors.New("PostgreSQL session authority is closed")
	}
	return a.conn, nil
}

func (a *postgresSessionAuthority) retain() (func() error, bool) {
	if a == nil {
		return nil, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.conn == nil {
		return nil, false
	}
	a.refs++
	var (
		mu   sync.Mutex
		done bool
	)
	return func() error {
		mu.Lock()
		defer mu.Unlock()
		if done {
			return nil
		}
		if err := a.release(); err != nil {
			return err
		}
		done = true
		return nil
	}, true
}

func (a *postgresSessionAuthority) release() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.refs <= 0 {
		return nil
	}
	if a.refs > 1 {
		a.refs--
		return nil
	}
	if a.activeTx != nil {
		return errors.New("close PostgreSQL session authority while its transaction is active")
	}
	if a.discardPending {
		return a.forceDiscardLocked()
	}
	if a.conn == nil {
		a.closed = true
		a.refs = 0
		return nil
	}
	if err := a.conn.Close(); err != nil {
		return err
	}
	a.conn = nil
	a.refs = 0
	a.closed = true
	return nil
}

func (a *postgresSessionAuthority) forceDiscard() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.discardPending = true
	if a.closed || a.conn == nil {
		return nil
	}
	if a.activeTx != nil {
		return nil
	}
	return a.forceDiscardLocked()
}

func (a *postgresSessionAuthority) forceDiscardLocked() error {
	rawErr := a.conn.Raw(func(any) error { return driver.ErrBadConn })
	if errors.Is(rawErr, driver.ErrBadConn) {
		rawErr = nil
	}
	closeErr := a.conn.Close()
	a.conn = nil
	a.refs = 0
	a.closed = true
	if rawErr != nil || (closeErr != nil && !errors.Is(closeErr, sql.ErrConnDone)) {
		return errors.Join(rawErr, closeErr)
	}
	return nil
}

func (a *postgresSessionAuthority) beginTx(ctx context.Context) (*sql.Tx, error) {
	if a == nil {
		return nil, errors.New("PostgreSQL session authority is missing")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.conn == nil {
		return nil, errors.New("PostgreSQL session authority is closed")
	}
	if a.activeTx != nil {
		return nil, errors.New("PostgreSQL session authority already has an active transaction")
	}
	tx, err := a.conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	a.activeTx = tx
	return tx, nil
}

func (a *postgresSessionAuthority) endTx(tx *sql.Tx) error {
	if a == nil || tx == nil {
		return errors.New("PostgreSQL session transaction is missing")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.activeTx != tx {
		return errors.New("PostgreSQL transaction does not match private session authority")
	}
	a.activeTx = nil
	if a.discardPending {
		return a.forceDiscardLocked()
	}
	return nil
}

func (a *postgresSessionAuthority) queryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	a.mu.Lock()
	tx := a.activeTx
	conn := a.conn
	a.mu.Unlock()
	if tx != nil {
		return tx.QueryRowContext(ctx, query, args...)
	}
	return conn.QueryRowContext(ctx, query, args...)
}

func (a *postgresSessionAuthority) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	a.mu.Lock()
	tx := a.activeTx
	conn := a.conn
	a.mu.Unlock()
	if tx != nil {
		return tx.QueryContext(ctx, query, args...)
	}
	return conn.QueryContext(ctx, query, args...)
}

func (a *postgresSessionAuthority) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return a.queryRowContext(ctx, query, args...)
}

func (l *sqlAdvisoryLockLease) BindContext(ctx context.Context) context.Context {
	if l == nil || l.session == nil {
		return ctx
	}
	return l.session.bindContext(ctx)
}

func (l *sqlAdvisoryLockLease) Release(ctx context.Context) error {
	return l.releaseWithRetirement(ctx)
}

func (l *sqlAdvisoryLockLease) releaseWithRetirement(ctx context.Context) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return nil
	}
	if l.session == nil {
		l.retireLocked()
		return errors.New("advisory lock lease has no private session authority")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = context.WithoutCancel(ctx)
	if !l.unlocked {
		var (
			unlocked bool
			err      error
		)
		if l.testUnlock != nil {
			unlocked, err = l.testUnlock(ctx, l.session, l.lockKey)
		} else {
			err = l.session.queryRowContext(ctx, `SELECT pg_advisory_unlock(hashtext($1))`, l.lockKey).Scan(&unlocked)
		}
		if err != nil {
			releaseErr := fmt.Errorf("release advisory lock: %w", err)
			discardErr := l.session.forceDiscard()
			l.retireLocked()
			return errors.Join(releaseErr, wrapAdvisoryDiscardError(discardErr))
		}
		if !unlocked {
			releaseErr := errors.New("release advisory lock: PostgreSQL session did not own the lock")
			discardErr := l.session.forceDiscard()
			l.retireLocked()
			return errors.Join(releaseErr, wrapAdvisoryDiscardError(discardErr))
		}
		l.unlocked = true
	}
	if l.releaseSession != nil {
		if err := l.releaseSession(); err != nil {
			closeErr := fmt.Errorf("close advisory lock session: %w", err)
			discardErr := l.session.forceDiscard()
			l.retireLocked()
			return errors.Join(closeErr, wrapAdvisoryDiscardError(discardErr))
		}
	}
	l.retireLocked()
	return nil
}

func wrapAdvisoryDiscardError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("discard advisory lock session: %w", err)
}

func (l *sqlAdvisoryLockLease) retireLocked() {
	if l.releaseCapacity != nil {
		l.releaseCapacity()
	}
	l.releaseCapacity = nil
	l.releaseSession = nil
	l.session = nil
	l.released = true
}

func acquireAdvisoryLockLease(ctx context.Context, db *sql.DB, lockKey string) (*sqlAdvisoryLockLease, bool, error) {
	return acquireAdvisoryLockLeaseWith(ctx, db, lockKey, nil)
}

type advisoryLockAcquire func(context.Context, *postgresSessionAuthority, string) (bool, error)

func acquireAdvisoryLockLeaseWith(
	ctx context.Context,
	db *sql.DB,
	lockKey string,
	acquire advisoryLockAcquire,
) (*sqlAdvisoryLockLease, bool, error) {
	if db == nil {
		return nil, false, nil
	}
	lockKey = strings.TrimSpace(lockKey)
	if lockKey == "" {
		return nil, false, fmt.Errorf("advisory lock key is required")
	}
	authority, borrowed, err := postgresSessionAuthorityFromContext(ctx)
	var releaseSession func() error
	if err != nil {
		return nil, false, err
	}
	if !borrowed {
		conn, err := db.Conn(ctx)
		if err != nil {
			return nil, false, fmt.Errorf("acquire advisory lock connection: %w", err)
		}
		authority = newPostgresSessionAuthority(conn)
		releaseSession = authority.release
	} else {
		var retained bool
		releaseSession, retained = authority.retain()
		if !retained {
			return nil, false, fmt.Errorf("acquire advisory lock connection: private session authority is closed")
		}
	}
	var acquired bool
	if acquire != nil {
		acquired, err = acquire(ctx, authority, lockKey)
	} else {
		err = authority.queryRowContext(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, lockKey).Scan(&acquired)
	}
	if err != nil {
		return nil, false, errors.Join(
			fmt.Errorf("acquire advisory lock: %w", err),
			wrapAdvisoryDiscardError(authority.forceDiscard()),
			releaseSession(),
		)
	}
	if !acquired {
		if err := releaseSession(); err != nil {
			return nil, false, fmt.Errorf("release unacquired advisory lock session: %w", err)
		}
		return nil, false, nil
	}
	return &sqlAdvisoryLockLease{
		session:        authority,
		lockKey:        lockKey,
		releaseSession: releaseSession,
	}, true, nil
}

func runPostgresSessionTransaction(
	ctx context.Context,
	db *sql.DB,
	fn func(context.Context, *sql.Tx) error,
) (err error) {
	if db == nil {
		return errors.New("PostgreSQL store is required")
	}
	if fn == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	session, borrowed, err := postgresSessionAuthorityFromContext(ctx)
	if err != nil {
		return err
	}
	if !borrowed {
		conn, connErr := db.Conn(ctx)
		if connErr != nil {
			return connErr
		}
		session = newPostgresSessionAuthority(conn)
		defer func() {
			err = errors.Join(err, session.release())
		}()
	}
	ctx = session.bindContext(ctx)
	tx, err := session.beginTx(ctx)
	if err != nil {
		return err
	}
	txctx := runtimepipeline.WithPipelineSQLTxContext(ctx, tx)
	if runErr := fn(txctx, tx); runErr != nil {
		return errors.Join(runErr, rollbackPostgresSessionTransaction(tx, session))
	}
	if commitErr := tx.Commit(); commitErr != nil {
		return errors.Join(commitErr, rollbackPostgresSessionTransaction(tx, session))
	}
	return session.endTx(tx)
}

func replayClaimLockKey(eventID string) string {
	return pipelineReplayClaimNamespace + strings.TrimSpace(eventID)
}

func scheduleClaimLockKey(sc runtimepipeline.Schedule) string {
	return scheduleClaimNamespace + strings.Join([]string{
		strings.TrimSpace(sc.EffectiveTimerID()),
		strings.TrimSpace(sc.EffectiveRunID()),
		strings.TrimSpace(sc.AgentID),
		strings.TrimSpace(sc.EventType),
		strings.TrimSpace(sc.EffectiveEntityID()),
		strings.TrimSpace(sc.EffectiveFlowInstance()),
		strings.TrimSpace(sc.TaskID),
	}, "|")
}
